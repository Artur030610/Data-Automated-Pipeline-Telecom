import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, leer_carpeta

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]🏠 ETL: CAMPAÑA COME BACK HOME (INCREMENTAL)[/]")
    
    # 1. Configuración y Rutas
    RUTA_RAW = PATHS["raw_comeback"]
    NOMBRE_GOLD = "ComeBackHome_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)

    # Columnas que esperamos que vengan en el Excel
    cols_esperadas = [
        "N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", 
        "Fecha Llamada", "Hora Llamada", "Tipo Llamada", "Tipo Respuesta", 
        "Detalle Respuesta", "Responsable", "Suscripción", "Grupo Afinidad", 
        "Franquicia", "Ciudad", "Observación", "Teléfono", "Detalle Suscripcion"
    ]

    # ---------------------------------------------------------
    # 2. DETECCIÓN DE FECHA DE CORTE (DEL HISTÓRICO)
    # ---------------------------------------------------------
    fecha_corte = pd.Timestamp('1900-01-01')
    df_historico = pd.DataFrame()

    if os.path.exists(RUTA_GOLD_COMPLETA):
        try:
            df_historico = pd.read_parquet(RUTA_GOLD_COMPLETA)
            # En el Gold la columna ya se llama "Fecha"
            if not df_historico.empty and "Fecha" in df_historico.columns:
                fecha_corte = pd.to_datetime(df_historico["Fecha"]).max()
                console.print(f"[green]✅ Histórico detectado. Última fecha cargada: {fecha_corte}[/]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Error leyendo histórico: {e}. Se hará carga completa.[/]")

    # ---------------------------------------------------------
    # 3. LECTURA Y FILTRADO (POR CONTENIDO)
    # ---------------------------------------------------------
    console.print("[cyan]📂 Escaneando archivos Raw...[/]")
    df_nuevo = leer_carpeta(
        RUTA_RAW, 
        filtro_exclusion="Consolidado",
        columnas_esperadas=cols_esperadas
    )
    
    if df_nuevo.empty:
        console.print("[red]❌ No hay datos para procesar.[/]")
        return

    # A. Normalización previa (Evita error si falta columna fecha)
    if "Fecha Llamada" not in df_nuevo.columns:
        console.print("[red]❌ Error Crítico: No se encuentra la columna 'Fecha Llamada' en los archivos nuevos.[/]")
        return

    # B. Filtro Incremental
    # Convertimos la fecha del Excel temporalmente para comparar
    df_nuevo["Fecha_Temp"] = pd.to_datetime(df_nuevo["Fecha Llamada"], dayfirst=True, errors='coerce')
    
    # Nos quedamos solo con lo nuevo (> fecha_corte)
    df_nuevo = df_nuevo[df_nuevo["Fecha_Temp"] > fecha_corte].copy()
    df_nuevo = df_nuevo.drop(columns=["Fecha_Temp"])

    if df_nuevo.empty:
        console.print("[bold green]✅ Campaña CBH al día. No hay registros nuevos.[/]")
        return

    # ---------------------------------------------------------
    # 4. TRANSFORMACIÓN (SOLO LOTE NUEVO)
    # ---------------------------------------------------------
    console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} registros nuevos...[/]")
    filas_raw = len(df_nuevo)

    # Renombres (Estandarización)
    df_nuevo = df_nuevo.rename(columns={
        "Fecha Llamada": "Fecha", 
        "Hora Llamada": "Hora",
        "Franquicia": "Nombre Franquicia" 
    })

    # Limpieza de Tipos
    df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce")
    
    # Limpieza de ID (Robusta)
    # Quita .0, quita espacios, convierte nulos a cadena vacía
    if "N° Abonado" in df_nuevo.columns:
        df_nuevo["N° Abonado"] = df_nuevo["N° Abonado"].astype(str).str.strip()
        df_nuevo["N° Abonado"] = df_nuevo["N° Abonado"].str.replace(r'\.0$', '', regex=True)
        df_nuevo["N° Abonado"] = df_nuevo["N° Abonado"].replace({'nan': '', 'None': '', 'NaT': ''})

    # Limpieza de Textos (Mayúsculas)
    cols_texto = ["Cliente", "Tipo Respuesta", "Detalle Respuesta", "Responsable", "Ciudad", "Nombre Franquicia", "Grupo Afinidad"]
    for col in cols_texto:
        if col in df_nuevo.columns:
            df_nuevo[col] = df_nuevo[col].astype(str).str.upper().str.strip()

    # Enriquecimiento
    df_nuevo["Tipo de afluencia"] = "COME BACK HOME"

    # Selección de Columnas Finales
    cols_finales = [
        "Source.Name", "N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha", "Hora", "Tipo Respuesta", "Detalle Respuesta", 
        "Responsable", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Tipo de afluencia"
    ]
    
    # Reindex para garantizar estructura (rellena con NaN si falta algo)
    df_nuevo = df_nuevo.reindex(columns=cols_finales)
    
    # Deduplicación del lote nuevo
    df_nuevo = df_nuevo.drop_duplicates()

    # ---------------------------------------------------------
    # 5. UNIFICACIÓN Y GUARDADO
    # ---------------------------------------------------------
    df_final = pd.DataFrame()
    
    if not df_historico.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    else:
        df_final = df_nuevo

    if not df_final.empty:
        # Ordenamos por fecha descendente
        if "Fecha" in df_final.columns:
            df_final = df_final.sort_values(by="Fecha", ascending=False)

        # Deduplicación Final (Keep First para priorizar la data más reciente cargada)
        # Usamos un subset de columnas clave para identificar duplicados reales
        subset_dedupe = ["N° Abonado", "Documento", "Fecha", "Hora", "Tipo Respuesta", "Responsable"]
        # Aseguramos que las columnas existan antes de deduplicar
        subset_dedupe = [c for c in subset_dedupe if c in df_final.columns]
        
        df_final = df_final.drop_duplicates(subset=subset_dedupe, keep='first')
        
        # Limpieza final para Power BI
        df_final = limpiar_nulos_powerbi(df_final)

        guardar_parquet(
            df_final, 
            NOMBRE_GOLD,
            filas_iniciales=len(df_nuevo) if not df_nuevo.empty else len(df_final)
        )
        console.print(f"[bold green]✅ CBH Gold actualizado. Total filas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, standard_hours, ingesta_incremental_polars

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]🏠 ETL: CAMPAÑA COME BACK HOME (POLARS BRONZE + PANDAS GOLD)[/]")
    
    # 1. Configuración y Rutas
    RUTA_RAW = PATHS["raw_comeback"]
    NOMBRE_GOLD = "ComeBackHome_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "ComeBackHome_Raw_Bronze.parquet")

    # -------------------------------------------------------------------------
    # 2. INGESTA BRONZE (POLARS - UPSERT POR FECHA)
    # ---------------------------------------------------------
    console.print("[cyan]🚀 Fase 1: Actualizando capa Bronze con Polars...[/]")
    # Delega la lectura de Excel, extracción de fechas y borrado/inserción inteligente a Polars
    ingesta_exitosa = ingesta_incremental_polars(
        ruta_raw=RUTA_RAW, 
        ruta_bronze_historico=RUTA_BRONZE, 
        columna_fecha="Fecha Llamada"
    )

    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold red]❌ No existe archivo Bronze para procesar el Gold.[/]")
        return

    # -------------------------------------------------------------------------
    # 3. LECTURA DESDE BRONZE (PANDAS - LECTURA ATÓMICA)
    # ---------------------------------------------------------
    console.print("[cyan]🚀 Fase 2: Construyendo Gold desde Bronze...[/]")
    try:
        df_total = pd.read_parquet(RUTA_BRONZE, dtype_backend="pyarrow")
    except Exception as e:
        console.print(f"[bold red]❌ Error leyendo Bronze: {e}[/]")
        return

    if df_total.empty:
        console.print("[yellow]⚠️ El Bronze está vacío.[/]")
        return

    # ---------------------------------------------------------
    # 4. TRANSFORMACIÓN Y LIMPIEZA TOTAL
    # ---------------------------------------------------------
    console.print(f"[cyan]🛠️ Transformando {len(df_total)} registros totales...[/]")

    with console.status("[bold green]Procesando reglas de negocio...[/]", spinner="dots"):
        # A. Renombres (Estandarización)
        df_total = df_total.rename(columns={
            "Fecha Llamada": "Fecha", 
            "Hora Llamada": "Hora",
            "Franquicia": "Nombre Franquicia" 
        })

        # B. Limpieza de Tipos y Fechas
        # Validamos formato y eliminamos fechas futuras imposibles
        df_total["Fecha"] = pd.to_datetime(df_total["Fecha"], dayfirst=True, errors="coerce")
        limite_futuro = pd.Timestamp.now() + pd.Timedelta(days=1)
        mask_errores = df_total["Fecha"] > limite_futuro
        if mask_errores.any():
            console.print(f"[yellow]⚠️ Se descartaron {mask_errores.sum()} registros con fecha futura (Error DD/MM).[/]")
            df_total = df_total[~mask_errores]

        # C. Limpieza de ID (Robusta)
        if "N° Abonado" in df_total.columns:
            df_total["N° Abonado"] = df_total["N° Abonado"].astype(str).str.strip()
            df_total["N° Abonado"] = df_total["N° Abonado"].str.replace(r'\.0$', '', regex=True)
            df_total["N° Abonado"] = df_total["N° Abonado"].replace({'nan': '', 'None': '', 'NaT': ''})

        # D. Limpieza de Textos (Mayúsculas)
        cols_texto = ["Cliente", "Tipo Respuesta", "Detalle Respuesta", "Responsable", "Ciudad", "Nombre Franquicia", "Grupo Afinidad"]
        for col in cols_texto:
            if col in df_total.columns:
                df_total[col] = df_total[col].astype(str).str.upper().str.strip()

        # E. Enriquecimiento
        df_total["Tipo de afluencia"] = "COME BACK HOME"

        # F. Selección de Columnas Finales
        cols_finales = [
            "Source.Name", "N° Abonado", "Documento", "Cliente", "Estatus", 
            "Fecha", "Hora", "Tipo Respuesta", "Detalle Respuesta", 
            "Responsable", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
            "Ciudad", "Tipo de afluencia"
        ]
        
        # Aseguramos que todas existan antes del reindex
        for c in cols_finales:
            if c not in df_total.columns:
                df_total[c] = None
                
        df_total = df_total.reindex(columns=cols_finales)

    # ---------------------------------------------------------
    # 5. DEDUPLICACIÓN GLOBAL Y GUARDADO
    # ---------------------------------------------------------
    # Ordenamos por fecha descendente
    df_total = df_total.sort_values(by="Fecha", ascending=False)

    # Deduplicación Final (Keep First sobre orden descendente prioriza la data de la tarde)
    subset_dedupe = ["N° Abonado", "Fecha", "Hora"]
    subset_dedupe = [c for c in subset_dedupe if c in df_total.columns]
    
    df_final = df_total.drop_duplicates(subset=subset_dedupe, keep='first')
    
    # Estandarización final
    df_final = limpiar_nulos_powerbi(df_final)
    df_final = standard_hours(df_final, 'Hora')

    guardar_parquet(
        df_final, 
        NOMBRE_GOLD,
        filas_iniciales=len(df_total),
        ruta_destino=PATHS.get("gold", "")
    )
    console.print(f"[bold green]✅ CBH Gold reconstruido a velocidad luz. Total filas únicas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
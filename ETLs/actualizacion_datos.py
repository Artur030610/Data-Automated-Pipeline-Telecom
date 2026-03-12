import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, FOLDERS_ACT_DATOS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console, archivos_raw

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]5. ETL: ACTUALIZACIÓN DE DATOS (INCREMENTAL/CORREGIDO)[/]")
    
    # 1. DEFINICIÓN DE RUTAS Y ESTRUCTURAS
    BASE_PATH_RAW = PATHS["raw_act_datos"]
    NOMBRE_GOLD = "Actualizacion_Datos_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Actualizacion_Datos_Raw_Bronze.parquet")
    # ARCHIVOS BRONZE
    try:
        archivos_raw(BASE_PATH_RAW, RUTA_BRONZE)
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó, pero el ETL continuará. Error: {e}[/]")
    # --- CORRECCIÓN AQUÍ: Quitamos 'Tipo Respuesta' de las comunes ---
    cols_comunes = [
        "N° Abonado", "Estatus", "Saldo", 
        "Responsable", "Suscripción", 
        "Grupo Afinidad", "Ciudad", "Zona", "Barrio", "Dirección"
    ]

    # ---------------------------------------------------------
    # 2. CARGA DEL HISTÓRICO (GOLD)
    # ---------------------------------------------------------
    df_historico = pd.DataFrame()
    fecha_corte = None

    if os.path.exists(RUTA_GOLD_COMPLETA):
        try:
            df_historico = pd.read_parquet(RUTA_GOLD_COMPLETA)
            df_historico["Fecha"] = pd.to_datetime(df_historico["Fecha"], dayfirst=True, errors="coerce")
            
            if not df_historico.empty:
                fecha_corte = df_historico["Fecha"].max()
                console.print(f"[blue]📜 Histórico encontrado. Fecha corte: {fecha_corte}[/]")
            else:
                console.print("[yellow]⚠️ Archivo Gold vacío. Se hará carga total.[/]")
        except Exception as e:
            console.print(f"[red]❌ Error leyendo histórico: {e}. Se hará carga total.[/]")
    else:
        console.print("[yellow]⚠️ No existe histórico previo. Se hará carga inicial.[/]")

    # ---------------------------------------------------------
    # 3. PROCESAMIENTO DE FUENTES
    # ---------------------------------------------------------
    dfs_para_anexar = []
    
    for carpeta in FOLDERS_ACT_DATOS:
        ruta_completa = os.path.join(BASE_PATH_RAW, carpeta)
        
        # --- ESTRATEGIA A: CALL CENTER y OOCC (Aquí SÍ pedimos Tipo Respuesta) ---
        if "CALL CENTER" in carpeta or "OOCC" in carpeta:
            # Agregamos 'Tipo Respuesta' explícitamente solo aquí
            cols_source = cols_comunes + ["Tipo Respuesta", "Fecha Llamada", "Hora Llamada", "Detalle Respuesta", "Franquicia"]
            
            df_temp = leer_carpeta(ruta_completa, columnas_esperadas=cols_source, filtro_exclusion="Consolidado")
            
            if not df_temp.empty:
                df_temp = df_temp.rename(columns={
                    "Fecha Llamada": "Fecha",
                    "Hora Llamada": "Hora",
                    "Franquicia": "Nombre Franquicia"
                })

        # --- ESTRATEGIA B: OBSERVACIONES (Aquí NO pedimos Tipo Respuesta) ---
        elif "OBSERVACIONES" in carpeta:
            cols_source = cols_comunes + ["Fecha", "Hora", "Observacion", "Asunto", "Franquicia"]
            
            df_temp = leer_carpeta(ruta_completa, columnas_esperadas=cols_source, filtro_exclusion="Consolidado")
            
            if not df_temp.empty:
                # Lógica para crear el Detalle Respuesta
                if "Detalle Respuesta" not in df_temp.columns:
                    df_temp["Detalle Respuesta"] = np.nan
                
                df_temp["Detalle Respuesta"] = df_temp["Detalle Respuesta"].fillna(df_temp.get("Observacion"))
                df_temp["Detalle Respuesta"] = df_temp["Detalle Respuesta"].fillna(df_temp.get("Asunto"))
                
                # IMPORTANTE: Como OBS no tiene Tipo Respuesta, lo creamos vacío o con un valor por defecto
                if "Tipo Respuesta" not in df_temp.columns:
                    df_temp["Tipo Respuesta"] = "GESTION INTERNA / OBS" 

                df_temp = df_temp.rename(columns={"Franquicia": "Nombre Franquicia"})
                
                # Limpieza columnas aux
                df_temp = df_temp.drop(columns=[c for c in ["Observacion", "Asunto"] if c in df_temp.columns])
        
        else:
            continue 

        # --- FILTRADO INCREMENTAL ---
        if not df_temp.empty:
            df_temp["Fecha"] = pd.to_datetime(df_temp["Fecha"], dayfirst=True, errors="coerce")
            
            if fecha_corte is not None:
                # Filtramos solo lo que sea estrictamente mayor a la última fecha del histórico
                df_temp = df_temp[df_temp["Fecha"] > fecha_corte].copy()

            if not df_temp.empty:
                df_temp["Origen"] = df_temp.get("Source.Name", "").astype(str).str.upper()
                dfs_para_anexar.append(df_temp)

    # ---------------------------------------------------------
    # 4. CONSOLIDACIÓN
    # ---------------------------------------------------------
    df_nuevo = pd.DataFrame()
    
    if dfs_para_anexar:
        df_nuevo = pd.concat(dfs_para_anexar, ignore_index=True)
        console.print(f"[cyan]✨ Se encontraron {len(df_nuevo)} registros NUEVOS para procesar.[/]")
    else:
        if not df_historico.empty:
            console.print("[green]✅ El sistema está actualizado. No hay data nueva posterior al corte.[/]")
            return 
        else:
            console.print("[warning]⚠️ No se encontraron datos en raw.[/]")
            return

    # ---------------------------------------------------------
    # 5. TRANSFORMACIÓN FINAL (Solo Data Nueva)
    # ---------------------------------------------------------
    if not df_nuevo.empty:
        # Limpieza Texto
        df_nuevo["Detalle Respuesta"] = df_nuevo["Detalle Respuesta"].fillna("").astype(str).str.upper().str.strip()
        df_nuevo["Detalle Respuesta"] = df_nuevo["Detalle Respuesta"].str.split("ANT: ").str[0].str.strip()
        
        # Aseguramos que Tipo Respuesta exista y sea string
        df_nuevo["Tipo Respuesta"] = df_nuevo["Tipo Respuesta"].fillna("SIN CLASIFICAR").astype(str).str.upper()

        reemplazos = {
            "EMAIL": "CORREO ELECTRÓNICO",
            "CÉDULA DE IDENTIDAD": "CEDULA",
            "CELULAR": "TELEFONO",
            "NÚMERO TELEFÓNICO": "TELEFONO"
        }
        for viejo, nuevo in reemplazos.items():
            df_nuevo["Detalle Respuesta"] = df_nuevo["Detalle Respuesta"].str.replace(viejo, nuevo, regex=False)

        cols_finales = [
            "Origen", "N° Abonado", "Estatus", "Fecha", "Hora", 
            "Detalle Respuesta", "Tipo Respuesta", "Responsable", "Suscripción", 
            "Grupo Afinidad", "Nombre Franquicia", "Ciudad"
        ]
        
        df_nuevo = df_nuevo.reindex(columns=cols_finales)
        df_nuevo = df_nuevo.drop_duplicates()

    # ---------------------------------------------------------
    # 6. UNIFICACIÓN Y GUARDADO
    # ---------------------------------------------------------
    df_final = pd.DataFrame()

    if not df_historico.empty and not df_nuevo.empty:
        df_historico = df_historico.reindex(columns=df_nuevo.columns)
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    elif not df_nuevo.empty:
        df_final = df_nuevo
    else:
        df_final = df_historico

    if not df_final.empty:
        # Deduplicación Final (Mantener el último registro válido)
        subset_duplicados = ["N° Abonado", "Fecha", "Hora", "Detalle Respuesta", "Responsable"]
        df_final = df_final.drop_duplicates(subset=subset_duplicados, keep='last')

        guardar_parquet(
            df_final, 
            NOMBRE_GOLD,
            filas_iniciales=len(df_nuevo) if not df_nuevo.empty else len(df_final)
        )
        console.print(f"[bold green]✅ Actualización Datos Gold completada. Total filas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
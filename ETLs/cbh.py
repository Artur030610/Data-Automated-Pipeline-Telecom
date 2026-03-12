import pandas as pd
import numpy as np
import os
from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console, archivos_raw, limpiar_nulos_powerbi

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]15. ETL: VUELVE A CASA (CBH)[/]")
    
    # 1. Rutas y Configuración
    RUTA_RAW = PATHS["raw_comeback"]
    # Definimos la ruta de salida para el equipo (Bronze)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "CBH_Raw_Bronze.parquet")
    
    # -------------------------------------------------------------------------
    # PASO NUEVO: GENERAR CAPA BRONZE (Data cruda para el equipo)
    # -------------------------------------------------------------------------
    try:
        archivos_raw(RUTA_RAW, RUTA_BRONZE)
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se pudo generar: {e}[/]")

    # 2. Definición de Columnas Esperadas
    cols_esperadas = [
        "N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha Llamada", "Hora Llamada", "Tipo Respuesta",
        "Detalle Respuesta", "Responsable", "Suscripción", 
        "Grupo Afinidad", "Franquicia", "Ciudad"
    ]

    # 3. Lectura de archivos (Pandas)
    df = leer_carpeta(
        RUTA_RAW, 
        filtro_exclusion="Consolidado",
        columnas_esperadas=cols_esperadas
    )
    
    if df.empty:
        console.print("[red]❌ No se encontraron datos en la carpeta Raw.[/]")
        return

    filas_raw = len(df)

    # --- TRANSFORMACIÓN ---
    # Renombramos para que coincida con tu lógica de abajo
    df = df.rename(columns={
        "Fecha Llamada": "Fecha",
        "Hora Llamada": "Hora"
    })

    # Convertimos strings vacíos o con espacios a NaN real
    df["Fecha"] = df["Fecha"].replace(r'^\s*$', np.nan, regex=True)
    
    # Filtro de seguridad: Quitar registros sin fecha
    df = df[df['Fecha'].notna()].copy()
    
    # Deduplicación basada en columnas clave
    # (Asegúrate de que 'Fecha' y 'Hora' ya existan por el rename anterior)
    df = df.drop_duplicates(subset=[
        "N° Abonado", "Documento", "Fecha", "Hora", "Responsable"
    ])

    # Limpieza final (Nulos y Mayúsculas)
    df = limpiar_nulos_powerbi(df)
    
    # 4. Guardado en GOLD
    guardar_parquet(
        df, 
        "CBH_Gold.parquet", 
        ruta_destino=PATHS["gold"], 
        filas_iniciales=filas_raw
    )
    
    console.print(f"[bold green]✅ Proceso CBH completado. Capa Bronze y Gold generadas.[/]")

if __name__ == "__main__":
    ejecutar()
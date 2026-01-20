import pandas as pd
import os
# Importamos la nueva lista de carpetas
from config import PATHS, FOLDERS_ACT_DATOS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]5. ETL: ACTUALIZACIÓN DE DATOS (MULTIPLES FUENTES)[/]")
    
    cols_esperadas = [
        "N° Abonado", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
        "Tipo Respuesta", "Detalle Respuesta", "Responsable", "Suscripción", 
        "Grupo Afinidad", "Franquicia", "Ciudad", "Zona", "Barrio", "Dirección"
    ]

    # --- 1. LECTURA ITERATIVA (Como en Reclamos) ---
    dfs_acumulados = []
    
    # Ruta base definida en config: .../11-Act. de Datos
    base_path = PATHS["raw_act_datos"] 

    for subcarpeta in FOLDERS_ACT_DATOS:
        ruta_completa = os.path.join(base_path, subcarpeta)
        
        # Leemos cada subcarpeta
        df_temp = leer_carpeta(
            ruta_completa, 
            filtro_exclusion="Consolidado",
            columnas_esperadas=cols_esperadas
        )
        
        if not df_temp.empty:
            dfs_acumulados.append(df_temp)

    # Si no encontró nada en ninguna de las 3 carpetas
    if not dfs_acumulados: 
        console.print("[warning]⚠️ No se encontraron datos en las subcarpetas de Act. Datos.[/]")
        return

    # Consolidamos todo en un solo DataFrame Raw
    df = pd.concat(dfs_acumulados, ignore_index=True)
    filas_raw = len(df) # Capturamos total inicial

    # --- 2. TRANSFORMACIÓN (Igual que antes) ---
    df = df.rename(columns={
        "Fecha Llamada": "Fecha", 
        "Hora Llamada": "Hora",
        "Franquicia": "Nombre Franquicia"
    })

    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
    df["Origen"] = df["Source.Name"].astype(str).str.upper()

    # --- 3. LIMPIEZA DE TEXTO (Detalle Respuesta) ---
    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper().str.strip()
    
    # Cortar en "ANT:"
    df["Detalle Respuesta"] = df["Detalle Respuesta"].str.split("ANT: ").str[0].str.strip()

    # Reemplazos masivos
    reemplazos = {
        "EMAIL": "CORREO ELECTRÓNICO",
        "CÉDULA DE IDENTIDAD": "CEDULA",
        "CELULAR": "TELEFONO",
        "NÚMERO TELEFÓNICO": "TELEFONO"
    }
    
    for viejo, nuevo in reemplazos.items():
        df["Detalle Respuesta"] = df["Detalle Respuesta"].str.replace(viejo, nuevo, regex=False)

    # --- 4. SELECCIÓN FINAL ---
    cols_finales = [
        "Origen", "N° Abonado", "Estatus", "Fecha", "Hora", 
        "Detalle Respuesta", "Responsable", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad"
    ]
    
    df_final = df.reindex(columns=cols_finales)
    df_final = df_final.drop_duplicates()

    # --- 5. CARGA ---
    guardar_parquet(
        df_final, 
        "Actualizacion_Datos_Gold.parquet",
        filas_iniciales=filas_raw
    )
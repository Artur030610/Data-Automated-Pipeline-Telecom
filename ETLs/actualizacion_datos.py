import pandas as pd
import numpy as np
import os
from config import PATHS, FOLDERS_ACT_DATOS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]5. ETL: ACTUALIZACIÓN DE DATOS (ESTRATEGIA POWER QUERY)[/]")
    
    # --- 1. DEFINICIÓN DE ESTRUCTURAS ---
    # Columnas que TODOS tienen en común
    cols_comunes = [
        "N° Abonado", "Estatus", "Saldo", 
        "Tipo Respuesta", "Responsable", "Suscripción", 
        "Grupo Afinidad", "Ciudad", "Zona", "Barrio", "Dirección"
    ]

    # Lista para guardar los DataFrames limpios
    dfs_para_anexar = []
    
    base_path = PATHS["raw_act_datos"] 

    # --- 2. PROCESAMIENTO POR FUENTE (CARPETA POR CARPETA) ---
    
    for carpeta in FOLDERS_ACT_DATOS:
        ruta_completa = os.path.join(base_path, carpeta)
        console.print(f"[cyan]📂 Procesando fuente: {carpeta}...[/]")
        
        # --- ESTRATEGIA A: CALL CENTER y OOCC ---
        # Regla: Tienen "Fecha Llamada" y "Hora Llamada". "Franquicia" suele llamarse así.
        if "CALL CENTER" in carpeta or "OOCC" in carpeta:
            # Definimos inputs específicos para esta fuente
            cols_source = cols_comunes + ["Fecha Llamada", "Hora Llamada", "Detalle Respuesta", "Franquicia"]
            
            df_temp = leer_carpeta(ruta_completa, columnas_esperadas=cols_source, filtro_exclusion="Consolidado")
            
            if not df_temp.empty:
                # Transformación INMEDIATA (Homologación)
                df_temp = df_temp.rename(columns={
                    "Fecha Llamada": "Fecha",
                    "Hora Llamada": "Hora",
                    "Franquicia": "Nombre Franquicia"
                })
                dfs_para_anexar.append(df_temp)

        # --- ESTRATEGIA B: OBSERVACIONES ---
        # Regla: Tienen "Fecha", "Hora", y el detalle está en "Observacion" o "Asunto".
        elif "OBSERVACIONES" in carpeta:
            # Probamos traer variantes de detalle
            cols_source = cols_comunes + ["Fecha", "Hora", "Observacion", "Asunto", "Franquicia"]
            
            df_temp = leer_carpeta(ruta_completa, columnas_esperadas=cols_source, filtro_exclusion="Consolidado")
            
            if not df_temp.empty:
                # 1. Unificamos el detalle (Observacion o Asunto -> Detalle Respuesta)
                if "Detalle Respuesta" not in df_temp.columns:
                    df_temp["Detalle Respuesta"] = np.nan
                
                # Prioridad: Observacion > Asunto
                if "Observacion" in df_temp.columns:
                    df_temp["Detalle Respuesta"] = df_temp["Detalle Respuesta"].fillna(df_temp["Observacion"])
                if "Asunto" in df_temp.columns:
                    df_temp["Detalle Respuesta"] = df_temp["Detalle Respuesta"].fillna(df_temp["Asunto"])
                
                # 2. Homologación final
                df_temp = df_temp.rename(columns={"Franquicia": "Nombre Franquicia"})
                
                # Limpieza de columnas auxiliares que ya usamos
                cols_drop = ["Observacion", "Asunto"]
                df_temp = df_temp.drop(columns=[c for c in cols_drop if c in df_temp.columns])
                
                dfs_para_anexar.append(df_temp)

    # --- 3. CONSOLIDACIÓN (ANEXAR) ---
    if not dfs_para_anexar: 
        console.print("[warning]⚠️ No se encontraron datos en ninguna carpeta.[/]")
        return

    # Como ya todos se llaman "Fecha" y "Detalle Respuesta", el concat es perfecto
    df = pd.concat(dfs_para_anexar, ignore_index=True)
    filas_raw = len(df)
    console.print(f"[green]✅ Unión exitosa. Total registros brutos: {filas_raw}[/]")

    # --- 4. TRANSFORMACIÓN FINAL (LIMPIEZA COMÚN) ---
    
    # Fechas (Ya todo está en la columna "Fecha")
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
    
    # Origen
    df["Origen"] = df["Source.Name"].astype(str).str.upper()

    # Limpieza Texto Detalle
    df["Detalle Respuesta"] = df["Detalle Respuesta"].fillna("").astype(str).str.upper().str.strip()
    df["Detalle Respuesta"] = df["Detalle Respuesta"].str.split("ANT: ").str[0].str.strip()

    # Reemplazos estándar
    reemplazos = {
        "EMAIL": "CORREO ELECTRÓNICO",
        "CÉDULA DE IDENTIDAD": "CEDULA",
        "CELULAR": "TELEFONO",
        "NÚMERO TELEFÓNICO": "TELEFONO"
    }
    for viejo, nuevo in reemplazos.items():
        df["Detalle Respuesta"] = df["Detalle Respuesta"].str.replace(viejo, nuevo, regex=False)

    # --- 5. AUDITORÍA Y FILTRADO DE NULOS ---
    
    # Columnas finales deseadas
    cols_finales = [
        "Origen", "N° Abonado", "Estatus", "Fecha", "Hora", 
        "Detalle Respuesta", "Responsable", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad"
    ]
    
    # Reindex para ordenar y asegurar estructura
    df_final = df.reindex(columns=cols_finales)
    
    # Auditoría de Fechas Nulas
    nulos_fecha = df_final["Fecha"].isna().sum()
    if nulos_fecha > 0:
        # Opcional: Eliminar los nulos si confirmamos que no sirven
        # df_final = df_final.dropna(subset=['Fecha'])
        console.print(f"[yellow]⚠️ Advertencia: Quedaron {nulos_fecha} registros sin fecha válida (eran nulos en origen o formato incorrecto).[/]")
    
    df_final = df_final.drop_duplicates()

    # --- 6. CARGA ---
    guardar_parquet(
        df_final, 
        "Actualizacion_Datos_Gold.parquet",
        filas_iniciales=filas_raw
    )
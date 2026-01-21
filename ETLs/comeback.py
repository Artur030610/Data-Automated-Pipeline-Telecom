import pandas as pd
from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]8. ETL: CAMPAÑA COME BACK HOME (CBH)[/]")
    
    # 1. Definición de Columnas de Entrada 
    # (Incluimos todas las que Power Query intenta leer, aunque luego borremos algunas)
    cols_esperadas = [
        "N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", 
        "Fecha Llamada", "Hora Llamada", "Tipo Llamada", "Tipo Respuesta", 
        "Detalle Respuesta", "Responsable", "Suscripción", "Grupo Afinidad", 
        "Franquicia", "Ciudad", "Observación", "Teléfono", "Detalle Suscripcion"
    ]

    # 2. Lectura
    df = leer_carpeta(
        PATHS["raw_comeback"], 
        filtro_exclusion="Consolidado",
        columnas_esperadas=cols_esperadas
    )
    
    if df.empty: return

    filas_raw = len(df) # Capturamos para el reporte

    # 3. Transformación y Limpieza
    
    # Renombres para estandarizar con el resto del Data Lake
    df = df.rename(columns={
        "Fecha Llamada": "Fecha", 
        "Hora Llamada": "Hora",
        "Franquicia": "Nombre Franquicia" 
    })

    # Tipos de datos seguros
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
    df["N° Abonado"] = df["N° Abonado"].fillna("").astype(str).str.replace(".0", "", regex=False)
    
    # Limpieza de textos (Mayúsculas y espacios)
    cols_texto = ["Cliente", "Tipo Respuesta", "Detalle Respuesta", "Responsable", "Ciudad", "Nombre Franquicia", "Grupo Afinidad"]
    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()

    # 4. Lógica específica de Negocio (Si aplica)
    # En tu script M no hay filtros complejos, solo selección de columnas.
    # Pero agregamos la columna "Origen" por trazabilidad
    df["Tipo de afluencia"] = "COME BACK HOME"

    # 5. Selección Final (Según tu script M original)
    # Power Query final: {"N° Abonado", "Documento", "Cliente", "Estatus", "Fecha Llamada", "Hora Llamada", "Tipo Respuesta", "Detalle Respuesta", "Responsable", "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad"}
    
    cols_finales = [
        "Source.Name", "N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha", "Hora", "Tipo Respuesta", "Detalle Respuesta", 
        "Responsable", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Tipo de afluencia"
    ]
    
    # Reindex asegura que si falta alguna columna no explote, sino que ponga NaN
    df_final = df.reindex(columns=cols_finales)
    df_final = df_final.drop_duplicates()
    df_final = limpiar_nulos_powerbi(df_final)
    # 6. Carga
    guardar_parquet(
        df_final, 
        "ComeBackHome_Gold.parquet",
        filas_iniciales=filas_raw
    )
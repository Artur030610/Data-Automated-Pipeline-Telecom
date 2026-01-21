import pandas as pd
from config import PATHS, MAPA_MESES
from utils import *

@reportar_tiempo
def ejecutar():
    print("--- 🎧 Iniciando ETL Atención al Cliente ---")
    
    cols_input = [
        "N° Abonado", "Documento", "Cliente", "Estatus", "Fecha Llamada", 
        "Hora Llamada", "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", 
        "Responsable", "Suscripción", "Teléfono", "Grupo Afinidad", 
        "Franquicia", "Ciudad", "Teléfono verificado", "Detalle Suscripcion", "Saldo"
    ]

    df = leer_carpeta(
        PATHS["raw_atencion"], 
        filtro_exclusion="Consolidado", 
        columnas_esperadas=cols_input
    )
    
    if df.empty: return
    filas_raw=len(df)
    # --- TRANSFORMACIÓN ---
    df = df.rename(columns={
        "Franquicia": "Nombre Franquicia", "Fecha Llamada": "Fecha", 
        "Responsable": "Vendedor", "Hora Llamada": "Hora"
    })

    df['N° Abonado'] = df['N° Abonado'].astype(str).replace('nan', None)
    df['Documento'] = df['Documento'].astype(str).replace('nan', None)
    # dayfirst=True fuerza a interpretar DD/MM/AAAA
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
    df['Vendedor'] = df['Vendedor'].astype(str).str.upper()

    # Filtro de filas
    df = df[~df['Tipo Respuesta'].isin(["AFILIACION DE SERVICIO", "PAGO DEL SERVICIO"])].copy()

    df['Tipo de afluencia'] = "ATENCIÓN AL CLIENTE"
    df['Mes'] = df['Fecha'].dt.month.map(MAPA_MESES)
    df = df.drop_duplicates()
    df = limpiar_nulos_powerbi(df)
    cols_output = [
        "Source.Name", "N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha", "Hora", "Tipo Respuesta", "Detalle Respuesta", 
        "Vendedor", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Tipo de afluencia", "Mes"
    ]
    guardar_parquet(df.reindex(columns=cols_output), "Atencion_Cliente_Gold.parquet", filas_iniciales=filas_raw)
    print(f"✅ ETL Atención al Cliente finalizado. Filas iniciales: {  filas_raw }, Filas finales: {len(df)}")
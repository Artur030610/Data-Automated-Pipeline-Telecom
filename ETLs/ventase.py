import pandas as pd
from config import PATHS 
import utils

@utils.reportar_tiempo
def ejecutar():
    
    # 1. CARGA DE DATOS
    df = utils.leer_carpeta(PATHS["ventas_estatus"], filtro_exclusion="Consolidado")
    
    if df.empty: return
    filas_raw = len(df)

    # ==========================================
    # 2. NORMALIZACIÓN DE COLUMNAS
    # ==========================================
    # A. Limpieza de espacios invisibles (Vital)
    df.columns = df.columns.str.strip()

    # B. Renombrado Estándar
    mapa_columnas = {
        "Fecha Venta": "Fecha", 
        "Franquicia": "Nombre Franquicia", 
        "Hora venta": "Hora",
        "Cédula": "Documento",    # Sinónimos comunes
        "Rif": "Documento",
        "Nro. Doc": "Documento"
    }
    df = df.rename(columns=mapa_columnas)
    
    # C. Asegurar existencia de columnas críticas
    # Esto evita KeyErrors si algún archivo viene incompleto
    cols_check = ["Paquete/Servicio", "Vendedor", "N° Abonado", "Cliente", "Estatus", "Documento"]
    for col in cols_check:
        if col not in df.columns:
            df[col] = "" 

    # ==========================================
    # 3. TRANSFORMACIONES
    # ==========================================
    # Estandarización de textos
    df["Paquete/Servicio"] = df["Paquete/Servicio"].astype(str).str.upper()
    df["Vendedor"] = df["Vendedor"].astype(str).str.upper().str.strip()
    
    # Filtro 1: Excluir Fibex Play
    df = df[~df["Paquete/Servicio"].str.contains("FIBEX PLAY|FIBEXPLAY", na=False, regex=True)].copy()

    # Etiquetado
    df["Tipo de afluencia"] = "Ventas"
    
    # Filtro 2: Excluir Ventas Calle/Agente
    df = df[~df["Vendedor"].str.contains("VENTAS CALLE|AGENTE", regex=True, na=False)].copy()

    # Extracción: Oficina del nombre del vendedor
    patron_oficina = r'.*(?:OFICINA|OFIC|OFI)\s+(.*)$'
    df["Oficina"] = df["Vendedor"].str.extract(patron_oficina)[0].str.strip()

    # Conversión de Fecha (Fundamental para Power BI)
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")

    # ==========================================
    # 4. DEDUPLICACIÓN (SOLICITADA)
    # ==========================================
    # Eliminamos filas que sean idénticas en estas 5 columnas clave
    subset_dedup = ["N° Abonado", "Documento", "Hora", "Fecha", "Vendedor"]
    
    df = df.drop_duplicates(subset=subset_dedup)

    # ==========================================
    # 5. SELECCIÓN FINAL Y GUARDADO
    # ==========================================
    cols_finales = [
        "N° Abonado", "Documento", "Estatus", "Fecha", "Vendedor", 
        "Costo", "Grupo Afinidad", "Nombre Franquicia", "Ciudad", 
        "Hora", "Tipo de afluencia", "Oficina"
    ]

    df_final = df.reindex(columns=cols_finales)
    
    utils.guardar_parquet(df_final, "Ventas_Estatus_Gold.parquet", filas_iniciales=filas_raw)
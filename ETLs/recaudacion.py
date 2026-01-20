import pandas as pd
import numpy as np
from config import PATHS, MAPA_MESES
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold white]PIPELINE DE RECAUDACIÓN[/]")
    
    # 1. Definición de Inputs
    cols_input = [
        "ID Contrato", "N° Abonado", "Fecha", "Total Pago", 
        "Forma de Pago", "Banco", "Nombre Caja", "Oficina Cobro", 
        "Fecha Contrato", "Estatus", "Suscripción", "Grupo Afinidad", 
        "Nombre Franquicia", "Ciudad", "Cobrador"
    ]
    
    # 2. Extracción (La barra de carga aparecerá aquí automáticamente)
    df = leer_carpeta(
        PATHS["raw_recaudacion"], 
        filtro_exclusion="Consolidado", 
        columnas_esperadas=cols_input
    )
    
    if df.empty: return
    filas_raw = len(df)
    # 3. Transformación
    # Tipos
    df['N° Abonado'] = df['N° Abonado'].astype(str).replace('nan', None)
    df['ID Contrato'] = df['ID Contrato'].astype(str).replace('nan', None)
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")
    df['Total Pago'] = pd.to_numeric(df['Total Pago'], errors='coerce').fillna(0)

    # Filtros Exclusión
    palabras_excluir = "VIRTUAL|Virna|Fideliza|Externa|Unicenter|Compensa"
    df = df[~df['Oficina Cobro'].astype(str).str.contains(palabras_excluir, case=False, na=False, regex=True)].copy()

    # Columnas Nuevas
    df['Tipo de afluencia'] = "RECAUDACIÓN"
    df['Mes'] = df['Fecha'].dt.month.map(MAPA_MESES)

    # Lógica Oficinas Propias (Lista completa basada en tu script)
    oficinas_propias = [
        "OFC COMERCIAL CUMANA", "OFC- LA ASUNCION", "OFC SAN ANTONIO DE CAPYACUAL", "OFC TINACO", 
        "OFC VILLA ROSA", "OFC-SANTA FE", "OFI CARIPE MONAGAS", "OFI TINAQUILLO", "OFI-BARCELONA", 
        "OFI-BARINAS", "OFI-BQTO", "OFIC GALERIA EL PARAISO", "OFIC SAMBIL-VALENCIA", 
        "OFIC. PARRAL VALENCIA", "OFIC. TORRE FIBEX VIÑEDO", "OFI-CARACAS PROPATRIA", 
        "OFIC-BOCA DE UCHIRE", "OFIC-CARICUAO", "OFIC-COMERCIAL SANTA FE", "OFICINA ALIANZA MALL", 
        "OFICINA MARGARITA", "OFICINA SAN JUAN DE LOS MORROS", "OFIC-JUAN GRIEGO-MGTA", 
        "OFIC-METROPOLIS-BQTO", "OFIC-MGTA_DIAZ", "OFI-LECHERIA", "OFI-METROPOLIS", "OFI-PARAISO", 
        "OFI-PASEO LAS INDUSTRIAS", "OFI-PTO CABELLO", "OFI-PTO LA CRUZ", "OFI-SAN CARLOS", "OFI-VIA VENETO"
    ]
    
    df['Clasificacion'] = np.where(
        df['Oficina Cobro'].isin(oficinas_propias), 
        "OFICINAS PROPIAS", 
        "ALIADOS Y DESARROLLO"
    )
    
    df = df.rename(columns={"Oficina Cobro": "Oficina"})
    df = df.rename(columns={"Cobrador": "Vendedor"})
    df = df.drop_duplicates()
    # 4. Selección Final
    cols_output = [
        "Source.Name", "ID Contrato", "N° Abonado", "Fecha", "Total Pago", 
        "Forma de Pago", "Banco", "Nombre Caja", "Oficina", "Fecha Contrato", 
        "Estatus", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Vendedor", "Tipo de afluencia", "Mes", "Clasificacion"
    ]
    
    # 5. Carga
    guardar_parquet(df.reindex(columns=cols_output), "Recaudacion_Gold.parquet", filas_iniciales=filas_raw)
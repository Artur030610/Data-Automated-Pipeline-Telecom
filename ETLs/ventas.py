import pandas as pd
import numpy as np
from config import PATHS, LISTA_VENDEDORES_OFICINA, LISTA_VENDEDORES_PROPIOS, MAPA_MESES
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console

def clasificar_canal(row):
    """
    Determina el canal de venta basado en prioridades y listas blancas.
    Se apoya en las correcciones previas hechas en 'nombre_detectado' y 'Oficina'.
    """
    vendedor = str(row["Vendedor"]).lower()
    nombre_detectado = str(row["nombre_detectado"])
    tipo_coincidencia = str(row["tipo_coincidencia"])
    
    # PRIORIDAD 1: CALL CENTER
    if "televentas" in vendedor or "call center" in vendedor:
        return "CALL CENTER"
    
    # PRIORIDAD 2: OFICINA COMERCIAL
    # Si fue detectado por Fuzzy, por Regla Bejuma, o está en lista blanca.
    condicion_oficina_detectada = (
        nombre_detectado != "nan" and 
        nombre_detectado != "" and 
        tipo_coincidencia != "Pendiente de Revisión" and 
        tipo_coincidencia != "No detectado" and 
        "administrador" not in vendedor
    )
    
    if condicion_oficina_detectada or (vendedor in LISTA_VENDEDORES_OFICINA):
        return "OFICINA COMERCIAL"
    
    # PRIORIDAD 3: VENDEDORES PROPIOS (Ventas Calle)
    condicion_calle = (
        "ventas" in vendedor and 
        "televentas" not in vendedor
    )
    
    if condicion_calle or (vendedor in LISTA_VENDEDORES_PROPIOS):
        return "VENDEDORES PROPIOS"
    
    # RESTO
    return "ALIADOS"

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]9. ETL: VENTAS (LISTADO GENERAL)[/]")
    
    # 1. Definición de Columnas Esperadas
    cols_esperadas = [
        "ID", "N° Abonado", "Fecha Contrato", "Estatus", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor", 
        "Serv/Paquete", "nombre_detectado", "Estado", "oficina_comercial", 
        "tipo_coincidencia", "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"
    ]

    # 2. Lectura
    df = leer_carpeta(
        PATHS["ventas_abonados"], 
        filtro_exclusion="Data_Consolidado_Ventas", 
        columnas_esperadas=cols_esperadas
    )
    
    if df.empty: return

    filas_raw = len(df)

    # 3. Transformación Básica
    # Renombramos oficina_comercial -> Oficina
    df = df.rename(columns={"oficina_comercial": "Oficina"})
    
    # Columna Fija
    df["Tipo de afluencia"] = "VENTAS"
    
    # -------------------------------------------------------------------------
    # --- CORRECCIÓN DE FECHAS (BLINDAJE HÍBRIDO) ---
    # -------------------------------------------------------------------------
    # Soluciona el problema de fechas mixtas (Serials Excel vs Texto) que causaban el 1970
    console.print("[yellow]   Corrigiendo fechas mixtas (Serials Excel vs Texto)...[/]")
    
    # Intentamos convertir todo a numérico. Lo que sea texto se vuelve NaN.
    ser_numerica = pd.to_numeric(df["Fecha Contrato"], errors='coerce')
    
    # Identificamos seriales válidos (mayores a 35000 son fechas post-1995)
    mask_es_serial = ser_numerica.notna() & (ser_numerica > 35000)
    
    fechas_finales = pd.Series(pd.NaT, index=df.index)
    
    # A) Convertimos los números de Excel (origen 1899)
    if mask_es_serial.any():
        fechas_finales[mask_es_serial] = pd.to_datetime(
            ser_numerica[mask_es_serial], 
            unit='D', 
            origin='1899-12-30'
        )
    
    # B) Convertimos los textos normales
    if (~mask_es_serial).any():
        fechas_finales[~mask_es_serial] = pd.to_datetime(
            df.loc[~mask_es_serial, "Fecha Contrato"], 
            dayfirst=True, 
            errors='coerce'
        )
        
    df["Fecha Contrato"] = fechas_finales

    # Mes (Recalculamos con la fecha ya limpia)
    df["Mes"] = df["Fecha Contrato"].dt.month.map(MAPA_MESES).str.capitalize()

    # -------------------------------------------------------------------------
    # --- LIMPIEZA DE TEXTO ---
    # -------------------------------------------------------------------------
    df["Vendedor"] = df["Vendedor"].fillna("").astype(str).str.lower().str.strip()
    df["Ciudad"] = df["Ciudad"].fillna("").astype(str).str.upper().str.strip()
    df["nombre_detectado"] = df["nombre_detectado"].fillna("").astype(str).str.upper().str.strip()
    df["Oficina"] = df["Oficina"].fillna("").astype(str).str.upper().str.strip()

    # -------------------------------------------------------------------------
    # --- CORRECCIÓN FORZADA: BEJUMA ---
    # -------------------------------------------------------------------------
    # Regla Estricta: Debe contener "bejuma" Y TAMBIÉN "ofic".
    # Esto asegura que sea la oficina física y no un vendedor de calle en Bejuma.
    
    console.print("[yellow]   Aplicando reglas forzadas de Bejuma (Solo Oficinas)...[/]")
    
    tiene_bejuma = df["Vendedor"].str.contains("bejuma", case=False, na=False)
    tiene_oficina = df["Vendedor"].str.contains("ofic", case=False, na=False)
    
    mask_bejuma = tiene_bejuma & tiene_oficina
    
    if mask_bejuma.any():
        # Forzamos valores para que clasifique correctamente
        df.loc[mask_bejuma, "Oficina"] = "BEJUMA"
        df.loc[mask_bejuma, "tipo_coincidencia"] = "Oficina Detectada"
        df.loc[mask_bejuma, "nombre_detectado"] = "OFICINA BEJUMA"
    # -------------------------------------------------------------------------

    # 4. Lógica de Negocio (Canal)
    console.print("[cyan]   Calculando Canal de Venta...[/]")
    # Ahora que Bejuma tiene datos válidos, esta función le asignará OFICINA COMERCIAL
    df["Canal"] = df.apply(clasificar_canal, axis=1)

    # 5. Selección y Limpieza Final
    # Eliminamos duplicados
    df = df.drop_duplicates(subset=[
        "N° Abonado", "ID", "Fecha Contrato", "Vendedor","Ciudad"])
    
    # Eliminamos columnas técnicas del Fuzzy
    cols_a_borrar = ["Estado", "Cliente", "ID", "Source.Name", "Serv/Paquete", 
                     "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"]
    
    df = df.drop(columns=cols_a_borrar, errors="ignore")
    df = df.drop_duplicates()
    
    # Filtro final de calidad: Eliminamos lo que siga pendiente
    df = df[df['tipo_coincidencia'] != 'Pendiente de Revisión']
    
    # 6. Carga
    guardar_parquet(
        df, 
        "Ventas_Listado_Gold.parquet",
        filas_iniciales=filas_raw
    )
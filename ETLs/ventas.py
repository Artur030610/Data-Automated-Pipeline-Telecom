import pandas as pd
import numpy as np
from config import PATHS, LISTA_VENDEDORES_OFICINA, LISTA_VENDEDORES_PROPIOS, MAPA_MESES
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console

def clasificar_canal(row):
    """
    Replica la lógica condicional compleja de Power Query (M) para determinar el Canal.
    """
    vendedor = str(row["Vendedor"]).lower()
    nombre_detectado = str(row["nombre_detectado"])
    tipo_coincidencia = str(row["tipo_coincidencia"])
    
    # PRIORIDAD 1: CALL CENTER
    if "televentas" in vendedor or "call center" in vendedor:
        return "CALL CENTER"
    
    # PRIORIDAD 2: OFICINA COMERCIAL
    # Lógica M: ([nombre_detectado] <> null y validaciones...) O está en la lista blanca
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
    # Lógica M: (Dice 'ventas' pero no 'televentas') O está en lista blanca
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
        filtro_exclusion="Data_Consolidado_Ventas", # Excluir el output viejo
        columnas_esperadas=cols_esperadas
    )
    
    if df.empty: return

    filas_raw = len(df)

    # 3. Transformación Básica
    df = df.rename(columns={"oficina_comercial": "Oficina"})
    
    # Columna Fija
    df["Tipo de afluencia"] = "VENTAS"
    
    # Fechas (Blindaje con dayfirst=True)
    df["Fecha Contrato"] = pd.to_datetime(df["Fecha Contrato"], dayfirst=True, errors="coerce")
    
    # Mes (Usando el mapa o nombre)
    # Replicamos: Date.MonthName([Fecha Contrato])
    # Para asegurar español usamos tu MAPA_MESES si lo tienes, o strftime si tienes locale configurado.
    # Usaremos el mapa del config para consistencia.
    df["Mes"] = df["Fecha Contrato"].dt.month.map(MAPA_MESES).str.capitalize() # Capitalize para "Enero"

    # Limpieza de Texto (Vendedor a minúsculas, Ciudad a Mayúsculas)
    df["Vendedor"] = df["Vendedor"].fillna("").astype(str).str.lower().str.strip()
    df["Ciudad"] = df["Ciudad"].fillna("").astype(str).str.upper().str.strip()
    df["nombre_detectado"] = df["nombre_detectado"].fillna("").astype(str).str.upper().str.strip()
    df["Oficina"] = df["Oficina"].fillna("").astype(str).str.upper().str.strip()

    # Reemplazo específico (Como en M)
    # "oficina bejuma ventas oficina bejuma" -> "oficina bejuma"
    df["Vendedor"] = df["Vendedor"].str.replace("oficina bejuma ventas oficina bejuma", "oficina bejuma", regex=False)

    # 4. Lógica de Negocio (Canal)
    console.print("[cyan]   Calculando Canal de Venta...[/]")
    # Aplicamos la función fila por fila (axis=1)
    df["Canal"] = df.apply(clasificar_canal, axis=1)

    # 5. Selección y Limpieza Final
    # Eliminamos duplicados
    df = df.drop_duplicates()
    
    # Columnas a eliminar según M
    cols_a_borrar = ["Estado", "Cliente", "ID", "Source.Name", "Serv/Paquete", 
                     "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"]
    
    df = df.drop(columns=cols_a_borrar, errors="ignore")
    df = df.drop_duplicates()
    # 6. Carga
    guardar_parquet(
        df, 
        "Ventas_Listado_Gold.parquet",
        filas_iniciales=filas_raw
    )
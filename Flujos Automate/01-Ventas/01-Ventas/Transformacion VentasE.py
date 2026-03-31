import pandas as pd
import glob
import os
import sys
import warnings
import re # Importamos librería de expresiones regulares
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme

# --- CONFIGURACIÓN VISUAL ---
custom_theme = Theme({"success": "bold green", "error": "bold red", "warning": "yellow", "info": "cyan"})
console = Console(theme=custom_theme)
warnings.simplefilter(action='ignore')

# ==========================================
# 1. CONFIGURACIÓN GLOBAL (RUTAS)
# ==========================================
USUARIO = os.path.expanduser("~")
RUTA_BASE = os.path.join(USUARIO, "Documents", "A-DataStack", "01-Proyectos", "01-Data_PipelinesFibex", "02_Data_Lake")

CONFIG = {
    # RUTAS RAW
    "raw_reclamos": os.path.join(RUTA_BASE, "raw_data", "3-Reclamos"),
    "raw_ventas": os.path.join(RUTA_BASE, "raw_data", "1-Ventas Estatus"), # Nueva ruta
    
    # RUTA GOLD (SALIDA)
    "gold": os.path.join(RUTA_BASE, "gold_data"),
    
    # SUB-CARPETAS RECLAMOS
    "folders_general": ["1-Data-Reclamos por CC", "2-Data-Reclamos por OOCC", "3-Data-Reclamos por RRSS"],
    "sub_app": "4-Data-Reclamos por APP",
    "sub_banco": "5-Data-Reclamos OB"
}

# Aseguramos carpeta de salida
os.makedirs(CONFIG["gold"], exist_ok=True)

# ==========================================
# 2. HERRAMIENTAS (FUNCIONES DE SOPORTE)
# ==========================================
def leer_carpeta(ruta_carpeta, filtro_exclusion=None):
    """Lee Excels de una ruta (y subcarpetas si es necesario)"""
    if not os.path.exists(ruta_carpeta):
        console.print(f"[error]❌ La carpeta no existe: {ruta_carpeta}[/]")
        return pd.DataFrame()

    archivos = glob.glob(os.path.join(ruta_carpeta, "*.xlsx"))
    lista_dfs = []
    
    nombre_carpeta = os.path.basename(ruta_carpeta)
    console.log(f"[info]📂 Escaneando {nombre_carpeta}... ({len(archivos)} archivos)[/]")
    
    for archivo in archivos:
        nombre = os.path.basename(archivo)
        
        if nombre.startswith("~") or "$" in nombre: continue
        if filtro_exclusion and filtro_exclusion in nombre: continue
            
        try:
            df = pd.read_excel(archivo, engine="calamine")
            df["Source.Name"] = nombre 
            lista_dfs.append(df)
        except Exception as e:
            console.print(f"[warning]⚠️ Error leyendo {nombre}: {e}[/]")
            
    if not lista_dfs: return pd.DataFrame()
    return pd.concat(lista_dfs, ignore_index=True)

def guardar_parquet(df, nombre_archivo):
    """Guarda en Parquet con validación de vacíos"""
    if df.empty:
        console.print(f"[warning]⚠️ Dataset vacío para {nombre_archivo}. Omitido.[/]")
        return

    ruta_salida = os.path.join(CONFIG["gold"], nombre_archivo)
    try:
        df.to_parquet(ruta_salida, index=False)
        console.print(f"[success]✅ GUARDADO: {nombre_archivo} -> {len(df):,} filas.[/]")
    except Exception as e:
        console.print(f"[error]❌ FALLO GUARDANDO {nombre_archivo}: {e}[/]")

# ==========================================
# 3. LÓGICA DE NEGOCIO (ETLs)
# ==========================================

# --- A. NUEVO ETL: VENTAS ESTATUS ---
def etl_ventas_estatus():
    console.rule("[bold cyan]4. ETL: VENTAS ESTATUS[/]")
    
    # 1. Extracción
    df = leer_carpeta(CONFIG["raw_ventas"], filtro_exclusion="Consolidado")
    if df.empty: return

    # 2. Transformación inicial (Tipos y Renombres)
    df = df.rename(columns={
        "Fecha Venta": "Fecha",
        "Franquicia": "Nombre Franquicia",
        "Hora venta": "Hora"
    })

    # Aseguramos columnas clave antes de filtrar
    cols_requeridas = ["Paquete/Servicio", "Vendedor", "N° Abonado", "Cliente", "Estatus"]
    for col in cols_requeridas:
        if col not in df.columns:
            df[col] = "" # Evitar crash si falta columna

    # 3. Filtros de Filas (Logic M: not Text.Contains "FIBEX PLAY")
    # Convertimos a string para buscar seguro
    df["Paquete/Servicio"] = df["Paquete/Servicio"].astype(str).str.upper()
    df = df[~df["Paquete/Servicio"].str.contains("FIBEX PLAY", na=False)].copy()
    df = df[~df["Paquete/Servicio"].str.contains("FIBEXPLAY", na=False)].copy()

    # 4. Limpieza y Deduplicación 1
    df["Vendedor"] = df["Vendedor"].astype(str).str.upper().str.strip()
    
    # M: Table.Distinct con claves específicas
    subset_duplicados = ["N° Abonado", "Cliente", "Estatus", "Vendedor", "Fecha"]
    # Aseguramos que existan en el df para no dar error
    subset_existente = [c for c in subset_duplicados if c in df.columns]
    df = df.drop_duplicates(subset=subset_existente)

    # 5. Columna Nueva
    df["Tipo de afluencia"] = "Ventas"

    # 6. Deduplicación 2 (Distinct total) y Filtros Vendedor
    df = df.drop_duplicates()
    
    # Filtros Vendedor (CALLE, AGENTE)
    filtros_vendedor = ["VENTAS CALLE", "AGENTE"]
    # Usamos join con '|' para buscar cualquiera de las dos palabras
    patron_excluir = '|'.join(filtros_vendedor)
    df = df[~df["Vendedor"].str.contains(patron_excluir, regex=True, na=False)].copy()

    # 7. LÓGICA COMPLEJA: Extraer OFICINA
    # Regex explicada: Busca desde el inicio (.*) hasta encontrar el último grupo (OFICINA|OFIC|OFI),
    # luego un espacio, y captura lo que sigue ((.*)$) en el grupo 1.
    patron_oficina = r'.*(?:OFICINA|OFIC|OFI)\s+(.*)$'
    df["Oficina"] = df["Vendedor"].str.extract(patron_oficina)[0]
    
    # Limpieza final de la columna extraída (Trim)
    df["Oficina"] = df["Oficina"].str.strip()

    # 8. Selección Final y Tipos
    cols_finales = [
        "N° Abonado", "Documento", "Estatus", "Fecha", "Vendedor", 
        "Costo", "Grupo Afinidad", "Nombre Franquicia", "Ciudad", 
        "Hora", "Tipo de afluencia", "Oficina"
    ]
    
    # Reindex para garantizar estructura
    df_final = df.reindex(columns=cols_finales)

    # Conversión de Tipos Segura (Evitar 'Expected bytes, got float')
    df_final["Fecha"] = pd.to_datetime(df_final["Fecha"], errors="coerce")
    
    cols_texto = ["N° Abonado", "Documento", "Estatus", "Vendedor", "Ciudad", "Oficina"]
    for col in cols_texto:
        df_final[col] = df_final[col].fillna("").astype(str)

    guardar_parquet(df_final, "Ventas_Estatus_Gold.parquet")


# --- B. ETLs ANTERIORES (RECLAMOS) ---
def etl_reclamos_generales():
    console.rule("[bold cyan]1. ETL: RECLAMOS GENERALES[/]")
    dfs_acumulados = []
    
    for carpeta_nombre in CONFIG["folders_general"]:
        ruta = os.path.join(CONFIG["raw_reclamos"], carpeta_nombre)
        df_temp = leer_carpeta(ruta, filtro_exclusion="Consolidado")
        if not df_temp.empty: dfs_acumulados.append(df_temp)
    
    if not dfs_acumulados: return

    df = pd.concat(dfs_acumulados, ignore_index=True)
    df = df.rename(columns={"Tipo Llamada": "Origen"})
    if "Barrio" in df.columns: df["Barrio"] = df["Barrio"].astype(str).str.upper()
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    
    cols = ["N° Abonado", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", "Origen", 
            "Tipo Respuesta", "Detalle Respuesta", "Responsable", "Suscripción", 
            "Grupo Afinidad", "Franquicia", "Ciudad"]
    
    df_final = df.reindex(columns=cols)
    for c in ["N° Abonado", "Responsable", "Detalle Respuesta", "Origen"]:
        df_final[c] = df_final[c].fillna("").astype(str)
        
    guardar_parquet(df_final, "Reclamos_General_Gold.parquet")

def etl_fallas_app():
    console.rule("[bold cyan]2. ETL: FALLAS APP[/]")
    ruta = os.path.join(CONFIG["raw_reclamos"], CONFIG["sub_app"])
    df = leer_carpeta(ruta)
    if df.empty: return

    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper()
    df["OrdenCategoria"] = df.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    
    cols = ["N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", "Fecha Llamada", 
            "Hora Llamada", "Detalle Respuesta", "Responsable", "Suscripción", 
            "Grupo Afinidad", "Franquicia", "Ciudad", "OrdenCategoria"]
    
    df_final = df.reindex(columns=cols)
    for c in ["N° Abonado", "Documento"]:
        df_final[c] = df_final[c].astype(str).replace('nan', None)
        
    guardar_parquet(df_final, "Reclamos_App_Gold.parquet")

def etl_fallas_banco():
    console.rule("[bold cyan]3. ETL: FALLAS BANCOS[/]")
    ruta = os.path.join(CONFIG["raw_reclamos"], CONFIG["sub_banco"])
    df = leer_carpeta(ruta)
    if df.empty: return

    target = ["FALLA BNC", "FALLA CON BDV", "FALLA CON R4", "FALLA MERCANTIL"]
    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper().str.strip()
    df = df[df["Detalle Respuesta"].isin(target)].copy()
    
    if df.empty:
        console.print("[warning]⚠️ Sin datos válidos para bancos.[/]")
        return

    df["Detalle Respuesta"] = df["Detalle Respuesta"].str.replace("FALLA CON ", "", regex=False).str.replace("FALLA ", "", regex=False).str.strip()
    df["TotalCuenta"] = df.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")

    cols = ["N° Abonado", "Cliente", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
            "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
            "Observación", "Grupo Afinidad", "Franquicia", "Ciudad", "TotalCuenta"]
    
    df_final = df.reindex(columns=cols)
    for c in ["Observación", "N° Abonado", "Cliente", "Responsable"]:
        df_final[c] = df_final[c].fillna("").astype(str)

    guardar_parquet(df_final, "Reclamos_Banco_Gold.parquet")

# ==========================================
# 4. ORQUESTADOR PRINCIPAL
# ==========================================
if __name__ == "__main__":
    console.print(Panel.fit("[bold white]PIPELINE MASTER FIBEX[/]", style="bold blue"))
    
    # Lista de funciones a ejecutar
    procesos = [
        etl_reclamos_generales,
        etl_fallas_app,
        etl_fallas_banco,
        etl_ventas_estatus # <--- Agregado el nuevo proceso
    ]

    for proceso in procesos:
        try:
            proceso()
        except Exception as e:
            console.print(f"[error]💥 Error Crítico en {proceso.__name__}: {e}[/]")

    console.rule("[bold green]PROCESO FINALIZADO[/]")
    console.print(f"📂 Archivos en: [underline]{CONFIG['gold']}[/]")
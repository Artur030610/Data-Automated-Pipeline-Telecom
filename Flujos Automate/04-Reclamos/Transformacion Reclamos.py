import pandas as pd
import glob
import os
import sys
import warnings

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(scripts_dir)

from config import PATHS
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
CONFIG = {
    # Raíz donde están todas las carpetas numéricas
    "raw_root": str(PATHS.get("raw_reclamos")), 
    
    "gold": str(PATHS.get("gold")),
    
    # LISTA DE CARPETAS QUE COMPONEN EL REPORTE GENERAL
    "folders_general": [
        "1-Data-Reclamos por CC",
        "2-Data-Reclamos por OOCC",
        "3-Data-Reclamos por RRSS"
    ],
    
    # CARPETAS INDIVIDUALES
    "sub_app": "4-Data-Reclamos por APP",
    "sub_banco": "5-Data-Reclamos OB"
}

# Aseguramos que exista la carpeta de salida
os.makedirs(CONFIG["gold"], exist_ok=True)

# ==========================================
# 2. HERRAMIENTAS (FUNCIONES DE SOPORTE)
# ==========================================
def leer_carpeta(ruta_carpeta, filtro_exclusion=None):
    """
    Lee todos los excels de una ruta dada.
    filtro_exclusion: texto que, si está en el nombre del archivo, lo ignora.
    """
    # Verificamos que la carpeta exista antes de intentar leer
    if not os.path.exists(ruta_carpeta):
        console.print(f"[error]❌ La carpeta no existe: {ruta_carpeta}[/]")
        return pd.DataFrame()

    archivos = glob.glob(os.path.join(ruta_carpeta, "*.xlsx"))
    lista_dfs = []
    
    nombre_carpeta = os.path.basename(ruta_carpeta)
    console.log(f"[info]📂 Escaneando {nombre_carpeta}... ({len(archivos)} archivos)[/]")
    
    for archivo in archivos:
        nombre = os.path.basename(archivo)
        
        # Filtros básicos de seguridad
        if nombre.startswith("~") or "$" in nombre:
            continue
            
        if filtro_exclusion and filtro_exclusion in nombre:
            continue
            
        try:
            df = pd.read_excel(archivo, engine="calamine")
            df["Source.Name"] = nombre 
            lista_dfs.append(df)
        except Exception as e:
            console.print(f"[warning]⚠️ Error leyendo {nombre}: {e}[/]")
            
    if not lista_dfs:
        return pd.DataFrame()
        
    return pd.concat(lista_dfs, ignore_index=True)

def guardar_parquet(df, nombre_archivo):
    """Guarda el DataFrame en Parquet y reporta el resultado."""
    if df.empty:
        console.print(f"[warning]⚠️ Dataset vacío para {nombre_archivo}. No se guardó nada.[/]")
        return

    ruta_salida = os.path.join(CONFIG["gold"], nombre_archivo)
    try:
        df.to_parquet(ruta_salida, index=False)
        console.print(f"[success]✅ GUARDADO: {nombre_archivo} -> {len(df):,} filas.[/]")
    except Exception as e:
        console.print(f"[error]❌ FALLO GUARDANDO {nombre_archivo}: {e}[/]")

# ==========================================
# 3. LÓGICA DE NEGOCIO (TUS ETLs)
# ==========================================

def etl_reclamos_generales():
    """Procesa CC, OOCC y RRSS en un solo archivo"""
    console.rule("[bold cyan]1. ETL: RECLAMOS GENERALES (CC + OOCC + RRSS)[/]")
    
    dfs_acumulados = []
    
    # --- BUCLE PARA LEER LAS 3 CARPETAS ---
    for carpeta_nombre in CONFIG["folders_general"]:
        ruta_completa = os.path.join(CONFIG["raw_root"], carpeta_nombre)
        df_temp = leer_carpeta(ruta_completa, filtro_exclusion="Consolidado")
        
        if not df_temp.empty:
            dfs_acumulados.append(df_temp)
    
    if not dfs_acumulados:
        console.print("[warning]⚠️ No se encontraron datos en ninguna de las 3 carpetas generales.[/]")
        return

    # Unimos todo en un solo DataFrame gigante
    df = pd.concat(dfs_acumulados, ignore_index=True)

    # 2. Transformación
    df = df.rename(columns={"Tipo Llamada": "Origen"})
    
    if "Barrio" in df.columns:
        df["Barrio"] = df["Barrio"].astype(str).str.upper()
        
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    
    cols_finales = ["N° Abonado", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
                    "Origen", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
                    "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad"]
    
    df_final = df.reindex(columns=cols_finales)

    # Limpieza de tipos para evitar errores al guardar
    columnas_texto = ["N° Abonado", "Responsable", "Detalle Respuesta", "Origen"]
    for col in columnas_texto:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna("").astype(str)
    
    # 3. Carga
    guardar_parquet(df_final, "Reclamos_General_Gold.parquet")


def etl_fallas_app():
    """Procesa las fallas reportadas por la App"""
    console.rule("[bold cyan]2. ETL: FALLAS APP[/]")
    
    ruta_app = os.path.join(CONFIG["raw_root"], CONFIG["sub_app"])
    df = leer_carpeta(ruta_app)
    
    if df.empty: return

    # 2. Transformación
    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper()
    df["OrdenCategoria"] = df.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    
    cols_finales = ["N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", 
                    "Fecha Llamada", "Hora Llamada", "Detalle Respuesta", "Responsable", 
                    "Suscripción", "Grupo Afinidad", "Franquicia", "Ciudad", "OrdenCategoria"]
    
    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    df_final = df.reindex(columns=cols_finales)

    # Limpieza de tipos
    df_final["N° Abonado"] = df_final["N° Abonado"].astype(str).replace('nan', None)
    df_final["Documento"] = df_final["Documento"].astype(str).replace('nan', None)
    
    # 3. Carga
    guardar_parquet(df_final, "Reclamos_App_Gold.parquet")


def etl_fallas_banco():
    """Procesa las fallas bancarias"""
    console.rule("[bold cyan]3. ETL: FALLAS BANCOS[/]")
    
    ruta_banco = os.path.join(CONFIG["raw_root"], CONFIG["sub_banco"])
    df = leer_carpeta(ruta_banco)
    
    if df.empty: return

    # 2. Transformación
    fallas_target = ["FALLA BNC", "FALLA CON BDV", "FALLA CON R4", "FALLA MERCANTIL"]
    df["Detalle Respuesta"] = df["Detalle Respuesta"].astype(str).str.upper().str.strip()
    
    df = df[df["Detalle Respuesta"].isin(fallas_target)].copy()
    
    if df.empty:
        console.print("[warning]⚠️ Ninguna fila cumple filtros de Banco.[/]")
        return

    df["Detalle Respuesta"] = (df["Detalle Respuesta"]
                               .str.replace("FALLA CON ", "", regex=False)
                               .str.replace("FALLA ", "", regex=False)
                               .str.strip())
    
    df["TotalCuenta"] = df.groupby("Detalle Respuesta")["Detalle Respuesta"].transform("count")
    
    cols_finales = ["N° Abonado", "Cliente", "Estatus", "Saldo", "Fecha Llamada", "Hora Llamada", 
                    "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", "Responsable", 
                    "Observación", "Grupo Afinidad", "Franquicia", "Ciudad", "TotalCuenta"]

    df["Fecha Llamada"] = pd.to_datetime(df["Fecha Llamada"], errors="coerce")
    df_final = df.reindex(columns=cols_finales)

    # Limpieza de tipos (Observación suele fallar)
    columnas_texto = ["Observación", "N° Abonado", "Cliente", "Responsable"]
    for col in columnas_texto:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna("").astype(str)

    # 3. Carga
    guardar_parquet(df_final, "Reclamos_Banco_Gold.parquet")

# ==========================================
# 4. ORQUESTADOR (MAIN)
# ==========================================
if __name__ == "__main__":
    console.print(Panel.fit("[bold white]PIPELINE MASTER: GESTIÓN DE RECLAMOS[/]", style="bold blue"))
    
    try:
        etl_reclamos_generales()
    except Exception as e:
        console.print(f"[error]💥 Error Crítico en Generales: {e}[/]")

    try:
        etl_fallas_app()
    except Exception as e:
        console.print(f"[error]💥 Error Crítico en App: {e}[/]")

    try:
        etl_fallas_banco()
    except Exception as e:
        console.print(f"[error]💥 Error Crítico en Bancos: {e}[/]")

    console.rule("[bold green]PROCESO FINALIZADO[/]")
    console.print(f"📂 Archivos disponibles en: [underline]{CONFIG['gold']}[/]")
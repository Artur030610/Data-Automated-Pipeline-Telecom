import pandas as pd
from rapidfuzz import process, fuzz
import re
import os
import sys
from unidecode import unidecode

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, MAPA_MESES
from utils import guardar_parquet, reportar_tiempo, console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# =============================================================================
# 1. CONFIGURACIÓN DE PATRONES (LÓGICA BLINDADA)
# =============================================================================
KEYWORDS_NO_HUMANOS = [
    r'\bINTERCOM\b', r'\bINVERSIONES\b', r'\bSOLUCIONES\b', 
    r'\bTECNOLOGIA\b', r'\bTELECOMUNICACIONES\b', r'\bMULTISERVICIOS\b',
    r'\bCOMERCIALIZADORA\b', r'\bCORPORACION\b', r'\bSISTEMAS\b',
    r'\bASOCIADOS\b', r'\bCONSORCIO\b', r'\bGRUPO\b',
    r'\bAGENTE\b', r'\bALIADO\b', r'\bAUTORIZADO\b',
    r'\bOFICINA\b', r'\bSUCURSAL\b', r'\bCANAL\b', r'\bTAQUILLA\b',
    r'\bFIBEX\b', r'\bOFI\b', r'\bOFIC\b', r'\bOFC\b'
]
PATRON_NO_HUMANO = re.compile('|'.join(KEYWORDS_NO_HUMANOS), re.IGNORECASE)

# --- FUNCIONES AUXILIARES ---
def normalize_text(text):
    if isinstance(text, str):
        text = unidecode(text).lower()
        text = re.sub(r'[^a-z0-9\s]', '', text)
        return re.sub(r'\s+', ' ', text).strip()
    return ""

def preparar_maestro_gold_blindado(df):
    if df.empty or 'Nombre_Completo' not in df.columns: return {}, []
    
    mask_humanos = ~df['Nombre_Completo'].astype(str).str.contains(PATRON_NO_HUMANO, regex=True, na=False)
    df_humanos = df[mask_humanos].copy()
    
    col_ofi = 'OficinaSistema' if 'OficinaSistema' in df_humanos.columns else 'Oficina'
    df_humanos[col_ofi] = df_humanos[col_ofi].replace(r'^\s*$', None, regex=True)
    
    nombres = df_humanos['Nombre_Completo'].astype(str).apply(normalize_text)
    df_temp = pd.DataFrame({'nombre': nombres, 'oficina': df_humanos[col_ofi]}).dropna()
    
    mapa_ofi = dict(zip(df_temp['nombre'], df_temp['oficina']))
    return mapa_ofi, list(mapa_ofi.keys())

def preparar_universo(df):
    if df.empty: return {}, []
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()
    
    for col in ['Nombre', 'Apellido', 'Oficina']:
        if col not in df.columns: df[col] = ''
    
    df['Nombre_Completo'] = df['Nombre'].astype(str) + " " + df['Apellido'].astype(str)
    df['Oficina'] = df['Oficina'].replace(r'^\s*$', None, regex=True)
    
    nombres = df['Nombre_Completo'].apply(normalize_text)
    mapa_ofi = dict(zip(nombres, df['Oficina']))
    return mapa_ofi, list(mapa_ofi.keys())

# =============================================================================
# 2. EJECUCIÓN DEL PIPELINE (MODO FULL REFRESH)
# =============================================================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]4. ETL SILVER: MATCHING BLINDADO (MODO FULL REFRESH)[/]")
    
    # --- DEFINICIÓN DE RUTAS ---
    nombre_archivo_silver = "Afluencia_Consolidada_Silver.parquet"
    ruta_silver_destino = os.path.join(PATHS["silver"], nombre_archivo_silver)

    cols_necesarias = [
        "N° Abonado","Documento", "Estatus", "Fecha", "Vendedor", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad", 
        "Tipo de afluencia", "Oficina", "Clasificacion", "Hora"
    ]

    files = {
        "Ventas": os.path.join(PATHS["gold"], "Ventas_Estatus_Gold.parquet"),
        "ATC": os.path.join(PATHS["gold"], "Atencion_Cliente_Gold.parquet"),
        "Recaudacion": os.path.join(PATHS["gold"], "Recaudacion_Gold.parquet")
    }

    # --- PASO 1: LECTURA TOTAL DE FUENTES ---
    dfs_all = []
    for origen, ruta in files.items():
        if os.path.exists(ruta):
            try:
                df_source = pd.read_parquet(ruta)
                for col in cols_necesarias:
                    if col not in df_source.columns: df_source[col] = None
                
                df_source = df_source[cols_necesarias].copy()
                dfs_all.append(df_source)
                console.print(f"   🔹 {origen}: {len(df_source)} registros cargados.")
            except Exception as e:
                console.print(f"[red]❌ Error cargando {origen}: {e}[/]")

    if not dfs_all:
        console.print("[bold red]❌ No hay datos origen para procesar.[/]")
        return ruta_silver_destino

    df_total = pd.concat(dfs_all, ignore_index=True)
    df_total["Fecha"] = pd.to_datetime(df_total["Fecha"], dayfirst=True, errors="coerce")
    df_total["Mes"] = df_total["Fecha"].dt.month.map(MAPA_MESES).str.lower()
    
    # --- PASO 2: MATCHING INTELIGENTE ---
    path_maestro = os.path.join(PATHS["gold"], "Maestro_Empleados_Gold.parquet")
    df_maestro = pd.read_parquet(path_maestro) if os.path.exists(path_maestro) else pd.DataFrame()
    path_univ = os.path.join(PATHS["raw_asesores_univ_14"], "Data_Universo_Asesores.xlsx")
    df_univ = pd.read_excel(path_univ) if os.path.exists(path_univ) else pd.DataFrame()
    
    mapa_gold, lista_gold = preparar_maestro_gold_blindado(df_maestro)
    mapa_univ, lista_univ = preparar_universo(df_univ)
    
    df_total['Vendedor_Clean'] = df_total['Vendedor'].fillna('').astype(str).apply(normalize_text)
    vendedores_unicos = [v for v in df_total['Vendedor_Clean'].unique() if len(v) > 2]
    
    resultados = {}
    with console.status("[blue]Ejecutando Matching de Vendedores con RapidFuzz...[/]"):
        for vend in vendedores_unicos:
            oficina_asignada = None
            if lista_gold:
                candidates = process.extract(vend, lista_gold, scorer=fuzz.token_set_ratio, limit=3)
                top = [c for c in candidates if c[1] >= 95]
                if top: oficina_asignada = mapa_gold.get(max(top, key=lambda x: (x[1], len(x[0])))[0])
            
            if not oficina_asignada and lista_univ:
                candidates_univ = process.extract(vend, lista_univ, scorer=fuzz.token_set_ratio, limit=3)
                top_u = [c for c in candidates_univ if c[1] >= 95]
                if top_u: oficina_asignada = mapa_univ.get(max(top_u, key=lambda x: (x[1], len(x[0])))[0])
            
            if not oficina_asignada:
                vu = vend.upper()
                if any(x in vu for x in ["INTERCOM", "INVERSIONES", "SOLUCIONES", "TECNOLOGIA", "AGENTE", "COMERCIALIZADORA", "TROMP"]):
                    oficina_asignada = "ALIADO / AGENTE"
                elif "CALLE" in vu: oficina_asignada = "FUERZA DE VENTA EXTERNA"
                elif any(x in vu for x in ["TELEVENTAS", "CALL CENTER"]): 
                    oficina_asignada = "TELEVENTAS / CALL CENTER"
            
            if oficina_asignada: resultados[vend] = oficina_asignada

    df_total['Oficina'] = df_total['Vendedor_Clean'].map(resultados).combine_first(df_total['Oficina'])
    df_total.drop(columns=['Vendedor_Clean'], inplace=True)

    # --- PASO 3: DEDUPLICACIÓN Y GUARDADO ---
    df_final = df_total.copy()

    guardar_parquet(df_final, nombre_archivo_silver, filas_iniciales=len(df_final), ruta_destino=PATHS["silver"])
    console.print(f"[bold green]✨ Silver Consolidado generado: {len(df_final):,} registros únicos.[/]")
    
    return ruta_silver_destino

if __name__ == "__main__":
    ejecutar()
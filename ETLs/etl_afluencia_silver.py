import pandas as pd
from rapidfuzz import process, fuzz
import re
import os
from unidecode import unidecode
from config import PATHS, MAPA_MESES
from utils import guardar_parquet, reportar_tiempo, console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# =============================================================================
# 1. CONFIGURACIÓN DE PATRONES (LÓGICA BLINDADA)
# =============================================================================
# Compilamos una sola vez para evitar lentitud en el proceso.

# A. Palabras que indican que el registro NO es un humano (Blacklist para Matching)
# Si el nombre en el maestro tiene esto, NO lo usaremos para buscar.
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
    """
    Lee el maestro y aplica el FILTRO DE HUMANOS.
    Retorna un diccionario limpio solo con personas reales.
    """
    if df.empty: return {}, []
    if 'Nombre_Completo' not in df.columns: return {}, []
    
    # 1. Aplicar Filtro Anti-Empresas
    # Creamos una máscara con los que NO (~) coinciden con el patrón de empresas
    mask_humanos = ~df['Nombre_Completo'].astype(str).str.contains(PATRON_NO_HUMANO, regex=True, na=False)
    
    df_humanos = df[mask_humanos].copy()
    df_descartados = df[~mask_humanos] # Solo para reporte
    
    if not df_descartados.empty:
        console.print(f"[yellow]   🛡️  Maestro Blindado: Se ocultaron {len(df_descartados)} registros tipo 'Empresa' para evitar falsos positivos.[/]")

    # 2. Indexar solo a los Humanos
    col_ofi = 'OficinaSistema' if 'OficinaSistema' in df_humanos.columns else 'Oficina'
    df_humanos[col_ofi] = df_humanos[col_ofi].replace(r'^\s*$', None, regex=True)
    
    nombres = df_humanos['Nombre_Completo'].astype(str).apply(normalize_text)
    mapa_ofi = dict(zip(nombres, df_humanos[col_ofi]))
    
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

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]4. ETL SILVER: MATCHING BLINDADO (FILTRO REGEX + DESEMPATE)[/]")
    
    # --- PASO 1: CONSOLIDACIÓN DE FUENTES ---
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
    
    dfs = []
    for origen, ruta in files.items():
        if os.path.exists(ruta):
            try:
                df_t = pd.read_parquet(ruta)
                for col in cols_necesarias:
                    if col not in df_t.columns: df_t[col] = None
                dfs.append(df_t[cols_necesarias].copy())
            except Exception: pass

    if not dfs:
        console.print("[red]❌ No hay datos para consolidar.[/]")
        return None

    df_final = pd.concat(dfs, ignore_index=True)
    df_final["Fecha"] = pd.to_datetime(df_final["Fecha"], dayfirst=True, errors="coerce")
    df_final["Mes"] = df_final["Fecha"].dt.month.map(MAPA_MESES).str.lower()
    
    # --- PASO 2: MATCHING INTELIGENTE ---
    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(), console=console
    ) as progress:
        
        task_match = progress.add_task("[cyan]Cruzando con Maestros...", total=1)
        
        # Carga de Maestros
        path_maestro = os.path.join(PATHS["gold"], "Maestro_Empleados_Gold.parquet")
        df_maestro = pd.read_parquet(path_maestro)
        path_univ = os.path.join(PATHS["raw_asesores_univ_14"], "Data_Universo_Asesores.xlsx")
        df_univ = pd.read_excel(path_univ) if os.path.exists(path_univ) else pd.DataFrame()
        
        # Preparación (Aquí ocurre el filtrado Anti-Empresas)
        mapa_gold, lista_gold = preparar_maestro_gold_blindado(df_maestro)
        mapa_univ, lista_univ = preparar_universo(df_univ)
        
        # Limpieza inicial de la columna Vendedor (Facts)
        df_final['Vendedor_Clean'] = df_final['Vendedor'].fillna('').astype(str).apply(normalize_text)
        vendedores_unicos = [v for v in df_final['Vendedor_Clean'].unique() if len(v) > 2]
        
        resultados = {}
        
        for vend in vendedores_unicos:
            oficina_asignada = None
            
            # --- 1. MAESTRO GOLD (Prioridad 1: Solo Humanos) ---
            # Buscamos en la lista filtrada. "INTERCOM" ya no existe aquí.
            candidates = process.extract(vend, lista_gold, scorer=fuzz.token_set_ratio, limit=5)
            top_candidates = [c for c in candidates if c[1] >= 95]
            
            if top_candidates:
                # Desempate: Si "JESUS" y "JESUS BRICEÑO" dan 100, gana el más largo.
                ganador = max(top_candidates, key=lambda x: (x[1], len(x[0])))
                nom_gold = ganador[0]
                oficina_asignada = mapa_gold.get(nom_gold)
            
            # --- 2. UNIVERSO (Prioridad 2) ---
            if not oficina_asignada and lista_univ:
                candidates_univ = process.extract(vend, lista_univ, scorer=fuzz.token_set_ratio, limit=5)
                top_univ = [c for c in candidates_univ if c[1] >= 95]
                
                if top_univ:
                    ganador_univ = max(top_univ, key=lambda x: (x[1], len(x[0])))
                    nom_univ = ganador_univ[0]
                    oficina_asignada = mapa_univ.get(nom_univ)
            
            # --- 3. REGLAS SEMÁNTICAS (Red de Seguridad) ---
            # Aquí cae todo lo que filtramos en el paso 1 (Empresas, Agentes, Oficinas)
            if not oficina_asignada:
                vu = vend.upper()
                
                # A. Reglas Corporativas (Atrapan a los excluidos del maestro)
                if "INTERCOM" in vu: oficina_asignada = "ALIADO / AGENTE"
                elif "INVERSIONES" in vu: oficina_asignada = "ALIADO / AGENTE"
                elif "SOLUCIONES" in vu: oficina_asignada = "ALIADO / AGENTE"
                elif "TECNOLOGIA" in vu: oficina_asignada = "ALIADO / AGENTE"
                elif "AGENTE" in vu: oficina_asignada = "ALIADO / AGENTE"
                elif "COMERCIALIZADORA" in vu: oficina_asignada = "ALIADO / AGENTE"
                
                # B. Reglas de Infraestructura
                elif "CALLE" in vu: oficina_asignada = "FUERZA DE VENTA EXTERNA"
                elif "TELEVENTAS" in vu or "CALL CENTER" in vu: oficina_asignada = "TELEVENTAS / CALL CENTER"
                elif "TROMP" in vu: oficina_asignada = "ALIADO / AGENTE"
            
            resultados[vend] = oficina_asignada
        
        # Aplicar resultados y limpiar
        df_final['Oficina'] = df_final['Vendedor_Clean'].map(resultados)
        df_final.drop(columns=['Vendedor_Clean'], inplace=True)
        df_final = df_final.drop_duplicates()
        progress.update(task_match, advance=1)

    nombre_archivo = "Afluencia_Consolidada_Silver.parquet"
    ruta_destino = os.path.join(PATHS["silver"], nombre_archivo)
    
    filas_con_oficina = df_final['Oficina'].notna().sum()
    console.print(f"[green]✔ Matching Blindado completado. {filas_con_oficina:,} registros asignados.[/]")
    
    guardar_parquet(df_final, nombre_archivo, filas_iniciales=len(df_final), ruta_destino=ruta_destino)
    return ruta_destino
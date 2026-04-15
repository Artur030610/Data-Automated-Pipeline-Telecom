import pandas as pd
import re
import os
import glob
from datetime import datetime
import sys

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, archivos_raw

# =============================================================================
# 1. CONFIGURACIÓN DE FILTROS
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

# =============================================================================
# 2. FUNCIONES AUXILIARES
# =============================================================================
def extraer_fecha_archivo(nombre_archivo):
    """ Parsea fechas del nombre del archivo (ddmmyyyy) """
    match = re.search(r'(\d{6,8})', str(nombre_archivo))
    if not match: return datetime(1900, 1, 1)

    numeros = match.group(1)
    try:
        anio = int(numeros[-4:])
        resto = numeros[:-4]
        dia, mes = 1, 1
        if len(resto) == 4:
            dia, mes = int(resto[0:2]), int(resto[2:4])
        elif len(resto) == 2:
            dia, mes = int(resto[0]), int(resto[1])
        elif len(resto) == 3:
            posible_dia = int(resto[0:2])
            if posible_dia > 12: 
                 dia, mes = posible_dia, int(resto[2])
            else:
                 dia, mes = int(resto[0:2]), int(resto[2])
        return datetime(anio, mes, dia)
    except:
        return datetime(1900, 1, 1)

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]👤 ETL: MAESTRO DE EMPLEADOS (SCD FULL REFRESH BLINDADO)[/]")

    RUTA_RAW = PATHS["raw_empleados"]
    NOMBRE_GOLD = "Maestro_Empleados_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", ""), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Maestro_Empleados_Raw_Bronze.parquet")

    # Respaldamos la raw en bronze para mantener la arquitectura
    try:
        archivos_raw(RUTA_RAW, RUTA_BRONZE)
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó, pero el ETL continuará. Error: {e}[/]")

    # ---------------------------------------------------------
    # 1. LECTURA TOTAL ORDENADA (Protección de Integridad)
    # ---------------------------------------------------------
    # 🚀 OPTIMIZACIÓN RAM: Leemos directamente el Parquet que Polars acaba de generar
    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold red]❌ No se encontró la capa Bronze. Imposible continuar.[/]")
        return

    console.print("[cyan]🚀 Leyendo histórico completo desde capa Bronze (Parquet)...[/]")
    df_nuevo = pd.read_parquet(RUTA_BRONZE, dtype_backend="pyarrow")
    
    if df_nuevo.empty: return

    # Excluimos los archivos Consolidados usando la metadata inyectada por Polars
    if 'Source.Name' in df_nuevo.columns:
        df_nuevo = df_nuevo[~df_nuevo['Source.Name'].str.contains("Consolidado", na=False, case=False)]

    # ---------------------------------------------------------
    # 2. TRANSFORMACIONES Y LIMPIEZA
    # ---------------------------------------------------------
    # Inyectamos la fecha del nombre como Fecha_Inicio
    df_nuevo['Fecha_Inicio'] = df_nuevo['Source.Name'].apply(extraer_fecha_archivo)
    df_nuevo['Fecha_Inicio'] = pd.to_datetime(df_nuevo['Fecha_Inicio'], errors='coerce')

    # Renombrado y Normalización
    df_nuevo.columns = df_nuevo.columns.str.strip().str.title()
    mapa_renombre = {
        "Nombre": "Nombre_Completo", 
        "Apellido": "OficinaSae",
        "Oficina": "OficinaSistema",
        "Doc. De Identidad": "Doc_Identidad", "Doc. de Identidad": "Doc_Identidad", "Cedula": "Doc_Identidad"
    }
    df_nuevo.rename(columns=mapa_renombre, inplace=True)

    # Limpieza de texto y Filtro Robots
    cols_texto = ["Nombre_Completo", "OficinaSae", "OficinaSistema", "Doc_Identidad"]
    for col in [c for c in cols_texto if c in df_nuevo.columns]:
        df_nuevo[col] = df_nuevo[col].fillna("").astype(str).str.upper().str.strip()

    df_nuevo = df_nuevo[~df_nuevo['Nombre_Completo'].str.contains(PATRON_NO_HUMANO, regex=True, na=False)].copy()

    # Creación del 'Combinado' para identidad única (Persona + Ubicación)
    df_nuevo["Combinado"] = (df_nuevo["Nombre_Completo"] + " " + df_nuevo["OficinaSae"]).str.strip()

    # --- PROTECCIÓN CONTRA ACTUALIZACIONES DE LA TARDE ---
    # Si hay 2 archivos del mismo día, nos quedamos con la última versión leída
    df_unificado = df_nuevo.drop_duplicates(subset=["Doc_Identidad", "Combinado", "Fecha_Inicio"], keep='last').copy()

    # ---------------------------------------------------------
    # 3. LÓGICA DE DIMENSIÓN DINÁMICA (SCD)
    # ---------------------------------------------------------
    # Ordenamos para procesar las vigencias en la línea de tiempo
    df_unificado.sort_values(by=["Combinado", "Fecha_Inicio"], ascending=[True, True], inplace=True)

    # Generamos la 'Fecha_Fin': es la Fecha_Inicio del siguiente registro para ese mismo 'Combinado'
    df_unificado['Fecha_Fin'] = df_unificado.groupby('Combinado')['Fecha_Inicio'].shift(-1)

    # Columna de estado para Power BI
    df_unificado['Estado_Vigencia'] = df_unificado['Fecha_Fin'].apply(lambda x: 'ACTIVO' if pd.isnull(x) else 'HISTÓRICO')

    # ---------------------------------------------------------
    # 4. SELECCIÓN FINAL Y GUARDADO
    # ---------------------------------------------------------
    cols_finales = [
        "Doc_Identidad", "Nombre_Completo", "Combinado", 
        "OficinaSae", "OficinaSistema", "Fecha_Inicio", "Fecha_Fin", 
        "Estado_Vigencia", "Franquicia Vendedor", "Franquicia Cobrador"
    ]
    df_final = df_unificado[[c for c in cols_finales if c in df_unificado.columns]]
    df_final = limpiar_nulos_powerbi(df_final)

    console.print(f"[green]✔ Maestro actualizado. Total registros (vigentes + históricos): {len(df_final)}[/]")
    guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=len(df_final), ruta_destino=PATHS.get("gold", ""))

if __name__ == "__main__":
    ejecutar()
import pandas as pd
import re
import os
import glob
from datetime import datetime
from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console

# =============================================================================
# 1. CONFIGURACIÓN DE FILTROS (Mantenida)
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
    console.rule("[bold magenta]👤 ETL: MAESTRO DE EMPLEADOS (INCREMENTAL SCD DINÁMICO)[/]")

    RUTA_RAW = PATHS["raw_empleados"]
    NOMBRE_GOLD = "Maestro_Empleados_Gold.parquet"
    # Buscamos la ruta en config, si no existe usamos una por defecto
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", ""), NOMBRE_GOLD)

    # ---------------------------------------------------------
    # 1. DETECCIÓN INCREMENTAL
    # ---------------------------------------------------------
    archivos_todos = glob.glob(os.path.join(RUTA_RAW, "*.xlsx"))
    archivos_a_leer = []
    df_historico = pd.DataFrame()
    fecha_corte = pd.Timestamp('1900-01-01')

    if os.path.exists(RUTA_GOLD_COMPLETA):
        try:
            df_historico = pd.read_parquet(RUTA_GOLD_COMPLETA)
            if not df_historico.empty and 'Fecha_Inicio' in df_historico.columns:
                # Usamos Fecha_Inicio para saber cuál fue el último archivo procesado
                fecha_corte = pd.to_datetime(df_historico['Fecha_Inicio'], errors='coerce').max()
                console.print(f"[green]✅ Histórico cargado. Última versión: {fecha_corte.date()}[/]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Error leyendo histórico ({e}). Se procesará todo de nuevo.[/]")

    for arch in archivos_todos:
        if "Consolidado" in arch or "~$" in arch: continue
        f_arch = pd.Timestamp(extraer_fecha_archivo(arch))
        if f_arch > fecha_corte:
            archivos_a_leer.append(arch)

    if not archivos_a_leer:
        console.print("[bold green]✅ El Maestro ya está actualizado.[/]")
        return

    # ---------------------------------------------------------
    # 2. CARGA Y TRANSFORMACIONES ORIGINALES
    # ---------------------------------------------------------
    console.print(f"[cyan]🚀 Procesando {len(archivos_a_leer)} archivos nuevos...[/]")
    df_nuevo = leer_carpeta(archivos_especificos=archivos_a_leer)
    
    if df_nuevo.empty: return

    # Inyectamos la fecha del nombre como Fecha_Inicio
    df_nuevo['Fecha_Inicio'] = df_nuevo['Source.Name'].apply(extraer_fecha_archivo)
    df_nuevo['Fecha_Inicio'] = pd.to_datetime(df_nuevo['Fecha_Inicio'], errors='coerce')

    # Renombrado y Normalización (Tu lógica original)
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

    # ---------------------------------------------------------
    # 3. LÓGICA DE DIMENSIÓN DINÁMICA (SCD)
    # ---------------------------------------------------------
    # Unimos lo nuevo con lo viejo
    df_unificado = pd.concat([df_historico, df_nuevo], ignore_index=True)

    # Ordenamos para procesar las vigencias
    df_unificado.sort_values(by=["Combinado", "Fecha_Inicio"], ascending=[True, True], inplace=True)

    # Generamos la 'Fecha_Fin': es la Fecha_Inicio del siguiente registro para ese mismo 'Combinado'
    # Si es el último registro, la Fecha_Fin será nula (Vigente)
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
    guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=len(df_nuevo))

if __name__ == "__main__":
    ejecutar()
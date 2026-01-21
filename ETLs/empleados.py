import pandas as pd
import re
import os
from datetime import datetime
from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console
from unidecode import unidecode
from rich.table import Table

# =============================================================================
# 1. CONFIGURACIÓN DE FILTROS (ANTI-ROBOTS)
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
# 2. FUNCIONES AUXILIARES (LÓGICA DE FECHAS ROBUSTA)
# =============================================================================
def extraer_fecha_archivo(nombre_archivo):
    """
    Parsea fechas del nombre del archivo manejando longitudes variables.
    Soporta: ddmmyyyy (8), dmyyyy (6), dmmyyyy (7), etc.
    """
    match = re.search(r'(\d{6,8})', str(nombre_archivo))
    if not match:
        return datetime.min

    numeros = match.group(1)
    
    try:
        # El año siempre son los últimos 4 dígitos
        anio = int(numeros[-4:])
        resto = numeros[:-4] # Lo que queda son dia y mes
        
        dia = 1
        mes = 1
        
        # Lógica para desempatar días y meses de longitud variable
        if len(resto) == 4:   # Ej: 0812 -> 08 y 12 (ddmm)
            dia = int(resto[0:2])
            mes = int(resto[2:4])
        elif len(resto) == 2: # Ej: 61 -> 6 y 1 (dm)
            dia = int(resto[0])
            mes = int(resto[1])
        elif len(resto) == 3: # El caso difícil: 161 o 712
            # Si los primeros 2 dígitos son > 12, seguro es un día (ej. 161 -> Dia 16, Mes 1)
            posible_dia = int(resto[0:2])
            if posible_dia > 12: 
                 dia = posible_dia
                 mes = int(resto[2])
            else:
                 # Asumimos formato dmm (ej 712 -> 7/12) o ddm (112 -> 11/2)
                 # Ante la ambigüedad, priorizamos ddm si el dia es válido
                 dia = int(resto[0:2])
                 mes = int(resto[2])
                 
        return datetime(anio, mes, dia)
    except:
        return datetime.min

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]👤 ETL: MAESTRO DE EMPLEADOS (MULTIPLE UBICACIÓN - SCD)[/]")

    ruta_carpeta = PATHS["raw_empleados"]

    # --- BLOQUE DE AUDITORÍA VISUAL (OPCIONAL PERO RECOMENDADO) ---
    if os.path.exists(ruta_carpeta):
        archivos = [f for f in os.listdir(ruta_carpeta) if f.endswith('xlsx') or f.endswith('xls')]
        console.print(f"[cyan]ℹ️ Archivos encontrados en origen: {len(archivos)}[/]")
    # -------------------------------------------------------------

    # 1. CARGA MASIVA
    df_consolidado = leer_carpeta(ruta_carpeta)

    if df_consolidado.empty:
        console.print("[red]❌ No se cargaron datos. Verifica la ruta o los archivos.[/]")
        return

    # 2. GENERAR FECHAS 
    console.print("[cyan]Calculando fechas de archivos...[/]")
    df_consolidado['Fecha_Origen'] = df_consolidado['Source.Name'].apply(extraer_fecha_archivo)

    # 3. RENOMBRADO DE COLUMNAS
    df_consolidado.columns = df_consolidado.columns.str.strip().str.title()
    
    mapa_renombre = {
        "Nombre": "Nombre_Completo", 
        "Apellido": "OficinaSae",      # Apellido trae la Oficina SAE
        "Oficina": "OficinaSistema",   # Oficina trae la Oficina Sistema
        "Doc. De Identidad": "Doc_Identidad",
        "Doc. de Identidad": "Doc_Identidad",
        "Cedula": "Doc_Identidad"
    }
    df_consolidado.rename(columns=mapa_renombre, inplace=True)

    if "Nombre_Completo" not in df_consolidado.columns:
        console.print("[red]❌ Error: Falta columna 'Nombre' en los archivos.[/]")
        return

    # 4. LIMPIEZA DE TEXTO
    cols_texto = ["Nombre_Completo", "OficinaSae", "OficinaSistema", "Doc_Identidad"]
    for col in cols_texto:
        if col in df_consolidado.columns:
            df_consolidado[col] = df_consolidado[col].fillna("").astype(str).str.upper().str.strip()

    # 5. FILTRO ANTI-ROBOTS
    filas_antes = len(df_consolidado)
    mask_humanos = ~df_consolidado['Nombre_Completo'].str.contains(PATRON_NO_HUMANO, regex=True, na=False)
    df_humanos = df_consolidado[mask_humanos].copy()
    
    if len(df_humanos) < filas_antes:
        console.print(f"[yellow]🛡️ Se filtraron {filas_antes - len(df_humanos)} registros corporativos/robots.[/]")

    # =========================================================================
    # ⚡ CAMBIO CLAVE: LOGICA SCD (DIMENSIONES CAMBIANTES)
    # =========================================================================

    # 6. GENERAR COLUMNA COMBINADA (ADELANTADO)
    # Creamos la identidad única (Nombre + Oficina) ANTES de borrar duplicados.
    if "OficinaSistema" in df_humanos.columns:
        df_humanos["Combinado"] = (
            df_humanos["Nombre_Completo"] + " " + df_humanos["OficinaSistema"]
        ).str.strip()
    else:
        df_humanos["Combinado"] = df_humanos["Nombre_Completo"]

    # 7. DEDUPLICACIÓN POR 'COMBINADO'
    # Ordenamos: 
    #   1. Combinado (Agrupa por Empleado+Ubicación)
    #   2. Fecha Descendente (Para quedarnos con la versión más reciente de ESA ubicación)
    df_humanos.sort_values(by=["Combinado", "Fecha_Origen"], ascending=[True, False], inplace=True)
    
    # El subset es 'Combinado'. Esto permite que existan:
    # "WILLIAMS CRUZ EL PARRAL" y "WILLIAMS CRUZ TORRE FIBEX" al mismo tiempo.
    df_final = df_humanos.drop_duplicates(subset=["Combinado"], keep='first').copy()

    # =========================================================================

    # 8. SELECCIÓN FINAL
    cols_finales_orden = [
        "Doc_Identidad", 
        "Nombre_Completo", 
        "Combinado", 
        "OficinaSae", 
        "OficinaSistema", 
        "Fecha_Origen",
        "Franquicia Vendedor", 
        "Franquicia Cobrador"
    ]
    cols_existentes = [c for c in cols_finales_orden if c in df_final.columns]
    df_final = df_final[cols_existentes]
    df_final = limpiar_nulos_powerbi(df_final)

    # Reporte de control
    if 'Fecha_Origen' in df_final.columns:
        fechas_ordenadas = df_final['Fecha_Origen'].sort_values(ascending=False)
        fecha_max = fechas_ordenadas.iloc[0].strftime('%d-%m-%Y')
        console.print(f"[cyan]ℹ️ Fecha más reciente procesada: {fecha_max}[/]")

    # Verificación visual rápida (Solo si hay data)
    console.print(f"[cyan]ℹ️ Filas totales (Histórico de ubicaciones): {len(df_final)}[/]")

    # 9. GUARDADO
    console.print(f"[green]✔ Maestro Generado Exitosamente.[/]")
    guardar_parquet(df_final, "Maestro_Empleados_Gold.parquet", filas_iniciales=len(df_consolidado))

if __name__ == "__main__":
    ejecutar()
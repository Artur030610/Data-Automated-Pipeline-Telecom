import pandas as pd
import numpy as np 
import glob
import os
import warnings
import time
import datetime
import calendar
import re
from functools import wraps
from rich.console import Console
from rich.progress import (
    Progress, 
    SpinnerColumn, 
    BarColumn, 
    TextColumn, 
    TimeElapsedColumn
)
from rich.table import Table 
from rich.panel import Panel 
from config import PATHS, THEME_COLOR 

console = Console(theme=THEME_COLOR)
warnings.simplefilter(action='ignore')

# --- DECORADOR CRONÓMETRO ---
def reportar_tiempo(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        console.print(f"[bold yellow]⏱️ Tiempo total bloque: {duration:.2f} segundos[/]\n")
        return result
    return wrapper

# --- LECTURA (MEJORADA CON VALIDACIÓN) ---
def leer_carpeta(ruta_carpeta=None, filtro_exclusion=None, columnas_esperadas=None, dtype=None, archivos_especificos=None):
    """
    Carga Excels de manera flexible (Carpeta completa o Lista específica).
    Incluye validación de columnas faltantes.
    """
    lista_dfs = []
    
    # --- CAMBIO 1: LÓGICA DE SELECCIÓN HÍBRIDA ---
    if archivos_especificos:
        archivos = archivos_especificos
        contexto = "Modo Incremental"
        nombre_carpeta = "Lista Seleccionada"
    elif ruta_carpeta and os.path.exists(ruta_carpeta):
        archivos = glob.glob(os.path.join(ruta_carpeta, "*.xlsx"))
        contexto = "Modo Full"
        nombre_carpeta = os.path.basename(ruta_carpeta)
    else:
        console.print(f"[bold red]❌ Error: Debes enviar 'ruta_carpeta' o 'archivos_especificos'.[/]")
        return pd.DataFrame()

    if not archivos:
        console.print(f"[yellow]⚠️ No hay archivos para procesar en {nombre_carpeta}.[/]")
        return pd.DataFrame()

    console.print(f"[info]📂 {contexto} | Procesando: [bold white]{len(archivos)}[/] archivos[/]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]({task.completed}/{task.total})[/]"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TextColumn("[dim]{task.description}"),
        console=console
    ) as progress:
        
        task = progress.add_task("", total=len(archivos))
        
        for archivo in archivos:
            nombre = os.path.basename(archivo)
            progress.update(task, description=f"📄 {nombre}")
            
            # --- FILTROS DE EXCLUSIÓN ---
            
            # 1. Archivos temporales de Excel (~$Arch.xlsx)
            if nombre.startswith("~") or "$" in nombre: 
                progress.advance(task); continue
            
            # 2. Filtro personalizado que pases como argumento
            if filtro_exclusion and filtro_exclusion in nombre: 
                progress.advance(task); continue
            
            # 3. NUEVO: Ignorar consolidados (Case insensitive) 
            if "consolidado" in nombre.lower():
                progress.advance(task); continue
                
            try:
                # Lectura
                df = pd.read_excel(archivo, engine="calamine", dtype=dtype)
                
                # Sanitizar headers
                df.columns = df.columns.astype(str).str.strip()

                # --- VALIDACIÓN DE COLUMNAS ---
                if columnas_esperadas:
                    columnas_presentes = set(df.columns)
                    columnas_requeridas = set(columnas_esperadas)
                    faltantes = columnas_requeridas - columnas_presentes
                    
                    if faltantes:
                        console.print(f"[yellow]⚠️  Advertencia en {nombre}: Faltan columnas {faltantes}[/]")
                    
                    # Reindex rellena con NaN lo que falte y descarta lo que sobre
                    df = df.reindex(columns=columnas_esperadas)
                
                # Metadata indispensable
                df["Source.Name"] = nombre 
                lista_dfs.append(df)

            except Exception as e:
                console.print(f"[warning]⚠️ Error leyendo {nombre}: {e}[/]")
            
            progress.advance(task)
            
    if not lista_dfs:
        return pd.DataFrame()
        
    return pd.concat(lista_dfs, ignore_index=True)

def ingesta_inteligente(ruta_raw, ruta_gold, col_fecha_corte=None, **kwargs):
    """
    Decide qué archivos leer comparando la fecha máxima del Gold contra 
    las fechas en los NOMBRES de los archivos Raw.
    
    Args:
        ruta_raw: Carpeta de excels.
        ruta_gold: Archivo Parquet histórico.
        col_fecha_corte: Nombre de la columna en el GOLD que usaremos como referencia.
        **kwargs: Argumentos extra para 'leer_carpeta' (ej: filtro_exclusion="Consolidado").
    """
    import os
    import glob
    import pandas as pd
    
    console.rule(f"[bold purple]ANALIZADOR INCREMENTAL (Ref: {col_fecha_corte})[/]")
    
    # 1. Listar archivos RAW
    archivos_raw = glob.glob(os.path.join(ruta_raw, "*.xlsx"))
    df_historico = pd.DataFrame()
    archivos_a_leer = []
    
    # --- ESCENARIO 1: EXISTE GOLD ---
    if os.path.exists(ruta_gold) and col_fecha_corte:
        try:
            # 1. Obtener la "Marca de Agua" (Fecha Máxima)
            # Solo leemos la columna necesaria para ser rápidos
            df_fechas = pd.read_parquet(ruta_gold, columns=[col_fecha_corte])
            
            if not df_fechas.empty:
                # Aseguramos formato fecha
                df_fechas[col_fecha_corte] = pd.to_datetime(df_fechas[col_fecha_corte], errors='coerce')
                
                # LA CLAVE: Fecha más reciente en el sistema
                fecha_max_gold = df_fechas[col_fecha_corte].max()
                
                console.print(f"[green]✅ Última data en Gold ({col_fecha_corte}): {fecha_max_gold.date()}[/]")
                
                # 2. Filtrar Archivos RAW
                # Si el usuario pasó un filtro de exclusión (ej. "Consolidado"), lo aplicamos antes de comparar fechas
                filtro_excl = kwargs.get('filtro_exclusion', None)
                
                with console.status("[bold blue]Comparando fechas...[/]"):
                    for archivo in archivos_raw:
                        # Pre-filtro: Si el archivo debe excluirse por nombre, lo saltamos ya
                        if filtro_excl and filtro_excl in os.path.basename(archivo):
                            continue

                        # Análisis de Fecha en el nombre
                        inicio, fin, etiqueta = obtener_rango_fechas(archivo)
                        
                        if fin:
                            # Si la fecha del archivo es POSTERIOR a lo que ya tengo -> LEER
                            if fin > fecha_max_gold:
                                archivos_a_leer.append(archivo)
                        else:
                            # Si no tiene fecha en el nombre, ante la duda, LEER
                            archivos_a_leer.append(archivo)

                # Si encontramos archivos nuevos, cargamos el histórico completo para unirlo luego
                if archivos_a_leer:
                     # Leemos el histórico completo
                     df_historico = pd.read_parquet(ruta_gold)

            else:
                console.print("[yellow]⚠️ Gold vacío (sin filas). Carga FULL.[/]")
                archivos_a_leer = archivos_raw

        except Exception as e:
            console.print(f"[red]❌ Error leyendo columna '{col_fecha_corte}' en Gold: {e}[/]")
            console.print("[yellow]⚡ Se forzará carga FULL.[/]")
            archivos_a_leer = archivos_raw
            df_historico = pd.DataFrame()
            
    # --- ESCENARIO 2: NO EXISTE GOLD ---
    else:
        console.print("[bold blue]ℹ️ Carga Inicial (FULL).[/]")
        archivos_a_leer = archivos_raw

    # --- EJECUCIÓN ---
    if archivos_a_leer:
        console.print(f"[bold cyan]🚀 Procesando {len(archivos_a_leer)} archivos nuevos...[/]")
        
        # AQUÍ ESTÁ LA MAGIA: Pasamos **kwargs a leer_carpeta
        # Esto permite que 'filtro_exclusion', 'columnas_esperadas', etc., funcionen
        df_nuevo = leer_carpeta(archivos_especificos=archivos_a_leer, **kwargs)
    else:
        console.print("[bold green]✨ Sistema actualizado. No hay archivos nuevos.[/]")
        df_nuevo = pd.DataFrame()

    return df_nuevo, df_historico

# --- LIMPIEZA DE NULOS (PODEROSA) ---
def limpiar_nulos_powerbi(df):
    """
    Limpia un DataFrame para Power BI.
    Elimina espacios en blanco, strings vacíos y textos 'nan'.
    """
    df_clean = df.copy()
    
    # Seleccionamos columnas de tipo objeto
    cols_texto = df_clean.select_dtypes(include=['object']).columns
    
    # --- CORRECCIÓN 2: LIMPIEZA PROFUNDA CON REGEX ---
    # 1. Convertir espacios (' '), vacíos ('') y tabs a NaN real
    df_clean[cols_texto] = df_clean[cols_texto].replace(r'^\s*$', np.nan, regex=True)
    
    # 2. Convertir textos literales "nan", "None", "null" a NaN real
    valores_basura = ['nan', 'NaN', 'NAN', 'None', 'null', 'Null']
    df_clean[cols_texto] = df_clean[cols_texto].replace(valores_basura, np.nan)

    return df_clean

def limpiar_ids_documentos(df, columnas):
    """
    Normaliza columnas de identificación (Cédulas, Contratos, RIFs).
    1. Convierte a string.
    2. Elimina el sufijo decimal '.0' (típico de Excel).
    3. Elimina los puntos de miles '31.456.789' -> '31456789'.
    4. Convierte 'nan' o vacíos a None real.
    """
    if df.empty: return df
    
    # Trabajamos sobre una copia para evitar SettingWithCopyWarning si viene de un slice
    df_out = df.copy()

    for col in columnas:
        if col not in df_out.columns:
            continue
            
        # A. Convertir a string y quitar espacios
        df_out[col] = df_out[col].astype(str).str.strip()

        # B. Quitar '.0' al final (4473825.0 -> 4473825)
        df_out[col] = df_out[col].str.replace(r'\.0$', '', regex=True)

        # C. Quitar puntos de miles (31.743.084 -> 31743084)
        # OJO: Solo usar en IDs que NO deban tener puntos (no usar en IPs o Emails)
        df_out[col] = df_out[col].str.replace('.', '', regex=False)

        # D. Limpieza de basura ("nan", "None", vacíos)
        df_out[col] = df_out[col].replace({'nan': None, 'None': None, '': None})
        
    return df_out

# --- GUARDADO (SILVER/GOLD) ---
def guardar_parquet(df, nombre_archivo, filas_iniciales=None, ruta_destino=None):
    """
    Guarda el archivo en Parquet asegurando compatibilidad con Power BI.
    Incluye lógica 'Anti-Lock' para evitar errores de archivo abierto en Windows.
    """
    if df.empty:
        console.print(f"[warning]⚠️ Dataset vacío para {nombre_archivo}. Omitido.[/]")
        return

    # --- LÓGICA DE RUTAS ---
    if ruta_destino:
        carpeta_salida = ruta_destino
        # Si la ruta_destino incluye el nombre del archivo, extraemos solo la carpeta
        if nombre_archivo in ruta_destino:
            carpeta_salida = os.path.dirname(ruta_destino)
            ruta_salida = ruta_destino
        else:
            ruta_salida = os.path.join(carpeta_salida, nombre_archivo)
    else:
        # Comportamiento por defecto: Ir a Gold
        carpeta_salida = PATHS.get("gold", "data/gold")
        ruta_salida = os.path.join(carpeta_salida, nombre_archivo)

    # 2. Creamos la carpeta si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    try:
        # --- FIX: ELIMINACIÓN PREVIA (EVITA WINERROR 183 y PERMISSION ERROR) ---
        if os.path.exists(ruta_salida):
            try:
                os.remove(ruta_salida)
                # console.print(f"[dim]🗑️  Archivo previo eliminado: {nombre_archivo}[/]")
            except PermissionError:
                console.print(Panel(
                    f"[bold red]❌ ERROR CRÍTICO: ARCHIVO BLOQUEADO[/]\n"
                    f"El archivo [cyan]{nombre_archivo}[/] está abierto en otro programa (Power BI/Excel).\n"
                    f"⚠️  Ciérralo e intenta de nuevo.",
                    title="ACCESO DENEGADO",
                    style="red"
                ))
                raise # Re-lanzamos el error para detener el script si es necesario
            except Exception as e:
                console.print(f"[red]⚠️ Advertencia: No se pudo eliminar el archivo anterior. Razón: {e}[/]")

        # --- LIMPIEZA PARA POWER BI ---
        # Convierte objetos a string para evitar conflictos en PBI
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x != "" else None)
            
        df.to_parquet(ruta_salida, index=False)
        
        # --- REPORTE VISUAL ---
        filas_finales = len(df)
        
        if filas_iniciales is not None:
            filas_eliminadas = filas_iniciales - filas_finales
            
            grid = Table.grid(padding=(0, 2))
            grid.add_column(justify="left", style="cyan")
            grid.add_column(justify="left", style="bold white")
            
            grid.add_row("📥 Filas Leídas:", f"{filas_iniciales:,}")
            grid.add_row("🧹 Filas Eliminadas:", f"[red]- {filas_eliminadas:,}[/]")
            grid.add_row("💾 Filas Guardadas:", f"[green]{filas_finales:,}[/]")
            
            # Detectamos visualmente dónde cayó para el log
            tipo = "CUSTOM"
            if PATHS.get("silver") and PATHS.get("silver") in ruta_salida: tipo = "SILVER"
            if PATHS.get("gold") and PATHS.get("gold") in ruta_salida: tipo = "GOLD"
            
            console.print(grid)
            console.print(f"[bold green]✅ ARCHIVO {tipo} GENERADO: {nombre_archivo}[/]")
            console.print(f"   📂 Ruta: {ruta_salida}")
            
        else:
            console.print(f"[bold green]✅ GUARDADO: {nombre_archivo} ({filas_finales:,} filas)[/]")
            
    except Exception as e:
        console.print(f"[bold red]❌ FALLO GUARDANDO {nombre_archivo}: {e}[/]")
        raise # Importante hacer raise para que el script principal sepa que falló

def tiempo(tiempo_inicio):
    """
    Calcula el tiempo total desde tiempo_inicio hasta ahora.
    """
    tiempo_fin = time.time()
    total_segundos = tiempo_fin - tiempo_inicio
    
    minutos, segundos = divmod(total_segundos, 60)
    
    console.print("\n")
    console.print(Panel(
        f"[bold white] FIN DE EJECUCIÓN [/]\n"
        f"[yellow] Tiempo Total de la Suite:[/][bold green] {int(minutos)} min {int(segundos)} seg[/]",
        title="RESUMEN GLOBAL",
        style="bold blue",
        expand=False
    ))

def obtener_rango_fechas(nombre_archivo):
    """
    Parsea archivos con formato: 'Data - IdF 1-12-2025 al 15-1-2026.xlsx'
    Retorna: FechaInicio, FechaFin, Etiqueta (Ej: ENE 2026 Q1)
    """
    nombre_limpio = os.path.basename(nombre_archivo).lower()
    
    # Expresión regular para capturar: dia-mes-año al dia-mes-año
    patron = r"(\d{1,2}-\d{1,2}-\d{4})\s+al\s+(\d{1,2}-\d{1,2}-\d{4})"
    
    match = re.search(patron, nombre_limpio)
    
    if not match:
        return None, None, None

    try:
        str_inicio, str_fin = match.groups()
        
        fecha_inicio = datetime.datetime.strptime(str_inicio, "%d-%m-%Y")
        fecha_fin = datetime.datetime.strptime(str_fin, "%d-%m-%Y")

        # --- Generar Etiqueta para Power BI ---
        quincena_str = "Q1" if fecha_fin.day <= 15 else "Q2"
        
        meses = ["", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
                 "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]
        
        nombre_etiqueta = f"{meses[fecha_fin.month]} {fecha_fin.year} {quincena_str}"
        
        return fecha_inicio, fecha_fin, nombre_etiqueta

    except Exception as e:
        print(f"Error parseando fechas en {nombre_archivo}: {e}")
        return None, None, None
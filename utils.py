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
def leer_carpeta(ruta_carpeta, filtro_exclusion=None, columnas_esperadas=None, dtype=None):
    """
    Carga Excels de una carpeta usando Calamine.
    - Valida si faltan columnas antes de reindexar.
    """
    if not os.path.exists(ruta_carpeta):
        console.print(f"[bold red]❌ La carpeta no existe: {ruta_carpeta}[/]")
        return pd.DataFrame()

    archivos = glob.glob(os.path.join(ruta_carpeta, "*.xlsx"))
    lista_dfs = []
    nombre_carpeta = os.path.basename(ruta_carpeta)
    
    console.print(f"[info]📂 Carpeta: {nombre_carpeta} | Archivos encontrados: [bold white]{len(archivos)}[/][/]")

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
            
            # Filtros de archivos temporales
            if nombre.startswith("~") or "$" in nombre: 
                progress.advance(task)
                continue
            if filtro_exclusion and filtro_exclusion in nombre: 
                progress.advance(task)
                continue
                
            try:
                # Lectura rápida con calamine
                df = pd.read_excel(
                    archivo, 
                    engine="calamine",
                    dtype=dtype 
                )
                
                # Sanitizar cabeceras (eliminar espacios en nombres de columnas)
                df.columns = df.columns.astype(str).str.strip()

                if columnas_esperadas:
                    # --- CORRECCIÓN 1: VALIDACIÓN DE COLUMNAS ---
                    columnas_presentes = set(df.columns)
                    columnas_requeridas = set(columnas_esperadas)
                    faltantes = columnas_requeridas - columnas_presentes
                    
                    if faltantes:
                        console.print(f"[yellow]⚠️  Advertencia en {nombre}: Faltan columnas {faltantes}[/]")
                    
                    # Reindex rellena con NaN lo que falte y descarta lo que sobre
                    df = df.reindex(columns=columnas_esperadas)
                
                df["Source.Name"] = nombre 
                lista_dfs.append(df)
            except Exception as e:
                console.print(f"[warning]⚠️ Error leyendo {nombre}: {e}[/]")
            
            progress.advance(task)
            
    if not lista_dfs:
        return pd.DataFrame()
        
    return pd.concat(lista_dfs, ignore_index=True)

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

# --- GUARDADO (SILVER/GOLD) ---
def guardar_parquet(df, nombre_archivo, filas_iniciales=None, ruta_destino=None):
    """
    Guarda el archivo en Parquet asegurando compatibilidad con Power BI.
    
    Argumentos:
        ruta_destino (str, opcional): Carpeta donde guardar. 
                                      Si es None, usa PATHS["gold"] por defecto.
    """
    if df.empty:
        console.print(f"[warning]⚠️ Dataset vacío para {nombre_archivo}. Omitido.[/]")
        return

    # --- LÓGICA DE RUTAS (CORREGIDA) ---
    # 1. Decidimos la CARPETA destino
    if ruta_destino:
        carpeta_salida = ruta_destino
    else:
        # Comportamiento por defecto: Ir a Gold
        carpeta_salida = PATHS.get("gold", "data/gold") # Usa un fallback si no existe la key

    # 2. Creamos la carpeta si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # 3. Construimos la ruta completa (Carpeta + Nombre Archivo)
    ruta_salida = os.path.join(carpeta_salida, nombre_archivo)
    
    try:
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
            
            console.print(grid)
            
            # Detectamos visualmente dónde cayó para el log
            tipo = "CUSTOM"
            if ruta_destino == PATHS.get("silver"): tipo = "SILVER"
            if ruta_destino == PATHS.get("gold") or ruta_destino is None: tipo = "GOLD"
            
            console.print(f"[bold green]✅ ARCHIVO {tipo} GENERADO: {nombre_archivo}[/]")
            console.print(f"   📂 Ruta: {ruta_salida}")
            
        else:
            console.print(f"[bold green]✅ GUARDADO: {nombre_archivo} ({filas_finales:,} filas)[/]")
            
    except Exception as e:
        console.print(f"[bold red]❌ FALLO GUARDANDO {nombre_archivo}: {e}[/]")
        
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
    # \d{1,2} significa 1 o 2 dígitos (ej: 1 o 15)
    # \d{4} significa 4 dígitos (ej: 2025)
    patron = r"(\d{1,2}-\d{1,2}-\d{4})\s+al\s+(\d{1,2}-\d{1,2}-\d{4})"
    
    match = re.search(patron, nombre_limpio)
    
    if not match:
        return None, None, None

    try:
        # Extraemos las cadenas de texto detectadas
        str_inicio, str_fin = match.groups()
        
        # Convertimos a objetos datetime (El formato es dia-mes-año)
        fecha_inicio = datetime.datetime.strptime(str_inicio, "%d-%m-%Y")
        fecha_fin = datetime.datetime.strptime(str_fin, "%d-%m-%Y")

        # --- Generar Etiqueta para Power BI ---
        # Lógica: Si termina el 15 o antes -> Q1. Si termina después -> Q2.
        quincena_str = "Q1" if fecha_fin.day <= 15 else "Q2"
        
        meses = ["", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
                 "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]
        
        nombre_etiqueta = f"{meses[fecha_fin.month]} {fecha_fin.year} {quincena_str}"
        
        return fecha_inicio, fecha_fin, nombre_etiqueta

    except Exception as e:
        print(f"Error parseando fechas en {nombre_archivo}: {e}")
        return None, None, None
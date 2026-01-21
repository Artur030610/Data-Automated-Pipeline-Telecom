import pandas as pd
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

# --- LECTURA ---
def leer_carpeta(ruta_carpeta, filtro_exclusion=None, columnas_esperadas=None):
    if not os.path.exists(ruta_carpeta):
        console.print(f"[error]❌ La carpeta no existe: {ruta_carpeta}[/]")
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
            
            if nombre.startswith("~") or "$" in nombre: 
                progress.advance(task)
                continue
            if filtro_exclusion and filtro_exclusion in nombre: 
                progress.advance(task)
                continue
                
            try:
                # Usamos calamine para velocidad
                df = pd.read_excel(archivo, engine="calamine")
                
                # Sanitizar cabeceras
                df.columns = df.columns.astype(str).str.strip()

                if columnas_esperadas:
                    df = df.reindex(columns=columnas_esperadas)
                
                df["Source.Name"] = nombre 
                lista_dfs.append(df)
            except Exception as e:
                console.print(f"[warning]⚠️ Error leyendo {nombre}: {e}[/]")
            
            progress.advance(task)
            
    if not lista_dfs:
        return pd.DataFrame()
        
    return pd.concat(lista_dfs, ignore_index=True)

# --- GUARDADO FLEXIBLE (SILVER/GOLD) ---
def guardar_parquet(df, nombre_archivo, filas_iniciales=None, ruta_destino=None):
    """
    Guarda el archivo y muestra estadísticas de limpieza.
    - Si ruta_destino es None: Guarda en carpeta GOLD (Comportamiento default).
    - Si ruta_destino tiene valor: Guarda en esa ruta específica (Para Silver).
    """
    if df.empty:
        console.print(f"[warning]⚠️ Dataset vacío para {nombre_archivo}. Omitido.[/]")
        return

    # LÓGICA DE RUTAS
    if ruta_destino:
        # Modo Personalizado (ej: Silver)
        ruta_salida = ruta_destino
        # Aseguramos que la carpeta exista (por si es la primera vez que se crea silver_data)
        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    else:
        # Modo Default (Gold) - Compatible con scripts antiguos
        os.makedirs(PATHS["gold"], exist_ok=True)
        ruta_salida = os.path.join(PATHS["gold"], nombre_archivo)
    
    try:
        # Sanitización de objetos a string para Parquet
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str)
            
        df.to_parquet(ruta_salida, index=False)
        
        # --- REPORTE VISUAL DE LIMPIEZA ---
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
            
            # Mensaje diferenciado según destino
            tipo_archivo = "SILVER" if "silver" in ruta_salida.lower() else "GOLD"
            console.print(f"[success]✅ ARCHIVO {tipo_archivo} GENERADO: {nombre_archivo}[/]")
            
        else:
            console.print(f"[success]✅ GUARDADO: {nombre_archivo} -> {filas_finales:,} filas.[/]")
            
    except Exception as e:
        console.print(f"[error]❌ FALLO GUARDANDO {nombre_archivo}: {e}[/]")
    
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

def obtener_rango_fechas(nombre_archivo, anio_base=2025):
    """
    Extrae FechaInicio y FechaFin basado en el nombre del archivo (ej: 'IDF ENE Q1').
    Replica la lógica de Power Query para asignar quincenas.
    
    Args:
        nombre_archivo (str): Nombre del archivo (ej: "Reporte IDF ENE Q1.xlsx")
        anio_base (int): Año a evaluar (por defecto 2025, según lógica original)
        
    Returns:
        tuple: (fecha_inicio, fecha_fin, nombre_quincena_formateado)
               Retorna (None, None, None) si no detecta el patrón.
    """
    nombre_lower = nombre_archivo.lower()

    # Diccionario de meses (Español e Inglés para robustez)
    mapa_meses = {
        'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
        'jan': 1, 'apr': 4, 'aug': 8, 'dec': 12 
    }

    # 1. Detectar Mes
    mes_num = None
    for key, val in mapa_meses.items():
        if key in nombre_lower:
            mes_num = val
            break
    
    # 2. Detectar Quincena
    quincena = None
    if "q1" in nombre_lower:
        quincena = "q1"
    elif "q2" in nombre_lower:
        quincena = "q2"

    if not mes_num or not quincena:
        return None, None, None # Archivo no cumple patrón

    # 3. Calcular Fechas (Lógica de Negocio)
    try:
        if quincena == "q1":
            # Regla: Q1 evalúa del día 1 del mes ANTERIOR al 15 del mes ACTUAL
            # Si es Enero (1), el mes anterior es Diciembre (12) del año pasado
            mes_inicio = mes_num - 1
            anio_inicio = anio_base
            if mes_inicio == 0: 
                mes_inicio = 12
                anio_inicio -= 1
            
            fecha_inicio = datetime.datetime(anio_inicio, mes_inicio, 1)
            fecha_fin = datetime.datetime(anio_base, mes_num, 15)

        else: # q2
            # Regla: Q2 evalúa del día 15 del mes ANTERIOR al FIN del mes ACTUAL
            mes_inicio = mes_num - 1
            anio_inicio = anio_base
            if mes_inicio == 0:
                mes_inicio = 12
                anio_inicio -= 1

            fecha_inicio = datetime.datetime(anio_inicio, mes_inicio, 15)
            
            # Último día del mes actual (ej: 28, 30 o 31)
            ultimo_dia_mes = calendar.monthrange(anio_base, mes_num)[1]
            fecha_fin = datetime.datetime(anio_base, mes_num, ultimo_dia_mes)
            
        # Formatear nombre para reporte (ej: "ENE Q1")
        # Buscamos el nombre del mes en texto (key) basado en el número
        nombre_mes_str = [k for k, v in mapa_meses.items() if v == mes_num][0].upper()
        nombre_quincena = f"{nombre_mes_str} {quincena.upper()}"

        return fecha_inicio, fecha_fin, nombre_quincena

    except Exception as e:
        console.print(f"[red]❌ Error calculando fechas para {nombre_archivo}: {e}[/]")
        return None, None, None
    
def limpiar_nulos_powerbi(df):
    """
    Limpia un DataFrame para que los valores nulos sean interpretados 
    correctamente como BLANK en Power BI (vía Parquet).
    
    1. Reemplaza 'nan' (string) por None.
    2. Reemplaza np.nan (float) por None en columnas de objeto.
    3. Reemplaza NaT (tiempo) por None.
    """
    # Hacemos una copia para no modificar el original inesperadamente
    df_clean = df.copy()

    # 1. Identificar columnas de texto (object)
    # En pandas, las columnas mixtas o de texto son 'object'
    cols_texto = df_clean.select_dtypes(include=['object']).columns

    # 2. Reemplazo masivo en columnas de texto
    # Reemplazamos la cadena literal "nan" y el valor nan de numpy por None
    # None es lo único que PyArrow (Parquet) traduce a NULL real
    df_clean[cols_texto] = df_clean[cols_texto].replace(
        to_replace=['nan', 'NaN', 'NAN', np.nan], 
        value=None
    )

    # 3. Limpieza de Fechas (NaT -> None)
    cols_fecha = df_clean.select_dtypes(include=['datetime', 'datetime64[ns]']).columns
    # A veces es necesario convertir a object para que acepte None si falla
    # Pero usualmente Parquet maneja NaT bien. 
    # Sin embargo, para asegurar compatibilidad total:
    for col in cols_fecha:
        df_clean[col] = df_clean[col].replace({pd.NaT: None})

    return df_clean
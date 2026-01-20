import pandas as pd
import glob
import os
import warnings
import time
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
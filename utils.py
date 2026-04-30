#En utils.py se encuentran todas las utilidades, es decir funciones modulares que se reutilizan a fin de optimizar las transformaciones. 
#La intención es cumplir con los principios de no repetición, modularidad y claridad. de igual forma se busca que las funciones tengan una 
#única responsabilidad, es decir que cada función cumpla con una tarea específica y bien definida. 
# En cada función se encontrara una descripción detallada de su propósito, sus parámetros de entrada y su valor de retorno.

import pandas as pd
import numpy as np 
import glob
import os
import warnings
import time
import datetime
import re
import shutil
import duckdb
import threading
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
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import polars as pl
import pyarrow as pa

def get_ram_usage_str():
    """Retorna el uso actual de RAM del proceso de Python en MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / (1024 * 1024)
        return f"{mem_mb:.1f} MB"
    except ImportError:
        return "N/A"

def get_ram_usage_mb():
    """Retorna el uso actual de RAM en MB como valor numérico."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    except (ImportError, Exception):
        return None
    
def monitorear_promedio_ram(detener_event, muestras):
    """Función para el hilo que recolecta muestras de RAM."""
    while not detener_event.is_set():
        uso = get_ram_usage_mb()
        if uso is not None:
            muestras.append(uso)
        time.sleep(0.2) # Muestreo cada 200ms
    
console = Console(theme=THEME_COLOR)
warnings.simplefilter(action='ignore')

def liberar_ram_os():
    """
    Fuerza a Python y al Pool de Memoria de C++ (PyArrow) 
    a devolver la RAM retenida al Sistema Operativo instantáneamente a fin de 
    evitar Out-Of-Memory.
    """
    import gc
    gc.collect()
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    
# --- CONFIGURACIÓN DEL MOTOR DE LOGS ---
# 1. Definir ruta relativa al archivo actual
LOG_PATH = Path(__file__).parent / "logs" / "fibex_pipeline_audit.log"
LOG_PATH.parent.mkdir(exist_ok=True)

# 2. Configurar el "Handler" (Rotación de archivos: 1MB y guarda 3 respaldos)
handler = RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=3, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# 3. Crear el objeto Logger único para Fibex
logger = logging.getLogger("FibexAudit")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# 4. Crear el objeto Logger para la Extracción (Scrapers)
LOG_PATH_EXT = Path(__file__).parent / "logs" / "extraccion_logs.txt"
handler_ext = RotatingFileHandler(LOG_PATH_EXT, maxBytes=1_000_000, backupCount=3, encoding='utf-8')
handler_ext.setFormatter(formatter)
logger_extraccion = logging.getLogger("FibexExtraccion")
logger_extraccion.addHandler(handler_ext)
logger_extraccion.setLevel(logging.INFO)

def audit_performance(func):
    """Registra inicio, fin, registros y el consumo 
    promedio de RAM, es utilizada para la evaluación de desempeño
    de los flujos, asi como para el registro en los logs."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.time()
        filas = f"{args[0].shape[0]:,}" if args and hasattr(args[0], 'shape') else "N/A"
        
        # --- Monitoreo ---
        muestras_ram = []
        detener_monitor = threading.Event()
        hilo = threading.Thread(target=monitorear_promedio_ram, args=(detener_monitor, muestras_ram), daemon=True)
        hilo.start()
        
        ram_inicio = f"{get_ram_usage_mb():.1f} MB" if get_ram_usage_mb() else "N/A"
        logger.info(f"INICIO: [{func.__name__}] | Registros: {filas} | RAM: {ram_inicio}")
        
        try:
            resultado = func(*args, **kwargs)
            detener_monitor.set()
            hilo.join(timeout=1)
            
            duracion = time.time() - inicio
            promedio = sum(muestras_ram) / len(muestras_ram) if muestras_ram else 0
            ram_avg_str = f"{promedio:.1f} MB (Media)"
            
            logger.info(f"EXITO:  [{func.__name__}] | Duración: {duracion:.2f}s | RAM: {ram_avg_str}")
            return resultado
        except Exception as e:
            detener_monitor.set()
            logger.error(f"FALLO:  [{func.__name__}] | Error: {str(e)}", exc_info=True)
            raise e 
    return wrapper

# La intención de esta función es la implementación del star schema en el modelo de Power BI
# Se asigna un Cliente_SK el cual sera un numero entero a fin de funcionar como llave principal en Dim_Cliente y llave foranea en las tablas de hechos
# lo que permite disminuir la cardinalidad del modelo. 
# Con DaxStudio (Software gratuito) se puede comprobar la cardinalidad de las columnas del modelo. 
# El resultado sugiere una reducción en 'N° de abonado' sustituyendo esta clave por un entero a traves de llaves subrogadas.

def asignar_cliente_sk(df, col_abonado="N° Abonado"): 
    """
    Enriquece una tabla de hechos agregando la clave subrogada entera 'Cliente_SK' 
    desde la Dimensión Cliente. Conserva el tipo de DataFrame original (Pandas o Polars).
    Asigna -1 a los registros que no crucen para mantener la integridad referencial.
    """
    ruta_dim = os.path.join(PATHS.get("gold", "data/gold"), "Dim_Cliente.parquet").replace("\\", "/")
    
    if not os.path.exists(ruta_dim):
        console.print("[yellow]⚠️ Dim_Cliente.parquet no existe. Se asignará Cliente_SK = -1 por defecto.[/]")
        if isinstance(df, pd.DataFrame):
            df["Cliente_SK"] = -1
        else:
            df = df.with_columns(pl.lit(-1).alias("Cliente_SK"))
        return df
        
    console.print("[dim]🔗 Cruzando con Dim_Cliente para obtener Cliente_SK (ID Entero)...[/]")
    
    con = duckdb.connect(database=':memory:')
    
    # Manejo transparente de LazyFrames de Polars
    is_lazy = False
    if hasattr(df, "collect"):
        df = df.collect()
        is_lazy = True
        
    con.register('df_hechos', df)
    
    query = f"""
        SELECT 
            COALESCE(d.Cliente_SK, -1) AS Cliente_SK,
            h.*
        FROM df_hechos h
        LEFT JOIN read_parquet('{ruta_dim}') d
          ON CAST(h."{col_abonado}" AS VARCHAR) = CAST(d."N° Abonado" AS VARCHAR)
    """
    
    try:
        if isinstance(df, pd.DataFrame):
            df_res = con.execute(query).df()
        else:
            df_res = con.execute(query).pl()
            if is_lazy: df_res = df_res.lazy()
        return df_res
    except Exception as e:
        console.print(f"[bold red]❌ Error al asignar Cliente_SK: {e}[/]")
        return df
    finally:
        con.close()

def reportar_tiempo(func):
    """Registra tiempos detallados, selección de logger y el promedio de consumo de RAM."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        is_scraper = 'scraper' in func.__module__ or 'descargar' in func.__name__
        active_logger = logger_extraccion if is_scraper else logger
        
        # --- Monitoreo ---
        muestras_ram = []
        detener_monitor = threading.Event()
        hilo = threading.Thread(target=monitorear_promedio_ram, args=(detener_monitor, muestras_ram), daemon=True)
        hilo.start()

        ram_inicio = f"{get_ram_usage_mb():.1f} MB" if get_ram_usage_mb() else "N/A"
        active_logger.info(f"INICIO: [{func.__name__}] | RAM: {ram_inicio}")
        
        try:
            result = func(*args, **kwargs)
            detener_monitor.set()
            hilo.join(timeout=1)
            
            duration = time.time() - start_time
            minutos, segundos = divmod(duration, 60)
            promedio = sum(muestras_ram) / len(muestras_ram) if muestras_ram else 0
            ram_avg_str = f"{promedio:.1f} MB (Media)"

            console.print(f"[bold yellow]⏱️ Tiempo total bloque: {minutos:.0f} min y {segundos:.2f} seg | RAM Media: {ram_avg_str}[/]\n")
            active_logger.info(f"EXITO: [{func.__name__}] | Duración: {duration:.2f}s | RAM: {ram_avg_str}")
            return result
        except Exception as e:
            detener_monitor.set()
            duration = time.time() - start_time
            # Calculamos promedio hasta el fallo
            promedio = sum(muestras_ram) / len(muestras_ram) if muestras_ram else 0
            ram_avg_str = f"{promedio:.1f} MB (Media)"
            
            console.print(f"[bold red] FALLO CRÍTICO: {func.__name__} |  RAM Media: {ram_avg_str}[/]\n")
            active_logger.error(f"FALLO CRITICO: [{func.__name__}] | Duración: {duration:.2f}s | RAM: {ram_avg_str} | Error: {str(e)}", exc_info=True)
            raise e
        finally:
            liberar_ram_os()
    return wrapper

@audit_performance
def archivos_raw(ruta_raw, ruta_destino_parquet):
    """
    Extrae Excels crudos y los consolida en un único Parquet (Capa Bronze).
    Usa Polars y Calamine para máximo rendimiento. No altera los datos. Busca
    reproducir la fuente de la verdad tal cual está en los Excels. 
    Esto permite mantener un historico, al tiempo que se disminuye el computo necesario 
    para realizar los procesos de transformación posteriores.
    """
    archivos = glob.glob(os.path.join(ruta_raw, "*.xlsx"))
    archivos_validos = [
        f for f in archivos 
        if not os.path.basename(f).startswith("~$")
    ]
    
    if not archivos_validos:
        if not archivos:
            console.print(f"[yellow]⚠️ La carpeta {ruta_raw} está totalmente vacía.[/]")
        else:
            console.print(f"[yellow]⚠️ No hay archivos válidos en {ruta_raw} (solo hay temporales abiertos).[/]")
        return False
        
    console.print(f"[bold cyan]🥉 Iniciando consolidación Bronze (RAW) de {len(archivos)} archivos con Polars...[/]")
    
    dfs = []
    for archivo in archivos_validos:
        nombre = os.path.basename(archivo)
        try:
            # infer_schema_length=0 lee todo como texto para evitar que el script 
            # colapse si un Excel trae un número y otro trae un texto en la misma columna.
            df = pl.read_excel(archivo, engine="calamine", infer_schema_length=0)
            
            # Agregamos la metadata del origen (siempre es buena práctica)
            df = df.with_columns(pl.lit(nombre).alias("Source.Name"))
            
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error leyendo {nombre} en consolidación Bronze: {e}")
            console.print(f"[warning]⚠️ Error leyendo {nombre}: {e}[/]")

    if not dfs:
        return False

    try:
        # how="diagonal" une los archivos aunque uno tenga una columna extra que el otro no
        df_bronze = pl.concat(dfs, how="diagonal")
        
        dfs.clear()
        import gc; gc.collect()
        
        # Asegurar que la ruta exista
        os.makedirs(os.path.dirname(ruta_destino_parquet), exist_ok=True)
        
        # Guardar comprimido
        df_bronze.write_parquet(ruta_destino_parquet, compression="snappy")
        
        filas = df_bronze.height
        nombre_salida = os.path.basename(ruta_destino_parquet)
        
        # Usamos tu formato exacto de logs
        logger.info(f"DATA_QUALITY | BRONZE: {nombre_salida} | Guardadas: {filas:,}")
        console.print(f"[bold green]✅ ARCHIVO BRONZE GENERADO: {nombre_salida} ({filas:,} filas)[/]")
        
        del df_bronze
        if 'df' in locals(): del df
        liberar_ram_os()
        
        return True
        
    except Exception as e:
        logger.error(f"FALLO CRÍTICO CONCATENANDO BRONZE: {str(e)}", exc_info=True)
        console.print(f"[bold red]❌ FALLO CRÍTICO en Bronze: {e}[/]")
        raise

# Función principal, utilizada en cada uno de los scripts de transformación, 
# la cual se encarga de realizar la ingesta incremental utilizando Polars y DuckDB.
@audit_performance
def ingesta_incremental_polars(ruta_raw, ruta_bronze_historico, columna_fecha=None):
    """
    Ingesta Incremental (Upsert / Drop & Replace) usando Polars y DuckDB:
    1. Lee los Excels nuevos con Polars (calamine) a máxima velocidad.
    2. Descarga a disco como Parquets temporales, liberando la RAM archivo por archivo.
    3. Usa DuckDB para unificar el histórico con lo nuevo (UNION ALL BY NAME + DISTINCT), 
       cruzando Gigabytes de datos minimizando el consumo de RAM (Out-of-Core directo a disco).
    """
    ref_titulo = columna_fecha if columna_fecha else "Append / Unique"
    console.rule(f"[bold purple]⚡ INGESTA INCREMENTAL (Ref: {ref_titulo})[/]")
    
    archivos_raw = glob.glob(os.path.join(ruta_raw, "*.xlsx"))
    archivos_validos = [f for f in archivos_raw if not os.path.basename(f).startswith("~$")]
    
    if not archivos_validos:
        console.print("[yellow]⚠️ No hay archivos RAW nuevos para procesar en esta ruta.[/]")
        return False

    # --- DIRECTORIO TEMPORAL DINÁMICO ---
    nombre_base_bronze = os.path.basename(ruta_bronze_historico).replace(".parquet", "")
    temp_parts_path = os.path.join(ruta_raw, f"_temp_{nombre_base_bronze}")
    
    if os.path.exists(temp_parts_path): 
        shutil.rmtree(temp_parts_path)
    os.makedirs(temp_parts_path, exist_ok=True)
    
    hubo_archivos_procesados = False
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]({task.completed}/{task.total})[/]"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TextColumn("[dim]{task.description}"),
        console=console
    ) as progress:
        
        task = progress.add_task("", total=len(archivos_validos))
        
        for i, archivo in enumerate(archivos_validos):
            nombre = os.path.basename(archivo)
            progress.update(task, description=f"📄 Procesando a Disco: {nombre}")
            
            try:
                # 1. Lectura inicial ultra ligera (Todo a texto para evitar choques)
                df = pl.read_excel(archivo, engine="calamine", infer_schema_length=0)
                
                # 2. TRACTOR DE FECHAS
                if columna_fecha and columna_fecha in df.columns:
                    df = df.with_columns(
                        pl.coalesce([
                            pl.col(columna_fecha).str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S", strict=False),
                            pl.col(columna_fecha).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                            pl.col(columna_fecha).str.strptime(pl.Date, "%d/%m/%Y %H:%M:%S", strict=False),
                            pl.col(columna_fecha).str.strptime(pl.Date, "%d/%m/%Y", strict=False),
                            pl.col(columna_fecha).str.strptime(pl.Date, "%d-%m-%Y", strict=False)
                        ]).alias(columna_fecha)
                    )
                
                # 3. CAPTURA DE METADATA PARA CDC
                mtime_ts = os.path.getmtime(archivo)
                fecha_modificacion = datetime.datetime.fromtimestamp(mtime_ts)
                
                df = df.with_columns([
                    pl.lit(nombre).alias("Source.Name"),
                    pl.lit(fecha_modificacion).alias("Fecha_Modificacion_Archivo")
                ])

                # 4. DESCARGA A DISCO INMEDIATA
                path_part = os.path.join(temp_parts_path, f"part_{i}.parquet")
                df.write_parquet(path_part, compression="snappy")
                
                del df  # Elimina el dataframe de la memoria inmediatamente
                liberar_ram_os() # CRÍTICO: Forzamos liberación de caché de C++ (Rust Arrow)
                
                hubo_archivos_procesados = True
                
            except Exception as e:
                logger.error(f"Error leyendo {nombre}: {e}")
                progress.console.print(f"[red]❌ Error leyendo {nombre}: {e}[/]")
            
            progress.advance(task)
            
    if not hubo_archivos_procesados:
        shutil.rmtree(temp_parts_path)
        return False
        
    # =========================================================================
    # --- UNIFICACIÓN OUT-OF-CORE CON DUCKDB (CERO RAM) ---
    # =========================================================================
    console.print("[cyan]🚀 Consolidando y cruzando con histórico (Out-Of-Core via DuckDB)...[/]")
    liberar_ram_os()
    
   
    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA memory_limit='3GB'") # RAM relajada
    
    # Habilitamos el "Disk Spilling" de DuckDB para evitar Out of Memory
    temp_duck_dir = os.path.join(temp_parts_path, "duckdb_spill").replace("\\", "/")
    os.makedirs(temp_duck_dir, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{temp_duck_dir}'")
    
    archivos_temporales = os.path.join(temp_parts_path, "*.parquet").replace("\\", "/")
    fechas_nuevas = []
    
    if columna_fecha:
        try:
            # union_by_name=True salva la lectura de múltiples archivos con esquemas diferentes
            query_fechas = f"""
                SELECT DISTINCT CAST("{columna_fecha}" AS DATE) AS dt 
                FROM read_parquet('{archivos_temporales}', union_by_name=True) 
                WHERE "{columna_fecha}" IS NOT NULL
            """
            fechas_df = con.execute(query_fechas).df()
            if not fechas_df.empty:
                fechas_nuevas = fechas_df['dt'].astype(str).tolist()
            console.print(f"[green]📅 Fechas detectadas para Upsert: {len(fechas_nuevas)} días únicos.[/]")
        except Exception:
            pass
    else:
        console.print("[green]🔄 Ingresando datos en modo Deduplicación Continua (Sin Fecha).[/]")

    if os.path.exists(ruta_bronze_historico):
        ruta_bronze_norm = ruta_bronze_historico.replace("\\", "/")
        ruta_temp = ruta_bronze_historico + ".tmp"
        ruta_temp_norm = ruta_temp.replace("\\", "/")
        
        try:
            if columna_fecha and fechas_nuevas:
                # Upsert: Borra lo viejo que coincida en fecha y anexa lo nuevo
                fechas_sql = ", ".join([f"'{f}'" for f in fechas_nuevas])
                query_cruce = f"""
                    COPY (
                        SELECT * FROM read_parquet('{ruta_bronze_norm}', union_by_name=True)
                        WHERE CAST("{columna_fecha}" AS DATE) NOT IN ({fechas_sql})
                        UNION ALL BY NAME
                        SELECT * FROM read_parquet('{archivos_temporales}', union_by_name=True)
                    ) TO '{ruta_temp_norm}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
                """
            else:
                # Deduplicación absoluta nativa (Disk-spill enabled)
                query_cruce = f"""
                    COPY (
                        SELECT DISTINCT * FROM (
                            SELECT * FROM read_parquet('{ruta_bronze_norm}', union_by_name=True)
                            UNION ALL BY NAME
                            SELECT * FROM read_parquet('{archivos_temporales}', union_by_name=True)
                        )
                    ) TO '{ruta_temp_norm}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
                """
                
            con.execute(query_cruce)
            con.close()
            
            if os.path.exists(ruta_bronze_historico):
                os.remove(ruta_bronze_historico)
            os.rename(ruta_temp, ruta_bronze_historico)
            
        except Exception as e:
            con.close()
            logger.error(f"DuckDB error en histórico: {e}")
            console.print(f"[yellow]⚠️ Error de motor SQL, forzando anexado Lazy (Fallback): {e}[/]")
            
            # Fallback a Polars
            archivos_glob = glob.glob(os.path.join(temp_parts_path, "*.parquet"))
            lf_temporales = [pl.scan_parquet(f) for f in archivos_glob]
            lf_nuevo = pl.concat(lf_temporales, how="diagonal")
            lf_hist = pl.scan_parquet(ruta_bronze_historico)
            lf_final = pl.concat([lf_hist, lf_nuevo], how="diagonal")
            lf_final.collect(streaming=True).write_parquet(ruta_bronze_historico, compression="snappy") #type: ignore
    else:
        os.makedirs(os.path.dirname(ruta_bronze_historico), exist_ok=True)
        ruta_bronze_norm = ruta_bronze_historico.replace("\\", "/")
        
        try:
            query_inicial = f"""
                COPY (
                    SELECT DISTINCT * FROM read_parquet('{archivos_temporales}', union_by_name=True)
                ) TO '{ruta_bronze_norm}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
            con.execute(query_inicial)
        except Exception as e:
            logger.error(f"Error DuckDB carga inicial: {e}")
            archivos_glob = glob.glob(os.path.join(temp_parts_path, "*.parquet"))
            lf_temporales = [pl.scan_parquet(f) for f in archivos_glob]
            lf_nuevo = pl.concat(lf_temporales, how="diagonal")
            lf_nuevo.collect(streaming=True).write_parquet(ruta_bronze_historico, compression="snappy") #type: ignore
        finally:
            con.close()
            
    # Lectura de metadata súper ligera para evitar cargar el dataframe y contar filas
    import pyarrow.parquet as pq
    filas = pq.read_metadata(ruta_bronze_historico).num_rows
    
    logger.info(f"DATA_QUALITY | BRONZE INCREMENTAL | Guardadas: {filas:,}")
    console.print(f"[bold green]✅ ARCHIVO BRONZE ACTUALIZADO: {os.path.basename(ruta_bronze_historico)} ({filas:,} filas)[/]")
    
    # Limpiamos archivos temporales al final
    if os.path.exists(temp_parts_path):
        shutil.rmtree(temp_parts_path)
        
    liberar_ram_os()
    
    return True
def limpiar_nulos_powerbi(df):
    """
    Limpia un DataFrame para Power BI.
    Elimina espacios en blanco, strings vacíos y textos 'nan'.
    """
    cols_texto = df.select_dtypes(include=['object', 'string']).columns
    valores_basura = ['nan', 'NaN', 'NAN', 'None', 'null', 'Null', '']
    
    # PROCESAMIENTO COLUMNA A COLUMNA (Evita picos masivos de RAM en Pandas)
    for col in cols_texto:
        df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
        df[col] = df[col].replace(valores_basura, np.nan)
        # Forzar a que cualquier vacío sea un verdadero None nativo de Python (Parquet lo ama)
        df[col] = df[col].where(pd.notnull(df[col]), None)

    return df

#Función secundaria para la limpieza de columnas de identificación, como cédulas, contratos o RIFs.
def limpiar_ids_documentos(df, columnas):
    """
    Normaliza columnas de identificación (Cédulas, Contratos, RIFs).
    1. Convierte a string.
    2. Elimina el sufijo decimal '.0' (típico de Excel).
    3. Elimina los puntos de miles '31.456.789' -> '31456789'.
    4. Convierte 'nan' o vacíos a None real.
    """
    if df.empty: return df
    
    for col in columnas:
        if col not in df.columns:
            continue
            
        # A. Convertir a string y quitar espacios
        df[col] = df[col].astype(str).str.strip()

        # B. Quitar '.0' al final (4473825.0 -> 4473825)
        df[col] = df[col].str.replace(r'\.0$', '', regex=True)

        # C. Quitar puntos de miles (31.743.084 -> 31743084)
        # OJO: Solo usar en IDs que NO deban tener puntos (no usar en IPs o Emails)
        df[col] = df[col].str.replace('.', '', regex=False)

        # D. Limpieza de basura ("nan", "None", vacíos)
        df[col] = df[col].replace({'nan': None, 'None': None, '': None})
        
    return df

#Función principal para guardar los archivos en formato Parquet, 
# la cual incluye una lógica anti-lock para evitar errores comunes de archivos abiertos, 
# así como una limpieza optimizada para Power BI que preserva los null nativos de Python
def guardar_parquet(df, nombre_archivo, filas_iniciales=None, ruta_destino=None):
    """
    Guarda el archivo en Parquet asegurando compatibilidad con Power BI.
    Incluye registro en LOG para auditoría de calidad de datos.
    """
    if df.empty:
        logger.warning(f"Dataset vacío: {nombre_archivo}. Se omitió el guardado.")
        console.print(f"[warning]⚠️ Dataset vacío para {nombre_archivo}. Omitido.[/]")
        return

    # --- LÓGICA DE RUTAS ---
    if ruta_destino:
        carpeta_salida = ruta_destino
        if nombre_archivo in ruta_destino:
            carpeta_salida = os.path.dirname(ruta_destino)
            ruta_salida = ruta_destino
        else:
            ruta_salida = os.path.join(carpeta_salida, nombre_archivo)
    else:
        carpeta_salida = PATHS.get("gold", "data/gold")
        ruta_salida = os.path.join(carpeta_salida, nombre_archivo)

    os.makedirs(carpeta_salida, exist_ok=True)
    
    try:
        # --- LÓGICA ANTI-LOCK ---
        if os.path.exists(ruta_salida):
            try:
                os.remove(ruta_salida)
            except PermissionError:
                logger.error(f"ARCHIVO BLOQUEADO: {nombre_archivo} está abierto en otro programa.")
                console.print(Panel(
                    f"[bold red]❌ ERROR CRÍTICO: ARCHIVO BLOQUEADO[/]\n"
                    f"El archivo [cyan]{nombre_archivo}[/] está abierto en otro programa.\n"
                    f"⚠️  Ciérralo e intenta de nuevo.",
                    title="ACCESO DENEGADO", style="red"
                ))
                raise 
            except Exception as e:
                logger.warning(f"No se pudo eliminar previo en {nombre_archivo}: {e}")

        # --- LIMPIEZA PARA POWER BI (OPTIMIZADA O(1) RAM) ---
        cols_obj = df.select_dtypes(include=['object', 'string']).columns
        for col in cols_obj:
            # Preserva los null nativos de forma vectorized sin usar .loc
            df[col] = df[col].where(df[col].isnull(), df[col].astype(str))
            
        # GUARDADO FÍSICO
        df.to_parquet(ruta_salida, index=False)
        
        # --- REPORTE Y AUDITORÍA ---
        filas_finales = len(df)
        
        # Determinamos el tipo para el reporte
        tipo = "CUSTOM"
        if PATHS.get("silver") and PATHS.get("silver") in ruta_salida: tipo = "SILVER" #type: ignore
        if PATHS.get("gold") and PATHS.get("gold") in ruta_salida: tipo = "GOLD"       #type: ignore

        if filas_iniciales is not None:
            filas_eliminadas = filas_iniciales - filas_finales
            
            # REGISTRO EN LOG (Auditoría de Calidad)
            logger.info(f"DATA_QUALITY | {tipo}: {nombre_archivo} | Leídas: {filas_iniciales:,} | Eliminadas: {filas_eliminadas:,} | Guardadas: {filas_finales:,}")
            
            # REPORTE VISUAL (Rich)
            grid = Table.grid(padding=(0, 2))
            grid.add_column(justify="left", style="cyan")
            grid.add_column(justify="left", style="bold white")
            grid.add_row("📥 Filas Leídas:", f"{filas_iniciales:,}")
            grid.add_row("🧹 Filas Eliminadas:", f"[red]- {filas_eliminadas:,}[/]")
            grid.add_row("💾 Filas Guardadas:", f"[green]{filas_finales:,}[/]")
            
            console.print(grid)
            console.print(f"[bold green]✅ ARCHIVO {tipo} GENERADO: {nombre_archivo}[/]")
            
        else:
            logger.info(f"DATA_QUALITY | {tipo}: {nombre_archivo} | Guardadas: {filas_finales:,} (Carga simple)")
            console.print(f"[bold green]✅ GUARDADO: {nombre_archivo} ({filas_finales:,} filas)[/]")

    except Exception as e:
        logger.error(f"FALLO CRÍTICO GUARDANDO {nombre_archivo}: {str(e)}", exc_info=True)
        console.print(f"[bold red]❌ FALLO GUARDANDO {nombre_archivo}: {e}[/]")
        raise
    
def standard_hours(df, columna_hora):
    """
    Limpia el formato del ERP (a. m. / p. m.) y estandariza a bloques de 1 hora.
    """
    # 1. Limpieza robusta de strings (puntos y espacios)
    df[columna_hora] = (df[columna_hora]
                        .astype(str).str.lower()
                        .str.replace('.', '', regex=False)
                        .str.replace(' ', '', regex=False)
                        .str.strip())
    
    # 2. Conversión a objeto datetime y luego a string HH:00
    # CAMBIO: Usamos '' en lugar de 'Sin Registro' para evitar TYPEMISMATCH en Power BI
    df[columna_hora] = (pd.to_datetime(df[columna_hora], errors='coerce')
                        .dt.strftime('%H:00'))
    return df

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
        f"[yellow] Tiempo Total de la Suite:[/][bold green] {int(minutos)} min {int(segundos)} seg[/]\n"
        f"[yellow] RAM Retenida al Finalizar:[/][bold cyan] {get_ram_usage_str()}[/]",
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
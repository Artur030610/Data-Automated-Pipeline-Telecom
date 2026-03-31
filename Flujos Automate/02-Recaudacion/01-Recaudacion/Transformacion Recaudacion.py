import pandas as pd
import numpy as np
import glob
import os
import sys
import warnings  # <--- NUEVO: Para limpiar la consola
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from rich.theme import Theme

# --- SILENCIAR ADVERTENCIAS MOLESTAS ---
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- CONFIGURACIÓN VISUAL ---
custom_theme = Theme({"success": "bold green", "error": "bold red", "warning": "yellow", "info": "cyan"})
console = Console(theme=custom_theme)

# ==========================================
# 1. CONFIGURACIÓN DE RUTAS
# ==========================================
console.print(Panel.fit("[bold white]PIPELINE DE RECAUDACIÓN (OPTIMIZADO + TIPO STRING)[/]", style="bold blue"))

usuario_base = os.path.expanduser("~")

# Ruta Origen (Bronze)
ruta_bronze = os.path.join(
    usuario_base, "Documents", "A-DataStack", "01-Proyectos", 
    "01-Data_PipelinesFibex", "02_Data_Lake", "raw_data", "1-Recaudación"
)

# Ruta Destino (Gold)
ruta_gold = os.path.join(
    usuario_base, "Documents", "A-DataStack", "01-Proyectos", 
    "01-Data_PipelinesFibex", "02_Data_Lake", "gold_data"
)

patron_busqueda = os.path.join(ruta_bronze, "*Recaudaci*.xlsx")

# ==========================================
# 2. DEFINICIÓN DE REGLAS Y ESTÁNDARES
# ==========================================

columnas_esperadas_input = [
    "ID Contrato", "N° Abonado", "Fecha", "Total Pago", 
    "Forma de Pago", "Banco", "Nombre Caja", "Oficina Cobro", 
    "Fecha Contrato", "Estatus", "Suscripción", "Grupo Afinidad", 
    "Nombre Franquicia", "Ciudad", "Vendedor"
]

oficinas_propias = [
    "OFC COMERCIAL CUMANA", "OFC- LA ASUNCION", "OFC SAN ANTONIO DE CAPYACUAL", "OFC TINACO", 
    "OFC VILLA ROSA", "OFC-SANTA FE", "OFI CARIPE MONAGAS", "OFI TINAQUILLO", "OFI-BARCELONA", 
    "OFI-BARINAS", "OFI-BQTO", "OFIC GALERIA EL PARAISO", "OFIC SAMBIL-VALENCIA", 
    "OFIC. PARRAL VALENCIA", "OFIC. TORRE FIBEX VIÑEDO", "OFI-CARACAS PROPATRIA", 
    "OFIC-BOCA DE UCHIRE", "OFIC-CARICUAO", "OFIC-COMERCIAL SANTA FE", "OFICINA ALIANZA MALL", 
    "OFICINA MARGARITA", "OFICINA SAN JUAN DE LOS MORROS", "OFIC-JUAN GRIEGO-MGTA", 
    "OFIC-METROPOLIS-BQTO", "OFIC-MGTA_DIAZ", "OFI-LECHERIA", "OFI-METROPOLIS", "OFI-PARAISO", 
    "OFI-PASEO LAS INDUSTRIAS", "OFI-PTO CABELLO", "OFI-PTO LA CRUZ", "OFI-SAN CARLOS", "OFI-VIA VENETO"
]

palabras_excluir = ["VIRTUAL", "Virna", "Fideliza", "Externa", "Unicenter", "Compensa"]

mapa_meses = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

columnas_finales_output = [
    "Source.Name", "ID Contrato", "N° Abonado", "Fecha", "Total Pago", 
    "Forma de Pago", "Banco", "Nombre Caja", "Oficina", "Fecha Contrato", 
    "Estatus", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
    "Ciudad", "Vendedor", "Tipo de afluencia", "Mes", "Clasificacion"
]

# ==========================================
# 3. EXTRACCIÓN (EXTRACT) - MOTOR CALAMINE
# ==========================================
archivos = glob.glob(patron_busqueda)
lista_dfs = []

if not archivos:
    console.print(f"[error]❌ No se encontraron archivos en: {ruta_bronze}[/]")
    sys.exit()

console.print(f"[info]📂 Archivos detectados: {len(archivos)}[/]")

with Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}", justify="left"),
    BarColumn(),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    TimeElapsedColumn(),
    console=console
) as progress:
    
    task1 = progress.add_task("[cyan]Iniciando lectura...", total=len(archivos))
    
    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)
        
        # Texto dinámico en la barra
        progress.update(task1, description=f"[cyan]Leyendo: {nombre_archivo}")
        
        if not nombre_archivo.startswith("~") and nombre_archivo != "Data-ConsolidadoRecaudacion.xlsx":
            try:
                # 1. Lectura
                df_temp = pd.read_excel(archivo, engine="calamine")
                # 2. Estandarización
                df_temp = df_temp.reindex(columns=columnas_esperadas_input)
                # 3. Trazabilidad
                df_temp["Source.Name"] = nombre_archivo
                lista_dfs.append(df_temp)
            except Exception as e:
                console.log(f"[error]⚠️ Error leyendo {nombre_archivo}: {e}[/]")
        
        progress.advance(task1)

if not lista_dfs:
    console.print("[error]❌ No se cargaron datos válidos.[/]")
    sys.exit()

# ==========================================
# 4. TRANSFORMACIÓN (TRANSFORM)
# ==========================================
with console.status("[bold green]Procesando Lógica y Tipos de Datos...[/]", spinner="dots"):
    
    # Concatenación (La advertencia ya está silenciada)
    df = pd.concat(lista_dfs, ignore_index=True)
    filas_raw = len(df)
    
    # --- A. CORRECCIÓN DE TIPOS (AQUÍ ESTÁ EL ARREGLO) ---
    # Convertimos explícitamente a string. Si hay nulos, se vuelven "nan" (texto), 
    # lo cual es seguro para guardar.
    df['N° Abonado'] = df['N° Abonado'].astype(str).replace('nan', None)
    df['ID Contrato'] = df['ID Contrato'].astype(str).replace('nan', None)
    
    # Fechas y Números
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
    df['Total Pago'] = pd.to_numeric(df['Total Pago'], errors='coerce').fillna(0)

    # --- B. Filtrado ---
    regex_excluir = '|'.join(palabras_excluir)
    mask_excluir = df['Oficina Cobro'].astype(str).str.contains(regex_excluir, case=False, na=False)
    filas_excluidas = mask_excluir.sum()
    df = df[~mask_excluir].copy()

    # --- C. Nuevas Columnas ---
    df['Tipo de afluencia'] = "RECAUDACIÓN"
    df['Mes'] = df['Fecha'].dt.month.map(mapa_meses)

    df['Clasificacion'] = np.where(
        df['Oficina Cobro'].isin(oficinas_propias), 
        "OFICINAS PROPIAS", 
        "ALIADOS Y DESARROLLO"
    )

    # --- D. Selección Final ---
    df = df.rename(columns={"Oficina Cobro": "Oficina"})
    cols_validas = [c for c in columnas_finales_output if c in df.columns]
    df_final = df[cols_validas]

# ==========================================
# 5. CARGA (LOAD)
# ==========================================
if not os.path.exists(ruta_gold):
    os.makedirs(ruta_gold)

archivo_salida = os.path.join(ruta_gold, "Recaudacion_Gold.parquet")

try:
    df_final.to_parquet(archivo_salida, index=False)
    resultado_msg = "[success]Guardado Correctamente[/]"
except Exception as e:
    resultado_msg = f"[error]Falló guardado: {e}[/]"

# ==========================================
# 6. REPORTE FINAL
# ==========================================
table = Table(title="📊 Resumen del Pipeline ETL")

table.add_column("Métrica", justify="right", style="cyan", no_wrap=True)
table.add_column("Valor", style="magenta")

table.add_row("Archivos Procesados", str(len(lista_dfs)))
table.add_row("Filas Leídas (Bronze)", f"{filas_raw:,}")
table.add_row("Filas Eliminadas (Filtro)", f"[red]- {filas_excluidas:,}[/]")
table.add_row("Filas Finales (Gold)", f"[bold green]{len(df_final):,}[/]")
table.add_row("Estado Final", resultado_msg)
table.add_row("Ruta Salida", archivo_salida)

console.print("\n")
console.print(table)
console.print("\n[bold green]🚀 LISTO PARA POWER BI[/]")
import pandas as pd
import numpy as np
import glob
import os
import sys
import warnings
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from rich.theme import Theme

# --- SILENCIAR ADVERTENCIAS ---
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

# --- CONFIGURACIÓN VISUAL ---
custom_theme = Theme({"success": "bold green", "error": "bold red", "warning": "yellow", "info": "cyan"})
console = Console(theme=custom_theme)

# ==========================================
# 1. CONFIGURACIÓN DE RUTAS (CORREGIDA)
# ==========================================
console.print(Panel.fit("[bold white]PIPELINE ATC - ATENCIÓN AL CLIENTE[/]", style="bold blue"))

usuario_base = os.path.expanduser("~")


nombre_carpeta_raw = "4-Atencion al cliente"

ruta_bronze = os.path.join(
    usuario_base, "Documents", "A-DataStack", "01-Proyectos", 
    "01-Data_PipelinesFibex", "02_Data_Lake", "raw_data", nombre_carpeta_raw
)

ruta_gold = os.path.join(
    usuario_base, "Documents", "A-DataStack", "01-Proyectos", 
    "01-Data_PipelinesFibex", "02_Data_Lake", "gold_data"
)

# Verificación de seguridad antes de empezar
if not os.path.exists(ruta_bronze):
    console.print(f"[bold red]❌ ERROR CRÍTICO: No encuentro la carpeta raw.[/]")
    console.print(f"[yellow]Buscaba en: {ruta_bronze}[/]")
    console.print("[white]Verifica que el nombre '4-Atencion al cliente' sea exacto.[/]")
    sys.exit()

nombre_archivo_salida = "Atencion_Cliente_Gold.parquet"
ruta_salida_completa = os.path.join(ruta_gold, nombre_archivo_salida)

# Asegurar carpeta Gold
if not os.path.exists(ruta_gold):
    os.makedirs(ruta_gold)

patron_busqueda = os.path.join(ruta_bronze, "*.xlsx")

# ==========================================
# 2. DEFINICIÓN DE COLUMNAS
# ==========================================
columnas_esperadas_input = [
    "N° Abonado", "Documento", "Cliente", "Estatus", "Fecha Llamada", 
    "Hora Llamada", "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", 
    "Responsable", "Suscripción", "Teléfono", "Grupo Afinidad", 
    "Franquicia", "Ciudad", "Teléfono verificado", "Detalle Suscripcion", "Saldo"
]

mapa_meses = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

columnas_finales_output = [
    "Source.Name", "N° Abonado", "Documento", "Cliente", "Estatus", 
    "Fecha", "Hora", "Tipo Respuesta", "Detalle Respuesta", 
    "Vendedor", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
    "Ciudad", "Tipo de afluencia", "Mes"
]

# ==========================================
# 3. EXTRACCIÓN (EXTRACT)
# ==========================================
archivos = glob.glob(patron_busqueda)
lista_dfs = []

if not archivos:
    console.print(f"[error]❌ Carpeta encontrada pero VACÍA (sin .xlsx): {ruta_bronze}[/]")
    sys.exit()

with Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}", justify="left"),
    BarColumn(),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    TimeElapsedColumn(),
    console=console
) as progress:
    
    task1 = progress.add_task("[cyan]Procesando Archivos...", total=len(archivos))
    
    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)
        progress.update(task1, description=f"[cyan]Leyendo: {nombre_archivo}")
        
        if (not nombre_archivo.startswith("~") and 
            nombre_archivo != "Data_Consolidado_ATC.xlsx"):
            
            try:
                # Lectura rápida
                df_temp = pd.read_excel(archivo, engine="calamine")
                # Estandarización
                df_temp = df_temp.reindex(columns=columnas_esperadas_input)
                # Trazabilidad
                df_temp["Source.Name"] = nombre_archivo
                lista_dfs.append(df_temp)
            except Exception as e:
                console.log(f"[error]⚠️ Error en {nombre_archivo}: {e}[/]")
        
        progress.advance(task1)

if not lista_dfs:
    console.print("[error]❌ No hay datos válidos para procesar.[/]")
    sys.exit()

# ==========================================
# 4. TRANSFORMACIÓN (TRANSFORM)
# ==========================================
with console.status("[bold green]Aplicando Limpieza...[/]", spinner="dots"):
    
    df = pd.concat(lista_dfs, ignore_index=True)
    filas_raw = len(df)
    
    # Renombres
    df = df.rename(columns={
        "Franquicia": "Nombre Franquicia",
        "Fecha Llamada": "Fecha",
        "Responsable": "Vendedor",
        "Hora Llamada": "Hora"
    })

    # Tipos
    df['N° Abonado'] = df['N° Abonado'].astype(str).replace('nan', None)
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
    df['Documento'] = df['Documento'].astype(str).replace('nan', None)
    # Lógica
    df['Tipo de afluencia'] = "ATENCIÓN AL CLIENTE"
    df['Mes'] = df['Fecha'].dt.month.map(mapa_meses)
    df['Vendedor'] = df['Vendedor'].astype(str).str.upper()
    
    # Filtros
    filtro_excluir = ["AFILIACION DE SERVICIO", "PAGO DEL SERVICIO"]
    mask_borrar = df['Tipo Respuesta'].isin(filtro_excluir)
    filas_excluidas = mask_borrar.sum()
    df = df[~mask_borrar].copy()

    # Selección Final
    cols_validas = [c for c in columnas_finales_output if c in df.columns]
    df_final = df[cols_validas]

# ==========================================
# 5. CARGA (LOAD)
# ==========================================
try:
    df_final.to_parquet(ruta_salida_completa, index=False)
    resultado_msg = "[success]Guardado Correctamente[/]"
    ruta_absoluta = os.path.abspath(ruta_salida_completa)
except Exception as e:
    resultado_msg = f"[error]Falló guardado: {e}[/]"
    ruta_absoluta = "Error"

# ==========================================
# 6. REPORTE FINAL
# ==========================================
table = Table(title="📊 Resumen Pipeline ATC")
table.add_column("Métrica", justify="right", style="cyan")
table.add_column("Valor", style="magenta")

table.add_row("Archivos Encontrados", str(len(archivos)))
table.add_row("Filas Procesadas", f"{len(df_final):,}")
table.add_row("Estado", resultado_msg)
table.add_row("Ruta Leída", ruta_bronze)

console.print(table)
console.print(f"\n[bold yellow]👉 Archivo listo en:[/]\nfile:///{ruta_absoluta.replace(os.sep, '/')}")
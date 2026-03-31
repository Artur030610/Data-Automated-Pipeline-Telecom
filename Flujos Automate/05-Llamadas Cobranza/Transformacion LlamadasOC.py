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
# 1. CONFIGURACIÓN DE RUTAS
# ==========================================
console.print(Panel.fit("[bold white]PIPELINE - OPERATIVOS COBRANZA[/]", style="bold blue"))

usuario_base = os.path.expanduser("~")

# Basado en tu Power Query: "07-Operativos Cobranza"
nombre_carpeta_raw = "7-Operativos Cobranza"

ruta_bronze = os.path.join(
    usuario_base, "Documents", "A-DataStack", "01-Proyectos", 
    "01-Data_PipelinesFibex", "02_Data_Lake", "raw_data", nombre_carpeta_raw
)

ruta_gold = os.path.join(
    usuario_base, "Documents", "A-DataStack", "01-Proyectos", 
    "01-Data_PipelinesFibex", "02_Data_Lake", "gold_data"
)

# Verificación de seguridad
if not os.path.exists(ruta_bronze):
    console.print(f"[bold red]❌ ERROR CRÍTICO: No encuentro la carpeta raw.[/]")
    console.print(f"[yellow]Buscaba en: {ruta_bronze}[/]")
    console.print(f"[white]Asegúrate de crear la carpeta '{nombre_carpeta_raw}' y poner los Excels ahí.[/]")
    sys.exit()

nombre_archivo_salida = "Operativos_Cobranza_Gold.parquet"
ruta_salida_completa = os.path.join(ruta_gold, nombre_archivo_salida)

# Asegurar carpeta Gold
if not os.path.exists(ruta_gold):
    os.makedirs(ruta_gold)

patron_busqueda = os.path.join(ruta_bronze, "*.xlsx")

# ==========================================
# 2. DEFINICIÓN DE COLUMNAS
# ==========================================
# Columnas extraídas del paso #"Otras columnas quitadas" en M
columnas_esperadas_input = [
    "N° Abonado", 
    "Cliente",       # Se carga temporalmente, luego se elimina según el script M
    "Estatus", 
    "Saldo", 
    "Fecha Llamada", 
    "Hora Llamada", 
    "Tipo Respuesta", 
    "Detalle Respuesta", 
    "Responsable", 
    "Franquicia", 
    "Ciudad"
]

# Columnas finales que esperamos en el Parquet (incluyendo las calculadas)
columnas_finales_output = [
    "Source.Name", 
    "N° Abonado", 
    "Estatus", 
    "Saldo", 
    "Fecha Llamada", 
    "Hora",          # Renombrado
    "Tipo Respuesta", 
    "Detalle Respuesta", 
    "Responsable", 
    "Franquicia", 
    "Ciudad", 
    "Canal"          # Calculada
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
        
        if not nombre_archivo.startswith("~"): # Ignorar temporales de Excel abiertos
            try:
                # Lectura rápida con calamine
                df_temp = pd.read_excel(archivo, engine="calamine")
                
                # Selección preventiva de columnas existentes
                # (Esto evita error si falta alguna columna en un excel específico, rellenando con NaN)
                df_temp = df_temp.reindex(columns=columnas_esperadas_input)
                
                # Trazabilidad (Source.Name)
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
with console.status("[bold green]Aplicando Lógica de Negocio (M a Python)...[/]", spinner="dots"):
    
    df = pd.concat(lista_dfs, ignore_index=True)
    filas_raw = len(df)
    
    # 4.1. Text.Upper en Ciudad
    # M: Table.TransformColumns(#"Otras columnas quitadas",{{"Ciudad", Text.Upper, type text}})
    df['Ciudad'] = df['Ciudad'].astype(str).str.upper()

    # 4.2. Eliminar Cliente
    # M: Table.RemoveColumns(#"Texto en mayúsculas",{"Cliente"})
    if 'Cliente' in df.columns:
        df = df.drop(columns=['Cliente'])

    # 4.3. Lógica Condicional para "Canal"
    # M: if Text.Contains([Responsable], "OFI") or ...
    
    # Normalizamos Responsable a string para evitar errores en el contains
    df['Responsable'] = df['Responsable'].astype(str)
    
    condiciones = [
        df['Responsable'].str.contains("OFI|ASESOR", case=False, na=False),
        df['Responsable'].str.contains("PHONE", case=False, na=False),
        df['Responsable'].str.contains("CALL", case=False, na=False)
    ]
    
    resultados = [
        "OFICINA COMERCIAL", 
        "HELPHONE", 
        "CALL CENTER"
    ]
    
    # El 'default' es el 'else "ALIADOS"' del código M
    df['Canal'] = np.select(condiciones, resultados, default="ALIADOS")

    # 4.4. Renombres
    # M: Table.RenameColumns(...,{{"Hora Llamada", "Hora"}})
    df = df.rename(columns={"Hora Llamada": "Hora"})

    # 4.5. Tipos de Datos y Ordenamiento
    # M: Table.TransformColumnTypes(...,{{"Fecha Llamada", type date}, {"Hora", type time}})
    df['Fecha Llamada'] = pd.to_datetime(df['Fecha Llamada'], errors='coerce')
    
    # M: Table.Sort(...,{{"Fecha Llamada", Order.Descending}})
    df = df.sort_values(by='Fecha Llamada', ascending=False)

    # 4.6 Limpieza final de Nulos o conversiones extra necesarias
    df['N° Abonado'] = df['N° Abonado'].astype(str).replace('nan', None)

    # Selección Final para el orden deseado
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
table = Table(title="📊 Resumen Pipeline Operativos Cobranza")
table.add_column("Métrica", justify="right", style="cyan")
table.add_column("Valor", style="magenta")

table.add_row("Archivos Encontrados", str(len(archivos)))
table.add_row("Filas Totales", f"{len(df_final):,}")
table.add_row("Canales Detectados", str(df_final['Canal'].nunique()))
table.add_row("Estado", resultado_msg)

console.print(table)
console.print(f"\n[bold yellow]👉 Archivo listo en:[/]\nfile:///{ruta_absoluta.replace(os.sep, '/')}")
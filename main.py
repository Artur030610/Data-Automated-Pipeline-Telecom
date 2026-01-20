from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from config import THEME_COLOR
import time 
from utils import tiempo

# --- IMPORTACIÓN DE MÓDULOS ---
# Asegúrate de importar el nuevo script GOLD aquí
from ETLs import (
    etl_afluencia_silver, etl_afluencia_gold, # <--- IMPORTANTE: Importar ambos
    reclamos, ventase, recaudacion, atc, actualizacion_datos, comeback, 
    ventas, empleados, cobranza, dimclientes, estadistica_abonado 
)

inicio_global = time.time()
console = Console(theme=THEME_COLOR)

# --- ORQUESTADOR AFLUENCIA (EL PUENTE) ---
# Esta clase une los dos pasos (Silver -> Gold) para que el menú lo vea como uno solo
class PipelineAfluencia:
    def ejecutar(self):
        # 1. Ejecutar Silver (Consolidación)
        # Nota: etl_afluencia_silver.ejecutar() debe retornar la ruta del archivo generado
        ruta_silver = etl_afluencia_silver.ejecutar() 
        
        # 2. Ejecutar Gold (Enriquecimiento) si Silver funcionó
        if ruta_silver:
            etl_afluencia_gold.ejecutar(ruta_silver)

# Instanciamos el orquestador
afluencia_completa = PipelineAfluencia()


def ejecutar_wrapper(modulo):
    """Ejecuta el método .ejecutar() de un módulo o una lista de módulos."""
    try:
        if isinstance(modulo, list):
            for m in modulo:
                ejecutar_wrapper(m)
        else:
            modulo.ejecutar()
    except Exception as e:
        console.print(f"[bold red]Error crítico en el wrapper: {e}[/]")

# --- CONFIGURACIÓN DEL MENÚ ---
MENU = {
    # En la opción 1, reemplazamos 'etl_afluencia_silver' por 'afluencia_completa'
    "1":  {"icono": "🚀", "label": "EJECUTAR TODO (Full Pipeline)", "target": [
        ventase, reclamos, recaudacion, atc, actualizacion_datos, comeback, 
        afluencia_completa, # <--- AQUI EL CAMBIO
        ventas, empleados, cobranza, dimclientes
    ]},
    
    "2":  {"icono": "💼", "label": "Ventas Estatus",             "target": ventase},
    "3":  {"icono": "🛠️", "label": "Reclamos (Suite)",           "target": reclamos},
    "4":  {"icono": "💰", "label": "Recaudación",                "target": recaudacion},
    "5":  {"icono": "🎧", "label": "Atención al Cliente (ATC)",  "target": atc},
    "6":  {"icono": "📝", "label": "Actualización de Datos",     "target": actualizacion_datos},
    "7":  {"icono": "🏠", "label": "Come Back Home (CBH)",       "target": comeback},
    "8":  {"icono": "📊", "label": "Ventas (Listado)",           "target": ventas},
    "9":  {"icono": "🔄", "label": "Generar Afluencia (Silver+Gold)", "target": afluencia_completa}, 
    "10": {"icono": "👤", "label": "Maestro de Empleados",       "target": empleados},
    "11": {"icono": "📞", "label": "Llamadas de Cobranza",       "target": cobranza},
    "12": {"icono": "💎", "label": "Dimensión Clientes (Gold)",  "target": dimclientes},
    "13": {"icono": "📊", "label": "Estadística de Abonados",    "target": estadistica_abonado}
}

def mostrar_menu():
    """Genera una tabla visual con Rich basada en el diccionario MENU."""
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("ID", style="bold yellow", justify="right")
    table.add_column("Icono")
    table.add_column("Descripción", style="bold white")

    for key, val in MENU.items():
        estilo = "bold green" if key == "1" else "cyan"
        table.add_row(f"{key}.", val['icono'], f"[{estilo}]{val['label']}[/]")

    panel = Panel(
        table, 
        title="[bold blue]PIPELINE MASTER FIBEX[/]", 
        subtitle="[dim]Selecciona el ID del proceso a ejecutar[/]",
        expand=False
    )
    console.print(panel)

def main():
    mostrar_menu()
    
    opcion = Prompt.ask(
        "\n[bold yellow]¿Qué proceso deseas correr?[/]", 
        choices=list(MENU.keys()), 
        default="1"
    )
    
    console.print("\n")
    
    seleccion = MENU.get(opcion)
    if seleccion:
        console.rule(f"[bold blue]Iniciando: {seleccion['label']}")
        ejecutar_wrapper(seleccion['target'])
    
    console.rule("[bold green] FIN DE EJECUCIÓN[/]")
    tiempo(inicio_global)

if __name__ == "__main__":
    main()
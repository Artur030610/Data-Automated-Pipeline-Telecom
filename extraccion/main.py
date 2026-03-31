import os
import sys
import datetime
import time

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from utils import console, tiempo

# ========================================================
# 1. IMPORTACIÓN DE SCRAPERS (ROBOTS)
# ========================================================
import scraper_ventas_estatus
import scraper_recaudacion
import scraper_ventas
import scraper_atc
import scraper_horas_recaudacion
import scraper_cobranza
import scraper_act_datos

def pedir_fechas():
    console.print("\n[bold cyan]📅 Configuración de Fechas para la Extracción[/]")
    
    # Sugerir fechas por defecto (Día 1 del mes actual hasta hoy)
    hoy = datetime.datetime.today()
    primer_dia = hoy.replace(day=1).strftime("%d/%m/%Y")
    hoy_str = hoy.strftime("%d/%m/%Y")
    
    while True:
        fecha_inicial = Prompt.ask("[yellow]Ingrese Fecha Inicial (DD/MM/YYYY)[/]", default=primer_dia)
        try:
            datetime.datetime.strptime(fecha_inicial, "%d/%m/%Y")
            break
        except ValueError:
            console.print("[bold red]❌ Fecha inválida. Asegúrese de que sea una fecha real en formato DD/MM/YYYY.[/]")

    while True:
        fecha_final = Prompt.ask("[yellow]Ingrese Fecha Final (DD/MM/YYYY)[/]", default=hoy_str)
        try:
            datetime.datetime.strptime(fecha_final, "%d/%m/%Y")
            break
        except ValueError:
            console.print("[bold red]❌ Fecha inválida. Asegúrese de que sea una fecha real en formato DD/MM/YYYY.[/]")
        
    return fecha_inicial, fecha_final

# ========================================================
# 2. MENÚ DE OPCIONES
# ========================================================
MENU = {
    "1":  {"icono": "🚀", "label": "EJECUTAR TODOS LOS REPORTES", "target": [
        scraper_recaudacion.descargar_recaudacion,
        scraper_horas_recaudacion.descargar_recaudacion,
        scraper_ventas_estatus.descargar_ventas_estatus,
        scraper_ventas.descargar_ventas,
        scraper_atc.descargar_atc,
        scraper_cobranza.descargar_atc,
        scraper_act_datos.descargar_atc
    ]},
    "2":  {"icono": "💰", "label": "Recaudación", "target": scraper_recaudacion.descargar_recaudacion},
    "3":  {"icono": "🕒", "label": "Horas de Pago", "target": scraper_horas_recaudacion.descargar_recaudacion},
    "4":  {"icono": "👥", "label": "Ventas (Listado de Abonados)", "target": scraper_ventas.descargar_ventas},
    "5":  {"icono": "💼", "label": "Ventas (Estatus)", "target": scraper_ventas_estatus.descargar_ventas_estatus},
    "6":  {"icono": "🎧", "label": "Atención al Cliente", "target": scraper_atc.descargar_atc},
    "7":  {"icono": "📞", "label": "Operativos Cobranza", "target": scraper_cobranza.descargar_atc},
    "8":  {"icono": "🔄", "label": "Actualización de Datos", "target": scraper_act_datos.descargar_atc},
}

def mostrar_menu():
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("ID", style="bold yellow", justify="right")
    table.add_column("Icono")
    table.add_column("Descripción", style="bold white")

    for key, val in MENU.items():
        estilo = "bold green" if key == "1" else "cyan"
        table.add_row(f"{key}.", val['icono'], f"[{estilo}]{val['label']}[/]")

    panel = Panel(
        table, 
        title="[bold blue]ORQUESTADOR DE EXTRACCIÓN (SAE PLUS)[/]", 
        subtitle="[dim]Selecciona el robot a ejecutar[/]",
        expand=False
    )
    console.print(panel)

def ejecutar_wrapper(rutina, f_ini, f_fin):
    try:
        if isinstance(rutina, list):
            for r in rutina:
                ejecutar_wrapper(r, f_ini, f_fin)
        else:
            rutina(f_ini, f_fin)
    except Exception as e:
        console.print(f"[bold red]💥 Error crítico ejecutando extracción: {e}[/]")

def main():
    console.rule("[bold blue]🤖 BIENVENIDO AL ROBOT DE EXTRACCIÓN SAE[/]")
    
    f_ini, f_fin = pedir_fechas()
    
    console.print("\n")
    mostrar_menu()
    opcion = Prompt.ask("\n[bold yellow]¿Qué reporte deseas descargar?[/]", choices=list(MENU.keys()), default="1")
    console.print("\n")
    
    # Iniciamos el cronómetro justo después de las interacciones del usuario
    inicio_extraccion = time.time()
    
    seleccion = MENU.get(opcion)
    if seleccion:
        console.rule(f"[bold blue]Iniciando: {seleccion['label']} ({f_ini} al {f_fin})[/]")
        ejecutar_wrapper(seleccion['target'], f_ini, f_fin) 
    
    console.rule("[bold green]✅ FIN DE EXTRACCIÓN GLOBAL[/]")
    tiempo(inicio_extraccion)

if __name__ == "__main__":
    main()
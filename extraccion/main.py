import os
import sys
import datetime 
from datetime import timedelta
import time
import asyncio
from tenacity import retry, stop_after_attempt, wait_fixed

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from utils import console, tiempo, logger_extraccion
from notificaciones import enviar_notificacion_bot

# ========================================================
# 1. IMPORTACIÓN DE SCRAPERS (ROBOTS)
# ========================================================
import scraper_ventas_estatus
import scraper_recaudacion
import scraper_ventas
import scraper_atc
import scraper_cobranza
import scraper_act_datos
import scraper_reclamos
import scraper_ordenes_servicio
import scraper_comebackhome
import scraper_empleados
import scraper_estadisticas_abonados
import scraper_churn_risk
import scraper_abonados
import scraper_ont_off

def pedir_fechas(auto_mode=False):
    console.print("\n[bold cyan]📅 Configuración de Fechas para la Extracción[/]")
    
    # Sugerir fechas por defecto (Día 1 del mes actual hasta hoy)
    hoy = datetime.datetime.today()
    primer_dia = (hoy - timedelta(days=7)).strftime("%d/%m/%Y")
    hoy_str = hoy.strftime("%d/%m/%Y")
    
    if auto_mode:
        console.print(f"[bold green]🤖 Modo Automático Activado:[/] Usando fechas {primer_dia} al {hoy_str}")
        return primer_dia, hoy_str
        
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
        scraper_recaudacion.descargar_recaudacion_y_horas,
        scraper_ventas_estatus.descargar_ventas_estatus,
        scraper_ventas.descargar_ventas,
        scraper_atc.descargar_atc,
        scraper_cobranza.descargar_cobranza,
        scraper_act_datos.descargar_act_datos,
        scraper_reclamos.descargar_reclamos,
        scraper_estadisticas_abonados.descargar_estadisticas_abonados,
        #scraper_churn_risk.descargar_churn_risk,
        #scraper_ordenes_servicio.descargar_ordenes_servicio,
        scraper_comebackhome.descargar_comebackhome,
        scraper_empleados.descargar_empleados,
        scraper_ont_off.descargar_ont_off
    ]},
    "2":  {"icono": "💰", "label": "RECAUDACIÓN Y HORAS DE PAGO", "target": scraper_recaudacion.descargar_recaudacion_y_horas},
    "3":  {"icono": "👥", "label": "VENTAS LISTADO DE ABONADOS", "target": scraper_ventas.descargar_ventas},
    "4":  {"icono": "💼", "label": "VENTAS ESTATUS", "target": scraper_ventas_estatus.descargar_ventas_estatus},
    "5":  {"icono": "🎧", "label": "ATENCION AL CLIENTE", "target": scraper_atc.descargar_atc},
    "6":  {"icono": "📞", "label": "OPERATIVOS COBRANZA", "target": scraper_cobranza.descargar_cobranza},
    "7":  {"icono": "🔄", "label": "ACTUALIZACION DE DATOS", "target": scraper_act_datos.descargar_act_datos},
    "8":  {"icono": "🛠️", "label": "RECLAMOS (OOCC, CC, RRSS, APP Y BANCOS)", "target": scraper_reclamos.descargar_reclamos},
    "9":  {"icono": "📑", "label": "ÓRDENES DE SERVICIO (TICKETS IDF/SLA)", "target": scraper_ordenes_servicio.descargar_ordenes_servicio},
    "10": {"icono": "🏠", "label": "COMEBACKHOME", "target": scraper_comebackhome.descargar_comebackhome},
    "11": {"icono": "👤", "label": "EMPLEADOS (PLANTILLA ACTUAL)", "target": scraper_empleados.descargar_empleados},
    "12": {"icono": "🎧", "label": "ESTADÍSTICA DE ABONADOS", "target": scraper_estadisticas_abonados.descargar_estadisticas_abonados},
    "13": {"icono": "📈", "label": "CHURN RISK", "target": scraper_churn_risk.descargar_churn_risk},
    "14": {"icono": "📜", "label": "ONTs APAGADAS", "target": scraper_ont_off.descargar_ont_off},
    
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

@retry(stop=stop_after_attempt(3), wait=wait_fixed(15), reraise=True)
def ejecutar_scraper(rutina, f_ini, f_fin):
    """Ejecuta un solo scraper con reintentos automáticos en caso de fallo (Timeout/Red)."""
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())
    except Exception:
        pass
    rutina(f_ini, f_fin)

def ejecutar_wrapper(rutina, f_ini, f_fin):
    """Itera sobre la lista de scrapers y maneja los errores definitivos tras los reintentos."""
    if isinstance(rutina, list):
        for r in rutina:
            try:
                ejecutar_scraper(r, f_ini, f_fin)
            except Exception as e:
                logger_extraccion.error(f"Fallo definitivo en {r.__name__}: {e}", exc_info=True)
                console.print(f"[bold red]💥 Fallo definitivo en {r.__name__} tras 3 intentos: {e}[/]")
    else:
        try:
            ejecutar_scraper(rutina, f_ini, f_fin)
        except Exception as e:
            logger_extraccion.error(f"Fallo definitivo en {rutina.__name__}: {e}", exc_info=True)
            console.print(f"[bold red]💥 Fallo definitivo en {rutina.__name__} tras 3 intentos: {e}[/]")

def main():
    logger_extraccion.info("="*50)
    logger_extraccion.info("🤖 INICIO DE ORQUESTADOR DE EXTRACCIÓN")
    console.rule("[bold blue]🤖 BIENVENIDO AL ROBOT DE EXTRACCIÓN SAE[/]")
    
    auto_mode = "--auto" in sys.argv
    f_ini, f_fin = pedir_fechas(auto_mode)
    
    console.print("\n")
    
    if auto_mode:
        console.print("[bold green]🤖 Ejecutando todos los reportes (Opción 1) automáticamente...[/]")
        opcion = "1"
    else:
        mostrar_menu()
        opcion = Prompt.ask("\n[bold yellow]¿Qué reporte deseas descargar?[/]", choices=list(MENU.keys()), default="1")
        console.print("\n")
    
    # Iniciamos el cronómetro justo después de las interacciones del usuario
    inicio_extraccion = time.time()
    
    seleccion = MENU.get(opcion)
    if seleccion:
        rutinas = seleccion['target']
        
        # Si elige la extracción global y no está en modo automático, preguntamos desde dónde iniciar
        if opcion == "1" and isinstance(rutinas, list) and not auto_mode:
            console.print("\n[bold cyan]Secuencia de extracción programada:[/]")
            for i, r in enumerate(rutinas, 1):
                nombre_limpio = r.__name__.replace('descargar_', '').replace('_', ' ').title()
                console.print(f"  [green]{i}.[/] {nombre_limpio}")
            
            str_inicio = Prompt.ask(
                "\n[bold yellow]¿Desde qué número deseas reanudar? (Presiona Enter para empezar desde el 1)[/]", 
                default="1"
            )
            try:
                idx_inicio = int(str_inicio) - 1
                if 0 <= idx_inicio < len(rutinas):
                    rutinas = rutinas[idx_inicio:]
            except ValueError:
                pass # Si introduce texto no válido, asume el inicio (0)

        logger_extraccion.info(f"Ejecutando opción seleccionada: {seleccion['label']} | Fechas: {f_ini} al {f_fin}")
        console.rule(f"[bold blue]Iniciando: {seleccion['label']} ({f_ini} al {f_fin})[/]")
        ejecutar_wrapper(rutinas, f_ini, f_fin) 
    
    console.rule("[bold green]✅ FIN DE EXTRACCIÓN GLOBAL[/]")
    duration = time.time() - inicio_extraccion
    logger_extraccion.info(f"✅ FIN DE ORQUESTADOR DE EXTRACCIÓN | Tiempo Total: {duration:.2f}s")
    tiempo(inicio_extraccion)
    
    # --- NOTIFICACIÓN AL BOT ---
    enviar_notificacion_bot(
        mensaje=f"✅ *Extracción SAE Completada*\n🤖 Reporte(s): {seleccion['label'] if seleccion else 'Desconocido'}\n📅 Rango: {f_ini} al {f_fin}\n⏱️ Tiempo Total: {duration/60:.2f} minutos",
        plataforma="telegram" # Puedes cambiarlo a "webhook" si prefieres Slack o Discord
    )

if __name__ == "__main__":
    main()
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from config import THEME_COLOR
import time 
from utils import tiempo

# --- IMPORTACIÓN DE MÓDULOS ---
from ETLs import (
    # --- PROCESOS ACTIVOS (INGESTA) ---
    recaudacion,            # Carpeta 1
    ventas,                 # Carpeta 2
    ventase,                # Carpeta 2 (Estatus)
    reclamos,               # Carpeta 3
    atc,                    # Carpeta 4
    idf,                    # Carpeta 5
    cobranza,               # Carpeta 7
    actualizacion_datos,    # Carpeta 11
    comeback,               # Carpeta 12
    empleados,              # Carpeta 17
    
    # --- TRANSFORMACIONES ---
    etl_afluencia_silver, 
    etl_afluencia_gold,
    dimclientes,
    estadistica_abonado
)

inicio_global = time.time()
console = Console(theme=THEME_COLOR)

# --- ORQUESTADOR AFLUENCIA (EL PUENTE) ---
class PipelineAfluencia:
    def ejecutar(self):
        # 1. Ejecutar Silver (Consolidación)
        ruta_silver = etl_afluencia_silver.ejecutar() 
        
        # 2. Ejecutar Gold (Enriquecimiento) si Silver funcionó
        if ruta_silver:
            etl_afluencia_gold.ejecutar(ruta_silver)

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
    "1":  {"icono": "🚀", "label": "EJECUTAR TODO (Orden Lógico DW)", "target": [
        # -------------------------------------------------------
        # FASE 1: INGESTA DE DATOS CRUDOS (RAW LAYER)
        # -------------------------------------------------------
        recaudacion,            # 1. Financiero
        ventas,                 # 2. Comercial
        ventase,                # 2. Estatus
        reclamos,               # 3. Soporte
        atc,                    # 4. Atención
        idf,           # 5. Técnico
        cobranza,               # 7. Operativo
        actualizacion_datos,    # 11. Calidad de Datos
        comeback,               # 12. Retención
        empleados,              # 17. RRHH (Base para Dimensiones)
        
        # -------------------------------------------------------
        # FASE 2: DIMENSIONES (DIMENSIONS LAYER)
        # Se ejecutan antes para que los Hechos tengan con qué cruzar
        # -------------------------------------------------------
        dimclientes,            # Crea la dimensión maestra de clientes
        
        # -------------------------------------------------------
        # FASE 3: HECHOS Y AGREGACIONES (FACT LAYER)
        # Afluencia va al final porque consume a Empleados y Clientes
        # -------------------------------------------------------
        estadistica_abonado,    # Agregaciones
        afluencia_completa      # HECHO FINAL (Silver + Gold)
    ]},
    
    # --- OPCIONES INDIVIDUALES ---
    "2":  {"icono": "💰", "label": "1. Recaudación",             "target": recaudacion},
    "3":  {"icono": "📊", "label": "2. Ventas (General)",        "target": ventas},
    "4":  {"icono": "💼", "label": "2. Ventas (Estatus)",        "target": ventase},
    "5":  {"icono": "🛠️", "label": "3. Reclamos (Suite)",        "target": reclamos},
    "6":  {"icono": "🎧", "label": "4. Atención al Cliente",     "target": atc},
    "7":  {"icono": "📉", "label": "5. Índice de Falla",         "target": idf},
    "8":  {"icono": "📞", "label": "7. Llamadas Cobranza",       "target": cobranza},
    "9":  {"icono": "📝", "label": "11. Act. Datos",             "target": actualizacion_datos},
    "10": {"icono": "🏠", "label": "12. Come Back Home",         "target": comeback},
    "11": {"icono": "👤", "label": "17. Empleados",              "target": empleados},
    
    # --- TRANSFORMACIONES ---
    "12": {"icono": "💎", "label": "Dimensión Clientes",         "target": dimclientes},
    "13": {"icono": "📈", "label": "Estadística Abonado",        "target": estadistica_abonado},
    "14": {"icono": "🔄", "label": "Generar Afluencia (S+G)",    "target": afluencia_completa}
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
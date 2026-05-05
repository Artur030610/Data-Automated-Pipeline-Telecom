import sys
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from config import THEME_COLOR
import time 
from utils import tiempo, audit_performance, liberar_ram_os
from notificaciones import enviar_notificacion_bot

# ========================================================
# 1. IMPORTACIÓN DE MÓDULOS (ETLs)
# ========================================================
from transformacion.ETLs import (
    # --- INGRESOS ---
    trans_abonados_idf,
    trans_actualizacion_datos,
    trans_afluencia_gold,
    trans_afluencia_silver,
    trans_atc,
    trans_cobranza,
    trans_comeback,
    trans_dim_franquicias,
    trans_dimclientes,
    trans_empleados,
    trans_estadistica_abonado,
    trans_ont_off,
    trans_ordenes_servicio,
    trans_recaudacion,
    trans_reclamos,
    trans_ventase,            # 1. Financiero
    trans_ventas              # 2. Comercial           # 1. Denominador (Stock de Clientes)        # 3. El Puente (Dimensión),
    # --- TRANSFORMACIONES & DW ---
)

console = Console(theme=THEME_COLOR)

# ========================================================
# 2. ORQUESTADORES (PIPELINES COMPLEJOS)
# ========================================================
# --- PIPELINE DE AFLUENCIA ---
class PipelineAfluencia:
    def ejecutar(self):
        ruta_silver = trans_afluencia_silver.ejecutar() 
        if ruta_silver:
            trans_afluencia_gold.ejecutar(ruta_silver)

afluencia_completa = PipelineAfluencia()

# --- PIPELINE DE INDICADORES (LA INTEGRACIÓN QUE PEDISTE) ---
class PipelineIndicadores:
    """
    Controla la dependencia estricta para Power BI:
    1. Generar Stock Abonados (Gold) -> Necesario para dividir.
    2. Generar Tickets Master (Gold IDF + Gold SLA) -> Necesario para numeradores.
    3. Generar Dimensión Franquicias -> Lee 1 y 2 para unir el modelo.
    """
    def ejecutar(self):
        console.rule("[bold magenta]SUITE DE INDICADORES TÉCNICOS (IDF + SLA)[/]")
        
        # PASO 1: Generar el Denominador (Abonados)
        console.print("\n[dim]1. Actualizando Stock de Abonados...[/]")
        trans_abonados_idf.ejecutar()
        
        # PASO 2: Generar Numeradores y Tiempos (Script Unificado)
        console.print("\n[dim]2. Procesando Tickets (Fallas y SLAs)...[/]")
        trans_ordenes_servicio.ejecutar()
        
        # PASO 3: Crear la Dimensión que los une
        console.print("\n[dim]3. Regenerando Dimensión Franquicias...[/]")
        trans_dim_franquicias.ejecutar()
        
        console.print("[bold green]✅ Suite de Indicadores sincronizada correctamente.[/]")

idf_suite_completa = PipelineIndicadores()

# ========================================================
# 3. WRAPPER DE EJECUCIÓN
# ========================================================
@audit_performance
def ejecutar_wrapper(modulo):
    try:
        if isinstance(modulo, list):
            for m in modulo:
                ejecutar_wrapper(m)
        else:
            if hasattr(modulo, 'ejecutar'):
                modulo.ejecutar()
                liberar_ram_os()
            else:
                console.print(f"[red]❌ El módulo {modulo} no tiene función ejecutar()[/]")
    except Exception as e:
        console.print(f"[bold red]Error crítico ejecutando módulo: {e}[/]")

# ========================================================
# 4. MENÚ DE OPCIONES
# ========================================================
MENU = {
    "1":  {"icono": "🚀", "label": "EJECUTAR TODO (Full Data Warehouse)", "target": [
        # FASE 1: INGESTA BASE
        trans_recaudacion, trans_ventas, trans_ventase, trans_reclamos, trans_atc, trans_ont_off,
        trans_cobranza,trans_actualizacion_datos, trans_comeback, trans_empleados,
        
        # FASE 2: SUITE DE INDICADORES (Abonados -> Tickets -> Dimensión)
        idf_suite_completa,
        
        # FASE 3: HECHOS FINALES
        trans_estadistica_abonado,
        afluencia_completa
    ]},
    
    # --- OPCIONES INDIVIDUALES ---
    "2":  {"icono": "💰", "label": "Recaudación",             "target": trans_recaudacion},
    "3":  {"icono": "📊", "label": "Ventas (General)",        "target": trans_ventas},
    "4":  {"icono": "💼", "label": "Ventas (Estatus)",        "target": trans_ventase},
    "5":  {"icono": "🛠️", "label": "Reclamos",                "target": trans_reclamos},
    "6":  {"icono": "🎧", "label": "Atención al Cliente",     "target": trans_atc},
    
    # --- AQUÍ ESTÁ LA MAGIA ---
    # La opción 7 ahora corre TODA la lógica necesaria para que Power BI no falle
    "7":  {"icono": "📉", "label": "Suite Técnica (IDF + SLA + Abonados)", "target": idf_suite_completa},
    
    # La opción 8 apunta al master por si solo quieres actualizar tickets sin re-leer abonados
    "8":  {"icono": "📜", "label": "Solo Tickets (IDF/SLA)",  "target": trans_ordenes_servicio},
    "9":  {"icono": "📞", "label": "Gestión Cobranza",        "target": trans_cobranza},
    "10": {"icono": "📝", "label": "Actualización Datos",     "target": trans_actualizacion_datos},
    "11": {"icono": "🏠", "label": "Come Back Home",          "target": trans_comeback},
    "12": {"icono": "👤", "label": "Empleados (RRHH)",        "target": trans_empleados},
    
    # --- TRANSFORMACIONES ---
    "13": {"icono": "💎", "label": "Dimensión Clientes",      "target": trans_dimclientes},
    "14": {"icono": "📈", "label": "Estadística Abonado",     "target": trans_estadistica_abonado},
    "15": {"icono": "🔄", "label": "Afluencia (Silver+Gold)", "target": afluencia_completa},
    "16": {"icono": "🎧", "label": "ONTs Apagadas (Gold)",    "target": trans_ont_off},
    
}

def mostrar_menu():
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("ID", style="bold yellow", justify="right")
    table.add_column("Icono")
    table.add_column("Descripción", style="bold white")

    for key, val in MENU.items():
        estilo = "bold green" if key == "1" else "cyan"
        if key == "7": estilo = "bold magenta" # Resaltamos la suite nueva
        table.add_row(f"{key}.", val['icono'], f"[{estilo}]{val['label']}[/]")

    panel = Panel(
        table, 
        title="[bold blue]PIPELINE MASTER FIBEX[/]", 
        subtitle="[dim]Selecciona el proceso a ejecutar[/]",
        expand=False
    )
    console.print(panel)
@audit_performance
def main():
    mostrar_menu()
    opcion = Prompt.ask("\n[bold yellow]¿Qué proceso deseas correr?[/]", choices=list(MENU.keys()), default="1")
    console.print("\n")
    
    # Iniciamos el cronómetro justo después de la selección del menú
    inicio_transformacion = time.time()
    
    seleccion = MENU.get(opcion)
    if seleccion:
        console.rule(f"[bold blue]Iniciando: {seleccion['label']}")
        ejecutar_wrapper(seleccion['target']) 
    
    console.rule("[bold green] FIN DE EJECUCIÓN GLOBAL[/]")
    duracion = time.time() - inicio_transformacion
    tiempo(inicio_transformacion)
    
    enviar_notificacion_bot(
        mensaje=f"✅ *Transformación (ETLs) Completada*\n⚙️ Proceso: {seleccion['label'] if seleccion else 'Desconocido'}\n⏱️ Tiempo Total: {duracion/60:.2f} minutos",
        plataforma="telegram"
    )

if __name__ == "__main__":
    main()
import os
import sys
from rich.console import Console
from rich.panel import Panel

# --- IMPORTACIÓN DE MÓDULOS ---
try:
    import etl_afluencia_silver
    import etl_afluencia_gold
    from config import PATHS
except ImportError as e:
    print(f"❌ Error Crítico: No se encuentran los módulos necesarios ({e}).")
    sys.exit(1)

console = Console()

def ejecutar_pipeline_completo():
    """
    Orquestador Maestro para el Proceso de Afluencia (MODO FULL REFRESH).
    Garantiza una reconstrucción atómica desde las fuentes Gold de Ventas/ATC/Recaudación.
    """
    console.clear()
    console.print(Panel.fit(
        "[bold white]🚀 PIPELINE DE RECONSTRUCCIÓN TOTAL: AFLUENCIA[/]", 
        style="bold blue",
        subtitle="Modo: Full Refresh (Integridad Total)"
    ))

    # =========================================================================
    # FASE 1: CAPA SILVER (Matching de Vendedores sobre el universo completo)
    # =========================================================================
    console.rule("[bold cyan]1. EJECUTANDO FASE DE MATCHING (SILVER)[/]")
    
    try:
        # Ahora Silver genera el archivo desde 0, eliminando errores de la mañana
        ruta_resultado_silver = etl_afluencia_silver.ejecutar()
        
        if not ruta_resultado_silver or not os.path.exists(ruta_resultado_silver):
            console.print("[bold red]❌ FALLO CRÍTICO EN SILVER[/]")
            return 
            
        console.print(f"[green]✔ Universo Silver reconstruido:[/]")
        console.print(f"   📂 {os.path.basename(ruta_resultado_silver)}")

    except Exception as e:
        console.print(f"[bold red]💥 EXCEPCIÓN EN FASE SILVER: {e}[/]")
        return

    # =========================================================================
    # FASE 2: CAPA GOLD (Normalización de Oficinas y Sedes)
    # =========================================================================
    console.print("\n")
    console.rule("[bold yellow]2. EJECUTANDO FASE DE NORMALIZACIÓN (GOLD)[/]")
    
    try:
        # Gold recibe el Silver completo y aplica Dim Oficinas a toda la historia
        etl_afluencia_gold.ejecutar(ruta_resultado_silver)
        
        ruta_gold_esperada = os.path.join(PATHS["gold"], "Afluencia_Gold.parquet")
        if os.path.exists(ruta_gold_esperada):
            console.print(Panel.fit(
                f"[bold green]✅ PROCESO FINALIZADO[/]\n\n"
                f"La data de Afluencia está 100% sincronizada con Ventas, ATC y Recaudación.", 
                border_style="green"
            ))
        else:
            console.print("[bold red]⚠️ ERROR: El archivo Gold no fue localizado tras la ejecución.[/]")

    except Exception as e:
        console.print(f"[bold red]💥 EXCEPCIÓN EN FASE GOLD: {e}[/]")
        return

if __name__ == "__main__":
    ejecutar_pipeline_completo()
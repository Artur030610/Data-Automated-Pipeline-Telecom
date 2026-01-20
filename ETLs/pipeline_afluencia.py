import os
import sys
from rich.console import Console
from rich.panel import Panel

# --- IMPORTACIÓN DE MÓDULOS DEL PROYECTO ---
try:
    import etl_afluencia_silver
    import etl_afluencia_gold
    from config import PATHS
except ImportError as e:
    print(f"❌ Error Crítico: No se encuentran los módulos necesarios ({e}).")
    print("Verifica que etl_afluencia_silver.py, etl_afluencia_gold.py y config.py estén en la carpeta.")
    sys.exit(1)

console = Console()

def ejecutar_pipeline_completo():
    """
    Orquestador Maestro para el Proceso de Afluencia.
    Garantiza que Silver y Gold se ejecuten en secuencia y compartan los recursos.
    """
    console.clear()
    console.print(Panel.fit("[bold white]🚀 PIPELINE DE PROCESAMIENTO: AFLUENCIA[/]", style="bold blue"))

    # =========================================================================
    # FASE 1: CAPA SILVER (Limpieza y Consolidación)
    # =========================================================================
    console.rule("[bold cyan]1. EJECUTANDO CAPA SILVER[/]")
    
    try:
        # Ejecutamos Silver y capturamos la ruta del archivo que genera
        ruta_resultado_silver = etl_afluencia_silver.ejecutar()
        
        if not ruta_resultado_silver or not os.path.exists(ruta_resultado_silver):
            console.print("[bold red]❌ FALLO EN SILVER:[/]")
            console.print("   El proceso terminó pero no devolvió un archivo válido.")
            console.print("   Revise los logs de la fase Silver.")
            return # Detenemos todo, no tiene sentido seguir
            
        console.print(f"[green]✔ Silver completado. Archivo generado:[/]")
        console.print(f"   📂 {ruta_resultado_silver}")

    except Exception as e:
        console.print(f"[bold red]💥 EXCEPCIÓN NO CONTROLADA EN SILVER: {e}[/]")
        return

    # =========================================================================
    # FASE 2: CAPA GOLD (Enriquecimiento SCD y Normalización)
    # =========================================================================
    console.print("\n")
    console.rule("[bold yellow]2. EJECUTANDO CAPA GOLD[/]")
    
    try:
        # Le pasamos explícitamente el archivo que acabamos de crear
        # Esto elimina cualquier ambigüedad de rutas o versiones viejas
        etl_afluencia_gold.ejecutar(ruta_resultado_silver)
        
        # Verificación final
        ruta_gold_esperada = os.path.join(PATHS["gold"], "Afluencia_Gold.parquet")
        if os.path.exists(ruta_gold_esperada):
            console.print(Panel.fit(f"[bold green]✅ PROCESO COMPLETADO EXITOSAMENTE[/]\n\nOutput Final:\n{ruta_gold_esperada}", border_style="green"))
        else:
            console.print("[bold red]⚠️  ALERTA: El Gold se ejecutó pero no veo el archivo final.[/]")

    except Exception as e:
        console.print(f"[bold red]💥 EXCEPCIÓN NO CONTROLADA EN GOLD: {e}[/]")
        return

if __name__ == "__main__":
    ejecutar_pipeline_completo()
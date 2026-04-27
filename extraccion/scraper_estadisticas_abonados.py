import os
import sys
import datetime
import re
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, ejecutar_descarga

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_estadisticas_abonados(fecha_inicial_str: str, fecha_final_str: str):
    
    today = datetime.datetime.today()
    console.print(f" [bold cyan] Estadisticas desde SAE ({fecha_inicial_str} al {fecha_final_str})...[/]")
    console.print(f" [bold cyan]🎧 Iniciando extracción de Estadisticas abonado ({today.strftime('%d-%m-%Y')})...[/]")
    
    # =========================================================
    # 1. FECHAS Y NOMBRES DE ARCHIVO
    # =========================================================
    
    
    nombre_archivo = f"Data_Abonados{today.strftime('%d%m%Y')}.xlsx"
    
    # =========================================================
    # 2. DEFINIMOS LA RUTA DESTINO EXACTA (Ajustar a tu Data Lake)
    # =========================================================
    ruta_destino_dir = str(PATHS.get("raw_estad_abonados"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # =========================================================
    # 3. PLAYWRIGHT: NAVEGACIÓN Y EXTRACCIÓN
    # =========================================================
    with sync_playwright() as p:
        # IMPORTANTE: headless=False para poder ver y usar el page.pause()
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        # --- LOGIN (Reutilizando utilería) ---
        login_sae(page)

        page.wait_for_timeout(1000)
        # --- NAVEGACIÓN INTELIGENTE (Ajustar según tu SAE) ---
        console.print("🧭 Navegando al Reporte de Estadísticas de Abonados...")
        page.get_by_role("link", name=re.compile(r"Estadísticas|Estadistica", re.IGNORECASE)).click()
        page.wait_for_timeout(1000)
        btn_estadistica = page.get_by_role("link", name="Estadistica De Abonados")
        btn_estadistica.wait_for(state="attached", timeout=60000)
        btn_estadistica.click()
        page.wait_for_timeout(500)
        btn_gen_graph = page.get_by_role("button", name=re.compile(r"Generar Gráfica", re.IGNORECASE))
        btn_gen_graph.wait_for(state="attached", timeout=60000)
        btn_gen_graph.click()
        ejecutar_descarga(page,
                          ruta_destino,locator_descarga=page.locator("#resumen_grafica_wrapper").get_by_role("link").filter(has_text=re.compile(r"^$")))
        browser.close()

if __name__ == "__main__":
    # Fechas de prueba
    descargar_estadisticas_abonados("12/04/2026", "18/04/2026")
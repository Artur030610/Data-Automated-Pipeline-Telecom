import os
import sys
import datetime
import re
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, descargar_listado_llamadas

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_cobranza(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f" [bold cyan]🎧 Iniciando extracción de Llamadas de Cobranza ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    # =========================================================
    # 1. FECHAS Y NOMBRES DE ARCHIVO
    # =========================================================
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - OCobranza {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    # =========================================================
    # 2. DEFINIMOS LA RUTA DESTINO EXACTA (Ajustar a tu Data Lake)
    # =========================================================
    ruta_destino_dir = str(PATHS.get("raw_cobranza"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # =========================================================
    # 3. PLAYWRIGHT: NAVEGACIÓN Y EXTRACCIÓN
    # =========================================================
    with sync_playwright() as p:
        # IMPORTANTE: headless=False para poder ver y usar el page.pause()
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        # --- LOGIN (Reutilizando utilería) ---
        login_sae(page)

        descargar_listado_llamadas(
            page=page,
            fecha_inicial_str=fecha_inicial_str,
            fecha_final_str=fecha_final_str,
            ruta_destino=ruta_destino,
            tipo_llamada="GESTION COBRANZA OC"
        )
        
        browser.close()

if __name__ == "__main__":
    # Fechas de prueba
    descargar_cobranza("24/03/2026", "25/03/2026")
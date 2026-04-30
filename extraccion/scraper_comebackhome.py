import os
import sys
import datetime
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, descargar_listado_llamadas

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_comebackhome(fecha_inicial_str: str, fecha_final_str: str):
    """
    Descarga el reporte 'Comebackhome' (Campaña de Vuelve a Casa) basado en el Listado de Llamadas.
    """
    console.print(f" [bold magenta]🏠 Iniciando extracción de Comebackhome ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - CBH {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    # Si no tienes la ruta "raw_comeback" en tu config.py, puedes agregarla. Por defecto usa esta:
    ruta_destino_dir = str(PATHS.get("raw_comeback", os.path.join(parent_dir, "data", "raw_comeback")))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        login_sae(page)

        # =========================================================
        # LLAMADA AL MÓDULO ABSTRAÍDO DE LISTADO DE LLAMADAS
        # Nota: Ajusta los parámetros de abajo (tipo_llamada, etc.) 
        # exactamente a cómo se llaman en el desplegable de SAE.
        # =========================================================
        page.pause()
        descargar_listado_llamadas(
            page=page,
            fecha_inicial_str=fecha_inicial_str,
            fecha_final_str=fecha_final_str,
            ruta_destino=ruta_destino,
            tipo_llamada="VUELVE A CASA",         
            tipo_respuesta=None,
            detalle_respuesta=None
        )
        
        browser.close()

if __name__ == "__main__":
    descargar_comebackhome("10/04/2026", "30/04/2026")
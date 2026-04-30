import os
import sys
import datetime
import re
from playwright.sync_api import sync_playwright

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console
from scraper_utils import login_sae, ejecutar_descarga

@reportar_tiempo
def descargar_empleados(fecha_inicial_str: str = None, fecha_final_str: str = None): #type: ignore
    console.print(" [bold cyan]👤 Iniciando extracción de Empleados (Datos del día)...[/]")
    
    # El reporte de empleados muestra la plantilla actual, por lo que usamos solo la fecha de hoy
    hoy = datetime.datetime.today().strftime('%d-%m-%Y')
    nombre_archivo = f"Data - Empleados {hoy}.xlsx"
    
    # Guardamos en la ruta "raw_empleados" configurada en PATHS
    ruta_destino_dir = str(PATHS.get("raw_empleados"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        # 1. Login
        login_sae(page)
        # 2. Navegación usando los roles extraídos del codegen
        console.print("🧭 Navegando a Configuración -> Administrar Empleados...")
        btn_config = page.get_by_role("link", name=re.compile(r"Configuración", re.IGNORECASE)).first
        btn_config.wait_for(state="attached", timeout=5000)
        btn_config.evaluate("node => node.click()")
        page.wait_for_timeout(1000)
        
        btn_empleados = page.get_by_role("link", name=re.compile(r"Administrar Empleados", re.IGNORECASE)).first
        btn_empleados.wait_for(state="visible", timeout=5000)
        btn_empleados.click()

        console.print("⏳ Esperando a que cargue la tabla...")
        # Esperamos a que la primera celda de la tabla tenga datos reales (evita clics prematuros)

        page.locator("tbody td").first.wait_for(state="attached", timeout=60000)
        
        # 3. Configuración de columnas (Lógica Autónoma)
        console.print("⚙️ Configurando columnas y refrescando tabla de Empleados...")
        page.get_by_role("button", name=re.compile(r"COLUMNAS", re.IGNORECASE)).click()
        page.wait_for_timeout(500)
        
        # Como no existe el botón "Todos", marcamos las columnas requeridas manualmente
        try:
            page.get_by_role("option", name=re.compile(r"Franquicia Cobrador")).click(timeout=2000)
        except Exception: pass
        
        try:
            page.locator("a").filter(has_text=re.compile(r"^Oficina$")).click(timeout=2000)
        except Exception: pass
        
        try:
            page.get_by_role("option", name=re.compile(r"Tipo Cobranza")).click(timeout=2000)
        except Exception: pass

        page.keyboard.press("Escape")
        page.wait_for_timeout(500)
        page.get_by_title("Refrescar la tabla").click()
        page.wait_for_timeout(2000)

        # 4. Seleccionar "Todos" los registros (Lógica Autónoma)
        console.print("🗂️ Seleccionando 'Todos' los registros de la tabla...")
        combobox = page.get_by_label("Mostrar 102550100200Todos")
        
        #combobox.wait_for(state="attached", timeout=15000)
        
        try:
            combobox.select_option('-1', timeout=30000)
        except Exception:
            page.get_by_label("Mostrar 102550100200Todos").select_option('-1')
            
        page.locator("tbody td").first.wait_for(state="visible", timeout=30000)
        page.wait_for_timeout(1000)

        # 5. Descargar delegando a la función genérica solo el traslado del archivo
        btn_descarga = page.locator(".buttons-excel").first
        ejecutar_descarga(page=page, ruta_destino=ruta_destino, locator_descarga=btn_descarga, seleccionar_todos=False)
        
        browser.close()

if __name__ == "__main__":
    descargar_empleados()
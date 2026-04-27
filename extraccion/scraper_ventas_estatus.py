import os
import sys
import datetime
import re
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, ejecutar_descarga, llenar_fechas_sae

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_ventas_estatus(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f" [bold cyan]🚀 Iniciando extracción de Ventas Estatus ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    # =========================================================
    # 1. TRADUCCIÓN: Text.ConvertTextToDateTime
    # Convertimos los textos "DD/MM/YYYY" a objetos Date de Python
    # =========================================================
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - Ventas estatus {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    # =========================================================
    # 2. DEFINIMOS LA RUTA DESTINO EXACTA (Tu Data Lake)
    # =========================================================
    ruta_destino_dir = str(PATHS.get("ventas_estatus"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # =========================================================
    # 3. PLAYWRIGHT: NAVEGACIÓN Y DESCARGA DIRECTA
    # =========================================================
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        
        # Configuramos el contexto para aceptar descargas automáticamente
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        # ================== MAIN.TXT (LOGIN) ==================
        login_sae(page)

        # ================== SUBFLOW1.TXT (NAVEGACIÓN Y FILTROS) ==================
        print("🧭 Navegando al Reporte de Ventas...")
        page.get_by_role("link", name=re.compile(r"Reportes", re.IGNORECASE)).click()
        
        # ESPERA DINÁMICA: Ignoramos la animación CSS inyectando el clic directo al DOM
        btn_reporte = page.get_by_role("link", name="Reporte De Ventas")
        btn_reporte.wait_for(state="attached", timeout=5000)
        btn_reporte.evaluate("node => node.click()")
        print("🔍 Aplicando filtros (Tipo de Venta y Oficinas)...")
        # Filtro: VENTA INICIAL
        page.get_by_role("button", name="Seleccione...").nth(2).click()
        page.get_by_role("combobox").get_by_role("option", name="VENTA INICIAL").click()
        
        # Filtro: Oficinas (Todos)
        page.get_by_role("button", name="Seleccione...").nth(2).click()
        page.get_by_role("textbox", name="Search").fill("ofi")
        page.get_by_role("button", name="Todos").click()
        
        # Cerrar el menú desplegable para que no bloquee el botón de búsqueda
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)

        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        
        llenar_fechas_sae(
            page, 
            fecha_inicial_str, 
            fecha_final_str, 
            id_desde=page.get_by_role("textbox", name="Desde").first, 
            id_hasta=page.get_by_role("textbox", name="Hasta").first
        )
        
        print("🚀 Ejecutando búsqueda, esperando resultados...")
        # Forzar clic por JS para evitar cualquier intercepción visual
        page.locator("button#btn-rep-libroventa-cob").evaluate("node => node.click()")
        
        # ================== SELECCIÓN DE COLUMNAS ==================
        print("⏳ [SUBFLOW 2] Esperando a que el servidor devuelva los resultados...")
        
        try:
            # 1. Esperamos a que la tabla devuelva resultados (la paginación aparece cuando hay datos)
            page.wait_for_selector("select[name=\"datagrid1_length\"]", state="visible", timeout=60000)
            
            print("⚙️ [SUBFLOW 2] Desplegando menú de columnas...")
            # 2. ENFOQUE ESTRUCTURAL: Ignoramos el texto. Apuntamos al botón desplegable dentro de su contenedor y forzamos el primero.
            boton_columnas = page.locator("#boton_select_columnas button.dropdown-toggle").first
            boton_columnas.click()
            # 3. Al ser un control 'bootstrap-select', el menú usa la clase explícita 'bs-select-all' para el botón 'Todos'
            boton_todos = page.locator("button.bs-select-all:visible").first
            boton_todos.wait_for(state="visible", timeout=5000)
            boton_todos.click()
            
            page.keyboard.press("Escape") 
            page.wait_for_timeout(500)
            # Pausa para que veamos el proceso de cierre del menú y refresco de columnas
            print("🔄 Refrescando la tabla...")
            # 4. El botón de refrescar tiene un ID explícito muy robusto
            page.locator("button#refrescar").click()
            page.wait_for_timeout(3000) # Dejamos 3 segundos reales para asegurar que lleguen los datos del servidor

            mostrar = page.locator("select[name=\"datagrid1_length\"]")
            mostrar.select_option(label="Todos")
            page.wait_for_timeout(1000) # Pausa para asimilar que se seleccionó 'Todos' en la 2da tabla
        except Exception as e:
            print(f"⚠️ Aviso al configurar columnas: {e}")

        # ================== SUBFLOW2.TXT (DESCARGA) ==================
        ejecutar_descarga(
            page=page,
            ruta_destino=ruta_destino,
            locator_descarga=page.get_by_role("link").filter(has_text=re.compile(r"^$")).nth(2)
        )
        
        # TRADUCCIÓN: WebAutomation.CloseWebBrowser
        browser.close()

if __name__ == "__main__":
    # Prueba de concepto con fechas dummy
    descargar_ventas_estatus("01/01/2026", "15/01/2026")

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
    
    # Generamos el nombre exacto que pedía tu script de PAD:
    # "Data - Ventas estatus DD-MM al DD-MM.xlsx"
    nombre_archivo = f"Data - Ventas estatus {f_ini.day}-{f_ini.month} al {f_fin.day}-{f_fin.month}.xlsx"
    
    # =========================================================
    # 2. DEFINIMOS LA RUTA DESTINO EXACTA (Tu Data Lake)
    # =========================================================
    user_profile = os.environ.get("USERPROFILE")
    ruta_destino = os.path.join(
        user_profile,
        "Documents", "A-DataStack", "01-Proyectos", "01-Data_PipelinesFibex", 
        "02_Data_Lake", "raw_data", "2-Ventas", "1- Ventas Estatus", 
        nombre_archivo
    )
    
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

        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        
        # FECHA DESDE: Limpiamos con Control+A y Backspace, luego tipeamos para que la máscara no falle
        loc_desde = page.get_by_role("textbox", name="Desde")
        loc_desde.click()
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        loc_desde.press_sequentially(fecha_inicial_str, delay=100)
        
        # FECHA HASTA
        loc_hasta = page.get_by_role("textbox", name="hasta")
        loc_hasta.click()
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        loc_hasta.press_sequentially(fecha_final_str, delay=100)
        
        print("🚀 Ejecutando búsqueda, esperando resultados...")
        page.locator("button#btn-rep-libroventa-cob").click()
        
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
            
            print("🔄 Refrescando la tabla...")
            # 4. El botón de refrescar tiene un ID explícito muy robusto
            page.locator("button#refrescar").click()
            page.wait_for_timeout(3000) # Dejamos 3 segundos reales para asegurar que lleguen los datos del servidor
            
        except Exception as e:
            print(f"⚠️ Aviso al configurar columnas: {e}")

        # ================== SUBFLOW2.TXT (DESCARGA) ==================
        ejecutar_descarga(
            page=page, 
            ruta_destino=ruta_destino, 
            id_tabla="datagrid1"
        )
        
        # TRADUCCIÓN: WebAutomation.CloseWebBrowser
        browser.close()

if __name__ == "__main__":
    # Prueba de concepto con fechas dummy
    descargar_ventas_estatus("01/01/2026", "15/01/2026")

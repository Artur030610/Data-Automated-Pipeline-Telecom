import os
import sys
import datetime
import re
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_atc(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f" [bold cyan]🎧 Iniciando extracción de Atención al Cliente ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    # =========================================================
    # 1. FECHAS Y NOMBRES DE ARCHIVO
    # =========================================================
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - OCobranza {f_ini.day}-{f_ini.month}-{f_ini.year} al {f_fin.day}-{f_fin.month}-{f_fin.year}.xlsx"
    
    # =========================================================
    # 2. DEFINIMOS LA RUTA DESTINO EXACTA (Ajustar a tu Data Lake)
    # =========================================================
    user_profile = os.environ.get("USERPROFILE")
    ruta_destino = os.path.join(
        user_profile,
        "Documents", "A-DataStack", "01-Proyectos", "01-Data_PipelinesFibex", 
        "02_Data_Lake", "raw_data", "11-Act. de Datos", "2-OOCC",
        nombre_archivo
    )
    
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

        # --- NAVEGACIÓN ---
        print("🧭 Navegando al Reporte de Atención al Cliente...")
        page.get_by_role("link", name=re.compile(r"Reportes", re.IGNORECASE)).click()
        
        # Ajusta el nombre exacto del menú si es distinto (ej. "Reporte De Atención", "Tickets", etc.)
        btn_reporte = page.get_by_role("link", name=re.compile(r"Listado De LLamadas", re.IGNORECASE)).first

        btn_reporte.wait_for(state="attached", timeout=5000)
        btn_reporte.evaluate("node => node.click()")
        
        # --- LLENADO DE FILTROS ---
        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        
        # FECHA DESDE: Usamos el método seguro de tipeo
        try:
            loc_desde = page.locator("#desde_llamada")
            loc_desde.click()
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            loc_desde.press_sequentially(fecha_inicial_str, delay=50)
            
            # FECHA HASTA
            loc_hasta = page.locator("#hasta_llamada")
            loc_hasta.click()
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            loc_hasta.press_sequentially(fecha_final_str, delay=50)
        except Exception as e:
            print(f"⚠️ Alerta llenando fechas, revisa los selectores: {e}")
        #Click para evitar solapar la visibilidad del botón de buscar con los campos de fecha
        print("🚀 Ejecutando búsqueda...")
        page.keyboard.press("Escape")
        page.locator("body").click(position={"x": 0, "y": 0})
        page.wait_for_timeout(300) 
        page.pause()
        page.locator("#id_tll").select_option(label="GESTION OFICINA COMERCIAL")
        page.wait_for_timeout(300) 
        page.locator("#id_trl").select_option(label="ACTUALIZACIÓN")
        btn_buscar = page.get_by_role("button", name=re.compile(r"Buscar", re.IGNORECASE)).first
        btn_buscar.click()
        

        print("⏳ Esperando resultados y configurando columnas...")
        try:
             # Pausa para revisar que los datos hayan cargado antes de configurar columnas
            page.wait_for_timeout(500)
            
            page.get_by_role("button", name=re.compile(r"COLUMNAS", re.IGNORECASE)).click()
            page.wait_for_timeout(500)
            page.get_by_role("button", name=re.compile(r"Todos", re.IGNORECASE)).click()
            page.wait_for_timeout(500)
            page.get_by_title("Refrescar la tabla").click()
            page.wait_for_timeout(500)
            print("🔄 Refrescando la tabla...")
            page.get_by_role("gridcell").first.wait_for(state="visible", timeout=60000) # Dejamos 3 segundos para asegurar que lleguen los datos
           
            print("🗂️ Seleccionando 'Todos' los registros de la tabla...")
            # Implementando tu descubrimiento del combobox
            page.get_by_role("combobox", name="Mostrar registros").select_option(label="Todos")
            page.get_by_role("gridcell").first.wait_for(state="visible", timeout=60000)# Pausa breve para que se rendericen todas las filas
            
        except Exception as e:
            print(f"⚠️ Aviso al configurar tabla: {e}")
            page.pause() # Pausa para revisar que las columnas se hayan marcado correctamente
        # ================== DESCARGA DIRECT A E INDEPENDIENTE ==================
        print("📥 Interceptando y trasladando descarga a Excel...")
        try:
            if os.path.exists(ruta_destino):
                os.remove(ruta_destino)
        except Exception:
            pass

        with page.expect_download(timeout=120000) as download_info:
            # Selector múltiple y robusto (Clase genérica de DataTables o ID SAE común)
            try:
                page.locator(".buttons-excel, button#exportar_xlsx").first.click(timeout=5000)
            except Exception:
                # Fallback al link vacío que habías descubierto originalmente
                page.get_by_role("link").filter(has_text=re.compile(r"^$")).nth(1).click()
                
        download = download_info.value
        os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
        download.save_as(ruta_destino) # <-- Función nativa que traslada el archivo a la carpeta
        print(f"✅ Archivo trasladado y guardado exitosamente en:\n   {ruta_destino}")
        
        browser.close()

if __name__ == "__main__":
    # Fechas de prueba
    descargar_atc("24/03/2026", "25/03/2026")
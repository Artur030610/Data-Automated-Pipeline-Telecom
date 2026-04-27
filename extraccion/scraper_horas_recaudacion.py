import os
import sys
import datetime
import re
import pandas as pd
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, ejecutar_descarga, llenar_fechas_sae

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, EXCLUSIONES_RECAUDACION
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_recaudacion(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f"[bold cyan]💰 Iniciando extracción de Recaudación ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    # Ajusta el nombre según tu convención para Recaudación
    nombre_archivo = f"Data - HorasPago {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    ruta_destino_dir = str(PATHS.get("raw_horaspago"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    #Utilizacion de Headless en True para evitar que se abra la ventana del navegador durante la ejecución del scraper.
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        login_sae(page)

        print("🧭 Navegando al Reporte de Cobranza...")
        page.get_by_role("link", name=" Reportes").click()
        
        # Espera dinámica del submenú
        btn_cobranza = page.get_by_role("link", name=re.compile("Reporte De Cobranza", re.IGNORECASE))
        btn_cobranza.wait_for(state="visible", timeout=5000)
        btn_cobranza.click()

        print("🏢 Configurando Franquicia y Cajas...")
        try:
            print("   -> Desplegando Franquicias...")
            page.wait_for_timeout(1500) # Pausa vital para que Bootstrap-Select asimile los eventos JS
            
            # 1. Franquicia -> UN SOLO CLIC directo al data-id
            franquicia = page.locator("button[data-id='id_franq_cob']")
            franquicia.wait_for(state="visible", timeout=15000)
            franquicia.evaluate("node => node.click()")
            
            # Esperamos la lista y damos clic en "Seleccionar Todos"
            page.locator("ul.dropdown-menu.inner:visible li").first.wait_for(state="visible", timeout=15000)
            page.locator("button.bs-select-all:visible").first.click()
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
            
            print("   -> Desplegando Oficinas...")
            # 2. Oficinas -> Buscamos la etiqueta "Oficina", subimos al contenedor padre y damos clic a su botón
            boton_oficina = page.locator("label").filter(has_text=re.compile(r"Oficina", re.IGNORECASE)).locator("..").get_by_role("button").first
            boton_oficina.wait_for(state="visible", timeout=15000)
            boton_oficina.evaluate("node => node.click()")
            
            # Esperamos lista y damos clic en "Seleccionar Todos"
            page.locator("ul.dropdown-menu.inner:visible li").first.wait_for(state="visible", timeout=15000)
            page.locator("button.bs-select-all:visible").first.click()
            page.wait_for_timeout(500)
            
            print("   -> Aplicando exclusiones de Oficinas...")
            exclusiones = EXCLUSIONES_RECAUDACION
            
            buscador = page.locator("div.bs-searchbox:visible input[type='text']").first
            
            for exc in exclusiones:
                buscador.fill(exc)
                page.wait_for_timeout(600) # Pausa crucial para permitir que la animación de filtrado termine
                
                opciones_a_destildar = page.locator("ul.dropdown-menu.inner:visible li.selected:visible a")
                while opciones_a_destildar.count() > 0:
                    opciones_a_destildar.first.evaluate("node => node.click()")
                    page.wait_for_timeout(100)
                    
                page.wait_for_timeout(200)
                
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
        except Exception as e:
            print(f"⚠️ Aviso al configurar Cajas/Franquicias: {e}")

        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        
        llenar_fechas_sae(page, fecha_inicial_str, fecha_final_str, id_desde="input#desde2", id_hasta="input#hasta2")
        
        print("📋 Seleccionando Tipo de Reporte...")
        page.locator("#tipo_reporte").select_option(value="buscar_rep_libroventa_cob()")
        page.wait_for_timeout(500)
        
        print("🚀 Ejecutando búsqueda, esperando resultados...")
        # Selector recomendado por Playwright, usando regex para ignorar el ícono de lupa
        btn_buscar = page.get_by_role("button", name=re.compile("Reporte", re.IGNORECASE)).first
        btn_buscar.wait_for(state="visible", timeout=10000)
        # Usamos Javascript nativo para eludir bloqueos de eventos en Playwright
        btn_buscar.evaluate("node => node.click()")
        
        # ================== SELECCIÓN DE COLUMNAS ESPECÍFICAS ==================
        print("⏳ [SUBFLOW 2] Esperando a que el servidor devuelva los resultados...")
        
        try:
            # Esperamos que aparezca el botón de Excel como indicativo de que cargó la data
            page.wait_for_selector("button#exportar_xlsx", state="visible", timeout=60000)
            
            print("⚙️ [SUBFLOW 2] Configurando columnas específicas...")
            # El ID del contenedor de columnas específico de este reporte
            boton_columnas=page.get_by_role("button", name=re.compile(r"COLUMNAS", re.IGNORECASE))
            boton_columnas.wait_for(state="visible", timeout=5000)
            boton_columnas.click()
            page.wait_for_timeout(500)
            
            
            # 1. Clic en el botón de "Ninguno" para limpiar (Deselect All)
            boton_ninguno = page.locator("button.bs-deselect-all:visible").first
            boton_ninguno.wait_for(state="visible", timeout=5000)
            boton_ninguno.click()
            page.wait_for_timeout(500)
            
            # 2. Lista de las columnas EXACTAS requeridas por tu ETL (recaudacion.py)
            columnas_requeridas = [
                "N° Abonado", "Documento", "Nro Recibo", "Fecha", "Hora de Pago", "Oficina Cobro", "Suscripción"
            ]
            
            # 3. Marcamos cada columna individualmente
            menu_visible = page.locator(".dropdown-menu.open:visible").first
            for col in columnas_requeridas:
                try:
                    # Filtramos ignorando espacios al inicio/final y forzamos el clic con JS
                    opcion = menu_visible.locator("span.text").filter(has_text=re.compile(f"^\\s*{re.escape(col)}\\s*$", re.IGNORECASE)).first
                    opcion.wait_for(state="attached", timeout=1500)
                    opcion.evaluate("node => node.click()")
                except Exception:
                    print(f"⚠️ Aviso: No se encontró o falló la columna '{col}', omitiendo...")
            
            page.keyboard.press("Escape") 
            page.wait_for_timeout(500)
            
            print("🔄 Refrescando la tabla...")
            btn_buscar.evaluate("node => node.click()")
            page.wait_for_timeout(1000)
            try:
                # Esperamos que la API asimile el refresco antes de intentar descargar
                page.wait_for_selector("div.dataTables_processing", state="hidden", timeout=30000)
            except Exception:
                page.wait_for_timeout(3000)
            
        except Exception as e:
            print(f"⚠️ Aviso al configurar columnas: {e}")

        # ================== DESCARGA DIRECTA ==================
        print("📥 [SUBFLOW 2] Interceptando descarga directa a Excel...")
        try:
            if os.path.exists(ruta_destino):
                os.remove(ruta_destino)
        except Exception:
            pass

        with page.expect_download(timeout=300000) as download_info: # 5 minutos para meses completos
            # Forzamos el clic vía JS nativo para eludir bloqueos del DOM en Playwright
            page.locator("button#exportar_xlsx").evaluate("node => node.click()")
            
        download = download_info.value
        os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
        
        # 1. Guardar temporalmente el archivo crudo (.xls)
        ruta_temporal = ruta_destino.replace(".xlsx", ".xls")
        download.save_as(ruta_temporal)
        
        # 2. Conversión automática a .xlsx real usando Pandas
        print("🔄 Convirtiendo formato nativo de SAE a XLSX puro...")
        try:
            try:
                # Intento 1: Leer como tabla HTML disfrazada
                df_descarga = pd.read_html(ruta_temporal, decimal=',', thousands='.')[0]
            except Exception:
                # Intento 2: Leer con motor Calamine (soporta XLS/XML antiguos)
                df_descarga = pd.read_excel(ruta_temporal, engine="calamine")
                
            # Limpiamos columnas basura ocultas si es que la web las generó
            df_descarga = df_descarga.loc[:, ~df_descarga.columns.str.contains('^Unnamed')]
            df_descarga.to_excel(ruta_destino, index=False)
            os.remove(ruta_temporal)
            print(f"✅ [SUBFLOW 2] Archivo convertido y guardado exitosamente en:\n   {ruta_destino}")
        except Exception as e:
            print(f"⚠️ Error convirtiendo a XLSX: {e}. Se conservará en el formato original.")
            download.save_as(ruta_destino)
        
        browser.close()

if __name__ == "__main__":
    descargar_recaudacion("20/03/2026", "23/03/2026")

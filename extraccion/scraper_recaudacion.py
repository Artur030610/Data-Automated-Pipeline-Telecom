import os
import sys
import datetime
import re
import pandas as pd
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, ejecutar_descarga

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_recaudacion(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f"[bold cyan]💰 Iniciando extracción de Recaudación ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    # Ajusta el nombre según tu convención para Recaudación
    nombre_archivo = f"Data - Recaudacion {f_ini.day}-{f_ini.month}-{f_ini.year} al {f_fin.day}-{f_fin.month}-{f_fin.year}.xlsx"
    
    user_profile = os.environ.get("USERPROFILE")
    ruta_destino = os.path.join(
        user_profile,
        "Documents", "A-DataStack", "01-Proyectos", "01-Data_PipelinesFibex", 
        "02_Data_Lake", "raw_data", "1-Recaudación", 
        nombre_archivo
    )
    #Utilizacion de Headless en True para evitar que se abra la ventana del navegador durante la ejecución del scraper.
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) 
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
            # 1. Franquicia -> UN SOLO CLIC directo al data-id
            page.locator("button[data-id='id_franq_cob']").click()
            
            # Esperamos la lista y damos clic en "Seleccionar Todos"
            page.locator("ul.dropdown-menu.inner:visible li").first.wait_for(state="visible", timeout=60000)
            page.locator("button.bs-select-all:visible").first.click()
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
            
            print("   -> Desplegando Cajas...")
            # 2. Cajas -> UN SOLO CLIC al segundo botón
            page.locator("div#tab-content-puntoc button.dropdown-toggle").nth(1).click()
            
            # Esperamos lista y damos clic en "Seleccionar Todos"
            page.locator("ul.dropdown-menu.inner:visible li").first.wait_for(state="visible", timeout=60000)
            page.locator("button.bs-select-all:visible").first.click()
            page.wait_for_timeout(500)
            
            print("   -> Aplicando exclusiones de Cajas...")
            exclusiones = ["unicenter", "virtua", "virna", "externa", "fideliz", "compensacion"]
            
            buscador = page.locator("div.bs-searchbox:visible input[type='text']").first
            btn_ninguno = page.locator("button.bs-deselect-all:visible").first
            
            for exc in exclusiones:
                buscador.fill(exc)
                page.wait_for_timeout(300)
                btn_ninguno.click()
                page.wait_for_timeout(200)
                
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
        except Exception as e:
            print(f"⚠️ Aviso al configurar Cajas/Franquicias: {e}")
            # Si falla, se pausará automáticamente para que veamos por qué
            page.pause()

        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        
        # FECHA DESDE (Usando IDs extractados de PAD)
        loc_desde = page.locator("input#desde2")
        loc_desde.click()
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        loc_desde.press_sequentially(fecha_inicial_str, delay=50)
        
        # FECHA HASTA
        loc_hasta = page.locator("input#hasta2")
        loc_hasta.click()
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        loc_hasta.press_sequentially(fecha_final_str, delay=50)
        
        print("📋 Seleccionando Tipo de Reporte...")
        page.locator("select#tipo_reporte").select_option(label="DETALLE FORMA PAGO")
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
            boton_columnas = page.locator("#boton_select_columnas_detalle_forma_pago button.dropdown-toggle").first
            boton_columnas.click()
            
            # 1. Clic en el botón de "Ninguno" para limpiar (Deselect All)
            boton_ninguno = page.locator("button.bs-deselect-all:visible").first
            boton_ninguno.wait_for(state="visible", timeout=5000)
            boton_ninguno.click()
            page.wait_for_timeout(500)
            
            # 2. Lista de las columnas EXACTAS requeridas por tu ETL (recaudacion.py)
            columnas_requeridas = [
                "N° Abonado", "Nro Recibo", "Fecha",
                "Total Pago", "Total Pago Bs", "Estatus Pago",
                "Forma de Pago", "Monto Forma Pago", "Tasa de cambio del día", "Banco",
                "Referencia", "Cobrador", "Nombre Caja", "Oficina Cobro",
                "Fecha Contrato", "Estatus", "Suscripción", "Etiqueta",
                "Grupo Afinidad", "Nombre Franquicia", "Ciudad"
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

        with page.expect_download(timeout=120000) as download_info:
            page.locator("button#exportar_xlsx").click()
            
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

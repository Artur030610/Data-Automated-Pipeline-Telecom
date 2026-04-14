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
def descargar_recaudacion_y_horas(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f"[bold cyan]💰 Iniciando extracción Unificada de Recaudación y Horas ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    reportes_config = [
        {
            "nombre": "Recaudación General",
            "nombre_archivo": f"Data - Recaudacion {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx",
            "ruta_dir": str(PATHS.get("raw_recaudacion")),
            "tipo_reporte_label": "DETALLE FORMA PAGO",
            "tipo_reporte_value": None,
            "locator_col": "#boton_select_columnas_detalle_forma_pago button.dropdown-toggle",
            "columnas": ["N° Abonado", "Nro Recibo", "Fecha", "Total Pago", "Total Pago Bs",
                          "Estatus Pago", "Forma de Pago", "Monto Forma Pago", "Tasa de cambio del día", 
                          "Banco", "Referencia", "Cobrador", "Nombre Caja", "Oficina Cobro", "Fecha Contrato", "Estatus",
                            "Suscripción", "Etiqueta", "Grupo Afinidad", "Nombre Franquicia", "Ciudad"]
        },
        {
            "nombre": "Horas de Pago",
            "nombre_archivo": f"Data - HorasPago {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx",
            "ruta_dir": str(PATHS.get("raw_horaspago")),
            "tipo_reporte_label": None,
            "tipo_reporte_value": "buscar_rep_libroventa_cob()",
            "locator_col": None, # Usará el genérico
            "columnas": ["N° Abonado", "Documento", "Nro Recibo", "Fecha", "Hora de Pago", "Oficina Cobro", "Suscripción"]
        }
    ]

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

        print("🏢 Configurando Franquicia y Cajas (Común para ambos reportes)...")
        try:
            print("   -> Desplegando Franquicias...")
            page.wait_for_timeout(500) # Pausa vital para que Bootstrap-Select asimile los eventos JS
            
            # 1. Franquicia -> Búsqueda mediante Label
            franquicia = page.locator("label").filter(has_text=re.compile(r"Franquicia Cobranza", re.IGNORECASE)).locator("..").get_by_role("button").first
            franquicia.wait_for(state="visible", timeout=15000)
            franquicia.evaluate("node => node.click()")

            # Esperamos la lista y damos clic en "Seleccionar Todos"
            page.locator("ul.dropdown-menu.inner:visible li").first.wait_for(state="visible", timeout=60000)
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
                page.locator("button.bs-deselect-all:visible").first.evaluate("node => node.click()")
                page.wait_for_timeout(200)
                
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
        except Exception as e:
            print(f"⚠️ Aviso al configurar Cajas/Franquicias: {e}")

        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        
        llenar_fechas_sae(page, fecha_inicial_str, fecha_final_str, id_desde="input#desde2", id_hasta="input#hasta2")
        
        for config in reportes_config:
            console.print(f"\n[bold yellow]▶ Procesando variante: {config['nombre']}[/]")
            
            os.makedirs(config["ruta_dir"], exist_ok=True)
            ruta_destino = os.path.join(config["ruta_dir"], config["nombre_archivo"])
            
            print("📋 Seleccionando Tipo de Reporte...")
            
            # ESPERA CRUCIAL AJAX: El portal SAE carga estos reportes de forma asíncrona.
            # Esperamos hasta que el <select> contenga opciones en el DOM antes de actuar.
            page.wait_for_function("document.querySelectorAll('select#tipo_reporte option').length > 0", timeout=30000)
            page.wait_for_timeout(500) # Pausa de seguridad para estabilizar el DOM
            
            if config["tipo_reporte_label"]:
                # Inyección JS para saltarse bloqueos visuales y problemas de exact-match (espacios en blanco)
                page.locator("select#tipo_reporte").evaluate(f"""node => {{
                    const target = Array.from(node.options).find(opt => opt.text.toUpperCase().includes('{config["tipo_reporte_label"]}'.toUpperCase()));
                    if (target) {{
                        node.value = target.value;
                        node.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    }}
                }}""")
            else:
                page.locator("select#tipo_reporte").evaluate(f"""node => {{
                    node.value = '{config["tipo_reporte_value"]}';
                    node.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}""")
                
            page.wait_for_timeout(500)
            
            print("🚀 Ejecutando búsqueda, esperando resultados...")
            btn_buscar = page.get_by_role("button", name=re.compile("Reporte", re.IGNORECASE)).first
            btn_buscar.wait_for(state="visible", timeout=10000)
            btn_buscar.evaluate("node => node.click()")
            
            print("⏳ Esperando a que el servidor devuelva los resultados...")
            try:
                page.wait_for_selector("button#exportar_xlsx", state="visible", timeout=60000)
                
                print("⚙️ Configurando columnas específicas...")
                if config["locator_col"]:
                    boton_columnas = page.locator(config["locator_col"]).first
                else:
                    boton_columnas = page.get_by_role("button", name=re.compile(r"COLUMNAS", re.IGNORECASE)).first
                    
                boton_columnas.wait_for(state="visible", timeout=5000)
                boton_columnas.click()
                page.wait_for_timeout(500)
                
                boton_ninguno = page.locator("button.bs-deselect-all:visible").first
                boton_ninguno.wait_for(state="visible", timeout=5000)
                boton_ninguno.click()
                page.wait_for_timeout(500)
                
                menu_visible = page.locator(".dropdown-menu.open:visible").first
                for col in config["columnas"]:
                    try:
                        opcion = menu_visible.locator("span.text").filter(has_text=re.compile(f"^\\s*{re.escape(col)}\\s*$", re.IGNORECASE)).first
                        opcion.wait_for(state="attached", timeout=1500)
                        opcion.evaluate("node => node.click()")
                    except Exception:
                        print(f"⚠️ Aviso: No se encontró la columna '{col}', omitiendo...")
                
                page.keyboard.press("Escape") 
                page.wait_for_timeout(500)
                
                print("🔄 Refrescando la tabla...")
                btn_buscar.evaluate("node => node.click()")
                page.wait_for_timeout(1000)
                try:
                    page.wait_for_selector("div.dataTables_processing", state="hidden", timeout=30000)
                except Exception:
                    page.wait_for_timeout(3000)
                
            except Exception as e:
                print(f"⚠️ Aviso al configurar columnas: {e}")

            # ================== DESCARGA DIRECTA ==================
            print("📥 Interceptando descarga directa a Excel...")
            try:
                if os.path.exists(ruta_destino):
                    os.remove(ruta_destino)
            except Exception:
                pass

            with page.expect_download(timeout=300000) as download_info: 
                page.locator("button#exportar_xlsx").evaluate("node => node.click()")
                
            download = download_info.value
            
            # Guardar temporalmente el archivo crudo (.xls)
            ruta_temporal = ruta_destino.replace(".xlsx", ".xls")
            download.save_as(ruta_temporal)
            
            print("🔄 Convirtiendo formato nativo de SAE a XLSX puro...")
            try:
                try:
                    df_descarga = pd.read_html(ruta_temporal, decimal=',', thousands='.')[0]
                except Exception:
                    df_descarga = pd.read_excel(ruta_temporal, engine="calamine")
                    
                df_descarga = df_descarga.loc[:, ~df_descarga.columns.str.contains('^Unnamed')]
                df_descarga.to_excel(ruta_destino, index=False)
                os.remove(ruta_temporal)
                print(f"✅ Archivo convertido y guardado exitosamente en:\n   {ruta_destino}")
            except Exception:
                print(f"⚠️ Error convirtiendo a XLSX. Se conservará en el formato original.")
                download.save_as(ruta_destino)
        
        browser.close()

if __name__ == "__main__":
    descargar_recaudacion_y_horas("20/03/2026", "23/03/2026")

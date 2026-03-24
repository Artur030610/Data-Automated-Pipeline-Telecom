import os
import sys
import datetime
import re
import pandas as pd
import subprocess
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_ventas(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f"[bold cyan]📊 Iniciando extracción de Ventas Listado ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - Ventas  {f_ini.day}-{f_ini.month}-{f_ini.year} al {f_fin.day}-{f_fin.month}-{f_fin.year}.xlsx"
    
    ruta_destino_dir = PATHS.get("ventas_abonados")
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # Utilización de Headless en False para depuración o True para fondo
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        login_sae(page)

        print("🧭 Navegando al Listado de Abonados...")
        page.get_by_role("link", name=" Reportes").click()
        
        btn_listado = page.get_by_role("link", name=re.compile("Listado De Abonados", re.IGNORECASE))
        btn_listado.wait_for(state="visible", timeout=5000)
        btn_listado.click()

        print("☑️ Configurando Estatus de Contratos...")
        estatus_list = [
            "ACTIVO", "POR INSTALAR", "OBSTRUCCION", 
            "POR IMPLEMENTACION", "POR VGT", "POR REVISAR"
        ]
        for estatus in estatus_list:
            # Evitamos que falle si ya estaba marcado o si demora un poco
            checkbox = page.locator(f"input[id='{estatus}']")
            checkbox.wait_for(state="attached", timeout=5000)
            if not checkbox.is_checked():
                checkbox.check()

        print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        print("👉 Cambiando a la pestaña de Fechas...")
        page.get_by_role("tab", name=re.compile(r"Fecha", re.IGNORECASE)).click()
        page.wait_for_timeout(500)
        
        # FECHA DESDE Y HASTA (Forzadas vía JavaScript para evitar problemas de focus y máscaras)
        page.locator("input#desde_fecha").evaluate(f"node => {{ node.value = '{fecha_inicial_str}'; node.dispatchEvent(new Event('change', {{ bubbles: true }})); }}")
        
        page.locator("input#hasta_fecha").evaluate(f"node => {{ node.value = '{fecha_final_str}'; node.dispatchEvent(new Event('change', {{ bubbles: true }})); }}")
        page.wait_for_timeout(300)
        
        print("📋 Seleccionando Motivo (FECHA CONTRATO)...")
        page.locator("#motivo").evaluate("""node => {
            const target = Array.from(node.options).find(opt => opt.text.toUpperCase().includes('FECHA CONTRATO'));
            if (target) {
                node.value = target.value;
                node.dispatchEvent(new Event('change'));
            }
        }""")
        page.wait_for_timeout(500)
        
        print("🚀 Ejecutando búsqueda inicial...")
        btn_buscar = page.get_by_role("button", name=re.compile(r"Buscar", re.IGNORECASE)).first
        btn_buscar.wait_for(state="visible", timeout=5000)
        btn_buscar.evaluate("node => node.click()")
        
        # ================== SELECCIÓN DE COLUMNAS ESPECÍFICAS ==================
        print("⏳ Esperando a que el servidor devuelva los resultados...")
        try:
            page.wait_for_selector("button#exportar_xlsx", state="visible", timeout=60000)
            print("⚙️ Configurando columnas específicas (limpiando y marcando)...")
            
            boton_columnas = page.locator("div#boton_select_columnas button.dropdown-toggle").first
            boton_columnas.click()
            
            boton_ninguno = page.locator("button.bs-deselect-all:visible").first
            boton_ninguno.wait_for(state="visible", timeout=5000)
            boton_ninguno.click()
            page.wait_for_timeout(500)
            
            columnas_requeridas = [
                "N° Abonado", "Cliente", "Fecha Contrato", "Estatus",
                "Suscripción", "Grupo Afinidad", "Nombre Franquicia",
                "Ciudad", "Vendedor", "Serv/Paquete"
            ]
            
            menu_visible = page.locator(".dropdown-menu.open:visible").first
            for col in columnas_requeridas:
                try:
                    opcion = menu_visible.locator("span.text").filter(has_text=re.compile(f"^\\s*{re.escape(col)}\\s*$", re.IGNORECASE)).first
                    opcion.wait_for(state="attached", timeout=1500)
                    opcion.evaluate("node => node.click()")
                except Exception:
                    print(f"⚠️ Aviso: No se encontró la columna '{col}', omitiendo...")
            
            page.keyboard.press("Escape") 
            page.wait_for_timeout(500)
            
            print("🔄 Refrescando la tabla con las columnas seleccionadas...")
            btn_buscar.evaluate("node => node.click()")
            page.wait_for_timeout(3000)
            
        except Exception as e:
            print(f"⚠️ Aviso al configurar columnas: {e}")

        # ================== DESCARGA DIRECTA ==================
        print("📥 Interceptando descarga a Excel...")
        try:
            if os.path.exists(ruta_destino):
                os.remove(ruta_destino)
        except Exception:
            pass

        with page.expect_download(timeout=120000) as download_info:
            page.locator("button#exportar_xlsx").click()
            
        download = download_info.value
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
        except Exception as e:
            print(f"⚠️ Error convirtiendo a XLSX: {e}. Se conservará en el formato original.")
            download.save_as(ruta_destino)
            
        browser.close()

    # ================== POST-PROCESAMIENTO: EJECUTAR SCRIPT FUZZY ==================
    script_transform = os.path.join(parent_dir, "01-Ventas", "Transformar Archivo de Ventas.py")
    if os.path.exists(script_transform):
        console.print(f"\n[bold magenta]✨ Ejecutando limpieza Fuzzy Matching...[/]")
        subprocess.run([sys.executable, script_transform], check=True)

if __name__ == "__main__":
    descargar_ventas("20/03/2026", "23/03/2026")
import os
import sys
import datetime
import re
import shutil
from playwright.sync_api import sync_playwright

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console
from scraper_utils import login_sae, ejecutar_descarga, llenar_fechas_sae

@reportar_tiempo
def descargar_ordenes_servicio(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f" [bold cyan]📑 Iniciando extracción de Órdenes de Servicio ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo_idf = f"Data - IdF {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    nombre_archivo_sla = f"Data - SLA {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    # Definimos ambas rutas destino según tu config.py
    ruta_destino_idf_dir = str(PATHS.get("raw_idf"))
    ruta_destino_sla_dir = str(PATHS.get("raw_sla"))
    
    os.makedirs(ruta_destino_idf_dir, exist_ok=True)
    os.makedirs(ruta_destino_sla_dir, exist_ok=True)
    
    ruta_destino_idf = os.path.join(ruta_destino_idf_dir, nombre_archivo_idf)
    ruta_destino_sla = os.path.join(ruta_destino_sla_dir, nombre_archivo_sla)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        login_sae(page)

        console.print("🧭 Navegando al Reporte de Órdenes de Servicio...")
        page.get_by_role("link", name=re.compile(r"Reportes", re.IGNORECASE)).click()
        
        # Ajuste inteligente del link, comúnmente llamado "Ordenes De Servicio" en SAE
        btn_reporte = page.get_by_role("link", name=re.compile(r"Órdenes De Servicio", re.IGNORECASE)).first
        btn_reporte.wait_for(state="attached", timeout=5000)
        btn_reporte.evaluate("node => node.click()")
        page.wait_for_timeout(1000)

        console.print("📋 Seleccionando Motivo...")
        page.locator("#motivo").select_option(label="CREACIÓN")
        page.wait_for_timeout(500)
        
        
        console.print("☑️ Seleccionando Estatus...")
        # Buscamos la etiqueta "Estatus", subimos a su contenedor padre (..) y hacemos clic en SU botón
        page.locator("label").filter(has_text=re.compile(r"Estatus", re.IGNORECASE)).locator("..").get_by_role("button").first.click()
        page.locator("a").filter(has_text="CREADA").click()
        page.locator("a").filter(has_text="IMPRESA").click()
        page.locator("a").filter(has_text=re.compile(r"^FINALIZADA$")).click()
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)

        console.print("☑️ Seleccionando Tipo de Orden...")
        # Misma técnica: Buscamos "Tipo de Orden" sin importar en qué posición de la página esté
        page.locator("label").filter(has_text=re.compile(r"Tipo de Orden", re.IGNORECASE)).locator("..").get_by_role("button").first.click()
        page.locator("a").filter(has_text="ORDEN DE RECLAMOS FIBEX").click()
        page.locator("a").filter(has_text="ORDEN MEDIOS DIGITALES").click()
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)

        console.print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
        llenar_fechas_sae(
            page, fecha_inicial_str, fecha_final_str,
            id_desde=page.get_by_role("textbox", name=re.compile(r"Desde",re.IGNORECASE)).first,
            id_hasta=page.get_by_role("textbox", name=re.compile(r"hasta", re.IGNORECASE)).first
        )
        
        console.print("🚀 Ejecutando búsqueda...")
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
        btn_buscar = page.get_by_role("button", name=re.compile(r"Buscar", re.IGNORECASE)).first
        btn_buscar.click()

        console.print("⏳ Esperando respuesta del servidor y configurando columnas...")
        page.get_by_role("columnheader").nth(0).wait_for(state="visible", timeout=60000)
        
        console.print("⚙️ Configurando columnas específicas...")
        try:
            boton_columnas = page.get_by_role("button", name=re.compile(r"COLUMNAS", re.IGNORECASE))
            boton_columnas.wait_for(state="visible", timeout=5000)
            boton_columnas.click()
            page.wait_for_timeout(500)
            
            boton_ninguno = page.locator("button.bs-deselect-all:visible").first
            boton_ninguno.wait_for(state="visible", timeout=5000)
            boton_ninguno.click()
            page.wait_for_timeout(500)
            
            columnas_requeridas = [
                "N° Contrato", "Estatus contrato", "N° Orden", "Estatus_orden",
                "Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion", 
                "Grupo Afinidad", "Detalle Orden", "Franquicia",
                "Grupo Trabajo", "Usuario Emisión", "Usuario Impresión", 
                "Usuario Final", "Solucion Aplicada"
            ]
            
            menu_visible = page.locator(".dropdown-menu.open:visible").first
            for col in columnas_requeridas:
                try:
                    opcion = menu_visible.locator("span.text").filter(has_text=re.compile(f"^\\s*{re.escape(col)}\\s*$", re.IGNORECASE)).first
                    opcion.wait_for(state="attached", timeout=1500)
                    opcion.evaluate("node => node.click()")
                except Exception:
                    console.print(f"[yellow]⚠️ Aviso: No se encontró la columna '{col}'[/]")
            
            page.keyboard.press("Escape") 
            page.wait_for_timeout(500)
            
            console.print("🔄 Refrescando la tabla...")
            btn_buscar.evaluate("node => node.click()")
            page.wait_for_timeout(2000)
                
        except Exception as e:
            console.print(f"[bold red]⚠️ Aviso al configurar columnas: {e}[/]")

        ejecutar_descarga(page=page, ruta_destino=ruta_destino_idf, seleccionar_todos=False)
        
        console.print("🗂️ Duplicando el reporte para la carpeta SLA...")
        shutil.copy2(ruta_destino_idf, ruta_destino_sla)
        console.print(f"✅ Archivo SLA guardado exitosamente en:\n   {ruta_destino_sla}")

        browser.close()

if __name__ == "__main__":
    # Prueba rápida
    descargar_ordenes_servicio("01/02/2026", "15/03/2026")
import os
import sys
import datetime
import re
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, ejecutar_descarga, llenar_fechas_sae, mostrar_todas_columnas, descargar_listado_llamadas

# --- EL TRUCO DEL ASCENSOR PARA IMPORTAR UTILS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

def descargar_observaciones(page, fecha_inicial_str, fecha_final_str):
    """
    Navega a la sección de Reporte de Observaciones, aplica el filtro específico y descarga.
    """
    console.print(f" [bold blue]-- Iniciando descarga de: Reporte de Observaciones...[/]")
    
    # --- NAVEGACIÓN ---
    console.print("🧭 Navegando al Reporte de Observaciones...")
    btn_reporte = page.get_by_role("link", name=re.compile(r"Reporte de Observaciones", re.IGNORECASE)).first
    
    if not btn_reporte.is_visible():
        page.get_by_role("link", name=re.compile(r"Reportes", re.IGNORECASE)).click()
        
    btn_reporte.wait_for(state="attached", timeout=5000)
    btn_reporte.evaluate("node => node.click()")
    page.wait_for_timeout(1000)

    # --- LLENADO DE FILTROS ---
    console.print("📅 Aplicando filtros (Actualización de Datos Personales)...")
    asunto = page.locator("#motivo, select[name='motivo']").first 
    asunto.wait_for(state="attached", timeout=10000)
    asunto.evaluate("""node => {
        const target = Array.from(node.options).find(opt => opt.text.toUpperCase().includes('ACTUALIZACION DE LOS DATOS PERSONALES'));
        if (target) {
            node.value = target.value;
            node.dispatchEvent(new Event('change', { bubbles: true }));
        }
    }""")
    
    llenar_fechas_sae(page, fecha_inicial_str, fecha_final_str, id_desde="#desde_obser", id_hasta="#hasta_obser")

    page.keyboard.press("Escape")
    page.locator("body").click(position={"x": 0, "y": 0})
    page.wait_for_timeout(300)
    btn_buscar = page.get_by_role("button", name=re.compile(r"Buscar", re.IGNORECASE)).first
    btn_buscar.click()
    
    console.print("⏳ Esperando respuesta de la API del servidor...")
    page.get_by_role("columnheader").nth(0).wait_for(state="visible", timeout=15000)
    
    console.print("🔄 Configurando columnas y refrescando la tabla...")
    mostrar_todas_columnas(page)
    
    # --- CONFIGURACIÓN DE DESCARGA ---
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    nombre_archivo = f"Data - Actualizacion Gestion OBS {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    ruta_destino_dir = os.path.join(str(PATHS.get("raw_act_datos")), "3-OBSERVACIONES")
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    ejecutar_descarga(page=page, ruta_destino=ruta_destino, seleccionar_todos=True)

@reportar_tiempo
def descargar_act_datos(fecha_inicial_str: str, fecha_final_str: str):
    """
    Orquesta la descarga de todos los reportes referentes a Actualización de Datos.
    """
    console.print(f" [bold green]📊 Iniciando proceso de descarga de Act. de Datos ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    reportes_a_descargar = [
        {
            "tipo_llamada": "GESTION OFICINA COMERCIAL",
            "tipo_respuesta": "ACTUALIZACIÓN",
            "detalle_respuesta": None,
            "nombre_archivo_base": "Data - Actualizacion Gestion OOCC",
            "subcarpeta_destino": "2-OOCC"
        },
        {
            "tipo_llamada": "GESTIÓN CALL CENTER",
            "tipo_respuesta": "ACTUALIZACIÓN DE DATOS",
            "detalle_respuesta": None,
            "nombre_archivo_base": "Data - Actualizacion Gestion CC",
            "subcarpeta_destino": "1-CALL CENTER"
        },
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        login_sae(page)

        f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
        f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
        
        # 1. Bajar los del Listado de Llamadas
        for config in reportes_a_descargar:
            nombre_archivo = f"{config['nombre_archivo_base']} {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
            ruta_destino_dir = os.path.join(str(PATHS.get("raw_act_datos")), config["subcarpeta_destino"])
            os.makedirs(ruta_destino_dir, exist_ok=True)
            ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
            console.print(f" [bold blue]-- Iniciando descarga de: {config['nombre_archivo_base']}...[/]")
            descargar_listado_llamadas(
                page=page, fecha_inicial_str=fecha_inicial_str, fecha_final_str=fecha_final_str, 
                ruta_destino=ruta_destino, tipo_llamada=config["tipo_llamada"], 
                tipo_respuesta=config["tipo_respuesta"], detalle_respuesta=config["detalle_respuesta"]
            )
            page.wait_for_timeout(2000)

        # 2. Bajar el de Reporte de Observaciones
        descargar_observaciones(page, fecha_inicial_str, fecha_final_str)

        console.print(" [bold green]✅ Proceso de descarga de Act. de Datos completado.[/]")
        browser.close()

if __name__ == "__main__":
    # Fechas de prueba (mes anterior completo)
    today = datetime.date.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    
    fecha_ini_str = first_day_of_previous_month.strftime("%d/%m/%Y")
    fecha_fin_str = last_day_of_previous_month.strftime("%d/%m/%Y")

    descargar_act_datos(fecha_ini_str, fecha_fin_str)
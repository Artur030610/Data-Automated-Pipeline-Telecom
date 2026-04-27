
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
from scraper_utils import login_sae, descargar_listado_llamadas


@reportar_tiempo
def descargar_reclamos(fecha_inicial_str: str, fecha_final_str: str):
    """
    Orquesta la descarga de todos los reportes de reclamos.
    """
    console.print(f" [bold green]📊 Iniciando proceso de descarga de Reclamos ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    reportes_a_descargar = [
        {
            "tipo_llamada": "GESTION OFICINA COMERCIAL",
            "tipo_respuesta": "RECLAMO DEL SERVICIO",
            "detalle_respuesta": "DENUNCIA POR ATENCION INADECUADA AL CLIENTE",
            "nombre_archivo_base": "Data - Reclamos OOCC",
            "subcarpeta_destino": "2-Data-Reclamos por OOCC"
        },
        {
            "tipo_llamada": "GESTIÓN CALL CENTER",
            "tipo_respuesta": "SOPORTE TECNICO N1",
            "detalle_respuesta": "QUEJA PERSONAL TÉCNICO",
            "nombre_archivo_base": "Data - Reclamos CC",
            "subcarpeta_destino": "1-Data-Reclamos por CC"
        },
        {
            "tipo_llamada": "RRSS CALL CENTER",
            "tipo_respuesta": "INSTAGRAM",
            "detalle_respuesta": None,
            "nombre_archivo_base": "Data - Reclamos RRSS Igram",
            "subcarpeta_destino": "3-Data-Reclamos por RRSS"
        },
        {
            "tipo_llamada": "RRSS CALL CENTER",
            "tipo_respuesta": "FACEBOOK",
            "detalle_respuesta": None,
            "nombre_archivo_base": "Data - Reclamos RRSS Fbook",
            "subcarpeta_destino": "3-Data-Reclamos por RRSS"
        },
        {
            "tipo_llamada": "APP OFICINA MOVIL",
            "tipo_respuesta": "FALLA",
            "detalle_respuesta": None,
            "nombre_archivo_base": "Data - Fallas APP",
            "subcarpeta_destino": "4-Data-Reclamos por APP"
        },
        {
            "tipo_llamada": "GESTION OFICINA COMERCIAL",
            "tipo_respuesta": "PAGO DEL SERVICIO",
            "detalle_respuesta": None,
            "nombre_archivo_base": "Data - Fallas Bancos",
            "subcarpeta_destino": "5-Data-Reclamos OB"
        }
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        login_sae(page)

        f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
        f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
        
        for config in reportes_a_descargar:
            nombre_archivo = f"{config['nombre_archivo_base']} {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
            ruta_destino_dir = os.path.join(str(PATHS.get("raw_reclamos")), config["subcarpeta_destino"])
            os.makedirs(ruta_destino_dir, exist_ok=True)
            ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
            console.print(f" [bold blue]-- Iniciando descarga de: {config['nombre_archivo_base']}...[/]")
            descargar_listado_llamadas(
                page=page, 
                fecha_inicial_str=fecha_inicial_str, 
                fecha_final_str=fecha_final_str, 
                ruta_destino=ruta_destino,
                tipo_llamada=config["tipo_llamada"],
                tipo_respuesta=config["tipo_respuesta"],
                detalle_respuesta=config["detalle_respuesta"]
            )
            # Pausa suave en lugar de una redirección brusca para no cortar conexiones en progreso
            page.wait_for_timeout(2000)


        console.print(" [bold green]✅ Proceso de descarga de Reclamos completado.[/]")
        browser.close()


if __name__ == "__main__":
    # Fechas de prueba (mes anterior completo)
    today = datetime.date.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    
    fecha_ini_str = first_day_of_previous_month.strftime("%d/%m/%Y")
    fecha_fin_str = last_day_of_previous_month.strftime("%d/%m/%Y")

    descargar_reclamos('31/03/2026', '01/04/2026')

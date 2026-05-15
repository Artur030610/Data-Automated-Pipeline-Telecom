import os
import sys
import datetime
import re
import pandas as pd
import subprocess
import shutil
from rich.prompt import Prompt
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, listado_abonados, transformar_xls


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_abonados(fecha_inicial_str: str, fecha_final_str: str):
    '''Descarga el listado completo de abonados desde SAE, con opción de filtrar por fecha de contrato.
    Debido a la naturaleza del reporte, se recomienda usar una fecha inicial muy antigua para obtener todos
    los registros hasta la fecha actual.
    '''
    fecha_inicial_str = "01/01/1950" #Aseguramos una fecha muy antigua para que tome todos los registros hasta hoy

    console.print(f"[bold cyan]📊 Iniciando extracción de Abonados ({fecha_inicial_str} - {fecha_final_str})...[/]")
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - Abonados {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    ruta_destino_dir = str(PATHS.get("raw_clientes"))
        
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # Utilización de Headless en False para depuración o True para fondo
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        login_sae(page)
        
        col = ["N° Abonado", "Cliente", "Fecha Contrato", "Estatus",
                "Suscripción","Grupo Afinidad", "Nombre Franquicia",
                "Ciudad", "Vendedor", "Serv/Paquete", "Fecha nacimiento"]
        listado_abonados(page, fecha_inicial_str, fecha_final_str, motivo_str = None, estatus_list=None, col_table=col) #type: ignore
        

        # ================== DESCARGA DIRECTA ==================
        print("📥 Interceptando descarga a Excel...")
        try:
            if os.path.exists(ruta_destino):
                os.remove(ruta_destino)
        except Exception:
            pass

        with page.expect_download(timeout=300000) as download_info: # Aumentado a 5 minutos
            page.locator("button#exportar_xlsx").click()
            
        download = download_info.value
        ruta_temporal = ruta_destino.replace(".xlsx", ".xls")
        download.save_as(ruta_temporal)
        
        print("🔄 Convirtiendo formato nativo de SAE a XLSX puro...")
        try:
            print("   -> 🔎 Extrayendo datos (Bypass del DOM para ahorrar RAM)...")
            df_pl = transformar_xls(ruta_temporal)
            
            print("   -> 💾 Guardando a XLSX optimizado con Polars...")
            df_pl.write_excel(ruta_destino)
            
            os.remove(ruta_temporal)
            print(f"✅ Archivo convertido y guardado exitosamente en:\n   {ruta_destino}")
        except Exception as e:
            print(f"⚠️ Error convirtiendo a XLSX: {e}. Se conservará en el formato original.")
            if os.path.exists(ruta_temporal):
                os.rename(ruta_temporal, ruta_destino)
            
        browser.close()

    console.print("")
    console.rule("[bold cyan]SNAPSHOT QUINCENAL (IDF)[/]")
    respuesta = Prompt.ask("[bold yellow]¿Desea guardar una copia de este archivo como snapshot para el cálculo quincenal de IdF?[/] (s/n)", choices=["s", "n"], default="n")
    
    if respuesta.lower() == 's':
        nombre_idf = f"Data - Abonados hasta el {f_fin.strftime('%d-%m-%Y')}.xlsx"
        ruta_idf_dir = str(PATHS.get("raw_abonados_idf"))
        os.makedirs(ruta_idf_dir, exist_ok=True)
        ruta_idf = os.path.join(ruta_idf_dir, nombre_idf)
        shutil.copy2(ruta_destino, ruta_idf)
        console.print(f"[bold green]✅ Snapshot copiado exitosamente para IdF:[/]\n   -> {ruta_idf}")
    else:
        console.print("[dim]⏭️ Omitiendo guardado de snapshot IdF.[/]")

if __name__ == "__main__":
    descargar_abonados("20/03/2026", "15/05/2026")
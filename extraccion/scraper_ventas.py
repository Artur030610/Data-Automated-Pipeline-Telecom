import os
import sys
import datetime
import re
import pandas as pd
import subprocess
from playwright.sync_api import sync_playwright
from scraper_utils import login_sae, listado_abonados

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, ESTATUS_VENTAS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_ventas(fecha_inicial_str: str, fecha_final_str: str):
    console.print(f"[bold cyan]📊 Iniciando extracción de Ventas Listado ({fecha_inicial_str} - {fecha_final_str})...[/]")
    
    f_ini = datetime.datetime.strptime(fecha_inicial_str, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_final_str, "%d/%m/%Y")
    
    nombre_archivo = f"Data - Ventas {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    ruta_destino_dir = str(PATHS.get("ventas_abonados"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # Utilización de Headless en False para depuración o True para fondo
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        login_sae(page)

        col = [
            "N° Abonado", "Cliente", "Fecha Contrato", "Estatus",
            "Suscripción", "Grupo Afinidad", "Nombre Franquicia",
            "Ciudad", "Vendedor", "Serv/Paquete"
        ]
        
        # Centralizamos toda la lógica de navegación y columnas en la función unificada
        listado_abonados(
            page, 
            fecha_inicial_str, 
            fecha_final_str, 
            motivo_str=None, #type: ignore
            estatus_list=ESTATUS_VENTAS, 
            col_table=col
        )

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

    # ================== POST-PROCESAMIENTO: ENRIQUECIMIENTO FUZZY ==================
    # Se ejecuta un script dedicado que modifica el Excel recién descargado para añadir
    # la información del vendedor. Esto simplifica el ETL posterior.
    script_fuzzy = os.path.join(current_dir, "fuzzy_ventas.py")
    if os.path.exists(script_fuzzy):
        console.print(f"\n[bold magenta]✨ Ejecutando enriquecimiento Fuzzy Matching sobre el archivo descargado...[/]")
        subprocess.run([sys.executable, script_fuzzy], check=True)

if __name__ == "__main__":
    descargar_ventas("26/04/2026", "28/04/2026")
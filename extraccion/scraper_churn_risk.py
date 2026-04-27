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

from config import PATHS
from utils import reportar_tiempo, console

@reportar_tiempo
def descargar_churn_risk(fecha_ini: str, fecha_fin: str):
    
    today = datetime.datetime.today()
    #Estatus de los contratos que se consideran en riesgo de churn, ajusta esta lista según tu SAE
    states = ['ANULADO','REEMBOLSO SIN DATOS BANCO','ANULADO(R) CLIENTE NO CONTESTA',
    'CLIENTES INACTIVOS', 'POR CORTAR', 'REEMBOLSO-EN OFIC COMERCIALES', 
    'ANULADO(R) SIN RETIRO EF','CORTADO','POR RETIRAR','REEMBOLSO ADM',
    'RETIRADO','CORTADO HOTSPOT','REEMBOLSO PAGADOS']
    
    # =========================================================
    # 1. FECHAS Y NOMBRES DE ARCHIVO
    # =========================================================
    f_ini = datetime.datetime.strptime(fecha_ini, "%d/%m/%Y")
    f_fin = datetime.datetime.strptime(fecha_fin, "%d/%m/%Y")

    console.print(f" [bold cyan]🎧 Iniciando extracción de Early Churn Risk ({f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')})...[/]")
    nombre_archivo = f"Data_Churn_Risk {f_ini.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}.xlsx"
    
    # =========================================================
    # 2. DEFINIMOS LA RUTA DESTINO EXACTA (Ajustar a tu Data Lake)
    # =========================================================
    ruta_destino_dir = str(PATHS.get("raw_churn_risk"))
    os.makedirs(ruta_destino_dir, exist_ok=True)
    ruta_destino = os.path.join(ruta_destino_dir, nombre_archivo)
    
    # =========================================================
    # 3. PLAYWRIGHT: NAVEGACIÓN Y EXTRACCIÓN
    # =========================================================
    with sync_playwright() as p:
        
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        "Uso del modulo Login desde scraper_utils para autenticación en SAE"
        login_sae(page)

        page.wait_for_timeout(500)

        " Logica de negocio para descargar el reporte de Churn Risk, ajustada a la estructura de tu SAE."
        console.print("🧭 Navegando al Reporte de Churn Risk...")
        page.get_by_role("link", name=re.compile(r"Reporte", re.IGNORECASE)).click()
        page.wait_for_timeout(500)
        
        btn_listado = page.get_by_role("link", name=re.compile(r"Listado de abonados", re.IGNORECASE)).first
        btn_listado.wait_for(state="attached", timeout=60000)
        btn_listado.click()
        
        for state in states:
            page.get_by_role("checkbox", name=state, exact=True).check()
            page.wait_for_timeout(300)
        
        btn_fecha = page.get_by_role("tab", name=re.compile(r"Fecha", re.IGNORECASE)).first
        btn_fecha.click()

        '''Uso de modulo para llenar fechas en SAE desde scraper_utils, 
        ajustado a los campos específicos del reporte de Churn Risk
        debido a que el input de fechas selecciona automaticamente un input de calendario'''
        llenar_fechas_sae(page, fecha_ini, fecha_fin, id_desde="input#desde_fecha", id_hasta="input#hasta_fecha")

        page.locator("#motivo").select_option("instalacion")
        btn_buscar = page.get_by_role("button", name=re.compile(r"Buscar", re.IGNORECASE)).first
        btn_buscar.click()
        
        #Lista de columnas específicas para el análisis de churn risk, ajusta según tu reporte
        cols = ['N° Abonado', 'Documento', 'Fecha Contrato', 
               'Estatus', 'Fecha Últ. Factura', 'Fecha Últ. Pago',
               'Último Corte Finalizado', 'Nombre Franquicia',
               'Suscripción', 'Teléfono', 'Ciudad', 'Serv/Paquete']
        
        btn_col = page.get_by_role("button", name=re.compile(r"Columnas", re.IGNORECASE))
        btn_col.wait_for(state="attached", timeout=60000)
        btn_col.click()

        btn_ninguno = page.get_by_role("button", name="Ninguno")
        btn_ninguno.wait_for(state="attached", timeout=60000)
        btn_ninguno.click()
        #Seleccionamos las columnas necesarias para el análisis de churn risk, según lista
        for col in cols:
            page.locator("a").filter(has_text=re.compile(r"^" + col+"$", re.IGNORECASE)).click()
            page.wait_for_timeout(200)
        btn_buscar.click()
        'Uso del modulo ejecutar_descarga desde scraper_utils para descargar el reporte de Churn Risk'
        #ruta_temporal = ruta_destino
        _, download = ejecutar_descarga(
            page,
            ruta_destino,
            locator_descarga=page.get_by_role("button", name=re.compile(r"Exportar Todo a Excel")))
       
        print("🔄 Convirtiendo formato nativo de SAE a XLSX puro...")
        ruta_temporal = ruta_destino.replace(".xlsx", ".xls")
        try:
            if os.path.exists(ruta_temporal):
                os.remove(ruta_temporal)
            os.rename(ruta_destino, ruta_temporal)
            
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
            if os.path.exists(ruta_temporal):
                os.rename(ruta_temporal, ruta_destino)
            
        browser.close()

if __name__ == "__main__":
    # Fechas de prueba
    descargar_churn_risk("01/01/2026", "01/04/2026")
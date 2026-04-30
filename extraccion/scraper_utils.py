# En utils.py se encuentran todas las utilidades, es decir funciones modulares que se reutilizan a fin de optimizar las extracciones a traves de RPA. 
# Se utiliza playwright para la automatización de la navegación, selección de filtros y descarga de los reportes desde SAE Plus.
# La intención es cumplir con los principios de no repetición, modularidad y claridad. de igual forma se busca que las funciones tengan una 
# única responsabilidad, es decir que cada función cumpla con una tarea específica y bien definida. 
# En cada función se encontrara una descripción detallada de su propósito, sus parámetros de entrada y su valor de retorno

import os
import sys
import re
from playwright.sync_api import Page, Locator
from typing import Union

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils import logger_extraccion
from config import SAE_USUARIO, SAE_CLAVE

#Función principal utilizada en todos los RPA para automatizar el login.
def login_sae(page: Page, usuario: str = None, clave: str = None): #type: ignore
    """
    Emula la lógica de 'main.txt' en Power Automate.
    Ingresa al portal SAE y completa el login. 
    Utiliza el usuario y la clave como argumento
    """
    usuario_final = usuario or SAE_USUARIO
    clave_final = clave or SAE_CLAVE

    print("🔐 [MAIN] Ingresando al portal SAE...")
    page.goto("https://fibex.saeplus.com/")
    
    # Esperamos hasta 5 segundos a que aparezca el formulario
    try:
        page.wait_for_selector("input#login_usuario", timeout=8000)
        
        page.wait_for_timeout(1000) # 1 segundo de pausa antes de escribir
        
        # Volvemos a .fill() por velocidad y limpieza
        page.locator("input#login_usuario").fill(usuario_final)
        page.locator("input#pass_usuario").fill(clave_final)
        
        # Damos clic al botón explícitamente
        page.get_by_role("button", name=" Iniciar Sesión").click()
        
        # Manejo del bug del Pop-up de verificación en SAE
        try:
            # Esperamos 5 segundos a ver si el login fue exitoso directo
            page.wait_for_selector("a[href='#reportes']", timeout=1000)
        except Exception:
            print("⚠️ [MAIN] Posible pop-up de verificación detectado. Forzando segundo clic...")
            # Si no ha entrado, volvemos a darle clic al botón para saltar el error
            page.get_by_role("button", name=" Iniciar Sesión").click()
            
    except Exception:
        print("⚠️ [MAIN] No se detectó formulario de login (posible sesión activa).")
        
    # Validamos que cargue la página principal antes de seguir
    page.wait_for_selector("a[href='#reportes']", timeout=30000)
    logger_extraccion.info(f"Autenticación exitosa en SAE para el usuario: {usuario_final}")
    print("✅ [MAIN] Autenticación completada.")

    # --- MANEJO DEL POP-UP DE FACTURACIÓN (Días 1 al 15) ---
    try:
        # Pausa breve para que la animación del modal termine de aparecer en pantalla
        page.wait_for_timeout(1000)
        # Intento 1: Tecla Escape (Cierra el 90% de los modales modernos como Bootstrap o SweetAlert)
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)
        
        # Agregamos timeout ultra-corto para que no se quede congelado si no hay pop-up
        if page.get_by_text("OK").is_visible():
            page.get_by_text("OK").click(timeout=1000)
            
        # Intento 2: Buscar botón explícito si el Escape fue bloqueado
        btn_cerrar = page.locator("button:has-text('Cerrar'), button:has-text('OK'), button.close").first
        if btn_cerrar.is_visible():
            btn_cerrar.evaluate("node => node.click()")
    except Exception:
        pass # Si el código entra aquí significa que no hay popup o ya se cerró. Seguimos normal.

def llenar_fechas_sae(page: Page, fecha_ini: str, fecha_fin: str, id_desde: Union[str, Locator] = "#desde_llamada", id_hasta: Union[str, Locator] = "#hasta_llamada"):
    """
    Limpia y llena de forma segura los campos de fecha en SAE eludiendo errores de máscara.
    Acepta selectores CSS (str) o directamente un objeto Locator de Playwright.
    """
    try:
        loc_desde = page.locator(id_desde) if isinstance(id_desde, str) else id_desde
        loc_hasta = page.locator(id_hasta) if isinstance(id_hasta, str) else id_hasta
        
        # Forzamos el valor vía JavaScript para eludir el popup del calendario
        loc_desde.evaluate(f"node => {{ node.value = '{fecha_ini}'; node.dispatchEvent(new Event('change', {{ bubbles: true }})); }}")
        loc_hasta.evaluate(f"node => {{ node.value = '{fecha_fin}'; node.dispatchEvent(new Event('change', {{ bubbles: true }})); }}")
        
        # Pequeña pausa para que el DOM asimile el cambio
        page.wait_for_timeout(300)
    except Exception as e:
        print(f"⚠️ Aviso al llenar fechas: {e}")

def esperar_carga_tabla(page: Page, id_tabla: str = "datagrid1", timeout_ms: int = 30000):
    """
    Espera a que el backend devuelva los datos y desaparezca el indicador de "Procesando...".
    Evita colisiones de red al interactuar con la tabla.
    """
    try:
        page.wait_for_selector(f"div#{id_tabla}_processing", state="hidden", timeout=timeout_ms)
        page.wait_for_timeout(1000)
    except Exception:
        page.wait_for_timeout(3000) # Fallback de seguridad

def mostrar_todas_columnas(page: Page):
    """
    Despliega el menú de 'COLUMNAS', selecciona 'Todos' y refresca la tabla.
    """
    try:
        page.get_by_role("button", name=re.compile(r"COLUMNAS", re.IGNORECASE)).click()
        page.wait_for_timeout(500)
        page.get_by_role("button", name=re.compile(r"Todos", re.IGNORECASE)).click()
        page.wait_for_timeout(500)
        page.get_by_title("Refrescar la tabla").click()
        page.wait_for_timeout(2000) # Pausa para asimilar columnas
    except Exception as e:
        print(f"⚠️ Aviso: No se pudieron configurar las columnas ({e})")

def ejecutar_descarga(page: Page, ruta_destino: str, timeout_ms: int = 300000, seleccionar_todos: bool = False, locator_descarga: Union[str, Locator] = None, **kwargs): #type: ignore
    """
    Ejecuta la descarga a Excel
    usando selectores robustos sin depender de IDs específicos de tablas.
    Si 'seleccionar_todos' es True, muestra todos los registros de la tabla antes de exportar.
    """
    if seleccionar_todos:
        try:
            print("🗂️ Seleccionando 'Todos' los registros de la tabla...")
            combobox = page.get_by_role("combobox", name=re.compile(r"Mostrar", re.IGNORECASE)).first
            combobox.wait_for(state="attached", timeout=5000)
            combobox.select_option(label="Todos")
            # Pausa breve para que el backend envíe y renderice todas las filas
            page.get_by_role("gridcell").first.wait_for(state="visible", timeout=30000)
            page.wait_for_timeout(1000)
        except Exception as e:
            print(f"⚠️ Aviso al seleccionar 'Todos' los registros: {e}")

    print("📥 Interceptando y trasladando descarga a Excel...")
    try:
        if os.path.exists(ruta_destino):
            os.remove(ruta_destino)
    except Exception:
        pass

    # TRUCO DE ESTABILIDAD: Identificamos el botón ANTES del expect_download.
    # Anidar try-except dentro corrompe el event loop si ocurre un Timeout.
    ' Si el caller especifica un locator personalizado, lo usamos. Sino, intentamos detectar el botón de descarga de forma inteligente.'
    if locator_descarga:
        elemento_clic = page.locator(locator_descarga) if isinstance(locator_descarga, str) else locator_descarga
    else:
        btn_descarga = page.locator(".buttons-excel, button#exportar_xlsx").first
        if btn_descarga.is_visible():
            elemento_clic = btn_descarga
        else:
            elemento_clic = page.get_by_role("link").filter(has_text=re.compile(r"^$")).nth(1)

    with page.expect_download(timeout=timeout_ms) as download_info:
        elemento_clic.click()
            
    download = download_info.value
    os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
    download.save_as(ruta_destino) # <-- Función nativa que traslada el archivo a la carpeta
    logger_extraccion.info(f"Descarga exitosa: {os.path.basename(ruta_destino)}")
    print(f"✅ Archivo trasladado y guardado exitosamente en:\n   {ruta_destino}")
    return ruta_destino, download

# Automatizacion de descarga del reporte de listado de llamadas desde SAE PLUS, de sae, la ruta seria:
# Reportes -> Listado de Llamadas -> Seleccion de tipo de llamada, luego se aplican los filtros de fecha, tipo de respuesta y detalle de respuesta
def descargar_listado_llamadas(page: Page, fecha_inicial_str: str, fecha_final_str: str, ruta_destino: str, tipo_llamada = None, tipo_respuesta = None, detalle_respuesta = None):
    """
    Automatiza la navegación, filtrado y descarga del reporte 'Listado De Llamadas'.
    Permite configurar tipo de llamada, tipo de respuesta, detalle de respuesta y aplicar los filtros de fecha.
    Utiliza selectores robustos y funciones auxiliares para garantizar estabilidad y adaptabilidad a cambios menores en la interfaz de SAE
    La mayoria de los reportes: Atencion al cliente, Reclamos, Actualizacion de datos, etc... utilizan esta función.
    """
    print("🧭 Navegando al Reporte de Listado De LLamadas...")
    btn_reporte = page.get_by_role("link", name=re.compile(r"Listado De LLamadas", re.IGNORECASE)).first
    
    if not btn_reporte.is_visible():
        page.get_by_role("link", name=re.compile(r"Reportes", re.IGNORECASE)).click()
        
    btn_reporte.wait_for(state="attached", timeout=5000)
    btn_reporte.evaluate("node => node.click()")
    page.wait_for_timeout(1000)

    print("📅 Aplicando filtros en Listado de Llamadas...")
    if tipo_llamada:
        page.wait_for_selector("#id_tll", state="visible", timeout=60000)
        page.locator("#id_tll").select_option(label=tipo_llamada)
    
    llenar_fechas_sae(page, fecha_inicial_str, fecha_final_str)

    if tipo_respuesta:
        page.wait_for_timeout(500)
        page.locator("#id_trl").select_option(label=tipo_respuesta)
        
    if detalle_respuesta:
        page.wait_for_timeout(500)
        page.locator("#id_drl").select_option(label=detalle_respuesta)

    page.keyboard.press("Escape")
    page.locator("body").click(position={"x": 0, "y": 0})
    page.wait_for_timeout(300)
    btn_buscar = page.get_by_role("button", name=re.compile(r"Buscar", re.IGNORECASE)).first
    btn_buscar.click()
    
    print("⏳ Esperando respuesta de la API del servidor...")
    page.get_by_role("columnheader").nth(0).wait_for(state="visible", timeout=15000)
    
    print("🔄 Configurando columnas y refrescando la tabla...")
    mostrar_todas_columnas(page)
    
    ejecutar_descarga(page=page, ruta_destino=ruta_destino, seleccionar_todos=True)

#Automatización de descarga del reporte de listado de abonados desde SAE PLUS, de sae, la ruta seria: 
# Reportes -> Listado de Abonados -> Seleccion de estatus, luego se aplican los filtros de fecha y motivo 
# (Por defecto se selecciona "Instalación") en el motivo, el cual corresponde a la "Fecha Contrato",
# para finalmente descargar el reporte con las columnas necesarias. Siempre se pueden recrear los pasos con 
# playwright codegen o con el page.pause() y la grabación de la consola.
def listado_abonados(page: Page, fecha_inicial_str: str, fecha_final_str: str, motivo_str: str = None, estatus_list: list = None   ,col_table : list = None): #type: ignore
    """
    Función especializada para automatizar el reporte de Listado de Abonados.
    El reporte es utilizado para las ventas, churn risk y las dimensiones de cliente.
    Permite configurar estatus, motivo, columnas específicas y aplicar los filtros de fecha.
    """
    print("Navegando al Listado de Abonados...")
    page.get_by_role("link", name=re.compile(r"Reportes", re.IGNORECASE)).click()
        
    btn_listado = page.get_by_role("link", name=re.compile("Listado De Abonados", re.IGNORECASE))
    btn_listado.wait_for(state="visible", timeout=5000)
    btn_listado.click()
    
    #Validamos que solo recorra los estatus si se incluye la lista como argumento. 
    if estatus_list:
        print("☑️ Configurando Estatus de Contratos...")
        for estatus in estatus_list:
            # Evitamos que falle si ya estaba marcado o si demora un poco
            checkbox = page.locator(f"input[id='{estatus}']")
            checkbox.wait_for(state="attached", timeout=5000)
            if not checkbox.is_checked():
                checkbox.check()
    print("☑️ Configurando Estatus de Contratos...")
        
    # Aplicacion de los filtros de fecha
    print(f"📅 Aplicando filtros de fecha: {fecha_inicial_str} - {fecha_final_str}")
    print("👉 Cambiando a la pestaña de Fechas...")
    page.get_by_role("tab", name=re.compile(r"Fecha", re.IGNORECASE)).click()
    page.wait_for_timeout(500)
    
    # Función especializada para llenar las fechas que elude los errores de máscara del SAE
    llenar_fechas_sae(page, fecha_inicial_str, fecha_final_str, id_desde="input#desde_fecha", id_hasta="input#hasta_fecha")
        
    print(f"📋 Seleccionando Motivo ({motivo_str})...")
    # Busca la opción que contenga el texto (ignorando mayúsculas/minúsculas) y la selecciona
    if motivo_str:
        page.locator("#motivo").click()
        page.wait_for_selector("#motivo", state="visible", timeout=60000)
        page.locator("#motivo").select_option(label=re.compile(motivo_str, re.IGNORECASE)) # type: ignore
    else:
        #page.locator("#motivo").click() # Abrimos el dropdown
        page.locator("#motivo").select_option("instalacion") # type: ignore
    
    # Si la página requiere que se dispare el evento 'change' explícitamente después de seleccionar:
    page.locator("#motivo").dispatch_event("change")
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
            
            columnas_requeridas = col_table
            
            menu_visible = page.locator(".dropdown-menu.open:visible").first
            if columnas_requeridas:
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
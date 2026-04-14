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

def login_sae(page: Page, usuario: str = "JOAPEREZ", clave: str = "Jose304*"):
    """
    Emula la lógica de 'main.txt' en Power Automate.
    Ingresa al portal SAE y completa el login.
    """
    print("🔐 [MAIN] Ingresando al portal SAE...")
    page.goto("https://fibex.saeplus.com/")
    
    # Esperamos hasta 5 segundos a que aparezca el formulario
    try:
        page.wait_for_selector("input#login_usuario", timeout=8000)
        
        page.wait_for_timeout(1000) # 1 segundo de pausa antes de escribir
        
        # Volvemos a .fill() por velocidad y limpieza
        page.locator("input#login_usuario").fill(usuario)
        page.locator("input#pass_usuario").fill(clave)
        
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
    logger_extraccion.info(f"Autenticación exitosa en SAE para el usuario: {usuario}")
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

def ejecutar_descarga(page: Page, ruta_destino: str, timeout_ms: int = 300000, seleccionar_todos: bool = False, locator_descarga: Union[str, Locator] = None, **kwargs):
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

def descargar_listado_llamadas(page: Page, fecha_inicial_str: str, fecha_final_str: str, ruta_destino: str, tipo_llamada = None, tipo_respuesta = None, detalle_respuesta = None):
    """
    Abstrae la navegación, filtrado y descarga del reporte 'Listado De Llamadas'.
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
import os
import re
from playwright.sync_api import Page

def login_sae(page: Page, usuario: str = "JOAPEREZ", clave: str = "Jose304*"):
    """
    Emula la lógica de 'main.txt' en Power Automate.
    Ingresa al portal SAE y completa el login.
    """
    print("🔐 [MAIN] Ingresando al portal SAE...")
    page.goto("https://fibex.saeplus.com/")
    
    # Esperamos hasta 5 segundos a que aparezca el formulario
    try:
        page.wait_for_selector("input#login_usuario", timeout=5000)
        
        page.wait_for_timeout(1000) # 1 segundo de pausa antes de escribir
        
        # Volvemos a .fill() por velocidad y limpieza
        page.locator("input#login_usuario").fill(usuario)
        page.locator("input#pass_usuario").fill(clave)
        
        # Damos clic al botón explícitamente
        page.get_by_role("button", name=" Iniciar Sesión").click()
        
        # Manejo del bug del Pop-up de verificación en SAE
        try:
            # Esperamos 5 segundos a ver si el login fue exitoso directo
            page.wait_for_selector("a[href='#reportes']", timeout=5000)
        except Exception:
            print("⚠️ [MAIN] Posible pop-up de verificación detectado. Forzando segundo clic...")
            # Si no ha entrado, volvemos a darle clic al botón para saltar el error
            page.get_by_role("button", name=" Iniciar Sesión").click()
            
    except Exception:
        print("⚠️ [MAIN] No se detectó formulario de login (posible sesión activa).")
        
    # Validamos que cargue la página principal antes de seguir
    page.wait_for_selector("a[href='#reportes']", timeout=30000)
    print("✅ [MAIN] Autenticación completada.")

def ejecutar_descarga(page: Page, ruta_destino: str, timeout_ms: int = 60000, id_tabla: str = "datagrid1", custom_locator=None):
    """
    Emula la lógica de 'subflow2.txt' en Power Automate.
    Espera que la tabla cargue, presiona Exportar a Excel y guarda el archivo.
    """
    print(f"⏳ [SUBFLOW 2] Esperando a que cargue la tabla de resultados ({id_tabla})...")
    # 1. Esperamos a que el control de paginación de la tabla específica esté visible
    page.wait_for_selector(f"select[name=\"{id_tabla}_length\"]", state="visible", timeout=timeout_ms)
    
    print("🗂️ [SUBFLOW 2] Cambiando paginación a 'Todos' (-1)...")
    page.locator(f"select[name=\"{id_tabla}_length\"]").select_option("-1")
    
    # ESPERA DINÁMICA: DataTables muestra un elemento "_processing" cuando carga AJAX.
    # Esperamos explícitamente a que este elemento de carga desaparezca (hidden).
    try:
        page.wait_for_selector(f"div#{id_tabla}_processing", state="hidden", timeout=15000)
    except Exception:
        page.wait_for_timeout(3000) # Fallback de seguridad por si el ID no existe

    print("📥 [SUBFLOW 2] Interceptando descarga del archivo...")
    with page.expect_download(timeout=timeout_ms) as download_info:
        if custom_locator is not None:
            custom_locator.click()
        else:
            # Seleccionamos explícitamente el botón Excel de la tabla indicada
            # para evitar el error 'strict mode violation' cuando hay varias tablas en pantalla.
            page.locator(f"a.buttons-excel[aria-controls='{id_tabla}']").click()
        
    download = download_info.value
    os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
    download.save_as(ruta_destino)
    print(f"✅ [SUBFLOW 2] Archivo guardado exitosamente en:\n   {ruta_destino}")
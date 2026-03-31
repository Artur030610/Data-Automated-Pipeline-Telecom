import pandas as pd
from rapidfuzz import fuzz
import re
import unicodedata
import os
import glob
import sys
from unidecode import unidecode

# --- CONFIGURACIÓN DE RUTAS ---
# Definimos la raiz del Data Lake para construir rutas desde ahí
root_datalake = r"C:\Users\josperez\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\raw_data"

# Ruta específica donde llegan las ventas
folder_ventas = os.path.join(root_datalake, "2-Ventas", "3- Ventas Listado de abonados")

# Ruta específica de la maestra
ruta_maestra = os.path.join(root_datalake, "14-Universo de asesores", "Data_Universo_Asesores.xlsx")

# --- VALIDACIÓN DE RUTAS ---
if not os.path.exists(ruta_maestra):
    print(f"ERROR: No se encuentra la maestra en: {ruta_maestra}")
    print("Verifica que el nombre de la carpeta '14-Universo de asesores' sea exacto.")
    sys.exit(1)

# --- PASO 1: DETECTAR ARCHIVO MÁS RECIENTE ---
print("Buscando archivo de ventas más reciente...")

archivos_encontrados = glob.glob(os.path.join(folder_ventas, '*.xlsx'))

if not archivos_encontrados:
    print(f"ERROR: No hay archivos .xlsx en {folder_ventas}")
    sys.exit(1)

# Toma el archivo más nuevo por fecha de modificación
ruta_archivo_ventas = max(archivos_encontrados, key=os.path.getctime)
nombre_archivo = os.path.basename(ruta_archivo_ventas)

print(f"Procesando: {nombre_archivo}")

# --- FUNCIONES DE LIMPIEZA ---
def clean_text(text):
    if isinstance(text, str):
        if re.search(r'\d+/\d+', text):
            text = re.sub(r'[^0-9/\s]', '', text)
        else:
            text = re.sub(r'[^a-zA-Z0-9\sÀ-ÖØ-öø-ÿ]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    return text

def normalize_text(text):
    if isinstance(text, str):
        text = unicodedata.normalize("NFKD", text)
        text = unidecode(text)
        return text.lower().strip()
    return text

# --- CARGA DE DATOS ---
try:
    print("Cargando datos...")
    df_maestra = pd.read_excel(ruta_maestra)
    df_ventas = pd.read_excel(ruta_archivo_ventas)
except Exception as e:
    print(f"Error crítico al leer Excel: {e}")
    sys.exit(1)

# --- PROCESAMIENTO ---
print("Limpiando y Normalizando...")

if 'Vendedor' in df_ventas.columns:
    df_ventas['Vendedor'] = df_ventas['Vendedor'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df_ventas['Vendedor'] = df_ventas['Vendedor'].apply(clean_text)
    df_ventas['Vendedor'] = df_ventas['Vendedor'].apply(normalize_text)

# Preparación Maestra
if 'Nombre' in df_maestra.columns and 'Apellido' in df_maestra.columns:
    df_maestra["Nombre_normalized"] = df_maestra["Nombre"].apply(normalize_text).str.lower().str.strip()
    df_maestra["Apellido_normalized"] = df_maestra["Apellido"].apply(normalize_text).str.lower().str.strip()
    df_maestra["nombre_completo"] = df_maestra["Nombre_normalized"] + " " + df_maestra["Apellido_normalized"]
elif 'Nombre' in df_maestra.columns and 'Apeliido' in df_maestra.columns: # Manejo de typos en origen
     df_maestra["Nombre_normalized"] = df_maestra["Nombre"].apply(normalize_text).str.lower().str.strip()
     df_maestra["Apellido_normalized"] = df_maestra["Apeliido"].apply(normalize_text).str.lower().str.strip()
     df_maestra["nombre_completo"] = df_maestra["Nombre_normalized"] + " " + df_maestra["Apellido_normalized"]

# --- ALGORITMO FUZZY MATCHING ---
def identificar_vendedor(texto_asesor):
    texto_lower = str(texto_asesor).lower()

    if re.search(r'\bventas\b', texto_lower) or re.search(r'\bventas calle\b', texto_lower) or re.search(r'\bagente\b', texto_lower):
        return pd.Series([None, None, None, "No detectado", None, None, None])

    best_combined_score = -1
    match_type = "No detectado"
    matched_oficina = None
    matched_estado = None
    best_match = None
    best_score_nombre = -1
    best_score_apellido = -1
    fuzzy_threshold = 90
    review_lower_threshold = 86

    if 'nombre_completo' in df_maestra.columns: 
        for index, fila in df_maestra.iterrows():
            # Obtener datos de la maestra de forma segura
            nombre_temp = fila.get("Nombre_normalized", "")
            if pd.isna(nombre_temp): nombre_temp = ""
            
            apellido_temp = fila.get("Apellido_normalized", "")
            if pd.isna(apellido_temp): apellido_temp = ""
            
            nombre = str(nombre_temp)
            apellido = str(apellido_temp)
            
            nombre_completo = f"{nombre} {apellido}".strip()
            
            if not nombre or not apellido: continue

            if nombre_completo in texto_lower:
                return pd.Series([nombre_completo, fila.get("Estado"), fila.get("Oficina"), "Exacta", 100, 100, 100])

            score_nombre = fuzz.partial_ratio(nombre, texto_lower)
            score_apellido = fuzz.partial_ratio(apellido, texto_lower)
            combined_score = (score_nombre + score_apellido) / 2

            if combined_score > best_combined_score:
                 best_combined_score = combined_score
                 best_match = nombre_completo
                 matched_estado = fila.get("Estado")
                 matched_oficina = fila.get("Oficina")
                 best_score_nombre = score_nombre
                 best_score_apellido = score_apellido

    if best_match is not None:
        if best_combined_score >= fuzzy_threshold: match_type = "Fuzzy"
        elif best_combined_score >= review_lower_threshold: match_type = "Pendiente de Revisión"
        else: match_type = "No detectado"

    if match_type == "No detectado":
        match_ofi = re.search(r'ofi\w*', texto_lower)
        if match_ofi:
            match_extract_office = re.search(r'ofi\w*\s*(.*)', texto_lower)
            extracted_office = match_extract_office.group(1).strip() if match_extract_office else None
            return pd.Series([texto_asesor, None, extracted_office, "Oficina Detectada", None, None, None])

    if match_type != "No detectado":
         return pd.Series([best_match, matched_estado, matched_oficina, match_type, best_score_nombre, best_score_apellido, best_combined_score])
    else:
         return pd.Series([None, None, None, "No detectado", None, None, None])

print("Ejecutando Fuzzy Matching...")

if 'Vendedor' in df_ventas.columns:
    df_ventas[["nombre_detectado", "Estado", "oficina_comercial", "tipo_coincidencia", "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"]] = df_ventas["Vendedor"].apply(identificar_vendedor)
    
    # ---------------------------------------------------------
    # --- NUEVA LÓGICA: REGLA DE EXCEPCIÓN PARA BEJUMA ---
    # ---------------------------------------------------------
    print("Aplicando reglas de negocio específicas (Bejuma)...")
    
    # Creamos una máscara para identificar filas donde el vendedor contenga "bejuma"
    # case=False asegura que detecte 'Bejuma', 'BEJUMA', 'bejuma'
    mask_bejuma = df_ventas["Vendedor"].str.contains("bejuma", case=False, na=False)
    
    # Sobrescribimos la columna oficina_comercial en esas filas
    df_ventas.loc[mask_bejuma, "oficina_comercial"] = "OFICINA BEJUMA"
    
    # Opcional: Actualizamos el tipo de coincidencia para saber que fue detectado por esta regla
    df_ventas.loc[mask_bejuma, "tipo_coincidencia"] = "Oficina Detectada"
    
    # Opcional: Limpiamos el nombre detectado si venía sucio, o lo dejamos para referencia
    # df_ventas.loc[mask_bejuma, "nombre_detectado"] = "Oficina Bejuma" 
    
else:
    print("ADVERTENCIA: Columna 'Vendedor' no encontrada.")

# --- GUARDADO ---
print(f"Actualizando archivo: {nombre_archivo} ...")

try:
    df_ventas.to_excel(ruta_archivo_ventas, index=False)
    print("¡Éxito! Proceso terminado.")
except PermissionError:
    print("ERROR: Cierra el archivo Excel antes de ejecutar el script.")
    sys.exit(1)
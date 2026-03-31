import pandas as pd
from rapidfuzz import fuzz
import re
import unicodedata
import os
import glob
import sys
from unidecode import unidecode

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import console

# --- FUNCIONES DE LIMPIEZA (LÓGICA ORIGINAL) ---
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

def identificar_vendedor(texto_asesor, df_maestra):
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

def run_fuzzy_on_latest_sale():
    # --- CONFIGURACIÓN DE RUTAS ---
    folder_ventas = PATHS.get("ventas_abonados")
    ruta_maestra = os.path.join(PATHS.get("raw_asesores_univ_14"), "Data_Universo_Asesores.xlsx")

    if not os.path.exists(ruta_maestra):
        console.print(f"[red]ERROR: No se encuentra la maestra en: {ruta_maestra}[/]")
        sys.exit(1)

    # --- PASO 1: DETECTAR ARCHIVO MÁS RECIENTE ---
    console.print("🔎 Buscando archivo de ventas más reciente para aplicar Fuzzy...")
    archivos_encontrados = glob.glob(os.path.join(folder_ventas, '*.xlsx'))
    if not archivos_encontrados:
        console.print(f"[yellow]⚠️ No hay archivos .xlsx en {folder_ventas} para procesar.[/]")
        return

    ruta_archivo_ventas = max(archivos_encontrados, key=os.path.getctime)
    nombre_archivo = os.path.basename(ruta_archivo_ventas)
    console.print(f"   -> Procesando: {nombre_archivo}")

    # --- CARGA Y PROCESAMIENTO ---
    try:
        df_maestra = pd.read_excel(ruta_maestra)
        df_ventas = pd.read_excel(ruta_archivo_ventas)

        if 'Vendedor' in df_ventas.columns:
            df_ventas['Vendedor'] = df_ventas['Vendedor'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip().apply(clean_text).apply(normalize_text)

        # Preparación Maestra
        nombre_col = "Nombre"
        apellido_col = "Apellido" if "Apellido" in df_maestra.columns else "Apeliido"
        if nombre_col in df_maestra.columns and apellido_col in df_maestra.columns:
            df_maestra["Nombre_normalized"] = df_maestra[nombre_col].apply(normalize_text)
            df_maestra["Apellido_normalized"] = df_maestra[apellido_col].apply(normalize_text)
            df_maestra["nombre_completo"] = df_maestra["Nombre_normalized"] + " " + df_maestra["Apellido_normalized"]

        if 'Vendedor' in df_ventas.columns and 'nombre_completo' in df_maestra.columns:
            df_ventas[["nombre_detectado", "Estado", "oficina_comercial", "tipo_coincidencia", "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"]] = df_ventas["Vendedor"].apply(lambda x: identificar_vendedor(x, df_maestra))
            
            # Regla de negocio para Bejuma
            mask_bejuma = df_ventas["Vendedor"].str.contains("bejuma", case=False, na=False)
            df_ventas.loc[mask_bejuma, "oficina_comercial"] = "OFICINA BEJUMA"
            df_ventas.loc[mask_bejuma, "tipo_coincidencia"] = "Oficina Detectada"

        # --- GUARDADO (SOBREESCRITURA) ---
        console.print(f"💾 Actualizando archivo original: {nombre_archivo} ...")
        df_ventas.to_excel(ruta_archivo_ventas, index=False)
        console.print("[bold green]✅ ¡Éxito! Archivo de ventas enriquecido.[/]")

    except PermissionError:
        console.print(f"[bold red]ERROR CRÍTICO: El archivo '{nombre_archivo}' está abierto. Ciérralo y vuelve a intentarlo.[/]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error crítico durante el fuzzy matching: {e}[/]")
        sys.exit(1)

if __name__ == "__main__":
    run_fuzzy_on_latest_sale()
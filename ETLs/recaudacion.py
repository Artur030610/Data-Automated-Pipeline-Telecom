import pandas as pd
import numpy as np
import sys
import os

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# -----------------------------

from config import PATHS, MAPA_MESES
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold white]PIPELINE INTEGRAL: RECAUDACIÓN + HORAS[/]")
    
    # ==========================================
    # 1. EXTRACCIÓN
    # ==========================================
    
    # --- 1.1 Cargar Recaudación (Tabla Principal) ---
    console.print("[green]📥 Cargando Recaudación...[/]")
    cols_input_recaudacion = [
        "ID Contrato", "ID Pago", "N° Abonado", "Fecha", "Total Pago", 
        "Forma de Pago", "Banco", "Oficina Cobro", 
        "Fecha Contrato", "Estatus", "Suscripción", "Grupo Afinidad", 
        "Nombre Franquicia", "Ciudad", "Cobrador"]
    
    df_recaudacion = leer_carpeta(
        PATHS["raw_recaudacion"], 
        filtro_exclusion="Consolidado", 
        columnas_esperadas=cols_input_recaudacion
    )
    
    if df_recaudacion.empty: return
    filas_iniciales = len(df_recaudacion)

    # --- 1.2 Cargar Horas (Tabla Auxiliar) ---
    console.print("[cyan]📥 Cargando Horas de Pago...[/]")
    cols_input_horas = ["ID Pago", "Hora de Pago"]
    
    df_horas = leer_carpeta(
        PATHS["raw_horaspago"], 
        columnas_esperadas=cols_input_horas
    )

    # ==========================================
    # 2. LIMPIEZA DE LLAVES Y DUPLICADOS
    # ==========================================
    
    def limpiar_id(serie):
        return (serie.astype(str)
                     .str.replace("'", "", regex=False) 
                     .str.replace(r'\.0$', '', regex=True)
                     .str.strip())

    # --- A) Limpieza en Recaudación ---
    df_recaudacion['ID Pago'] = limpiar_id(df_recaudacion['ID Pago'])
    
    # [NUEVO] Eliminamos duplicados EXACTOS (Gemelos Malvados)
    # Si toda la fila es igual, es basura. Se va.
    # Si el ID es igual pero el monto cambia, ES DINERO REAL. Se queda.
    filas_antes = len(df_recaudacion)
    df_recaudacion = df_recaudacion.drop_duplicates()
    filas_borradas = filas_antes - len(df_recaudacion)
    
    if filas_borradas > 0:
        console.print(f"[yellow]🧹 Se eliminaron {filas_borradas} registros duplicados idénticos en Recaudación.[/]")

    # --- B) Limpieza en Horas ---
    if not df_horas.empty:
        df_horas = df_horas.rename(columns={"ID pago": "ID Pago"})
        df_horas['ID Pago'] = limpiar_id(df_horas['ID Pago'])
        
        # [CRÍTICO] En Horas SÍ forzamos unicidad de ID
        # Nos quedamos con la primera hora que encontremos para ese ID
        df_horas = df_horas.drop_duplicates(subset=['ID Pago'])
    
    # ==========================================
    # 3. MERGE (UNIFICACIÓN)
    # ==========================================
    console.print("🔄 Unificando tablas...")
    
    if not df_horas.empty:
        # Left Join: Mantiene todas las filas de recaudación (incluso las de pagos parciales)
        df_final = df_recaudacion.merge(df_horas, on='ID Pago', how='left')
    else:
        df_final = df_recaudacion.copy()
        df_final['Hora de Pago'] = None

    # ==========================================
    # 4. TRANSFORMACIÓN DE NEGOCIO
    # ==========================================
    console.print("⚙️ Aplicando reglas de negocio...")

    # Tipos
    df_final['N° Abonado'] = df_final['N° Abonado'].astype(str).replace('nan', None)
    df_final['ID Contrato'] = df_final['ID Contrato'].astype(str).replace('nan', None)
    df_final["Fecha"] = pd.to_datetime(df_final["Fecha"], dayfirst=True, errors="coerce")
    df_final['Total Pago'] = pd.to_numeric(df_final['Total Pago'], errors='coerce').fillna(0)

    # Filtros
    palabras_excluir = "VIRTUAL|Virna|Fideliza|Externa|Unicenter|Compensa"
    df_final = df_final[~df_final['Oficina Cobro'].astype(str).str.contains(palabras_excluir, case=False, na=False, regex=True)].copy()

    # Columnas calculadas
    df_final['Tipo de afluencia'] = "RECAUDACIÓN"
    df_final['Mes'] = df_final['Fecha'].dt.month.map(MAPA_MESES)

    # Oficinas Propias
    oficinas_propias = [
        "OFC COMERCIAL CUMANA", "OFC- LA ASUNCION", "OFC SAN ANTONIO DE CAPYACUAL", "OFC TINACO", 
        "OFC VILLA ROSA", "OFC-SANTA FE", "OFI CARIPE MONAGAS", "OFI TINAQUILLO", "OFI-BARCELONA", 
        "OFI-BARINAS", "OFI-BQTO", "OFIC GALERIA EL PARAISO", "OFIC SAMBIL-VALENCIA", 
        "OFIC. PARRAL VALENCIA", "OFIC. TORRE FIBEX VIÑEDO", "OFI-CARACAS PROPATRIA", 
        "OFIC-BOCA DE UCHIRE", "OFIC-CARICUAO", "OFIC-COMERCIAL SANTA FE", "OFICINA ALIANZA MALL", 
        "OFICINA MARGARITA", "OFICINA SAN JUAN DE LOS MORROS", "OFIC-JUAN GRIEGO-MGTA", 
        "OFIC-METROPOLIS-BQTO", "OFIC-MGTA_DIAZ", "OFI-LECHERIA", "OFI-METROPOLIS", "OFI-PARAISO", 
        "OFI-PASEO LAS INDUSTRIAS", "OFI-PTO CABELLO", "OFI-PTO LA CRUZ", "OFI-SAN CARLOS", "OFI-VIA VENETO"
    ]
    
    df_final['Clasificacion'] = np.where(
        df_final['Oficina Cobro'].isin(oficinas_propias), 
        "OFICINAS PROPIAS", 
        "ALIADOS Y DESARROLLO"
    )
    
    df_final = df_final.rename(columns={"Oficina Cobro": "Oficina", "Cobrador": "Vendedor"})
    
    # Limpieza final por si el merge generó redundancia inesperada
    df_final = df_final.drop_duplicates()

    # ==========================================
    # 5. CARGA
    # ==========================================
    cols_output = [
        "ID Contrato", "ID Pago", "N° Abonado", "Fecha", "Total Pago", 
        "Forma de Pago", "Banco", "Oficina", "Fecha Contrato", 
        "Estatus", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Vendedor", "Tipo de afluencia", "Mes", "Clasificacion",
        "Hora de Pago"
    ]
    
    df_final = df_final.reindex(columns=cols_output)
    guardar_parquet(df_final, "Recaudacion_Gold.parquet", filas_iniciales=filas_iniciales)

if __name__ == "__main__":
    ejecutar()
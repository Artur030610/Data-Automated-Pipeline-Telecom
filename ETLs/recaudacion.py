import pandas as pd
import numpy as np
import os
import sys
import glob 

# --- CAMBIO 1: IMPORTAMOS LA NUEVA FUNCIÓN DE POLARS ---
from utils import standard_hours, leer_carpeta, guardar_parquet, reportar_tiempo, console, ingesta_inteligente, obtener_rango_fechas
from utils import ingesta_incremental_polars  # <--- NUEVO

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# -----------------------------

from config import PATHS, MAPA_MESES

@reportar_tiempo
def ejecutar():
    console.rule("[bold white]PIPELINE INTEGRAL: RECAUDACIÓN + HORAS (BRONZE DUAL / GOLD FULL)[/]")
    
    # 1. RUTAS
    RUTA_RAW_RECAUDACION = PATHS["raw_recaudacion"]
    RUTA_RAW_HORAS = PATHS["raw_horaspago"]
    NOMBRE_GOLD = "Recaudacion_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    
    # --- RUTAS DE LAS DOS CAPAS BRONZE ---
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Recaudacion_Raw_Bronze.parquet")
    RUTA_BRONZE_HORAS = os.path.join(PATHS.get("bronze", "data/bronze"), "Horas_Raw_Bronze.parquet")

    # =========================================================
    # --- PASO 1: ACTUALIZACIÓN BRONZE DOBLE CON POLARS ---
    # =========================================================
    try:
        console.print("[dim]Actualizando Bronze de Recaudación...[/dim]")
        ingesta_incremental_polars(
            ruta_raw=RUTA_RAW_RECAUDACION,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha"
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze de Recaudación no se actualizó. Error: {e}[/]")

    try:
        console.print("[dim]Actualizando Bronze de Horas...[/dim]")
        ingesta_incremental_polars(
            ruta_raw=RUTA_RAW_HORAS,
            ruta_bronze_historico=RUTA_BRONZE_HORAS,
            columna_fecha=None  # Sin fecha para habilitar Append+Unique en Polars
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze de Horas no se actualizó. Error: {e}[/]")

    # =========================================================
    # --- PASO 2: LECTURA FULL DESDE AMBOS BRONZE ---
    # =========================================================
    if not os.path.exists(RUTA_BRONZE):
        console.print("[red]❌ No se encontró la capa Bronze de Recaudación. Ejecución abortada.[/]")
        return
        
    console.print("[cyan]📥 Leyendo histórico completo desde capa Bronze (Recaudación)...[/]")
    df_nuevo = pd.read_parquet(RUTA_BRONZE)

    if df_nuevo.empty:
        console.print("[bold green]✅ No hay datos de Recaudación en Bronze para procesar.[/]")
        return

    # Leemos el histórico de horas
    df_horas = pd.DataFrame()
    if os.path.exists(RUTA_BRONZE_HORAS):
        console.print("[cyan]📥 Leyendo histórico completo desde capa Bronze (Horas)...[/]")
        df_horas = pd.read_parquet(RUTA_BRONZE_HORAS)
    else:
        console.print("[warning]⚠️ No se encontró Bronze de Horas. Se asignarán horas en blanco.[/]")

    # =========================================================
    # --- PASO 3: PROCESAMIENTO PANDAS (Lógica de Negocio) ---
    # =========================================================
    console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} pagos totales...[/]")
    
    cols_input_recaudacion = [
        "ID Contrato", "ID Pago", "N° Abonado", "Fecha", "Total Pago", 
        "Forma de Pago", "Banco", "Oficina Cobro", 
        "Fecha Contrato", "Estatus", "Suscripción", "Grupo Afinidad", 
        "Nombre Franquicia", "Ciudad", "Cobrador"
    ]
    df_nuevo = df_nuevo.reindex(columns=cols_input_recaudacion)

    def limpiar_id(serie):
        return (serie.astype(str)
                .str.replace("'", "", regex=False) 
                .str.replace(r'\.0$', '', regex=True)
                .str.strip())

    df_nuevo['ID Pago'] = limpiar_id(df_nuevo['ID Pago'])
    df_nuevo = df_nuevo.drop_duplicates(subset=["ID Pago"])

    # --- FUSIÓN CON HORAS (Lógica Blindada contra Columnas Duplicadas) ---
    if not df_horas.empty:
        # 1. Resolvemos el conflicto si Polars trajo ambas columnas ("ID Pago" e "ID pago")
        if "ID pago" in df_horas.columns and "ID Pago" in df_horas.columns:
            df_horas["ID Pago"] = df_horas["ID Pago"].fillna(df_horas["ID pago"])
            df_horas = df_horas.drop(columns=["ID pago"])
        elif "ID pago" in df_horas.columns:
            df_horas = df_horas.rename(columns={"ID pago": "ID Pago"})
            
        # 2. Ahora es 100% seguro aplicar la limpieza a la Serie
        df_horas['ID Pago'] = limpiar_id(df_horas['ID Pago'])
        df_horas = df_horas.drop_duplicates(subset=['ID Pago'])
        
        # 3. Cruzamos directamente los dos históricos maestros
        df_nuevo = df_nuevo.merge(df_horas[['ID Pago', 'Hora de Pago']], on='ID Pago', how='left')
    else:
        df_nuevo['Hora de Pago'] = None

    # --- LÓGICA DE NEGOCIO ---
    df_nuevo['N° Abonado'] = df_nuevo['N° Abonado'].astype(str).replace('nan', None)
    df_nuevo['ID Contrato'] = df_nuevo['ID Contrato'].astype(str).replace('nan', None)
    df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce")
    df_nuevo['Total Pago'] = pd.to_numeric(df_nuevo['Total Pago'], errors='coerce').fillna(0)

    palabras_excluir = "VIRTUAL|Virna|Fideliza|Externa|Unicenter|Compensa"
    df_nuevo = df_nuevo[~df_nuevo['Oficina Cobro'].astype(str).str.contains(palabras_excluir, case=False, na=False, regex=True)].copy()

    df_nuevo['Tipo de afluencia'] = "RECAUDACIÓN"
    df_nuevo['Mes'] = df_nuevo['Fecha'].dt.month.map(MAPA_MESES)

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
    
    df_nuevo['Clasificacion'] = np.where(
        df_nuevo['Oficina Cobro'].isin(oficinas_propias), 
        "OFICINAS PROPIAS", 
        "ALIADOS Y DESARROLLO"
    )
    
    df_nuevo = df_nuevo.rename(columns={"Oficina Cobro": "Oficina", "Cobrador": "Vendedor"})
    
    cols_output = [
        "ID Contrato", "ID Pago", "N° Abonado", "Fecha", "Total Pago", 
        "Forma de Pago", "Banco", "Oficina", "Fecha Contrato", 
        "Estatus", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Vendedor", "Tipo de afluencia", "Mes", "Clasificacion",
        "Hora de Pago"
    ]
    df_nuevo = df_nuevo.reindex(columns=cols_output)

    # =========================================================
    # --- PASO 4: ESCRITURA FULL EN CAPA GOLD ---
    # =========================================================
    df_final = df_nuevo.copy()

    if not df_final.empty:
        filas_antes = len(df_final)
        df_final = df_final.drop_duplicates(subset=["ID Pago"], keep='last')
        df_final = standard_hours(df_final, 'Hora de Pago')
        guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=filas_antes)

if __name__ == "__main__":
    ejecutar()
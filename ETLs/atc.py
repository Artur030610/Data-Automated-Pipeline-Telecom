import pandas as pd
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, MAPA_MESES
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, standard_hours, ingesta_incremental_polars

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]🎧 ETL: ATENCIÓN AL CLIENTE (POLARS BRONZE + PANDAS GOLD)[/]")
    
    # 1. DEFINICIÓN DE RUTAS
    RUTA_RAW = PATHS["raw_atencion"]
    NOMBRE_GOLD = "Atencion_Cliente_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Atencion_Cliente_Raw_Bronze.parquet")

    # ---------------------------------------------------------
    # 2. INGESTA BRONZE (POLARS - UPSERT POR FECHA)
    # ---------------------------------------------------------
    console.print("[cyan]🚀 Fase 1: Actualizando capa Bronze con Polars...[/]")
    # Usamos tu función élite. La columna de fecha en los raw de ATC es "Fecha Llamada"
    ingesta_exitosa = ingesta_incremental_polars(
        ruta_raw=RUTA_RAW, 
        ruta_bronze_historico=RUTA_BRONZE, 
        columna_fecha="Fecha Llamada"
    )

    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold red]❌ No existe archivo Bronze para procesar el Gold.[/]")
        return

    # ---------------------------------------------------------
    # 3. LECTURA DESDE BRONZE (Milisegundos)
    # ---------------------------------------------------------
    console.print("[cyan]🚀 Fase 2: Construyendo Gold desde Bronze...[/]")
    try:
        df_total = pd.read_parquet(RUTA_BRONZE)
    except Exception as e:
        console.print(f"[bold red]❌ Error leyendo Bronze: {e}[/]")
        return

    if df_total.empty:
        console.print("[yellow]⚠️ El Bronze está vacío.[/]")
        return

    # ---------------------------------------------------------
    # 4. TRANSFORMACIÓN Y LIMPIEZA (PANDAS)
    # ---------------------------------------------------------
    # A. Renombramiento Inicial
    df_total = df_total.rename(columns={
        "Franquicia": "Nombre Franquicia", 
        "Fecha Llamada": "Fecha", 
        "Responsable": "Vendedor", 
        "Hora Llamada": "Hora"
    })

    # B. LIMPIEZA DE IDs
    cols_ids = ['N° Abonado', 'Documento']
    for col in [c for c in cols_ids if c in df_total.columns]:
        df_total[col] = df_total[col].astype(str).str.strip()
        df_total[col] = df_total[col].str.replace(r'\.0$', '', regex=True)
        df_total[col] = df_total[col].str.replace('.', '', regex=False)
        df_total[col] = df_total[col].replace({'nan': None, 'None': None, '': None})

    # C. FECHAS Y TEXTOS
    df_total["Fecha"] = pd.to_datetime(df_total["Fecha"], dayfirst=True, errors="coerce")
    df_total['Vendedor'] = df_total['Vendedor'].fillna('').astype(str).str.upper()

    # D. FILTROS DE NEGOCIO (Exclusiones)
    if 'Tipo Respuesta' in df_total.columns:
        mask_excluir = df_total['Tipo Respuesta'].isin(["AFILIACION DE SERVICIO", "PAGO DEL SERVICIO"])
        df_total = df_total[~mask_excluir].copy()

    # E. ENRIQUECIMIENTO
    df_total['Tipo de afluencia'] = "ATENCIÓN AL CLIENTE"
    df_total['Mes'] = df_total['Fecha'].dt.month.map(MAPA_MESES)

    # Selección y Orden de Columnas
    cols_output = [
        "N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha", "Hora", "Tipo Llamada","Tipo Respuesta", "Detalle Respuesta", 
        "Vendedor", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad", "Tipo de afluencia","Observación"
    ]
    
    # Aseguramos que todas existan antes del reindex
    for c in cols_output:
        if c not in df_total.columns:
            df_total[c] = None
            
    df_total = df_total.reindex(columns=cols_output)

    # ---------------------------------------------------------
    # 5. DEDUPLICACIÓN GLOBAL Y GUARDADO
    # ---------------------------------------------------------
    subset_final = [
        "N° Abonado", "Documento", "Fecha", "Hora", 
        "Tipo Respuesta", "Detalle Respuesta", "Vendedor"
    ]
    # Keep last como última red de seguridad
    df_final = df_total.drop_duplicates(subset=subset_final, keep='last')
    
    df_final = limpiar_nulos_powerbi(df_final)
    df_final = standard_hours(df_final, 'Hora')
    
    guardar_parquet(
        df_final, 
        NOMBRE_GOLD,
        filas_iniciales=len(df_total),
        ruta_destino=PATHS.get("gold", "")
    )
    console.print(f"[bold green]✅ ATC Gold generado. Total filas únicas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
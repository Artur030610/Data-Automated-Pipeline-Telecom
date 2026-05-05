import polars as pl
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)  # Sube el segundo nivel
sys.path.append(grandparent_dir)

from config import PATHS, MAPA_MESES
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, standard_hours, ingesta_incremental_polars

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]🎧 ONTs APAGADAS ")
    
    # 1. DEFINICIÓN DE RUTAS
    RUTA_RAW = PATHS["raw_ont_off"]
    NOMBRE_GOLD = "ONT_OFF_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_GOLD = PATHS.get("gold", "data/gold")
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "ONT_OFF_Raw_Bronze.parquet")

    # ---------------------------------------------------------
    # 2. INGESTA BRONZE (POLARS - UPSERT POR FECHA)
    # ---------------------------------------------------------
    console.print("[cyan]🚀 Fase 1: Actualizando capa Bronze con Polars...[/]")
    ingesta_incremental_polars(
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
        
        df_total= pl.scan_parquet(RUTA_BRONZE)
    except Exception as e:
        console.print(f"[bold red]❌ Error leyendo Bronze: {e}[/]")
        return

    # ---------------------------------------------------------
    # 4. TRANSFORMACIÓN Y LIMPIEZA (POLARS)
    # ---------------------------------------------------------
    # A. RENOMBRANDO COLUMNAS (Estandarización)
    df_total = df_total.rename({
        "Franquicia": "Nombre Franquicia", 
        "Fecha Llamada": "Fecha", 
        "Responsable": "Vendedor", 
        "Hora Llamada": "Hora"
    })
    df_total = df_total.select(["N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha", "Hora", "Tipo Llamada","Tipo Respuesta", "Detalle Respuesta", 
        "Vendedor", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad"])

    df_atc = pl.scan_parquet(os.path.join(RUTA_GOLD, "Atencion_Cliente_Gold.parquet"))
    # B. TRANSFORMACION DE TIPOS
    df_total = df_total.with_columns(pl.col("Hora").str.to_time("%H:%M:%S"))
    #df_atc = df_atc.with_columns(pl.col("Hora").str.to_time("%H:%M:%S"))
    df_atc = df_atc.with_columns(pl.col("Fecha").dt.date())
    # C. QUITAR DUPLICADOS
    
    # D. FILTRAR SOLO ONTs APAGADAS y
    df_atc = df_atc.filter(pl.col("Tipo Respuesta") == "CLIENTE ACTIVO CON ONT APAGADA")

    df_final = pl.concat([df_total, df_atc], how="diagonal_relaxed")

    df_final = df_final.unique(subset=["N° Abonado", "Fecha", "Hora"], keep='last')
    df_final = df_final.select(["N° Abonado", "Documento", "Cliente", "Estatus", 
        "Fecha", "Hora", "Tipo Llamada","Tipo Respuesta", "Detalle Respuesta", 
        "Vendedor", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
        "Ciudad"])
    
    df_final.sink_parquet(RUTA_GOLD_COMPLETA, compression="zstd", row_group_size=100000)
    print(df_final.describe())
    
if __name__ == "__main__":
    ejecutar()
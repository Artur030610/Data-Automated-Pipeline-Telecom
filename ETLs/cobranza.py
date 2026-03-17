import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, standard_hours, ingesta_incremental_polars

@reportar_tiempo
def ejecutar():
    console.rule("[bold blue]PIPELINE OPERATIVOS: COBRANZA (POLARS BRONZE + PANDAS GOLD)[/]")

    # 1. Rutas
    RUTA_RAW = PATHS["raw_cobranza"]
    NOMBRE_GOLD = "Llamadas_Cobranza_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Cobranza_Raw_Bronze.parquet")

    # -------------------------------------------------------------------------
    # 2. INGESTA BRONZE (POLARS - UPSERT POR FECHA)
    # -------------------------------------------------------------------------
    console.print("[cyan]🚀 Fase 1: Actualizando capa Bronze con Polars...[/]")
    # Usamos la función élite para que el I/O pesado de los Excel sea en milisegundos
    ingesta_exitosa = ingesta_incremental_polars(
        ruta_raw=RUTA_RAW, 
        ruta_bronze_historico=RUTA_BRONZE, 
        columna_fecha="Fecha Llamada"
    )

    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold red]❌ No existe archivo Bronze para procesar el Gold.[/]")
        return

    # -------------------------------------------------------------------------
    # 3. LECTURA DESDE BRONZE (PANDAS - LECTURA ATÓMICA)
    # -------------------------------------------------------------------------
    console.print("[cyan]🚀 Fase 2: Construyendo Gold desde Bronze...[/]")
    try:
        df_total = pd.read_parquet(RUTA_BRONZE)
    except Exception as e:
        console.print(f"[bold red]❌ Error leyendo Bronze: {e}[/]")
        return

    if df_total.empty:
        console.print("[yellow]⚠️ El Bronze está vacío.[/]")
        return

    # -------------------------------------------------------------------------
    # 4. TRANSFORMACIÓN Y LIMPIEZA TOTAL
    # ---------------------------------------------------------
    console.print(f"[cyan]🛠️ Transformando {len(df_total)} registros totales...[/]")
    
    with console.status("[bold green]Procesando reglas de negocio...[/]", spinner="dots"):
        
        # --- FECHAS Y TEXTOS ---
        # Garantizamos que la fecha se lea correctamente (Día/Mes)
        df_total["Fecha Llamada"] = pd.to_datetime(df_total["Fecha Llamada"], dayfirst=True, errors="coerce")
        
        # Filtro de Seguridad: Eliminamos fechas imposibles/futuras (Errores de tipeo del usuario)
        limite_futuro = pd.Timestamp.now() + pd.Timedelta(days=1)
        mask_errores = df_total["Fecha Llamada"] > limite_futuro
        if mask_errores.any():
            cant_errores = mask_errores.sum()
            console.print(f"[yellow]⚠️ Se descartaron {cant_errores} registros con fecha futura (posible error DD/MM).[/]")
            df_total = df_total[~mask_errores]

        df_total["Hora Llamada"] = df_total["Hora Llamada"].fillna("").astype(str)
        df_total["Ciudad"] = df_total["Ciudad"].fillna("NO ESPECIFICADO").astype(str).str.upper()

        # --- LIMPIEZA IDs ---
        df_total["N° Abonado"] = df_total["N° Abonado"].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_total["Documento"] = df_total["Documento"].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_total["Documento"] = df_total["Documento"].replace(['nan', 'None', ''], None)
        
        df_total = df_total[df_total["N° Abonado"].notna() & (df_total["N° Abonado"] != 'nan')]

        # --- LÓGICA DE CANAL ---
        resp = df_total["Responsable"].fillna("").astype(str)
        condiciones = [
            resp.str.contains("CALL|PHONE", case=False, na=False),       
            resp.str.contains("OFI|ASESOR", case=False, na=False) 
        ]
        opciones = ["CALL CENTER", "OFICINA COMERCIAL"]
        df_total["Canal"] = np.select(condiciones, opciones, default="ALIADOS")

        # --- FINALIZACIÓN ---
        df_total = df_total.rename(columns={"Hora Llamada": "Hora"})
        df_total = df_total.drop(columns=["Cliente"], errors="ignore")

    # -------------------------------------------------------------------------
    # 5. DEDUPLICACIÓN GLOBAL Y GUARDADO
    # ---------------------------------------------------------
    # Ordenamos descendente para que la versión más reciente quede arriba
    df_total = df_total.sort_values(by="Fecha Llamada", ascending=False)
    
    # Deduplicar final (Mantenemos tus columnas de corte exactas)
    cols_final_dedupe = ["N° Abonado", "Documento", "Saldo", "Fecha Llamada", "Hora", "Tipo Respuesta"]
    cols_final_dedupe = [c for c in cols_final_dedupe if c in df_total.columns]
    
    # keep='first' sobre orden descendente = Nos quedamos con la corrección más reciente
    df_final = df_total.drop_duplicates(subset=cols_final_dedupe, keep='first')
    
    df_final = limpiar_nulos_powerbi(df_final)
    df_final = standard_hours(df_final, 'Hora')
    
    guardar_parquet(
        df_final, 
        NOMBRE_GOLD, 
        filas_iniciales=len(df_total),
        ruta_destino=PATHS.get("gold", "")
    )
    console.print(f"[bold green]✅ Cobranza Gold reconstruido a velocidad luz. Total filas únicas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
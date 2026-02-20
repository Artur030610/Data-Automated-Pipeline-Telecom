import pandas as pd
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS, MAPA_MESES
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, ingesta_inteligente

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]🎧 ETL: ATENCIÓN AL CLIENTE (INCREMENTAL INTELIGENTE)[/]")
    
    # 1. DEFINICIÓN DE RUTAS
    RUTA_RAW = PATHS["raw_atencion"]
    NOMBRE_GOLD = "Atencion_Cliente_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)

    # ---------------------------------------------------------
    # 2. INGESTA INTELIGENTE (AHORA SÍ FUNCIONA CON FILTROS)
    # ---------------------------------------------------------
    # col_fecha_corte="Fecha": Es el nombre de la columna EN EL PARQUET (GOLD).
    # filtro_exclusion="Consolidado": Se pasa a utils gracias al cambio que hicimos.
    
    df_nuevo, df_historico = ingesta_inteligente(
        ruta_raw=RUTA_RAW,
        ruta_gold=RUTA_GOLD_COMPLETA,
        col_fecha_corte="Fecha", 
        filtro_exclusion="Consolidado"  # <--- ¡ESTO YA NO DARÁ ERROR!
    )

    if df_nuevo.empty and not df_historico.empty:
        # El mensaje de "Sistema actualizado" ya lo da ingesta_inteligente, 
        # así que aquí solo retornamos.
        return

    # ---------------------------------------------------------
    # 3. TRANSFORMACIÓN (SOLO LOTE NUEVO)
    # ---------------------------------------------------------
    if not df_nuevo.empty:
        console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} registros nuevos...[/]")
        
        # A. Renombramiento Inicial
        # OJO: El Raw trae "Fecha Llamada", el Gold tiene "Fecha".
        # Renombramos aquí para que coincidan antes de unir.
        df_nuevo = df_nuevo.rename(columns={
            "Franquicia": "Nombre Franquicia", 
            "Fecha Llamada": "Fecha", 
            "Responsable": "Vendedor", 
            "Hora Llamada": "Hora"
        })

        # B. LIMPIEZA DE IDs
        cols_ids = ['N° Abonado', 'Documento']
        for col in [c for c in cols_ids if c in df_nuevo.columns]:
            df_nuevo[col] = df_nuevo[col].astype(str).str.strip()
            df_nuevo[col] = df_nuevo[col].str.replace(r'\.0$', '', regex=True)
            df_nuevo[col] = df_nuevo[col].str.replace('.', '', regex=False)
            df_nuevo[col] = df_nuevo[col].replace({'nan': None, 'None': None, '': None})

        # C. FECHAS Y TEXTOS
        df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce")
        df_nuevo['Vendedor'] = df_nuevo['Vendedor'].fillna('').astype(str).str.upper()

        # D. FILTROS DE NEGOCIO
        mask_excluir = df_nuevo['Tipo Respuesta'].isin(["AFILIACION DE SERVICIO", "PAGO DEL SERVICIO"])
        df_nuevo = df_nuevo[~mask_excluir].copy()

        # E. ENRIQUECIMIENTO
        df_nuevo['Tipo de afluencia'] = "ATENCIÓN AL CLIENTE"
        df_nuevo['Mes'] = df_nuevo['Fecha'].dt.month.map(MAPA_MESES)

        # F. DEDUPLICACIÓN LOTE NUEVO
        subset_duplicados = [
            "N° Abonado", "Documento", "Fecha", "Hora", 
            "Tipo Respuesta", "Detalle Respuesta", "Vendedor"
        ]
        df_nuevo = df_nuevo.drop_duplicates(subset=subset_duplicados)

        # Selección de Columnas
        cols_output = [
             "N° Abonado", "Documento", "Cliente", "Estatus", 
            "Fecha", "Hora", "Tipo Llamada","Tipo Respuesta", "Detalle Respuesta", 
            "Vendedor", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
            "Ciudad", "Tipo de afluencia","Observación"
        ]
        df_nuevo = df_nuevo.reindex(columns=cols_output)

    # ---------------------------------------------------------
    # 4. UNIFICACIÓN Y GUARDADO
    # ---------------------------------------------------------
    df_final = pd.DataFrame()
    
    # Unimos Historia + Nuevo
    if not df_historico.empty and not df_nuevo.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    elif not df_nuevo.empty:
        df_final = df_nuevo
    else:
        df_final = df_historico

    if not df_final.empty:
        # Deduplicación Final (Keep Last para actualizaciones)
        subset_final = [
            "N° Abonado", "Documento", "Fecha", "Hora", 
            "Tipo Respuesta", "Detalle Respuesta", "Vendedor"
        ]
        df_final = df_final.drop_duplicates(subset=subset_final, keep='last')
        df_final = limpiar_nulos_powerbi(df_final)

        guardar_parquet(
            df_final, 
            NOMBRE_GOLD,
            filas_iniciales=len(df_nuevo) if not df_nuevo.empty else len(df_final)
        )
        console.print(f"[bold green]✅ ATC Gold actualizado. Total filas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
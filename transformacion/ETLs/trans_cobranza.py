import os
import sys
import polars as pl
import duckdb

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, standard_hours, ingesta_incremental_polars

@reportar_tiempo
def ejecutar():
    console.rule("[bold blue]PIPELINE OPERATIVOS: COBRANZA (POLARS BRONZE + DUCKDB GOLD)[/]")

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
    # 3. TRANSFORMACIÓN Y DEDUPLICACIÓN (DUCKDB OUT-OF-CORE)
    # -------------------------------------------------------------------------
    console.print("[cyan]🦆 Fase 2: Construyendo Gold con DuckDB (Out-Of-Core, RAM casi Cero)...[/]")
    try:
        con = duckdb.connect(database=':memory:')
        con.execute("PRAGMA memory_limit='1GB'") # Tope de RAM garantizado
        con.execute("SET preserve_insertion_order=false")
        
        # Carpeta temporal por si el volumen de datos excede 1GB
        temp_dir = os.path.join(PATHS.get("bronze", "data/bronze"), "duckdb_temp_cobranza").replace("\\", "/")
        os.makedirs(temp_dir, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{temp_dir}'")
        
        # Extraer el esquema nativo desde Bronze para limpiar dinámicamente (Magia)
        schema_info = pl.scan_parquet(RUTA_BRONZE).collect_schema()
        
        select_exprs = []
        for col_name, dtype in schema_info.items():
            if col_name == "Cliente":
                continue # Equivalente a drop_columns
                
            safe_col = f'"{col_name}"'
            
            # Transformaciones Específicas
            if col_name in ["N° Abonado", "Documento"]:
                expr = f"NULLIF(TRIM(REGEXP_REPLACE(CAST({safe_col} AS VARCHAR), '\\.0$', '')), 'nan') AS {safe_col}"
            elif col_name == "Fecha Llamada":
                expr = f"""
                COALESCE(
                    TRY_CAST({safe_col} AS DATE),
                    TRY_STRPTIME(CAST({safe_col} AS VARCHAR), '%d/%m/%Y %H:%M:%S')::DATE,
                    TRY_STRPTIME(CAST({safe_col} AS VARCHAR), '%d/%m/%Y')::DATE,
                    TRY_STRPTIME(CAST({safe_col} AS VARCHAR), '%Y-%m-%d %H:%M:%S')::DATE,
                    TRY_STRPTIME(CAST({safe_col} AS VARCHAR), '%Y-%m-%d')::DATE,
                    TRY_STRPTIME(CAST({safe_col} AS VARCHAR), '%d-%m-%Y')::DATE
                ) AS {safe_col}
                """
            elif col_name == "Ciudad":
                expr = f"UPPER(COALESCE(NULLIF(TRIM(CAST({safe_col} AS VARCHAR)), ''), 'NO ESPECIFICADO')) AS {safe_col}"
            elif col_name == "Hora Llamada":
                # Equivalente 1 a 1 de tu standard_hours
                expr = f"""
                COALESCE(
                    LPAD(EXTRACT('hour' FROM TRY_CAST(TRY_CAST({safe_col} AS TIMESTAMP) AS TIME))::VARCHAR, 2, '0') || ':00', 
                    LPAD(EXTRACT('hour' FROM TRY_CAST({safe_col} AS TIME))::VARCHAR, 2, '0') || ':00', 
                    REGEXP_EXTRACT(CAST({safe_col} AS VARCHAR), '([0-9]{2}):', 1) || ':00', 
                    '00:00'
                ) AS "Hora"
                """
            else:
                # Equivalente de limpiar_nulos_powerbi (limpieza robusta de strings)
                if dtype in (pl.Utf8, pl.Categorical):
                    expr = f"""
                    CASE 
                        WHEN REGEXP_MATCHES(CAST({safe_col} AS VARCHAR), '(?i)^(nan|none|null)$') THEN NULL
                        WHEN TRIM(CAST({safe_col} AS VARCHAR)) = '' THEN NULL
                        ELSE TRIM(CAST({safe_col} AS VARCHAR))
                    END AS {safe_col}
                    """
                else:
                    expr = safe_col
                    
            select_exprs.append(expr)
            
        # Canal Predictivo
        select_exprs.append("""
            CASE 
                WHEN REGEXP_MATCHES(COALESCE(CAST("Responsable" AS VARCHAR), ''), '(?i)CALL|PHONE') THEN 'CALL CENTER'
                WHEN REGEXP_MATCHES(COALESCE(CAST("Responsable" AS VARCHAR), ''), '(?i)OFI|ASESOR') THEN 'OFICINA COMERCIAL'
                ELSE 'ALIADOS'
            END AS "Canal"
        """)
        
        select_sql = ",\n".join(select_exprs)
        
        # Columnas dinámicas para particionar la deduplicación
        cols_dedupe = ["N° Abonado", "Documento", "Saldo", "Fecha Llamada", "Hora", "Tipo Respuesta"]
        part_cols = ", ".join([f'"{c}"' for c in cols_dedupe if c in schema_info or c == "Hora"])
        
        query = f"""
        COPY (
            WITH Base AS (
                SELECT 
                    {select_sql}
                FROM read_parquet('{RUTA_BRONZE.replace(chr(92), '/')}')
            ),
            Filtros AS (
                SELECT * 
                FROM Base
                WHERE "N° Abonado" IS NOT NULL
                  AND "Fecha Llamada" <= CURRENT_DATE + INTERVAL 1 DAY
            )
            SELECT *
            FROM Filtros
            -- Equivalente perfecto a .sort_values("Fecha Llamada", ascending=False).drop_duplicates(keep='first')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {part_cols}
                ORDER BY "Fecha Llamada" DESC
            ) = 1
        ) TO '{RUTA_GOLD_COMPLETA.replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """
        
        console.print("[cyan]⏳ Ejecutando consulta relacional de DuckDB...[/]")
        con.execute(query)
        
        # Obtener registro de filas para el log con un simple count en metadatos
        filas_finales = con.execute(f"SELECT COUNT(*) FROM read_parquet('{RUTA_GOLD_COMPLETA.replace(chr(92), '/')}')").fetchone()[0] #type: ignore
        con.close()
        
        console.print(f"[bold green]✅ Cobranza Gold reconstruido a velocidad luz. Total filas únicas: {filas_finales:,}[/]")
        
    except Exception as e:
        console.print(f"[bold red]❌ Error procesando con DuckDB: {e}[/]")

if __name__ == "__main__":
    ejecutar()
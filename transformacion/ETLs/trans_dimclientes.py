import duckdb
import os
import sys

# --- CONFIGURACIÓN DE RUTAS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)  # Sube el segundo nivel
sys.path.append(grandparent_dir)

from config import PATHS
from utils import ingesta_incremental_polars, reportar_tiempo, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold yellow]ETL: DIMENSIÓN CLIENTE (Incremental + Clave Subrogada Entera)[/]")

    # 1. Rutas configuradas para apuntar a las descargas del scraper de Abonados
    ruta_raw = PATHS.get("raw_clientes") 
    if not ruta_raw:
        console.print("[red]❌ Ruta 'raw_clientes' no definida en config.py.[/]")
        return
    
    NOMBRE_GOLD = "Dim_Cliente.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD).replace("\\", "/")
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Dim_Cliente_Raw_Bronze.parquet").replace("\\", "/")

    # =========================================================================
    # FASE 1: INGESTA INCREMENTAL A BRONZE (POLARS)
    # =========================================================================
    console.print("[cyan]🚀 Fase 1: Actualizando capa Bronze con Polars...[/]")
    try:
        ingesta_incremental_polars(
            ruta_raw=ruta_raw,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha Contrato"
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó. Error: {e}[/]")
        
    if not os.path.exists(RUTA_BRONZE):
        console.print("[bold red]❌ No existe el archivo Bronze para procesar la Dimensión.[/]")
        return

    # =========================================================================
    # FASE 2: CONSOLIDACIÓN Y GENERACIÓN DE SURROGATE KEY (DUCKDB)
    # =========================================================================
    console.print("[info]🦆 Fase 2: Iniciando motor DuckDB para asignar Claves Subrogadas (SK) Enteras...[/]")
    
    con = duckdb.connect(database=':memory:')
    
    existe_gold = os.path.exists(RUTA_GOLD_COMPLETA)
    max_sk = 0
    gold_join = ""
    sk_expr = "NEXTVAL('cliente_seq') AS Cliente_SK"

    if existe_gold:
        try:
            # Identificamos el ID entero más alto para no repetir secuencias
            max_sk_result = con.execute(f"SELECT MAX(Cliente_SK) FROM read_parquet('{RUTA_GOLD_COMPLETA}')").fetchone()
            if max_sk_result and max_sk_result[0] is not None:
                max_sk = int(max_sk_result[0])
            
            # Left join con el histórico para mantener el mismo ID entero a abonados que ya existían
            gold_join = f"""
            LEFT JOIN (
                SELECT Cliente_SK, "N° Abonado" 
                FROM read_parquet('{RUTA_GOLD_COMPLETA}')
                WHERE "N° Abonado" IS NOT NULL
            ) g ON b.abonado_norm = g."N° Abonado"
            """
            sk_expr = "COALESCE(g.Cliente_SK, CAST(NEXTVAL('cliente_seq') AS BIGINT)) AS Cliente_SK"
            console.print(f"[green]✅ Gold histórico detectado. ID máximo actual: {max_sk}. Conservando IDs existentes...[/]")
        except Exception as e:
            console.print(f"[yellow]⚠️ No se pudo leer la Gold histórica ({e}). Se regenerarán todos los IDs desde 1.[/]")

    # Creamos la secuencia a partir del último ID conocido
    con.execute(f"CREATE SEQUENCE cliente_seq START {max_sk + 1}")

    # --- QUERY MAESTRA OUT-OF-CORE ---
    sql = f"""
        --sql
        COPY (
            WITH bronze_clean AS (
                SELECT 
                    *,
                    NULLIF(TRIM(REGEXP_REPLACE(CAST("N° Abonado" AS VARCHAR), '\\.0$', '')), 'nan') AS abonado_norm
                FROM read_parquet('{RUTA_BRONZE}')
            ),
            bronze_dedup AS (
                SELECT *
                FROM bronze_clean
                WHERE abonado_norm IS NOT NULL AND abonado_norm != ''
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY abonado_norm 
                    ORDER BY "Fecha_Modificacion_Archivo" DESC, "Fecha Contrato" DESC
                ) = 1
            )
            SELECT 
                {sk_expr},
                b.* EXCLUDE("Source.Name", "Fecha_Modificacion_Archivo", "abonado_norm", "N° Abonado"),
                b.abonado_norm AS "N° Abonado"
            FROM bronze_dedup b
            {gold_join}
            ORDER BY Cliente_SK
        ) TO '{RUTA_GOLD_COMPLETA}.temp' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """
    
    try:
        console.print("[info]💾 Ejecutando cruce Out-Of-Core y escribiendo archivo Gold Parquet...[/]")
        con.execute(sql)
        
        # Reemplazo atómico seguro para Windows
        if os.path.exists(RUTA_GOLD_COMPLETA):
            os.remove(RUTA_GOLD_COMPLETA)
        os.rename(RUTA_GOLD_COMPLETA + ".temp", RUTA_GOLD_COMPLETA)
        
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{RUTA_GOLD_COMPLETA}')").fetchone()[0]
        console.print(f"[bold green]✅ Dim_Cliente generada exitosamente. Total abonados únicos: {count:,}[/]")

    except Exception as e:
        console.print(f"[bold red]❌ Error en DuckDB generando la Dimensión: {e}[/]")
        if os.path.exists(RUTA_GOLD_COMPLETA + ".temp"):
            os.remove(RUTA_GOLD_COMPLETA + ".temp")
    
    finally:
        con.close()

if __name__ == "__main__":
    ejecutar()
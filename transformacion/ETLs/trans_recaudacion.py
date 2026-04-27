import os
import sys
import polars as pl
import duckdb
import gc
import gc

from utils import  reportar_tiempo, console
from utils import ingesta_incremental_polars  

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# -----------------------------

from config import PATHS, MAPA_MESES

# --- CONSTANTES DE NEGOCIO ---
OFICINAS_PROPIAS = [
    'OFC COMERCIAL CUMANA', 'OFC- LA ASUNCION', 'OFC SAN ANTONIO DE CAPYACUAL', 
    'OFC TINACO', 'OFC VILLA ROSA', 'OFC-SANTA FE', 'OFI CARIPE MONAGAS', 
    'OFI TINAQUILLO', 'OFI-BARCELONA', 'OFI-BARINAS', 'OFI-BQTO', 
    'OFIC GALERIA EL PARAISO', 'OFIC SAMBIL-VALENCIA', 'OFIC. PARRAL VALENCIA', 
    'OFIC. TORRE FIBEX VIÑEDO', 'OFI-CARACAS PROPATRIA', 'OFIC-BOCA DE UCHIRE', 
    'OFIC-CARICUAO', 'OFIC-COMERCIAL SANTA FE', 'OFICINA ALIANZA MALL', 
    'OFICINA MARGARITA', 'OFICINA SAN JUAN DE LOS MORROS', 'OFIC-JUAN GRIEGO-MGTA', 
    'OFIC-METROPOLIS-BQTO', 'OFIC-MGTA_DIAZ', 'OFI-LECHERIA', 'OFI-METROPOLIS', 
    'OFI-PARAISO', 'OFI-PASEO LAS INDUSTRIAS', 'OFI-PTO CABELLO', 'OFI-PTO LA CRUZ', 
    'OFI-SAN CARLOS', 'OFI-VIA VENETO'
]

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

    # 🚀 OPTIMIZACIÓN RAM: Forzamos a vaciar los GBs del primer archivo antes de leer el segundo
    gc.collect()

    try:
        console.print("[dim]Actualizando Bronze de Horas...[/dim]")
        ingesta_incremental_polars(
            ruta_raw=RUTA_RAW_HORAS,
            ruta_bronze_historico=RUTA_BRONZE_HORAS,
            columna_fecha="Fecha"  
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze de Horas no se actualizó. Error: {e}[/]")

    # 🚀 OPTIMIZACIÓN RAM: Vaciamos los GBs del segundo archivo antes de encender DuckDB
    gc.collect()

    # =========================================================
    # --- PASO 2, 3 Y 4: PROCESAMIENTO LAZY Y MERGE CON DUCKDB ---
    # =========================================================
    if not os.path.exists(RUTA_BRONZE):
        console.print("[red]❌ No se encontró la capa Bronze de Recaudación. Ejecución abortada.[/]")
        return
        
    console.print("[cyan]🦆 Procesando y cruzando de forma 100% Nativa con DuckDB (Out-Of-Core, RAM casi cero)...[/]")
    
    try:
        con = duckdb.connect(database=':memory:')
        con.execute("PRAGMA threads=8") 
        con.execute("PRAGMA memory_limit='3GB'") 
        con.execute("SET preserve_insertion_order=false") 
        
        temp_dir = os.path.join(PATHS.get("bronze", "data/bronze"), "duckdb_temp").replace("\\", "/")
        os.makedirs(temp_dir, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{temp_dir}'")

        # Exploración Lazy del Schema (Sin subir datos a la RAM)
        schema_rec_lower = {c.lower(): c for c in pl.scan_parquet(RUTA_BRONZE).collect_schema().names()}
        
        if "id pago" not in schema_rec_lower:
            console.print("[bold green]✅ No se detectó 'ID Pago' en la estructura. Operación abortada de forma segura.[/]")
            return
            
        def safe_col_name(col_name):
            return f'"{schema_rec_lower[col_name.lower()]}"' if col_name.lower() in schema_rec_lower else "NULL"

        def safe_cast(col_name, data_type="VARCHAR", default="NULL"):
            if col_name.lower() in schema_rec_lower:
                real_name = schema_rec_lower[col_name.lower()]
                return f'COALESCE(CAST(r."{real_name}" AS {data_type}), {default})'
            return default

        # Manejo de Horas nativo de DuckDB
        join_horas = ""
        hora_select = "'00:00'"
        
        if os.path.exists(RUTA_BRONZE_HORAS):
            schema_h_lower = {c.lower(): c for c in pl.scan_parquet(RUTA_BRONZE_HORAS).collect_schema().names()}
            if "id pago" in schema_h_lower and "hora de pago" in schema_h_lower:
                id_col_h = schema_h_lower["id pago"]
                hora_col_h = schema_h_lower["hora de pago"]
                join_horas = f"""
                LEFT JOIN (
                    SELECT 
                        TRIM(REGEXP_REPLACE(CAST("{id_col_h}" AS VARCHAR), '\\.0$', '')) AS id_pago_norm, 
                        MAX("{hora_col_h}") AS hora_pago
                    FROM read_parquet('{RUTA_BRONZE_HORAS.replace("\\", "/")}')
                    WHERE "{id_col_h}" IS NOT NULL
                    GROUP BY 1
                ) h ON r.id_pago_norm = h.id_pago_norm
                """
                hora_select = """COALESCE(
                    LPAD(EXTRACT('hour' FROM TRY_CAST(TRY_CAST(h.hora_pago AS TIMESTAMP) AS TIME))::VARCHAR, 2, '0') || ':00', 
                    LPAD(EXTRACT('hour' FROM TRY_CAST(h.hora_pago AS TIME))::VARCHAR, 2, '0') || ':00', 
                    REGEXP_EXTRACT(CAST(h.hora_pago AS VARCHAR), '([0-9]{2}):', 1) || ':00', 
                    '00:00'
                )"""
            else:
                console.print("[warning]⚠️ El Bronze de Horas no tiene las columnas requeridas.[/]")
        else:
            console.print("[warning]⚠️ No se encontró Bronze de Horas. Se asignarán horas en blanco.[/]")

        # Convertimos la lista de Python a una cadena SQL lista para el IN (...)
        oficinas_sql = ", ".join([f"'{ofi}'" for ofi in OFICINAS_PROPIAS])

        # Consulta SQL para procesamiento "Out-of-Core" (Usa disco si se llena la RAM)
        query = f"""--sql
        COPY (
            WITH 
            Recaudacion AS (
                SELECT 
                    *,
                    -- CRÍTICO: Normalizamos el ID Pago limpiando los decimales .0 y espacios para asegurar el match
                    TRIM(REGEXP_REPLACE(CAST({safe_col_name('ID Pago')} AS VARCHAR), '\\.0$', '')) AS id_pago_norm
                FROM read_parquet('{RUTA_BRONZE.replace("\\", "/")}')
                WHERE {safe_col_name('ID Pago')} IS NOT NULL
                  AND NOT REGEXP_MATCHES(COALESCE(CAST({safe_col_name('Oficina Cobro')} AS VARCHAR), ''), '(?i)VIRTUAL|Virna|Fideliza|Externa|Unicenter|Compensa')
            ),
            Cruce AS (
                SELECT 
                    {safe_cast('ID Contrato', 'VARCHAR', "NULL")} AS "ID Contrato",
                    {safe_cast('ID Pago', 'VARCHAR', "NULL")} AS "ID Pago",
                    {safe_cast('N° Abonado', 'VARCHAR', "''")} AS "N° Abonado",
                    
                    COALESCE(
                        TRY_CAST({safe_col_name('Fecha')} AS DATE),
                        TRY_STRPTIME(CAST({safe_col_name('Fecha')} AS VARCHAR), '%d/%m/%Y %H:%M:%S')::DATE,
                        TRY_STRPTIME(CAST({safe_col_name('Fecha')} AS VARCHAR), '%d/%m/%Y')::DATE,
                        TRY_STRPTIME(CAST({safe_col_name('Fecha')} AS VARCHAR), '%d-%m-%Y %H:%M:%S')::DATE,
                        TRY_STRPTIME(CAST({safe_col_name('Fecha')} AS VARCHAR), '%d-%m-%Y')::DATE,
                        TRY_STRPTIME(CAST({safe_col_name('Fecha')} AS VARCHAR), '%Y-%m-%d %H:%M:%S')::DATE,
                        TRY_STRPTIME(CAST({safe_col_name('Fecha')} AS VARCHAR), '%Y-%m-%d')::DATE
                    ) AS "Fecha",
                    
                    COALESCE(TRY_CAST(REPLACE(CAST({safe_col_name('Total Pago')} AS VARCHAR), ',', '.') AS DOUBLE), 0.0) AS "Total Pago",
                    {safe_cast('Forma de Pago', 'VARCHAR', "''")} AS "Forma de Pago",
                    {safe_cast('Banco', 'VARCHAR', "''")} AS "Banco",
                    {safe_cast('Oficina Cobro', 'VARCHAR', "''")} AS "Oficina",
                    {safe_cast('Fecha Contrato', 'VARCHAR', "NULL")} AS "Fecha Contrato",
                    {safe_cast('Estatus', 'VARCHAR', "''")} AS "Estatus",
                    {safe_cast('Suscripción', 'VARCHAR', "''")} AS "Suscripción",
                    {safe_cast('Grupo Afinidad', 'VARCHAR', "''")} AS "Grupo Afinidad",
                    {safe_cast('Nombre Franquicia', 'VARCHAR', "''")} AS "Nombre Franquicia",
                    {safe_cast('Ciudad', 'VARCHAR', "''")} AS "Ciudad",
                    {safe_cast('Cobrador', 'VARCHAR', "''")} AS "Vendedor",
                    'RECAUDACIÓN' AS "Tipo de afluencia",
                    
                    CASE 
                        WHEN {safe_cast('Oficina Cobro', 'VARCHAR', "''")} IN ({oficinas_sql}) THEN 'OFICINAS PROPIAS'
                        ELSE 'ALIADOS Y DESARROLLO'
                    END AS "Clasificacion",
                    
                    {hora_select} AS "Hora de Pago"
                    
                FROM Recaudacion r
                {join_horas}
            )
            SELECT 
                "ID Contrato", 
                "N° Abonado", 
                "Fecha", 
                "Total Pago", 
                "Forma de Pago", 
                "Banco", 
                "Oficina", 
                "Fecha Contrato", 
                "Estatus", 
                "Suscripción", 
                "Grupo Afinidad", 
                "Nombre Franquicia", 
                "Ciudad", 
                "Vendedor", 
                "Tipo de afluencia", 
                "Clasificacion",
                "Hora de Pago"
            FROM Cruce
            -- Deduplicación Nativa equivalente a Polars .unique(keep="last")
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID Pago" ORDER BY "Fecha" DESC) = 1
            
        ) TO '{RUTA_GOLD_COMPLETA.replace("\\", "/")}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """
        
        console.print("[cyan]⏳ Ejecutando proceso SQL Out-Of-Core directo a Disco...[/]")
        con.execute(query)
        con.close()
        del con
        
        # 🚀 OPTIMIZACIÓN RAM: Forzamos al sistema a liberar instantáneamente 
        # los 2GB que DuckDB tenía reservados para el cruce.
        gc.collect()
        
        console.print(f"[bold green]✅ Archivo {NOMBRE_GOLD} generado y exportado exitosamente (RAM casi Cero).[/]")
        
    except Exception as e:
        console.print(f"[bold red]❌ Error ejecutando motor DuckDB: {e}[/]")
        console.print(f"[bold red]❌ Error en proceso de Recaudación: {e}[/]")
        return

if __name__ == "__main__":
    ejecutar()
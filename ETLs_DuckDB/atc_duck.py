import duckdb
import pandas as pd
import os
import sys

# --- CONFIGURACIÓN ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import PATHS
from utils import leer_carpeta, reportar_tiempo, console

@reportar_tiempo
def ejecutar_etl():
    console.rule("[bold cyan]🦆 ETL DUCKDB: ATENCIÓN AL CLIENTE -> GOLD[/]")
    
    # 1. DEFINIR RUTAS
    directorio_actual = os.path.dirname(__file__)
    ruta_sql = os.path.join(directorio_actual, 'sql', 'atc.sql') # <--- Apuntamos al SQL nuevo

    # Ruta de Salida (Gold Data)
    ruta_base = os.path.abspath(os.path.join(directorio_actual, '..', '..'))
    carpeta_gold = os.path.join(ruta_base, '01-Data_PipelinesFibex', '02_Data_Lake', 'gold_data')
    os.makedirs(carpeta_gold, exist_ok=True)
    
    nombre_archivo = "Atencion_Cliente_Gold.parquet"
    ruta_salida_sql = os.path.join(carpeta_gold, nombre_archivo).replace('\\', '/')

    console.print(f"[yellow]📂 Destino: {ruta_salida_sql}[/]")

    # 2. LEER RAW (Pandas)
    # Lista exacta de columnas que espera tu Excel
    cols_input = [
        "N° Abonado", "Documento", "Cliente", "Estatus", "Fecha Llamada", 
        "Hora Llamada", "Tipo Llamada", "Tipo Respuesta", "Detalle Respuesta", 
        "Responsable", "Suscripción", "Teléfono", "Grupo Afinidad", 
        "Franquicia", "Ciudad", "Teléfono verificado", "Detalle Suscripcion", "Saldo"
    ]
    
    console.print("[cyan]📥 1. Python: Leyendo Excels Raw...[/]")
    df_raw = leer_carpeta(
        PATHS["raw_atencion"], 
        filtro_exclusion="Consolidado", 
        columnas_esperadas=cols_input
    )
    
    if df_raw.empty: 
        console.print("[red]❌ No hay datos para procesar.[/]")
        return

    # 3. NORMALIZACIÓN DE FECHAS (Escudo Anti-Errores)
    console.print("[cyan]🛡️ Python: Normalizando fechas...[/]")
    # Usamos el nombre original del Excel ("Fecha Llamada") antes de pasarlo a SQL
    df_raw["Fecha Llamada"] = pd.to_datetime(
        df_raw["Fecha Llamada"], 
        dayfirst=True, 
        errors="coerce"
    )

    # 4. PROCESAMIENTO DUCKDB
    conn = duckdb.connect() 
    console.print("[cyan]🔌 2. DuckDB: Ejecutando transformación SQL...[/]")
    
    # Registramos la tabla con el nombre que usamos en el FROM del SQL
    conn.register('raw_atc', df_raw)

    if not os.path.exists(ruta_sql):
        console.print(f"[bold red]❌ No encuentro el SQL: {ruta_sql}[/]")
        return

    with open(ruta_sql, 'r', encoding='utf-8') as f:
        query_template = f.read()

    # Inyectamos la ruta de salida
    query_final = query_template.format(ruta_salida=ruta_salida_sql)

    try:
        conn.execute(query_final)
        console.print(f"[bold green]✅ ÉXITO: Reporte generado en:[/]\n{ruta_salida_sql}")
    except Exception as e:
        console.print(f"[bold red]💥 Error SQL:[/]\n{e}")
    finally:
        conn.close()

if __name__ == "__main__":
    ejecutar_etl()
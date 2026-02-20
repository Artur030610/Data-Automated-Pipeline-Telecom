import duckdb
import pandas as pd
import os
import sys

# --- CONFIGURACIÓN DE RUTAS ---
# Ajusta esto según tu estructura de carpetas real
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import PATHS
from utils import leer_carpeta, reportar_tiempo, console

@reportar_tiempo
def ejecutar_etl_ventas_estatus():
    console.rule("[bold cyan]🦆 ETL DUCKDB: VENTAS ESTATUS -> GOLD[/]")

    # 1. DEFINICIÓN DE RUTAS
    directorio_actual = os.path.dirname(__file__)
    ruta_sql = os.path.join(directorio_actual, 'sql', 'ventase.sql')
    
    # Ruta de salida (Gold Data)
    ruta_base = os.path.abspath(os.path.join(directorio_actual, '..', '..'))
    carpeta_gold = os.path.join(ruta_base, '01-Data_PipelinesFibex', '02_Data_Lake', 'gold_data')
    os.makedirs(carpeta_gold, exist_ok=True)
    
    archivo_salida = "Ventas_Estatus_Gold.parquet"
    ruta_salida_sql = os.path.join(carpeta_gold, archivo_salida).replace('\\', '/')

    # 2. LECTURA Y PRE-PROCESAMIENTO (PANDAS)
    # Usamos Pandas para leer porque es más robusto con Excels "sucios"
    console.print("[cyan]📥 Leyendo archivos Raw...[/]")
    df = leer_carpeta(PATHS["ventas_estatus"], filtro_exclusion="Consolidado")
    
    if df.empty:
        console.print("[red]❌ No hay datos para procesar.[/]")
        return

    # --- NORMALIZACIÓN DE COLUMNAS (CRÍTICO) ---
    # Esto asegura que el SQL reciba los nombres correctos
    df.columns = df.columns.str.strip()
    
    mapa_columnas = {
        "Fecha Venta": "Fecha", 
        "Franquicia": "Nombre Franquicia", 
        "Hora venta": "Hora",
        "Cédula": "Documento", 
        "Rif": "Documento", 
        "Nro. Doc": "Documento"
    }
    df = df.rename(columns=mapa_columnas)

    # Garantizar columnas mínimas (rellenar con None si no existen)
    cols_requeridas = [
        "Paquete/Servicio", "Vendedor", "N° Abonado", "Cliente", 
        "Estatus", "Documento", "Costo", "Grupo Afinidad", 
        "Nombre Franquicia", "Ciudad", "Hora"
    ]
    
    for col in cols_requeridas:
        if col not in df.columns:
            df[col] = None

    # Conversión de Fecha en Python (Más seguro para formatos latinos dd/mm/yyyy)
    df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True, errors="coerce")

    # 3. EJECUCIÓN DUCKDB
    console.print("[cyan]⚙️  Ejecutando SQL en DuckDB...[/]")
    conn = duckdb.connect()
    
    # Registramos el DataFrame como una tabla virtual 'raw_ventas'
    conn.register('raw_ventas', df)

    if not os.path.exists(ruta_sql):
        console.print(f"[bold red]❌ No se encontró el SQL: {ruta_sql}[/]")
        return

    # Leemos e inyectamos la ruta de salida
    with open(ruta_sql, 'r', encoding='utf-8') as f:
        query = f.read().format(ruta_salida=ruta_salida_sql)

    try:
        conn.execute(query)
        console.print(f"[bold green]✅ ÉXITO: Archivo generado en:[/]\n{ruta_salida_sql}")
    except Exception as e:
        console.print(f"[bold red]💥 Error en la ejecución SQL:[/]\n{e}")
    finally:
        conn.close()

if __name__ == "__main__":
    ejecutar_etl_ventas_estatus()
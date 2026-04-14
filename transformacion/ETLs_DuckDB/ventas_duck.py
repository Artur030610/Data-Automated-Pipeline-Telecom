import duckdb
import pandas as pd
import os
import sys

# --- 1. CONFIGURACIÓN DE RUTAS E IMPORTACIONES ---
# Subimos un nivel para poder importar config y utils desde la carpeta 02-Scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import PATHS, LISTA_VENDEDORES_OFICINA, LISTA_VENDEDORES_PROPIOS
from utils import leer_carpeta, reportar_tiempo, console

def lista_a_sql(lista_python):
    """
    Convierte una lista de Python ['Juan', 'Pedro'] 
    a un string válido para SQL "'juan', 'pedro'"
    """
    lista_limpia = [f"'{str(x).lower().strip()}'" for x in lista_python]
    return ", ".join(lista_limpia)

@reportar_tiempo
def ejecutar_etl():
    console.rule("[bold cyan]🦆 ETL DUCKDB: VENTAS -> GOLD LAYER[/]")
    
    # --- 2. DEFINICIÓN DE RUTAS Y ARCHIVOS ---
    directorio_actual = os.path.dirname(__file__) # Estamos en: ETLs_DuckDB
    ruta_sql = os.path.join(directorio_actual, 'sql', 'ventas.sql')

    carpeta_gold = str(PATHS.get("gold"))
    
    # Creamos la carpeta si no existe
    os.makedirs(carpeta_gold, exist_ok=True)
    
    # Definimos nombre y ruta final (normalizamos slashes para DuckDB)
    nombre_archivo = "Ventas_Listado_Gold.parquet"
    ruta_salida_sql = os.path.join(carpeta_gold, nombre_archivo).replace('\\', '/')

    console.print(f"[yellow]📂 Destino: {ruta_salida_sql}[/]")

    # --- 3. CARGA DE DATOS RAW (PANDAS) ---
    cols_esperadas = [
       "ID", "N° Abonado", "Fecha Contrato", "Estatus", "Suscripción", 
       "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor", 
       "Serv/Paquete", "nombre_detectado", "Estado", "oficina_comercial", 
       "tipo_coincidencia"
    ]
    
    console.print("[cyan]📥 1. Python: Leyendo Excels Raw...[/]")
    df_raw = leer_carpeta(
        PATHS["ventas_abonados"], 
        filtro_exclusion="Data_Consolidado", 
        columnas_esperadas=cols_esperadas
    )
    
    if df_raw.empty: 
        console.print("[red]❌ No se encontraron datos.[/]")
        return

    # --- 🛡️ CORRECCIÓN DEL ERROR BIGINT (CRUCIAL) ---
    # Convertimos explícitamente a fecha para que DuckDB no reciba números de Excel
    console.print("[cyan]🛡️ Python: Normalizando fechas...[/]")
    df_raw["Fecha Contrato"] = pd.to_datetime(
        df_raw["Fecha Contrato"], 
        dayfirst=True, 
        errors="coerce" # Si falla, pone NaT en vez de romper
    )

    # --- 4. PROCESAMIENTO CON DUCKDB ---
    conn = duckdb.connect() 
    console.print("[cyan]🔌 2. DuckDB: Registrando tabla...[/]")
    conn.register('raw_ventas', df_raw)

    # Leemos el template SQL
    if not os.path.exists(ruta_sql):
        console.print(f"[bold red]❌ No encuentro el archivo SQL en: {ruta_sql}[/]")
        return

    with open(ruta_sql, 'r', encoding='utf-8') as f:
        query_template = f.read()

    # Inyectamos variables y ruta de salida
    query_final = query_template.format(
        lista_oficina=lista_a_sql(LISTA_VENDEDORES_OFICINA),
        lista_propios=lista_a_sql(LISTA_VENDEDORES_PROPIOS),
        ruta_salida=ruta_salida_sql
    )

    # Ejecutamos
    console.print("[green]⚙️ 3. DuckDB: Generando Parquet...[/]")
    try:
        conn.execute(query_final)
        console.print(f"[bold green]✅ ÉXITO: Archivo guardado en:[/]\n{ruta_salida_sql}")
    except Exception as e:
        console.print(f"[bold red]💥 Error SQL:[/]\n{e}")
    finally:
        conn.close()

if __name__ == "__main__":
    ejecutar_etl()
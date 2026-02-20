import duckdb
import pandas as pd
import os
import sys
# --- CONFIGURACIÓN DE RUTAS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import leer_carpeta, reportar_tiempo, console

@reportar_tiempo
def ejecutar():
    console.rule("[bold yellow]ETL: DIMENSIÓN CLIENTE (Incremental con DuckDB + Union By Name)[/]")

    # 1. Definir rutas
    ruta_parquet_existente = os.path.join(PATHS["gold"], "Dim_Cliente.parquet")
    
    # 2. Leer Datos Nuevos (Excels)
    console.print("[info]📂 Leyendo archivos Excel crudos...[/]")
    cols_esperadas = [
        "ID", "N° Abonado", "Documento", "Cliente", 
        "Fecha Contrato", "Estatus", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor", "Serv/Paquete"
    ]
    
    df_new = leer_carpeta(
        PATHS["raw_hist_abonados"], 
        filtro_exclusion="~$", 
        columnas_esperadas=cols_esperadas
    )
    
    if df_new.empty: 
        console.print("[warning]⚠️ No hay datos nuevos en Excel. Finalizando.[/]")
        return

    # --- FASE DE LIMPIEZA EN PANDAS ---
    console.print("[info]🛠️  Normalizando datos nuevos...[/]")
    
    # Limpieza ID base
    for col in ["ID", "N° Abonado", "Documento"]:
        if col in df_new.columns:
            df_new[col] = (
                df_new[col].fillna(0).astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip().str.upper()
            )

    df_new["ID2"] = df_new["N° Abonado"] + "-" + df_new["Documento"]
    
    if "Fecha Contrato" in df_new.columns:
        df_new["Fecha Contrato"] = pd.to_datetime(df_new["Fecha Contrato"], dayfirst=True, errors="coerce")
    
    # Limpieza textos
    for col in ["Cliente", "Estatus", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor"]:
        if col in df_new.columns:
            df_new[col] = df_new[col].fillna("").astype(str).str.upper().str.strip()

    # --- FASE DUCKDB (Merge Inteligente) ---
    console.print("[info]🦆 Iniciando motor DuckDB con UNION BY NAME...[/]")
    
    con = duckdb.connect(database=':memory:')
    con.register('tb_nuevos', df_new)

    existe_historia = os.path.exists(ruta_parquet_existente)
    
    if existe_historia:
        console.print(f"[info]📚 Historia detectada en: {ruta_parquet_existente}[/]")
        query_historia = f"SELECT * FROM '{ruta_parquet_existente}'"
    else:
        console.print("[info]🆕 No existe historia previa. Creando carga inicial...[/]")
        # Usamos tb_nuevos para definir la estructura inicial vacía
        query_historia = "SELECT * FROM tb_nuevos WHERE 1=0"

    # --- QUERY MAESTRA: UNION BY NAME ---
    
    # Definimos columnas finales que QUEREMOS conservar en el orden deseado
    cols_sql = [
        "ID", "ID2", "N° Abonado", "Documento", "Cliente", 
        "Fecha Contrato", "Estatus", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor"
    ]
    cols_str = ", ".join([f'"{c}"' for c in cols_sql])

    # NOTA: UNION ALL BY NAME permite que las tablas tengan columnas diferentes o en distinto orden.
    # DuckDB alinea las que coinciden por nombre y pone NULL en las que faltan en una de las partes.
    sql = f"""
        SELECT {cols_str}
        FROM (
            SELECT * FROM ({query_historia})
            UNION ALL BY NAME
            SELECT * FROM tb_nuevos
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ID 
            ORDER BY "Fecha Contrato" DESC
        ) = 1
    """

    ruta_salida_temp = ruta_parquet_existente + ".temp"
    
    try:
        console.print("[info]💾 Escribiendo archivo Parquet consolidado...[/]")
        con.execute(f"COPY ({sql}) TO '{ruta_salida_temp}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')")
        
        if os.path.exists(ruta_parquet_existente):
            os.remove(ruta_parquet_existente)
        os.rename(ruta_salida_temp, ruta_parquet_existente)
        
        count = con.execute(f"SELECT COUNT(*) FROM '{ruta_parquet_existente}'").fetchone()[0]
        console.print(f"[success]✅ Dim_Cliente actualizada. Total registros únicos: {count:,}[/]")

    except Exception as e:
        console.print(f"[error]❌ Error en DuckDB: {e}[/]")
        if os.path.exists(ruta_salida_temp):
            os.remove(ruta_salida_temp)
    
    finally:
        con.close()

if __name__ == "__main__":
    ejecutar()
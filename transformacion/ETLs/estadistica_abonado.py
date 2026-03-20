import duckdb
import pandas as pd
import os
import sys
import glob
import re
from datetime import datetime

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import reportar_tiempo, console

# Mapa para convertir meses texto a número dentro de DuckDB
MAPA_MESES_SQL = """
    CASE UPPER(TRIM(MES_NOMBRE))
        WHEN 'ENERO' THEN 1 WHEN 'FEBRERO' THEN 2 WHEN 'MARZO' THEN 3 
        WHEN 'ABRIL' THEN 4 WHEN 'MAYO' THEN 5 WHEN 'JUNIO' THEN 6 
        WHEN 'JULIO' THEN 7 WHEN 'AGOSTO' THEN 8 WHEN 'SEPTIEMBRE' THEN 9 
        WHEN 'OCTUBRE' THEN 10 WHEN 'NOVIEMBRE' THEN 11 WHEN 'DICIEMBRE' THEN 12
        ELSE NULL 
    END
"""

def obtener_archivos_clave(ruta_directorio):
    """
    Analiza los nombres de archivo Data_AbonadosDDMMYYYY.xlsx
    Retorna: { 2024: {'fecha': datetime, 'ruta': path}, ... }
    Solo se queda con la versión más reciente encontrada para cada año.
    """
    archivos = glob.glob(os.path.join(ruta_directorio, "Data_Abonados*.xlsx"))
    mejores_archivos = {}
    
    patron = r"Data_Abonados(\d{2})(\d{2})(\d{4})" # Captura DD, MM, YYYY

    for archivo in archivos:
        nombre = os.path.basename(archivo)
        match = re.search(patron, nombre)
        
        if match:
            dia, mes, anio = match.groups()
            try:
                # Convertimos a int para comparar
                fecha_archivo = datetime(int(anio), int(mes), int(dia))
                anio_num = int(anio)
                
                # Si es el primer archivo de ese año O es más reciente que el que teníamos
                if anio_num not in mejores_archivos or fecha_archivo > mejores_archivos[anio_num]['fecha']:
                    mejores_archivos[anio_num] = {
                        'fecha': fecha_archivo,
                        'ruta': archivo
                    }
            except ValueError:
                continue

    return mejores_archivos

@reportar_tiempo
def ejecutar():
    console.rule("[bold cyan]14. ETL: ESTADÍSTICA ABONADOS (HISTÓRICO ANUAL)[/]")

    # 1. DEFINICIÓN DE RUTAS
    RUTA_RAW = PATHS["raw_estad_abonados"]
    NOMBRE_GOLD = "Estadistica_Abonados_Historico.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    
    # 2. SELECCIÓN INTELIGENTE DE ARCHIVOS
    archivos_clave = obtener_archivos_clave(RUTA_RAW)
    
    if not archivos_clave:
        console.print(f"[error]❌ No se encontraron archivos válidos (Data_Abonados*.xlsx) en: {RUTA_RAW}[/]")
        return

    console.print(f"[info]📅 Archivos seleccionados para la historia:[/]")
    for anio, data in sorted(archivos_clave.items()):
        console.print(f"  • Año [bold yellow]{anio}[/]: {os.path.basename(data['ruta'])} ({data['fecha'].strftime('%d/%m/%Y')})")

    # 3. PROCESAMIENTO CON DUCKDB
    con = duckdb.connect(database=':memory:')
    
    # Lista para acumular las sub-queries de cada año
    queries_union = []

    try:
        console.print("[cyan]⚙️  Procesando e ingiriendo archivos Excel...[/]")
        
        for anio, data in archivos_clave.items():
            archivo = data['ruta']
            nombre_tabla = f"raw_{anio}"
            
            # A. Leemos Excel con Pandas (usando engine='calamine' si está disponible, es más rápido)
            try:
                df_temp = pd.read_excel(archivo, engine="calamine")
            except ImportError:
                # Fallback a openpyxl si calamine no está instalado
                df_temp = pd.read_excel(archivo, engine="openpyxl")
            
            # B. Limpieza básica de columnas para SQL
            df_temp.columns = df_temp.columns.astype(str).str.strip().str.upper()
            
            # C. Registramos el DataFrame como tabla virtual en DuckDB
            con.register(nombre_tabla, df_temp)
            
            # D. Construimos la Query del Año (Unpivot + Inyección de Año + Fecha Real)
            query_anio = f"""
                SELECT 
                    ESTATUS,
                    UPPER(TRIM(Mes)) AS MES_NOMBRE,
                    {anio} AS ANIO,
                    make_date({anio}, {MAPA_MESES_SQL}, 1) AS FECHA_CORTE,
                    TRY_CAST(Cantidad AS INTEGER) AS CANTIDAD
                FROM {nombre_tabla}
                UNPIVOT (
                    Cantidad FOR Mes IN (COLUMNS(* EXCLUDE (ESTATUS)))
                )
                WHERE ESTATUS IN ('ACTIVO', 'ANULADO', 'CORTADO')
                  AND Cantidad IS NOT NULL
                  AND TRY_CAST(Cantidad AS INTEGER) > 0
            """
            queries_union.append(query_anio)

        # 4. UNIÓN Y EXPORTACIÓN MASIVA
        if queries_union:
            # Unimos todas las partes con UNION ALL
            query_maestra = " UNION ALL ".join(queries_union)

            console.print("[info]🦆 Ejecutando transformación y guardado en Parquet...[/]")
            
            # Ejecutamos el COPY directo a Parquet (Súper eficiente)
            con.execute(f"""
                COPY ({query_maestra}) 
                TO '{RUTA_GOLD_COMPLETA}' 
                (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """)
            
            # 5. VALIDACIÓN FINAL
            resumen = con.execute(f"""
                SELECT ANIO, COUNT(*) as Filas, SUM(CANTIDAD) as Total_Clientes
                FROM '{RUTA_GOLD_COMPLETA}' 
                GROUP BY ANIO 
                ORDER BY ANIO
            """).fetchall()
            
            console.print("\n[bold white]📊 Resumen por Año Procesado:[/]")
            for row in resumen:
                console.print(f"  • {row[0]}: {row[1]:,} filas | {row[2]:,} abonados (suma)")
            
            console.print(f"\n[bold green]✅ Archivo Gold generado exitosamente: {NOMBRE_GOLD}[/]")

    except Exception as e:
        console.print(f"[bold red]💥 Error crítico procesando la data: {e}[/]")
    
    finally:
        con.close()

if __name__ == "__main__":
    ejecutar()
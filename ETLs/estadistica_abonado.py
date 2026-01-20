import duckdb
import pandas as pd
import os
import glob
import re
from datetime import datetime
from config import PATHS
from utils import reportar_tiempo, console

# Mapa para convertir meses texto a número para crear fechas reales
MAPA_MESES_SQL = """
    CASE MES_NOMBRE
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
    Retorna un diccionario: { Año: 'Ruta_del_archivo_mas_reciente_de_ese_año' }
    """
    archivos = glob.glob(os.path.join(ruta_directorio, "Data_Abonados*.xlsx"))
    
    # Estructura: { 2024: {'fecha': datetime(...), 'ruta': '...'}, 2025: ... }
    mejores_archivos = {}
    
    patron = r"Data_Abonados(\d{2})(\d{2})(\d{4})" # Captura DD, MM, YYYY

    for archivo in archivos:
        nombre = os.path.basename(archivo)
        match = re.search(patron, nombre)
        
        if match:
            dia, mes, anio = match.groups()
            try:
                fecha_archivo = datetime(int(anio), int(mes), int(dia))
                anio_num = int(anio)
                
                # Lógica: Si no tenemos archivo para ese año, o este es más nuevo, lo guardamos
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
    console.rule("[bold cyan]ETL: ESTADÍSTICA HISTÓRICA (Lógica Anual)[/]")

    ruta_directorio = PATHS["raw_estad_abonados"]
    ruta_salida = os.path.join(PATHS["gold"], "Estadistica_Abonados_Historico.parquet")
    
    # 1. Seleccionar los archivos "ganadores" por año
    archivos_clave = obtener_archivos_clave(ruta_directorio)
    
    if not archivos_clave:
        console.print(f"[error]❌ No se encontraron archivos válidos en: {ruta_directorio}[/]")
        return

    console.print(f"[info]📅 Archivos seleccionados para la historia:[/]")
    for anio, data in sorted(archivos_clave.items()):
        console.print(f"   • Año [bold yellow]{anio}[/]: {os.path.basename(data['ruta'])} ({data['fecha'].strftime('%d/%m/%Y')})")

    # 2. Iniciar DuckDB
    con = duckdb.connect(database=':memory:')
    
    # Lista para acumular las tablas temporales de cada año
    tablas_sql = []

    try:
        for anio, data in archivos_clave.items():
            archivo = data['ruta']
            nombre_tabla = f"raw_{anio}"
            
            # Leemos con Pandas (Calamine) por velocidad y robustez
            df_temp = pd.read_excel(archivo, engine="calamine")
            df_temp.columns = df_temp.columns.astype(str).str.strip().str.upper()
            
            con.register(nombre_tabla, df_temp)
            
            # Query parcial por año (Unpivot + Inyección del Año)
            # NOTA: make_date(Year, Month, 1) crea una fecha real (01/01/2024)
            query_anio = f"""
                SELECT 
                    ESTATUS,
                    Mes AS MES_NOMBRE,
                    {anio} AS ANIO,
                    make_date({anio}, {MAPA_MESES_SQL}, 1) AS FECHA_CORTE,
                    TRY_CAST(Cantidad AS INTEGER) AS CANTIDAD
                FROM {nombre_tabla}
                UNPIVOT (
                    Cantidad FOR Mes IN (COLUMNS(* EXCLUDE (ESTATUS)))
                )
                WHERE ESTATUS IN ('ACTIVO', 'ANULADO', 'CORTADO')
                  AND CANTIDAD IS NOT NULL
                  AND CANTIDAD > 0
            """
            tablas_sql.append(query_anio)

        # 3. UNIÓN FINAL (UNION ALL)
        # Unimos las queries de todos los años en una sola gran query
        query_maestra = " UNION ALL ".join(tablas_sql)

        console.print("[info]🦆 Ejecutando transformación y unificación masiva...[/]")
        
        con.execute(f"""
            COPY ({query_maestra}) 
            TO '{ruta_salida}' 
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """)
        
        # Validación
        resumen = con.execute(f"""
            SELECT ANIO, COUNT(*) as Filas, SUM(CANTIDAD) as Total_Clientes
            FROM '{ruta_salida}' 
            GROUP BY ANIO 
            ORDER BY ANIO
        """).fetchall()
        
        console.print("\n[bold white]📊 Resumen por Año Procesado:[/]")
        for row in resumen:
            console.print(f"   • {row[0]}: {row[1]:,} registros | {row[2]:,} abonados sumados")
            
        console.print(f"\n[success]✅ Archivo Gold generado: {os.path.basename(ruta_salida)}[/]")

    except Exception as e:
        console.print(f"[error]💥 Error procesando la data: {e}[/]")
    
    finally:
        con.close()

if __name__ == "__main__":
    ejecutar()
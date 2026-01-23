import pandas as pd
import os
import sys
import calendar
from datetime import timedelta

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# -----------------------------

from config import PATHS, MAPA_MESES
from utils import (
    guardar_parquet, 
    reportar_tiempo, 
    console, 
    leer_carpeta, 
    limpiar_nulos_powerbi
)

def normalizar_quincena(fecha_archivo):
    """
    Recibe la fecha real del archivo y la asigna a la Quincena Estándar (Q1 o Q2).
    Lógica de negocio:
    - Día 01-05: Pertenece al cierre del mes ANTERIOR (Q2).
    - Día 06-20: Pertenece a la quincena del mes ACTUAL (Q1 -> Día 15).
    - Día 21-31: Pertenece al cierre del mes ACTUAL (Q2 -> Último día).
    """
    if pd.isnull(fecha_archivo):
        return pd.NaT

    dia = fecha_archivo.day
    mes = fecha_archivo.month
    anio = fecha_archivo.year

    # CASO 1: Inicios de mes (ej: 02-01-2025 -> Pertenece a Dic 2024)
    if dia <= 5:
        # Restamos días para caer en el mes anterior
        fecha_target = fecha_archivo.replace(day=1) - timedelta(days=1)
        # Ya estamos en el fin de mes anterior, listo.
        return fecha_target

    # CASO 2: Primera Quincena (ej: 12-11, 17-12 -> Pertenece al 15)
    elif dia <= 20:
        return fecha_archivo.replace(day=15)

    # CASO 3: Cierre de Mes (ej: 30-11, 31-10 -> Pertenece al fin de mes actual)
    else:
        # Calculamos el último día del mes actual
        ultimo_dia = calendar.monthrange(anio, mes)[1]
        return fecha_archivo.replace(day=ultimo_dia)

@reportar_tiempo
def ejecutar():
    console.rule("[bold cyan]PIPELINE: ABONADOS (ESTANDARIZADO POR QUINCENA)[/]")

    # 1. Definir Inputs
    if "raw_abonados_idf" not in PATHS:
        console.print("[red]❌ Error: Falta definir 'raw_abonados_idf' en config.py[/]")
        return

    cols_input = ["ID", "Franquicia"] 
    
    # 2. Carga Masiva
    df = leer_carpeta(
        ruta_carpeta=PATHS["raw_abonados_idf"],
        columnas_esperadas=cols_input,
        dtype=str 
    )
    
    if df.empty:
        console.print("[yellow]⚠️ No se cargaron datos de abonados.[/]")
        return

    console.print("📅 Extrayendo y normalizando fechas...")

    # 3. Extracción de Fecha "Cruda" del nombre del archivo
    df["Fecha_Archivo_Str"] = df["Source.Name"].str.extract(r'(\d{1,2}-\d{1,2}-\d{4})', expand=False)
    df["Fecha_Archivo"] = pd.to_datetime(df["Fecha_Archivo_Str"], format="%d-%m-%Y", errors="coerce")
    
    df = df.dropna(subset=["Fecha_Archivo"]) # Eliminar archivos sin fecha

    # 4. APLICACIÓN DEL "IMÁN DE FECHAS" 🧲
    # Aplicamos la función fila por fila (vectorizada en lo posible sería mejor, 
    # pero apply es seguro para lógica de calendario compleja)
    df["Fecha_Corte_Std"] = df["Fecha_Archivo"].apply(normalizar_quincena)

    # Creamos columnas auxiliares para facilitar la lectura en Power BI
    # Ejemplo: "2025-12-15" -> "DIC Q1"
    # Ejemplo: "2025-12-31" -> "DIC Q2"
    
    # Función lambda auxiliar para etiqueta
    df["Quincena_Label"] = df["Fecha_Corte_Std"].apply(
        lambda x: "Q1" if x.day == 15 else "Q2"
    )
    
    # Mapeo de meses usando tu diccionario global
    df["Mes_Label"] = df["Fecha_Corte_Std"].dt.month.map(MAPA_MESES).str.upper().str[:3] # ENE, FEB
    df["Periodo"] = df["Mes_Label"] + " " + df["Quincena_Label"] # "ENE Q1"

    # 5. Agrupación (Ahora agrupamos por la FECHA ESTÁNDAR)
    console.print("🔄 Agrupando por Franquicia y Fecha Estandarizada...")
    
    df_agrupado = df.groupby(
        ["Franquicia", "Fecha_Corte_Std", "Periodo"], 
        as_index=False
    )["ID"].nunique()
    
    df_agrupado.rename(columns={"ID": "Total_Abonados"}, inplace=True)

    # 6. Limpieza Final
    df_agrupado["Franquicia"] = df_agrupado["Franquicia"].str.upper().str.strip()
    df_agrupado = limpiar_nulos_powerbi(df_agrupado)

    # 7. Guardado
    guardar_parquet(
        df_agrupado, 
        "Abonados_Resumen_IdF.parquet", 
        filas_iniciales=len(df)
    )

if __name__ == "__main__":
    ejecutar()
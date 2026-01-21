# Archivo: ETLs/cobranza.py
import pandas as pd
import numpy as np
from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo,limpiar_nulos_powerbi, console
from rich.panel import Panel

@reportar_tiempo
def ejecutar():
    console.rule("[bold blue]PIPELINE OPERATIVOS COBRANZA[/]")

    # 1. Definición de Inputs
    cols_input = [
        "N° Abonado", "Cliente", "Estatus", "Saldo", 
        "Fecha Llamada", "Hora Llamada", "Tipo Respuesta", 
        "Detalle Respuesta", "Responsable", "Franquicia", "Ciudad"
    ]

    # 2. Extracción
    df = leer_carpeta(
        PATHS["raw_cobranza"], 
        columnas_esperadas=cols_input
    )

    if df.empty: 
        console.print("[warning]⚠️ No se encontraron datos para Cobranza.[/]")
        return

    filas_raw = len(df)

    # 3. Transformación
    with console.status("[bold green]Aplicando reglas de negocio...[/]", spinner="dots"):
        
        # --- A. LIMPIEZA CRÍTICA DE N° ABONADO (El "Por qué" de los vacíos) ---
        # 1. Forzar a String: Evita que "00123" se convierta en 123 o que letras rompan el tipo.
        df["N° Abonado"] = df["N° Abonado"].astype(str).str.strip()
        
        # 2. Detección de Basura: Convertimos 'nan', 'None' y vacíos a NaN real de Numpy
        #    Muchos "sin registro" son filas vacías de Excel o filas de totales.
        df["N° Abonado"] = df["N° Abonado"].replace(['nan', 'None', '', 'NaT'], np.nan)
        
        # 3. Filtrado: Eliminamos filas donde NO hay N° Abonado (no nos sirven sin ID)
        filas_antes_drop = len(df)
        df = df.dropna(subset=["N° Abonado"])
        filas_despues_drop = len(df)
        
        if filas_antes_drop != filas_despues_drop:
            diff = filas_antes_drop - filas_despues_drop
            console.print(f"[yellow]🧹 Se eliminaron {diff} filas sin 'N° Abonado' (Filas vacías o totales)[/]")

        # --- B. FECHAS VENEZUELA (DD/MM/YYYY) ---
        # dayfirst=True es la clave aquí. Si llega "01/02/2025", lo lee como 1 de Febrero.
        df["Fecha Llamada"] = pd.to_datetime(
            df["Fecha Llamada"], 
            dayfirst=True,  # <--- IMPORTANTE PARA VENEZUELA
            errors="coerce"
        ).dt.date # Nos quedamos solo con la fecha, sin hora

        # --- C. NORMALIZACIÓN ---
        df["Ciudad"] = df["Ciudad"].astype(str).str.upper()

        # --- D. LÓGICA CONDICIONAL (CANAL) ---
        resp = df["Responsable"].astype(str)
        
        condiciones = [
            resp.str.contains("OFI|ASESOR", case=False, na=False), 
            resp.str.contains("PHONE", case=False, na=False),      
            resp.str.contains("CALL", case=False, na=False)        
        ]
        opciones = ["OFICINA COMERCIAL", "HELPHONE", "CALL CENTER"]
        
        df["Canal"] = np.select(condiciones, opciones, default="ALIADOS")

        # --- E. LIMPIEZA FINAL ---
        df = df.rename(columns={"Hora Llamada": "Hora"})
        df = df.drop(columns=["Cliente"], errors="ignore")
        df = df.drop_duplicates()
        df = limpiar_nulos_powerbi(df)
        # Hora como string para evitar formateos extraños (0.54343)
        df["Hora"] = df["Hora"].astype(str)

        df = df.sort_values(by="Fecha Llamada", ascending=False)
        
    # 4. Carga
    # Nota: filas_iniciales=filas_raw mostrará en el reporte cuántas se eliminaron por no tener Abonado
    guardar_parquet(df, "Llamadas_Cobranza_Gold.parquet", filas_iniciales=filas_raw)
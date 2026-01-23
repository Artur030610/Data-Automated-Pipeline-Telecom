# Archivo: ETLs/cobranza.py
import pandas as pd
import numpy as np
from config import PATHS
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console

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
        
        # --- A. LIMPIEZA CRÍTICA DE N° ABONADO ---
        # Convertimos a string y limpiamos espacios
        df["N° Abonado"] = df["N° Abonado"].astype(str).str.strip()
        
        # Reemplazamos variantes de texto vacío por NaN real
        df["N° Abonado"] = df["N° Abonado"].replace(['nan', 'None', '', 'NaT'], np.nan)
        
        # Eliminamos filas sin ID (Totales o líneas vacías)
        filas_antes_drop = len(df)
        df = df.dropna(subset=["N° Abonado"])
        filas_despues_drop = len(df)
        
        if filas_antes_drop != filas_despues_drop:
            diff = filas_antes_drop - filas_despues_drop
            console.print(f"[yellow]🧹 Se eliminaron {diff} filas sin 'N° Abonado'[/]")

        # --- B. FECHAS VENEZUELA ---
        df["Fecha Llamada"] = pd.to_datetime(
            df["Fecha Llamada"], 
            dayfirst=True, 
            errors="coerce"
        ).dt.date # Solo fecha, sin hora

        # --- C. NORMALIZACIÓN ---
        df["Ciudad"] = df["Ciudad"].astype(str).str.upper()

        # --- D. LÓGICA CONDICIONAL (CANAL) ---
        # CORRECCIÓN: Prioridad ajustada para capturar "ASESOR HELPHONE" como Call Center.
        resp = df["Responsable"].astype(str)
        
        condiciones = [
            # PRIORIDAD 1: CALL CENTER
            # Busca "CALL" o "PHONE". Aquí cae "ASESOR HELPHONE" y "CALL CENTER".
            resp.str.contains("CALL|PHONE", case=False, na=False),       

            # PRIORIDAD 2: OFICINA COMERCIAL
            # Solo cae aquí si NO tenía la palabra PHONE/CALL (ej: "ASESOR TAQUILLA").
            resp.str.contains("OFI|ASESOR", case=False, na=False) 
        ]
        
        opciones = ["CALL CENTER", "OFICINA COMERCIAL"]
        
        # Todo lo demás (ej: Cobranza Externa, Juan Perez) será ALIADOS
        df["Canal"] = np.select(condiciones, opciones, default="ALIADOS")

        # --- E. LIMPIEZA FINAL ---
        df = df.rename(columns={"Hora Llamada": "Hora"})
        df = df.drop(columns=["Cliente"], errors="ignore")
        df = df.drop_duplicates()
        
        # Limpieza de nulos para Power BI
        df = limpiar_nulos_powerbi(df)
        
        # Aseguramos formato texto para la hora
        df["Hora"] = df["Hora"].astype(str)

        # Ordenamos
        df = df.sort_values(by="Fecha Llamada", ascending=False)
        
    # 4. Carga
    guardar_parquet(df, "Llamadas_Cobranza_Gold.parquet", filas_iniciales=filas_raw)
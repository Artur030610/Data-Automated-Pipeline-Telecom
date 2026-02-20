import pandas as pd
import numpy as np
import os
import sys
import glob # Necesario para listar los archivos de horas

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
# -----------------------------

from config import PATHS, MAPA_MESES
# Importamos obtener_rango_fechas para leer los nombres de los archivos
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console, ingesta_inteligente, obtener_rango_fechas

@reportar_tiempo
def ejecutar():
    console.rule("[bold white]PIPELINE INTEGRAL: RECAUDACIÓN + HORAS (INCREMENTAL INTELIGENTE)[/]")
    
    # 1. RUTAS
    RUTA_RAW_RECAUDACION = PATHS["raw_recaudacion"]
    RUTA_RAW_HORAS = PATHS["raw_horaspago"]
    NOMBRE_GOLD = "Recaudacion_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)

    # ---------------------------------------------------------
    # 2. INGESTA INTELIGENTE (RECAUDACIÓN)
    # ---------------------------------------------------------
    df_nuevo, df_historico = ingesta_inteligente(
        ruta_raw=RUTA_RAW_RECAUDACION,
        ruta_gold=RUTA_GOLD_COMPLETA,
        col_fecha_corte="Fecha"
    )

    if df_nuevo.empty and not df_historico.empty:
        console.print("[bold green]✅ Recaudación al día.[/]")
        return

    # ---------------------------------------------------------
    # 3. PROCESAMIENTO
    # ---------------------------------------------------------
    if not df_nuevo.empty:
        console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} pagos nuevos...[/]")
        
        # A. Limpieza básica Recaudación
        cols_input_recaudacion = [
            "ID Contrato", "ID Pago", "N° Abonado", "Fecha", "Total Pago", 
            "Forma de Pago", "Banco", "Oficina Cobro", 
            "Fecha Contrato", "Estatus", "Suscripción", "Grupo Afinidad", 
            "Nombre Franquicia", "Ciudad", "Cobrador"
        ]
        df_nuevo = df_nuevo.reindex(columns=cols_input_recaudacion)

        # Helper limpieza
        def limpiar_id(serie):
            return (serie.astype(str)
                    .str.replace("'", "", regex=False) 
                    .str.replace(r'\.0$', '', regex=True)
                    .str.strip())

        df_nuevo['ID Pago'] = limpiar_id(df_nuevo['ID Pago'])
        df_nuevo = df_nuevo.drop_duplicates()

        # =========================================================
        # 4. CARGA INTELIGENTE DE HORAS (EL FIX)
        # =========================================================
        # Estrategia: Solo leemos los archivos de horas que coincidan con las fechas de los pagos nuevos
        
        # 1. Detectamos el rango de fechas que necesitamos cubrir
        # Convertimos a datetime temporalmente para calcular min/max
        fechas_pagos = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors='coerce')
        fecha_min_req = fechas_pagos.min()
        fecha_max_req = fechas_pagos.max()
        
        console.print(f"[dim]📅 Buscando horas para el rango: {fecha_min_req.date()} al {fecha_max_req.date()}[/]")
        
        # 2. Escaneamos la carpeta de horas SIN leer los excels todavía
        todos_archivos_horas = glob.glob(os.path.join(RUTA_RAW_HORAS, "*.xlsx"))
        horas_a_leer = []
        
        with console.status("[bold blue]Filtrando archivos de Horas...[/]"):
            for archivo in todos_archivos_horas:
                # Usamos tu función de utils para leer la fecha del nombre del archivo
                inicio_arch, fin_arch, _ = obtener_rango_fechas(archivo)
                
                if inicio_arch and fin_arch:
                    # LÓGICA DE SUPERPOSICIÓN (OVERLAP)
                    # El archivo nos sirve si su rango toca nuestro rango de fechas requerido
                    # (InicioArchivo <= FinRequerido) Y (FinArchivo >= InicioRequerido)
                    if (inicio_arch <= fecha_max_req) and (fin_arch >= fecha_min_req):
                        horas_a_leer.append(archivo)
                else:
                    # Si el archivo no tiene fecha en el nombre, lo leemos por seguridad
                    horas_a_leer.append(archivo)
        
        # 3. Leemos SOLO los archivos necesarios
        if horas_a_leer:
            console.print(f"[bold cyan]📥 Leyendo {len(horas_a_leer)} archivos de Horas relevantes (de {len(todos_archivos_horas)} disponibles)...[/]")
            df_horas = leer_carpeta(archivos_especificos=horas_a_leer, columnas_esperadas=["ID Pago", "Hora de Pago"])
        else:
            console.print("[warning]⚠️ No se encontraron archivos de horas para estas fechas.[/]")
            df_horas = pd.DataFrame()

        # 4. Merge
        if not df_horas.empty:
            df_horas = df_horas.rename(columns={"ID pago": "ID Pago"})
            df_horas['ID Pago'] = limpiar_id(df_horas['ID Pago'])
            df_horas = df_horas.drop_duplicates(subset=['ID Pago'])
            
            df_nuevo = df_nuevo.merge(df_horas, on='ID Pago', how='left')
        else:
            df_nuevo['Hora de Pago'] = None

        # =========================================================
        # 5. RESTO DE LA LÓGICA DE NEGOCIO
        # =========================================================
        df_nuevo['N° Abonado'] = df_nuevo['N° Abonado'].astype(str).replace('nan', None)
        df_nuevo['ID Contrato'] = df_nuevo['ID Contrato'].astype(str).replace('nan', None)
        df_nuevo["Fecha"] = pd.to_datetime(df_nuevo["Fecha"], dayfirst=True, errors="coerce")
        df_nuevo['Total Pago'] = pd.to_numeric(df_nuevo['Total Pago'], errors='coerce').fillna(0)

        palabras_excluir = "VIRTUAL|Virna|Fideliza|Externa|Unicenter|Compensa"
        df_nuevo = df_nuevo[~df_nuevo['Oficina Cobro'].astype(str).str.contains(palabras_excluir, case=False, na=False, regex=True)].copy()

        df_nuevo['Tipo de afluencia'] = "RECAUDACIÓN"
        df_nuevo['Mes'] = df_nuevo['Fecha'].dt.month.map(MAPA_MESES)

        oficinas_propias = [
            "OFC COMERCIAL CUMANA", "OFC- LA ASUNCION", "OFC SAN ANTONIO DE CAPYACUAL", "OFC TINACO", 
            "OFC VILLA ROSA", "OFC-SANTA FE", "OFI CARIPE MONAGAS", "OFI TINAQUILLO", "OFI-BARCELONA", 
            "OFI-BARINAS", "OFI-BQTO", "OFIC GALERIA EL PARAISO", "OFIC SAMBIL-VALENCIA", 
            "OFIC. PARRAL VALENCIA", "OFIC. TORRE FIBEX VIÑEDO", "OFI-CARACAS PROPATRIA", 
            "OFIC-BOCA DE UCHIRE", "OFIC-CARICUAO", "OFIC-COMERCIAL SANTA FE", "OFICINA ALIANZA MALL", 
            "OFICINA MARGARITA", "OFICINA SAN JUAN DE LOS MORROS", "OFIC-JUAN GRIEGO-MGTA", 
            "OFIC-METROPOLIS-BQTO", "OFIC-MGTA_DIAZ", "OFI-LECHERIA", "OFI-METROPOLIS", "OFI-PARAISO", 
            "OFI-PASEO LAS INDUSTRIAS", "OFI-PTO CABELLO", "OFI-PTO LA CRUZ", "OFI-SAN CARLOS", "OFI-VIA VENETO"
        ]
        
        df_nuevo['Clasificacion'] = np.where(
            df_nuevo['Oficina Cobro'].isin(oficinas_propias), 
            "OFICINAS PROPIAS", 
            "ALIADOS Y DESARROLLO"
        )
        
        df_nuevo = df_nuevo.rename(columns={"Oficina Cobro": "Oficina", "Cobrador": "Vendedor"})
        
        cols_output = [
            "ID Contrato", "ID Pago", "N° Abonado", "Fecha", "Total Pago", 
            "Forma de Pago", "Banco", "Oficina", "Fecha Contrato", 
            "Estatus", "Suscripción", "Grupo Afinidad", "Nombre Franquicia", 
            "Ciudad", "Vendedor", "Tipo de afluencia", "Mes", "Clasificacion",
            "Hora de Pago"
        ]
        df_nuevo = df_nuevo.reindex(columns=cols_output)

    # 6. UNIFICACIÓN Y GUARDADO
    df_final = pd.DataFrame()

    if not df_historico.empty and not df_nuevo.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    elif not df_nuevo.empty:
        df_final = df_nuevo
    else:
        df_final = df_historico

    if not df_final.empty:
        filas_antes = len(df_final)
        df_final = df_final.drop_duplicates(subset=["ID Pago"], keep='last')
        
        guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=filas_antes)

if __name__ == "__main__":
    ejecutar()
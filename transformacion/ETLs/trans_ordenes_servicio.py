import pandas as pd
import numpy as np
import sys
import os
import glob
import re
import datetime
import polars as pl

# ==========================================
# 🔼 EL TRUCO DEL ASCENSOR 🔼
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
granparent_dir = os.path.dirname(parent_dir)  # Sube el segundo nivel
sys.path.append(granparent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, console, limpiar_nulos_powerbi, archivos_raw

ruta_silver = PATHS.get("silver")
ruta_gold = PATHS.get("gold")
ruta_bronze = PATHS.get("bronze","data/bronze")

# ==========================================
# 1. CONSTANTES DE NEGOCIO (TUS VALIDADAS)
# ==========================================
HORAS_SLA_META = 24.0

# --- CLASIFICACIÓN POR NATURALEZA DE LA SOLUCIÓN ---
SOLUCIONES_LOGICAS_NOC = [
    "RESETEO", "CONFIGURACION", "CONFIGURACIÓN", "SOPORTE REMOTO", "REINICIO", "REINICIAR",
    "ACTUALIZACION", "ACTUALIZACIÓN", "PROVISIONAMIENTO", "APROVISIONAMIENTO", "LIBERACION", 
    "MAC", "IP", "VERIFICACION", "SISTEMA", "APP", "SMART", "FIBEXPLAY", "FIBEX PLAY", 
    "CREDENCIALES", "ENRUTAMIENTO", "OLT", "DESCONECTADO", "SOLVENTADA", "DHCP", "WIFI DESACTIVADO", 
    "VLAN", "FRECUENCIA", "DNS", "ISP", "DFS", "CANALES", "SOFTWARE", "VERSION", 
    "AUDIO", "VIDEO", "SATURACION", "LIMITACION", "ASESORAMIENTO"
]

SOLUCIONES_FISICAS_OP = [
    "CAMBIO DE EQUIPO", "CAMBIO DE ONT", "CONECTOR", "CABLE", "FIBRA", "FALLA EN LA RED INTERNA",
    "CORTE", "VISITA", "EMPALME", "REPARACION", "REINSTALACION", "REINSTALACIÓN",
    "MANTENIMIENTO FISICO", "MUDANZA", "ROUTER", "CAJA", "ANTENA", "ACOPLE", 
    "ATENUACION", "ATENUACIÓN", "PATCHCORD", "DAÑADO", "UPGRADE", "FALLA LOS", 
    "REUBICACION", "REUBICACIÓN", "EQUIPO APAGADO", "EQUIPO DAÑADO"
]


SOLUCIONES_ADMINISTRATIVAS = [
    "CLIENTE SOLICITO REEMBOLSO", "CONSULTA DE SALDO",
    "PAGO CRUZADO", "PAGO RECHAZADO", "PAGO CONCILIADO", "PAGO REGISTRADO", "PAGO DUPLICADO",
    "NOTA CRÉDITO DIFERENCIAL CAMBIARIO", "CLIENTE NO CONTESTA"
]


NOC_USERS = [
    "GFARFAN", "JVELASQUEZ", "JOLUGO", "KUSEA", "SLOPEZ",
    "EDESPINOZA", "SANDYJIM", "JOCASTILLO", "JESUSGARCIA",
    "DFUENTES", "JOCANTO", "IXMONTILLA", "OMTRUJILLO",
    "LLZERPA", "LUIJIMENEZ","NSANTANA","WILLMARTINEZ",
    "DOURODRIGUEZ"
]

EXCLUIR_SOLUCIONES = [
    "LLAMADAS DE AGENDAMIENTO", "ORDEN REPETIDA", "ORDEN MAL GENERADA", "CAMBIO DE CLAVE"
]

# ==========================================
# 2. DEFINICIÓN DE COLUMNAS
# ==========================================
ORDEN_FINAL_SILVER = [
    "Quincena Evaluada", "FechaInicio", "FechaFin", 
    "Fecha Apertura", "Fecha Impresion", "Fecha Cierre", 
    "Fecha Apertura Date", "Fecha Cierre Date", 
    "Es_Falla", "Cumplio_SLA", "Duracion_Horas",
    "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min",
    "Franquicia", "Grupo Trabajo", "Grupo Afinidad", "Clasificacion",
    "N° Contrato", "N° Orden", "Estatus contrato", "Estatus_orden",
    "Usuario Final", "Usuario Emisión", "Usuario Impresión",
    "Solucion Aplicada", "Detalle Orden", "SLA Detalle Texto"
]

ORDEN_FINAL_GOLD_SLA = [
    "Quincena Evaluada", "Franquicia", "Clasificacion", "Fecha Apertura Date",
    "Total_Ordenes", "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min"   
]

ORDEN_FINAL_GOLD_IDF = [
    "Quincena Evaluada", "Franquicia", "Fecha Apertura Date", "Fecha Cierre Date", 
    "Total_Fallas"
]

ORDEN_FINAL_GOLD_IDF_DETALLE_SOLUCION = [
     "Quincena Evaluada", "Franquicia", "Solucion Aplicada", "Detalle Orden", "Total_Ordenes"
]

# LA JOYA DE LA CORONA PARA DAX (Stats Granulares)
ORDEN_SLA_STATS = [
    "Quincena Evaluada", "Franquicia", 
    "Clasificacion", "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min"
]

# --- LA NUEVA TABLA ÚNICA DE HECHOS PARA POWER BI ---
ORDEN_FINAL_FACT_TICKETS = [
    "Quincena Evaluada", "Franquicia", "Fecha Apertura Date", "Fecha Cierre Date",
    "N° Orden", "N° Contrato",
    "Solucion Aplicada", "Detalle Orden", "Clasificacion", "Grupo Afinidad",
    "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min",
    "Cumplio_SLA", "Duracion_Horas", "Es_Falla"
]

COLS_INPUT_RAW = [
    "FechaInicio", "FechaFin", "FechaInicioQuincena", "Quincena Evaluada",
    "N° Contrato", "Estatus contrato", "N° Orden", "Estatus_orden",
    "Fecha Creacion", "Fecha Impresion", "Fecha Finalizacion", 
    "Grupo Afinidad", "Detalle Orden", "Franquicia",
    "Grupo Trabajo", "Usuario Emisión", "Usuario Impresión", 
    "Usuario Final", "Solucion Aplicada"
]

# ==========================================
# 3. FUNCIONES UTILITARIAS
# ==========================================
def obtener_rango_fechas(nombre_archivo):
    try:
        nombre_limpio = os.path.basename(nombre_archivo).lower()
        patron = r"(\d{1,2}\W\d{1,2}\W\d{4})"
        fechas_encontradas = re.findall(patron, nombre_limpio)
        if len(fechas_encontradas) < 2: return None, None, None

        def norm(f): return re.sub(r"\W", "-", f)
        f_inicio = datetime.datetime.strptime(norm(fechas_encontradas[0]), "%d-%m-%Y")
        f_fin = datetime.datetime.strptime(norm(fechas_encontradas[1]), "%d-%m-%Y")

        quincena_str = "Q1" if f_fin.day <= 15 else "Q2"
        meses = ["", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]
        nombre_etiqueta = f"{meses[f_fin.month]} {f_fin.year} {quincena_str}"
        return f_inicio, f_fin, nombre_etiqueta
    except Exception: return None, None, None

def limpiar_fechas_mixtas(series):
    if pd.api.types.is_datetime64_any_dtype(series): return series
    series_nums = pd.to_numeric(series, errors='coerce')
    mask_es_serial = (series_nums > 35000) & (series_nums < 60000)
    fechas_excel = pd.to_datetime(series_nums[mask_es_serial], unit='D', origin='1899-12-30')
    resto = series[~mask_es_serial].astype(str).str.strip()
    fechas_texto = pd.to_datetime(resto, dayfirst=True, errors='coerce')
    return fechas_excel.combine_first(fechas_texto)

# ==========================================
# 4. PIPELINE PRINCIPAL (TU LÓGICA RESTAURADA)
# ==========================================
@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]PIPELINE MASTER: TICKETS (SLA + IDF)[/]")

    ruta_origen = PATHS.get("raw_idf") 
    archivo_bronze_salida = os.path.join(ruta_bronze, "Tickets_SLA_Raw_Bronze.parquet")
    
    # Mantenemos tu guardado en Bronze
    try: archivos_raw(ruta_origen, archivo_bronze_salida)
    except Exception: pass

    archivos = glob.glob(os.path.join(ruta_origen, "*.xlsx")) #type: ignore
    console.print(f"📂 Se encontraron {len(archivos)} archivos. Procesando...")

    dataframes_procesados = []

    for archivo in archivos:
        nombre_archivo = os.path.basename(archivo)
        if nombre_archivo.startswith("~$") or "Consolidado" in nombre_archivo: continue
        
        fecha_inicio, fecha_fin, quincena_nombre = obtener_rango_fechas(archivo)
        if not fecha_inicio: continue

        try:
            # OPTIMIZACIÓN RAM: Polars lee el Excel en C++ (mucho más ligero)
            # y lo convierte a Pandas usando PyArrow. Esto evita el pico de RAM de pd.read_excel.
            df = pl.read_excel(archivo, engine="calamine", infer_schema_length=0).to_pandas(use_pyarrow_extension_array=True)
            
            if df.empty: continue

            # --- LIMPIEZA FECHAS ---
            cols_fecha = ["Fecha Creacion", "Fecha Emisión", "Fecha Final", 
                          "Fecha Impresion", "Fecha Cierre", "Fecha Finalizacion"]
            for col in cols_fecha:
                if col in df.columns: df[col] = limpiar_fechas_mixtas(df[col])

            if "Fecha Creacion" not in df.columns: continue

            # --- FILTRO HÍBRIDO: EL AJUSTE PARA EL BACKLOG ---
            limite_inferior = fecha_fin.replace(day=fecha_inicio.day) #type: ignore
            limite_superior = fecha_fin + pd.Timedelta(days=1) - pd.Timedelta(seconds=1) #type: ignore
            
            mask_cerrados_aqui = (df["Fecha Finalizacion"] >= limite_inferior) & (df["Fecha Finalizacion"] <= limite_superior) if "Fecha Finalizacion" in df.columns else pd.Series(False, index=df.index)

            # CAMBIO CLAVE: Usamos fecha_inicio para capturar los 30 días de backlog previos
            mask_nacieron_aqui = (df["Fecha Creacion"] >= fecha_inicio) & (df["Fecha Creacion"] <= limite_superior)
            mask_siguen_abiertos = df["Fecha Finalizacion"].isna() if "Fecha Finalizacion" in df.columns else pd.Series(True, index=df.index)
            
            df_filtrado = df[mask_cerrados_aqui | (mask_nacieron_aqui & mask_siguen_abiertos)].copy()

            if len(df_filtrado) == 0: continue

            # --- ENRIQUECIMIENTO Y NORMALIZACIÓN (Tus reglas) ---
            df_filtrado = df_filtrado.assign(
                FechaInicio = fecha_inicio, FechaFin = fecha_fin,
                FechaInicioQuincena = fecha_inicio, Quincena_Evaluada = quincena_nombre,
                Solucion_Norm = lambda x: x["Solucion Aplicada"].fillna("").astype(str).str.upper(),
                Grupo_Norm    = lambda x: x["Grupo Trabajo"].fillna("").astype(str).str.upper(),
                Detalle_Norm  = lambda x: x["Detalle Orden"].fillna("").astype(str).str.upper(),
                Estatus_Norm  = lambda x: x["Estatus_orden"].fillna("").astype(str).str.upper()
            ).rename(columns={"Quincena_Evaluada": "Quincena Evaluada"})
            
            mask_excluir = (
                df_filtrado["Solucion_Norm"].isin(EXCLUIR_SOLUCIONES) |
                df_filtrado["Grupo_Norm"].str.contains("GT API FIBEX", na=False) |
                (df_filtrado["Detalle_Norm"] == "PRUEBA DE INTERNET") |
                df_filtrado["Estatus_Norm"].str.contains("CREACIÓN", na=False) |
                df_filtrado["Estatus_Norm"].isin(["ANULADA", "CANCELADA", "ELIMINADA"])
            )
            
            df_final = df_filtrado[~mask_excluir].copy()
            dataframes_procesados.append(df_final[[c for c in COLS_INPUT_RAW if c in df_final.columns]])
            console.print(f"   ✅ {quincena_nombre}: {len(df_final)} filas")

        except Exception as e:
            console.print(f"   ❌ Error en {nombre_archivo}: {e}")

    # ==========================================
    # 5. CONSOLIDACIÓN Y FIX DE DUPLICADOS
    # ==========================================
    if dataframes_procesados:
        df_total = pd.concat(dataframes_procesados, ignore_index=True)
        dataframes_procesados.clear()

        # EL FIX: Borramos columnas basura que causan el ValueError
        df_total = df_total.drop(columns=["Fecha Apertura", "Fecha Cierre"], errors="ignore")

        df_total = df_total.rename(columns={"Fecha Creacion": "Fecha Apertura", "Fecha Finalizacion": "Fecha Cierre"})
        df_total = df_total.loc[:, ~df_total.columns.duplicated()].copy()

        # Tu clasificación NOC/Operaciones
        df_total['Grupo_Norm'] = df_total['Grupo Trabajo'].fillna('').astype(str).str.upper()
        df_total['Usuario_Norm'] = df_total['Usuario Final'].fillna('').astype(str).str.upper()
        df_total['Solucion_Norm'] = df_total['Solucion Aplicada'].fillna('').astype(str).str.upper()
        
        patron_admin  = '|'.join(SOLUCIONES_ADMINISTRATIVAS)
        patron_logico = '|'.join(SOLUCIONES_LOGICAS_NOC)
        patron_fisico = '|'.join(SOLUCIONES_FISICAS_OP)
        
        # Evaluamos las condiciones en orden de prioridad estricto
        condiciones = [
            df_total['Solucion_Norm'].str.contains(patron_admin, regex=True, na=False),
            df_total['Usuario_Norm'].isin(NOC_USERS), 
            df_total['Grupo_Norm'].isin(NOC_USERS),
            df_total['Usuario_Norm'].str.contains('NOC', na=False), 
            df_total['Grupo_Norm'].str.contains('OPERACIONES|MESA DE CONTROL', na=False),
            df_total['Solucion_Norm'].str.contains(patron_fisico, regex=True, na=False),
            df_total['Solucion_Norm'].str.contains(patron_logico, regex=True, na=False)
        ]
        
        opciones = ['ADMINISTRATIVO', 'NOC', 'NOC', 'NOC', 'OPERACIONES', 'OPERACIONES', 'NOC']
        df_total['Clasificacion'] = np.select(condiciones, opciones, default='N/S')

        # Tu lógica de KPIs
        for c in ['Fecha Cierre', 'Fecha Apertura', 'Fecha Impresion']:
            df_total[c] = pd.to_datetime(df_total[c], errors='coerce', dayfirst=True)

        delta_res = df_total['Fecha Cierre'] - df_total['Fecha Apertura']
        
        # FIX: Prevenir ruido en DAX causado por tiempos negativos (errores de fecha en el ERP)
        df_total['SLA Resolucion Min'] = (delta_res.dt.total_seconds() / 60).round(2).clip(lower=0)
        df_total['SLA Despacho Min']   = ((df_total['Fecha Cierre'] - df_total['Fecha Impresion']).dt.total_seconds() / 60).round(2).clip(lower=0)
        df_total['SLA Impresion Min']  = ((df_total['Fecha Impresion'] - df_total['Fecha Apertura']).dt.total_seconds() / 60).round(2).clip(lower=0)
        
        # --- FIX NOC VS CALLE (VECTORIZADO PARA EVITAR BUG DE PYARROW) ---
        mask_cerrado = df_total['Fecha Cierre'].notna()
        mask_no_impreso = df_total['Fecha Impresion'].isna()
        mask_remoto = mask_cerrado & mask_no_impreso
        
        # Extraemos los cálculos base
        sla_res = df_total['SLA Resolucion Min']
        sla_des = df_total['SLA Despacho Min']
        sla_imp = df_total['SLA Impresion Min']

        # Llenamos nulos temporalmente solo para las matemáticas de los tickets cerrados
        sla_res_cerrado = np.where(mask_cerrado, sla_res.fillna(0), sla_res)
        sla_des_cerrado = np.where(mask_cerrado, sla_des.fillna(0), sla_des)

        # 1. Para tickets remotos cerrados, despacho es 0
        sla_des_cerrado = np.where(mask_remoto, 0.0, sla_des_cerrado)

        # 2. Blindaje para cerrados: Despacho no supera Resolución
        sla_des_final = np.where(mask_cerrado, np.minimum(sla_des_cerrado, sla_res_cerrado), sla_des)
        
        # 3. Impresión para cerrados = Res - Despacho. Para abiertos se mantiene intacto.
        sla_imp_final = np.where(mask_cerrado, sla_res_cerrado - sla_des_final, sla_imp)

        # Asignamos de vuelta de forma segura a las columnas de Power BI
        df_total['SLA Despacho Min'] = sla_des_final
        df_total['SLA Impresion Min'] = sla_imp_final

        df_total['Fecha Apertura Date'] = df_total['Fecha Apertura'].dt.normalize()
        df_total['Fecha Cierre Date'] = df_total['Fecha Cierre'].dt.normalize()
        df_total['Duracion_Horas'] = df_total['SLA Resolucion Min'] / 60
        df_total['Es_Falla'] = np.where(df_total['Clasificacion'] == 'ADMINISTRATIVO', 0, 1)
        
        df_total['Cumplio_SLA'] = np.where((df_total['Duracion_Horas'] > 0) & (df_total['Duracion_Horas'] <= HORAS_SLA_META), 1, 0)

        # --- FIX: Normalización de IDs ---
        # Polars puede leer números de Excel como "12345.0", lo que rompe la deduplicación
        for col in ["N° Orden", "N° Contrato"]:
            if col in df_total.columns:
                df_total[col] = df_total[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # Tu Deduplicación Blindada (Aislada por Quincena)
        # CRÍTICO: Se incluye "Quincena Evaluada" para permitir que un ticket viva en Q1 (Backlog) y en Q2 (Cerrado)
        df_total = df_total.sort_values(by=["Fecha Cierre"], na_position='first').drop_duplicates(
            subset=["Quincena Evaluada", "N° Orden"], keep="last"
        )
        
        df_total = limpiar_nulos_powerbi(df_total)

        # Salidas Master y Gold
        df_silver = df_total.reindex(columns=[c for c in ORDEN_FINAL_SILVER if c in df_total.columns])
        guardar_parquet(df_silver, "Tickets_Silver_Master.parquet", filas_iniciales=len(df_silver), ruta_destino=ruta_silver)
        
        df_gold_sla = df_silver.dropna(subset=['SLA Resolucion Min']).query("`SLA Resolucion Min` > 0").groupby(
            ["Quincena Evaluada", "Franquicia", "Clasificacion", "Fecha Apertura Date"], as_index=False
        ).agg(Total_Ordenes=("N° Orden", "nunique"), **{k: (k, "sum") for k in ["SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min"]})
        
        guardar_parquet(df_gold_sla.reindex(columns=ORDEN_FINAL_GOLD_SLA), "SLA_Gold.parquet", filas_iniciales=len(df_gold_sla), ruta_destino=ruta_gold)

        # --- C. GOLD IDF ---
        df_gold_idf = df_silver.groupby(
            ["Quincena Evaluada", "Franquicia", "Fecha Apertura Date", "Fecha Cierre Date"],
            as_index=False,
            dropna=False
        ).agg(Total_Fallas=("N° Orden", "nunique"))
        df_gold_idf = df_gold_idf.reindex(columns=ORDEN_FINAL_GOLD_IDF)
        guardar_parquet(df_gold_idf, "IDF_Gold.parquet", filas_iniciales=len(df_gold_idf), ruta_destino=ruta_gold)

        # --- D. GOLD IDF DETALLE SOLUCIÓN ---
        # Llenamos explícitamente los nulos para evitar que el groupby() elimine los tickets abiertos
        df_prep_sol = df_silver[df_silver["Es_Falla"] == 1][["Quincena Evaluada", "Franquicia", "Solucion Aplicada", "Detalle Orden", "N° Orden"]].copy()
        df_prep_sol["Solucion Aplicada"] = df_prep_sol["Solucion Aplicada"].fillna("EN PROCESO / SIN SOLUCIÓN")
        df_prep_sol["Detalle Orden"] = df_prep_sol["Detalle Orden"].fillna("SIN DETALLE REPORTADO")
        
        df_gold_idf_detalle = df_prep_sol.groupby(
            ["Quincena Evaluada", "Franquicia", "Solucion Aplicada", "Detalle Orden"],
            as_index=False,
            dropna=False
        ).agg(Total_Ordenes=("N° Orden", "count"))
        df_gold_idf_detalle = df_gold_idf_detalle.reindex(columns=ORDEN_FINAL_GOLD_IDF_DETALLE_SOLUCION)
        guardar_parquet(df_gold_idf_detalle, "IDF_Gold_Detalle_Solucion.parquet", filas_iniciales=len(df_gold_idf_detalle), ruta_destino=ruta_gold)

        # --- E. GOLD SLA-STATS ---
        console.print("🚀 Generando Gold: SLA-Stats (Precisión absoluta para DAX)...")
        df_gold_stats = df_silver[["Quincena Evaluada", "FechaFin", "Franquicia", "Clasificacion", "SLA Resolucion Min", "SLA Despacho Min", "SLA Impresion Min"]].copy()
        
        df_gold_stats = df_gold_stats.dropna(subset=['SLA Resolucion Min'])
        
        # # FIX ABSOLUTO: Filtrar y blindar para que SLA-Stats sea idéntico al SLA Normal
        # df_gold_stats = df_gold_stats[df_gold_stats['SLA Resolucion Min'] > 0]
        
        # df_gold_stats['SLA Resolucion Min'] = df_gold_stats['SLA Resolucion Min'].clip(lower=1)
        
        df_gold_stats = df_gold_stats.reindex(columns=ORDEN_SLA_STATS)
        guardar_parquet(df_gold_stats, "SLA_GOLD_STATS.parquet", filas_iniciales=len(df_gold_stats), ruta_destino=ruta_gold)

        # --- F. FACT TICKETS GOLD (LA TABLA DEFINITIVA PARA POWER BI) ---
        console.print("🚀 Generando Gold: Fact_Tickets (Modelo Estrella Optimizado)...")
        df_fact_tickets = df_silver.copy()
        # Nos quedamos solo con las columnas de negocio, fechas y métricas, descartando textos pesados
        df_fact_tickets = df_fact_tickets.reindex(columns=ORDEN_FINAL_FACT_TICKETS)
        df_fact_tickets["Solucion Aplicada"] = df_fact_tickets["Solucion Aplicada"].fillna("EN PROCESO / SIN SOLUCIÓN")
        guardar_parquet(df_fact_tickets, "Tickets_Fact_Gold.parquet", filas_iniciales=len(df_fact_tickets), ruta_destino=ruta_gold)

        console.print(f"[bold green]✨ Proceso Finalizado. Tickets Reales: {len(df_total):,}[/]")

if __name__ == "__main__":
    ejecutar()
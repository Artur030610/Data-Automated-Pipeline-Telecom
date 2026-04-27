import pandas as pd
import numpy as np
import os
from config import PATHS, LISTA_VENDEDORES_OFICINA, LISTA_VENDEDORES_PROPIOS, MAPA_MESES
from utils import (
    guardar_parquet, reportar_tiempo, console, 
    ingesta_incremental_polars, limpiar_nulos_powerbi
)

# --- LÓGICA DE CLASIFICACIÓN (Mantenida 100% igual) ---
def clasificar_canal(row):
    vendedor = str(row.get("Vendedor", "")).lower()
    nombre_detectado = str(row.get("nombre_detectado", ""))
    tipo_coincidencia = str(row.get("tipo_coincidencia", ""))
    
    if "televentas" in vendedor or "call center" in vendedor:
        return "CALL CENTER"
    
    condicion_oficina_detectada = (
        nombre_detectado != "nan" and 
        nombre_detectado != "" and 
        tipo_coincidencia != "Pendiente de Revisión" and 
        tipo_coincidencia != "No detectado" and 
        "administrador" not in vendedor
    )
    
    if condicion_oficina_detectada or (vendedor in LISTA_VENDEDORES_OFICINA):
        return "OFICINA COMERCIAL"
    
    condicion_calle = ("ventas" in vendedor and "televentas" not in vendedor)
    
    if condicion_calle or (vendedor in LISTA_VENDEDORES_PROPIOS):
        return "VENDEDORES PROPIOS"
    
    return "ALIADOS"

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]9. ETL: VENTAS (BRONZE INCREMENTAL / GOLD FULL REFRESH)[/]")
    
    # 1. Configuración de Rutas
    RUTA_RAW = PATHS["ventas_abonados"]
    NOMBRE_GOLD = "Ventas_Listado_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Ventas_Listado_Bronze.parquet")
    
    cols_esperadas = [
        "ID", "N° Abonado", "Fecha Contrato", "Estatus", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor", 
        "Serv/Paquete", "nombre_detectado", "Estado", "oficina_comercial", 
        "tipo_coincidencia", "fuzzy_score_nombre", "fuzzy_score_apellido", 
        "fuzzy_score_combinado", "Fecha_Modificacion_Archivo"
    ]

    # =========================================================
    # --- PASO 1: ACTUALIZACIÓN INCREMENTAL BRONZE (POLARS) ---
    # =========================================================
    try:
        # Usamos Polars para unificar los Raw nuevos en el Bronze histórico
        ingesta_incremental_polars(
            ruta_raw=RUTA_RAW,
            ruta_bronze_historico=RUTA_BRONZE,
            columna_fecha="Fecha Contrato"
        )
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó. Error: {e}[/]")

    # =========================================================
    # --- PASO 2: LECTURA FULL DESDE BRONZE (Fuente de Verdad) ---
    # =========================================================
    if not os.path.exists(RUTA_BRONZE):
        console.print("[red]❌ No se encontró la capa Bronze. Ejecución abortada.[/]")
        return
        
    console.print("[cyan]📥 Leyendo histórico completo desde capa Bronze...[/]")
    df_nuevo = pd.read_parquet(RUTA_BRONZE, dtype_backend="pyarrow")

    if df_nuevo.empty:
        console.print("[bold green]✅ No hay datos en Bronze para procesar.[/]")
        return

    # =========================================================
    # --- PASO 3: TRANSFORMACIÓN PANDAS (Lógica de Negocio) ---
    # =========================================================
    console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} registros totales...[/]")
    
    df_nuevo = df_nuevo.reindex(columns=cols_esperadas)
    df_nuevo = df_nuevo.rename(columns={"oficina_comercial": "Oficina"})
    df_nuevo["Tipo de afluencia"] = "VENTAS"
    
    # --- CORRECCIÓN DE FECHAS (Simplificada) ---
    df_nuevo["Fecha Contrato"] = pd.to_datetime(df_nuevo["Fecha Contrato"], errors='coerce')
    df_nuevo["Mes"] = df_nuevo["Fecha Contrato"].dt.month.map(MAPA_MESES).str.capitalize()

    # --- LIMPIEZA DE TEXTO ---
    columnas_texto = ["Vendedor", "Ciudad", "nombre_detectado", "Oficina", "N° Abonado"]
    cols_to_fix = [c for c in columnas_texto if c in df_nuevo.columns]
    
    df_nuevo[cols_to_fix] = df_nuevo[cols_to_fix].fillna("").astype(str).apply(lambda x: x.str.strip())
    
    # Upper/Lower
    cols_upper = [c for c in cols_to_fix if c != "Vendedor"]
    if cols_upper: df_nuevo[cols_upper] = df_nuevo[cols_upper].apply(lambda x: x.str.upper())
    if "Vendedor" in cols_to_fix: df_nuevo["Vendedor"] = df_nuevo["Vendedor"].str.lower()

    # --- REGLAS BEJUMA ---
    mask_bejuma = df_nuevo["Vendedor"].str.contains("bejuma", case=False, na=False) & \
                  df_nuevo["Vendedor"].str.contains("ofic", case=False, na=False)
    
    if mask_bejuma.any():
        df_nuevo.loc[mask_bejuma, "Oficina"] = "BEJUMA"
        df_nuevo.loc[mask_bejuma, "tipo_coincidencia"] = "Oficina Detectada"
        df_nuevo.loc[mask_bejuma, "nombre_detectado"] = "OFICINA BEJUMA"

    # --- CLASIFICACIÓN Y LIMPIEZA FINAL ---
    df_nuevo["Canal"] = df_nuevo.apply(clasificar_canal, axis=1)
    
    cols_a_borrar = ["Estado", "Cliente", "ID", "Serv/Paquete", 
                     "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"]
    df_nuevo = df_nuevo.drop(columns=cols_a_borrar, errors="ignore")
    df_nuevo = df_nuevo[df_nuevo['tipo_coincidencia'] != 'Pendiente de Revisión']

    # =========================================================
    # --- PASO 4: ESCRITURA FULL EN CAPA GOLD ---
    # =========================================================
    # Mantenemos el nombre de la variable df_final para la escritura
    df_final = df_nuevo.copy()

    if not df_final.empty:
        filas_antes = len(df_final)
        
        # 1. ORDENAMIENTO CRONOLÓGICO POR METADATA (Si existe)
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            console.print("[cyan]⏱️ Ordenando por metadata para preservar el registro más reciente...[/]")
            df_final = df_final.sort_values(by='Fecha_Modificacion_Archivo', ascending=True)
            
        # Último seguro contra duplicados por clave de negocio
        df_final = df_final.drop_duplicates(
            subset=["N° Abonado", "Fecha Contrato", "Vendedor", "Ciudad"], 
            keep='last'
        )
        
        # ELIMINAMOS LA COLUMNA FANTASMA (Hizo su trabajo y no sale a PBI)
        if 'Fecha_Modificacion_Archivo' in df_final.columns:
            df_final = df_final.drop(columns=['Fecha_Modificacion_Archivo'])
        
        # BLINDAJE FINAL CONTRA NULOS Y TYPEMISMATCH EN POWER BI
        df_final = limpiar_nulos_powerbi(df_final)
        
        guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=filas_antes)

if __name__ == "__main__":
    ejecutar()
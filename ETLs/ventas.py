import pandas as pd
import numpy as np
import os
from config import PATHS, LISTA_VENDEDORES_OFICINA, LISTA_VENDEDORES_PROPIOS, MAPA_MESES
# Agregamos la nueva función maestra a los imports
from utils import leer_carpeta, guardar_parquet, reportar_tiempo, console, ingesta_inteligente

# --- LÓGICA DE CLASIFICACIÓN (Intacta) ---
def clasificar_canal(row):
    """
    Determina el canal de venta basado en prioridades y listas blancas.
    """
    vendedor = str(row.get("Vendedor", "")).lower()
    nombre_detectado = str(row.get("nombre_detectado", ""))
    tipo_coincidencia = str(row.get("tipo_coincidencia", ""))
    
    # PRIORIDAD 1: CALL CENTER
    if "televentas" in vendedor or "call center" in vendedor:
        return "CALL CENTER"
    
    # PRIORIDAD 2: OFICINA COMERCIAL
    condicion_oficina_detectada = (
        nombre_detectado != "nan" and 
        nombre_detectado != "" and 
        tipo_coincidencia != "Pendiente de Revisión" and 
        tipo_coincidencia != "No detectado" and 
        "administrador" not in vendedor
    )
    
    if condicion_oficina_detectada or (vendedor in LISTA_VENDEDORES_OFICINA):
        return "OFICINA COMERCIAL"
    
    # PRIORIDAD 3: VENDEDORES PROPIOS
    condicion_calle = ("ventas" in vendedor and "televentas" not in vendedor)
    
    if condicion_calle or (vendedor in LISTA_VENDEDORES_PROPIOS):
        return "VENDEDORES PROPIOS"
    
    # RESTO
    return "ALIADOS"

@reportar_tiempo
def ejecutar():
    console.rule("[bold magenta]9. ETL: VENTAS (INCREMENTAL INTELIGENTE)[/]")
    
    # 1. Configuración de Rutas
    RUTA_RAW = PATHS["ventas_abonados"]
    NOMBRE_GOLD = "Ventas_Listado_Gold.parquet"
    # Construimos la ruta completa al Parquet para que la función la encuentre
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)

    # Columnas que esperamos del Raw
    cols_esperadas = [
        "ID", "N° Abonado", "Fecha Contrato", "Estatus", "Suscripción", 
        "Grupo Afinidad", "Nombre Franquicia", "Ciudad", "Vendedor", 
        "Serv/Paquete", "nombre_detectado", "Estado", "oficina_comercial", 
        "tipo_coincidencia", "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"
    ]

    # -------------------------------------------------------------------------
    # 2. INGESTA INTELIGENTE
    # -------------------------------------------------------------------------
    # Aquí le decimos: "Usa 'Fecha Contrato' para saber qué es nuevo"
    df_nuevo, df_historico = ingesta_inteligente(
        ruta_raw=RUTA_RAW, 
        ruta_gold=RUTA_GOLD_COMPLETA, 
        col_fecha_corte="Fecha Contrato"
    )
    
    # Si no hay nada nuevo y ya tenemos historia, terminamos aquí.
    if df_nuevo.empty and not df_historico.empty:
        console.print("[bold green]✅ No hay ventas nuevas. El proceso terminó temprano.[/]")
        return

    # -------------------------------------------------------------------------
    # 3. TRANSFORMACIÓN (SOLO A LO NUEVO)
    # -------------------------------------------------------------------------
    if not df_nuevo.empty:
        console.print(f"[cyan]🛠️ Transformando {len(df_nuevo)} registros nuevos...[/]")
        
        # 1. Asegurar columnas base
        df_nuevo = df_nuevo.reindex(columns=cols_esperadas)
        
        # 2. Renombres
        df_nuevo = df_nuevo.rename(columns={"oficina_comercial": "Oficina"})
        df_nuevo["Tipo de afluencia"] = "VENTAS"
        
        # 3. CORRECCIÓN DE FECHAS (Híbrida)
        # Convertimos 'Fecha Contrato' usando lógica Excel serial + Texto
        ser_numerica = pd.to_numeric(df_nuevo["Fecha Contrato"], errors='coerce')
        mask_es_serial = ser_numerica.notna() & (ser_numerica > 35000)
        
        fechas_finales = pd.Series(pd.NaT, index=df_nuevo.index)
        
        # A) Seriales numéricos
        if mask_es_serial.any():
            fechas_finales[mask_es_serial] = pd.to_datetime(ser_numerica[mask_es_serial], unit='D', origin='1899-12-30')
        
        # B) Texto normal
        if (~mask_es_serial).any():
            fechas_finales[~mask_es_serial] = pd.to_datetime(df_nuevo.loc[~mask_es_serial, "Fecha Contrato"], dayfirst=True, errors='coerce')
            
        df_nuevo["Fecha Contrato"] = fechas_finales
        df_nuevo["Mes"] = df_nuevo["Fecha Contrato"].dt.month.map(MAPA_MESES).str.capitalize()

        # 4. LIMPIEZA DE TEXTO
        # Convertimos todo a string limpio
        columnas_texto = ["Vendedor", "Ciudad", "nombre_detectado", "Oficina", "N° Abonado"]
        for col in columnas_texto:
            if col in df_nuevo.columns:
                df_nuevo[col] = df_nuevo[col].fillna("").astype(str).str.strip()
                if col != "Vendedor": 
                     df_nuevo[col] = df_nuevo[col].str.upper()
                else:
                     df_nuevo[col] = df_nuevo[col].str.lower()

        # 5. REGLAS BEJUMA (Forzado)
        tiene_bejuma = df_nuevo["Vendedor"].str.contains("bejuma", case=False, na=False)
        tiene_oficina = df_nuevo["Vendedor"].str.contains("ofic", case=False, na=False)
        mask_bejuma = tiene_bejuma & tiene_oficina
        
        if mask_bejuma.any():
            df_nuevo.loc[mask_bejuma, "Oficina"] = "BEJUMA"
            df_nuevo.loc[mask_bejuma, "tipo_coincidencia"] = "Oficina Detectada"
            df_nuevo.loc[mask_bejuma, "nombre_detectado"] = "OFICINA BEJUMA"

        # 6. CLASIFICACIÓN DE CANAL
        df_nuevo["Canal"] = df_nuevo.apply(clasificar_canal, axis=1)

        # 7. LIMPIEZA FINAL
        cols_a_borrar = ["Estado", "Cliente", "ID", "Serv/Paquete", 
                         "fuzzy_score_nombre", "fuzzy_score_apellido", "fuzzy_score_combinado"]
        df_nuevo = df_nuevo.drop(columns=cols_a_borrar, errors="ignore")
        
        # Filtro de calidad
        df_nuevo = df_nuevo[df_nuevo['tipo_coincidencia'] != 'Pendiente de Revisión']

    # -------------------------------------------------------------------------
    # 4. UNIFICACIÓN Y GUARDADO
    # -------------------------------------------------------------------------
    df_final = pd.DataFrame()

    # Unimos Histórico + Nuevo
    if not df_historico.empty and not df_nuevo.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
    elif not df_nuevo.empty:
        df_final = df_nuevo
    else:
        df_final = df_historico

    if not df_final.empty:
        filas_antes = len(df_final)
        
        # Deduplicación: Usamos 'keep=last' para que la versión nueva (quizás corregida) prevalezca
        df_final = df_final.drop_duplicates(
            subset=["N° Abonado", "Fecha Contrato", "Vendedor", "Ciudad"], 
            keep='last'
        )
        
        # Guardamos usando tu función de utils que maneja bloqueos de archivo
        guardar_parquet(
            df_final, 
            NOMBRE_GOLD,
            filas_iniciales=filas_antes
        )

if __name__ == "__main__":
    ejecutar()
import pandas as pd
import numpy as np
import os
import sys

# --- EL TRUCO DEL ASCENSOR ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console, leer_carpeta, standard_hours, archivos_raw

@reportar_tiempo
def ejecutar():
    console.rule("[bold blue]PIPELINE OPERATIVOS: COBRANZA")

    # 1. Rutas
    RUTA_RAW = PATHS["raw_cobranza"]
    NOMBRE_GOLD = "Llamadas_Cobranza_Gold.parquet"
    RUTA_GOLD_COMPLETA = os.path.join(PATHS.get("gold", "data/gold"), NOMBRE_GOLD)
    RUTA_BRONZE = os.path.join(PATHS.get("bronze", "data/bronze"), "Cobranza_Raw_Bronze.parquet")
    # ARCHIVOS BRONZE
    try:
        archivos_raw(RUTA_RAW, RUTA_BRONZE)
    except Exception as e:
        console.print(f"[yellow]⚠️ La capa Bronze no se actualizó, pero el ETL continuará. Error: {e}[/]")
    # -------------------------------------------------------------------------
    # 2. OBTENER FECHA DE CORTE Y PREPARAR HISTÓRICO
    # -------------------------------------------------------------------------
    fecha_corte = pd.Timestamp('1900-01-01')
    df_historico = pd.DataFrame()

    if os.path.exists(RUTA_GOLD_COMPLETA):
        try:
            df_historico = pd.read_parquet(RUTA_GOLD_COMPLETA)
            
            if not df_historico.empty and "Fecha Llamada" in df_historico.columns:
                # --- PASO CRÍTICO 1: CONVERSIÓN CORRECTA DEL HISTÓRICO ---
                # Usamos dayfirst=True para asegurar que si hay strings, se lean bien (DD/MM)
                df_historico["Fecha Llamada"] = pd.to_datetime(
                    df_historico["Fecha Llamada"], 
                    dayfirst=True, 
                    errors='coerce'
                )
                
                # --- 🚑 BLOQUE DE AUTO-REPARACIÓN (EL FIX) 🚑 ---
                # Detectamos fechas imposibles (mayores a mañana)
                # Esto elimina el error de "Diciembre" que bloquea tu carga
                limite_futuro = pd.Timestamp.now() + pd.Timedelta(days=1)
                mask_errores = df_historico["Fecha Llamada"] > limite_futuro
                
                if mask_errores.any():
                    cant_errores = mask_errores.sum()
                    console.print(f"[bold red]⚠️ ALERTA: Se detectaron {cant_errores} registros con fecha futura (Error DD/MM invertido).[/]")
                    console.print("[red]🧹 Eliminando registros corruptos para permitir la carga de hoy...[/]")
                    
                    # Filtramos el histórico: Solo nos quedamos con fechas válidas
                    df_historico = df_historico[~mask_errores]
                # --------------------------------------------------

                # Ahora sí calculamos el corte real
                if not df_historico.empty:
                    fecha_corte = df_historico["Fecha Llamada"].max()
                    console.print(f"[green]✅ Histórico validado. Última fecha cargada REAL: {fecha_corte}[/]")
                else:
                    fecha_corte = pd.Timestamp('1900-01-01')

            else:
                console.print("[yellow]⚠️ Histórico vacío o sin columna de fecha.[/]")
                
        except Exception as e:
            console.print(f"[yellow]⚠️ Error leyendo histórico: {e}. Se hará carga completa.[/]")
            df_historico = pd.DataFrame() 

    # ---------------------------------------------------------
    # 3. LECTURA MASIVA
    # ---------------------------------------------------------
    console.print("[cyan]📂 Escaneando contenido de archivos Raw...[/]")
    
    # Definimos columnas explícitas para evitar warnings
    cols_input = [
        "N° Abonado", "Documento", "Cliente", "Estatus", "Saldo", 
        "Fecha Llamada", "Hora Llamada", "Tipo Respuesta", 
        "Detalle Respuesta", "Responsable", "Franquicia", "Ciudad"
    ]
    
    df_nuevo = leer_carpeta(RUTA_RAW, filtro_exclusion="Consolidado")

    if df_nuevo.empty:
        console.print("[red]❌ No hay datos en la carpeta Raw.[/]")
        return

    # ---------------------------------------------------------
    # 4. FILTRADO INCREMENTAL
    # ---------------------------------------------------------
    cols_criticas = ["N° Abonado", "Documento", "Ciudad", "Responsable", "Fecha Llamada"]
    for col in cols_criticas:
        if col not in df_nuevo.columns:
            df_nuevo[col] = np.nan

    # --- PASO CRÍTICO 2: CONVERSIÓN DE LO NUEVO ---
    # Aquí forzamos dayfirst=True para que '12-02' sea 12 de Febrero, NO Diciembre
    df_nuevo["Fecha_Temp"] = pd.to_datetime(
        df_nuevo["Fecha Llamada"], 
        dayfirst=True, 
        errors='coerce'
    )

    # Filtro Incremental
    filas_totales = len(df_nuevo)
    df_nuevo = df_nuevo[df_nuevo["Fecha_Temp"] > fecha_corte].copy()
    
    # Limpieza columna temporal
    df_nuevo = df_nuevo.drop(columns=["Fecha_Temp"])
    
    filas_nuevas = len(df_nuevo)
    
    if filas_nuevas == 0:
        console.print(f"[bold green]✅ Cobranza al día. Se leyeron {filas_totales} filas pero ninguna es nueva.[/]")
        # NOTA: Si se limpió el histórico arriba, igual guardamos para "curar" el archivo Parquet
        if 'mask_errores' in locals() and mask_errores.any():
             console.print("[yellow]💾 Guardando corrección del histórico aunque no haya data nueva...[/]")
             # Pasa al bloque de guardado
        else:
            return
        
    console.print(f"[cyan]✨ Se detectaron {filas_nuevas} registros nuevos reales.[/]")

    # ---------------------------------------------------------
    # 5. TRANSFORMACIÓN (SOLO SOBRE LO NUEVO)
    # ---------------------------------------------------------
    if filas_nuevas > 0:
        with console.status("[bold green]Procesando nuevos registros...[/]", spinner="dots"):
            
            # --- LIMPIEZA IDs ---
            df_nuevo["N° Abonado"] = df_nuevo["N° Abonado"].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            df_nuevo["Documento"] = df_nuevo["Documento"].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            df_nuevo["Documento"] = df_nuevo["Documento"].replace(['nan', 'None', ''], None)
            
            df_nuevo = df_nuevo[df_nuevo["N° Abonado"].notna() & (df_nuevo["N° Abonado"] != 'nan')]

            # --- FECHAS Y TEXTOS ---
            # Aseguramos dayfirst=True aquí también por seguridad
            df_nuevo["Fecha Llamada"] = pd.to_datetime(df_nuevo["Fecha Llamada"], dayfirst=True, errors="coerce")
            
            df_nuevo["Hora Llamada"] = df_nuevo["Hora Llamada"].fillna("").astype(str)
            df_nuevo["Ciudad"] = df_nuevo["Ciudad"].fillna("NO ESPECIFICADO").astype(str).str.upper()

            # --- LÓGICA DE CANAL ---
            resp = df_nuevo["Responsable"].fillna("").astype(str)
            condiciones = [
                resp.str.contains("CALL|PHONE", case=False, na=False),       
                resp.str.contains("OFI|ASESOR", case=False, na=False) 
            ]
            opciones = ["CALL CENTER", "OFICINA COMERCIAL"]
            df_nuevo["Canal"] = np.select(condiciones, opciones, default="ALIADOS")

            # --- FINALIZACIÓN ---
            df_nuevo = df_nuevo.rename(columns={"Hora Llamada": "Hora"})
            df_nuevo = df_nuevo.drop(columns=["Cliente"], errors="ignore")

            cols_dedupe = ["N° Abonado", "Documento", "Saldo", "Fecha Llamada", "Hora", "Tipo Respuesta"]
            cols_dedupe = [c for c in cols_dedupe if c in df_nuevo.columns]
            df_nuevo = df_nuevo.drop_duplicates(subset=cols_dedupe)

    # ---------------------------------------------------------
    # 6. UNIFICACIÓN Y GUARDADO
    # ---------------------------------------------------------
    df_final = pd.DataFrame()
    
    # Concatenamos Histórico (YA LIMPIO) + Nuevo
    if not df_historico.empty:
        # Alineamos columnas
        columnas_union = df_nuevo.columns.intersection(df_historico.columns) if not df_nuevo.empty else df_historico.columns
        
        lista_dfs = [df_historico]
        if not df_nuevo.empty:
            lista_dfs.append(df_nuevo)
            
        df_final = pd.concat(lista_dfs, ignore_index=True)
    else:
        df_final = df_nuevo

    if not df_final.empty:
        # Ordenar
        df_final = df_final.sort_values(by="Fecha Llamada", ascending=False)
        
        # Deduplicar final
        cols_final_dedupe = ["N° Abonado", "Documento", "Saldo", "Fecha Llamada", "Hora", "Tipo Respuesta"]
        cols_final_dedupe = [c for c in cols_final_dedupe if c in df_final.columns]
        
        df_final = df_final.drop_duplicates(subset=cols_final_dedupe, keep='first')
        df_final = limpiar_nulos_powerbi(df_final)
        df_final = standard_hours(df_final, 'Hora') # Estandarizamos la hora después de unir para evitar problemas de formato
    
        # Guardar (Sobrescribe el archivo corrupto con el limpio)
        guardar_parquet(
            df_final, 
            NOMBRE_GOLD, 
            filas_iniciales=len(df_nuevo) if not df_nuevo.empty else len(df_final)
        )
        console.print(f"[bold green]✅ Cobranza Gold actualizado y curado. Total filas: {len(df_final):,}[/]")

if __name__ == "__main__":
    ejecutar()
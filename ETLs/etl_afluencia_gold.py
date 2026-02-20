import pandas as pd
import os
import sys

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console

@reportar_tiempo
def ejecutar(ruta_silver=None): 
    console.rule("[bold yellow]5. ETL GOLD: NORMALIZACIÓN DE OFICINAS (INCREMENTAL)[/]")

    # 1. Configuración de Rutas
    if not ruta_silver:
        ruta_silver = os.path.join(PATHS["silver"], "Afluencia_Consolidada_Silver.parquet")

    if not os.path.exists(ruta_silver):
        console.print("[red]❌ No se encontró el archivo Silver origen (Afluencia_Consolidada).[/]")
        return

    NOMBRE_GOLD = "Afluencia_Gold.parquet"
    RUTA_GOLD = os.path.join(PATHS["gold"], NOMBRE_GOLD)

    # -------------------------------------------------------------------------
    # 2. LÓGICA INCREMENTAL (DETECTAR QUÉ ES NUEVO)
    # -------------------------------------------------------------------------
    fecha_corte = None
    df_historico = pd.DataFrame()

    if os.path.exists(RUTA_GOLD):
        try:
            # Leemos solo la columna fecha para ser rápidos
            df_fechas = pd.read_parquet(RUTA_GOLD, columns=["Fecha"])
            if not df_fechas.empty:
                fecha_corte = pd.to_datetime(df_fechas["Fecha"]).max()
                console.print(f"[green]✅ Última fecha cargada en Gold: {fecha_corte.date()}[/]")
            
            # Cargamos el histórico para unirlo al final
            df_historico = pd.read_parquet(RUTA_GOLD)
        except Exception as e:
            console.print(f"[yellow]⚠️ Error leyendo Gold existente ({e}). Se hará carga completa.[/]")

    # -------------------------------------------------------------------------
    # 3. LECTURA Y FILTRADO DEL SILVER
    # -------------------------------------------------------------------------
    try:
        df_silver = pd.read_parquet(ruta_silver)
        
        # Aseguramos formato fecha
        if "Fecha" in df_silver.columns:
            df_silver["Fecha"] = pd.to_datetime(df_silver["Fecha"], dayfirst=True, errors="coerce")
        
        if fecha_corte:
            # Filtramos solo lo que sea POSTERIOR a lo que ya tenemos
            mask_nuevo = df_silver["Fecha"] > fecha_corte
            df_nuevo = df_silver[mask_nuevo].copy()
            
            if df_nuevo.empty:
                console.print("[bold green]✅ El Gold está al día. No hay registros nuevos por normalizar.[/]")
                return # Salimos temprano
            
            console.print(f"[cyan]🚀 Procesando {len(df_nuevo)} registros nuevos...[/]")
        else:
            # Si no hay histórico, tomamos todo
            df_nuevo = df_silver.copy()
            console.print(f"[cyan]🚀 Iniciando carga completa ({len(df_nuevo)} registros)...[/]")

    except Exception as e:
        console.print(f"[red]❌ Error leyendo archivo Silver: {e}[/]")
        return

    # -------------------------------------------------------------------------
    # 4. NORMALIZACIÓN (SOLO A LO NUEVO)
    # -------------------------------------------------------------------------
    path_map = os.path.join(PATHS["silver"], "Dim Oficinas.csv")
    
    if os.path.exists(path_map) and not df_nuevo.empty:
        try:
            with console.status("[blue]Aplicando Dim Oficinas a nuevos registros...[/]"):
                # Carga del Diccionario
                df_map = pd.read_csv(path_map, sep=',', encoding='latin-1')
                
                # Blindaje de columnas y datos del mapa
                df_map.columns = df_map.columns.astype(str).str.strip()
                df_map['Input_Original'] = df_map['Input_Original'].astype(str).str.upper().str.strip()
                
                # Creación de diccionarios
                dict_nom = dict(zip(df_map['Input_Original'], df_map['Nombre_Normalizado_Final']))
                dict_est = dict(zip(df_map['Input_Original'], df_map['Estado'])) if 'Estado' in df_map.columns else {}
                dict_tip = dict(zip(df_map['Input_Original'], df_map['Tipo_Sede'])) if 'Tipo_Sede' in df_map.columns else {}
                
                # Normalización de la Data Nueva
                col_k = df_nuevo['Oficina'].fillna('').astype(str).str.upper().str.strip()
                
                # Mapeo
                df_nuevo['Oficina_Normalizada'] = col_k.map(dict_nom).fillna(df_nuevo['Oficina'])
                df_nuevo['Oficina'] = df_nuevo['Oficina_Normalizada']
                
                # Enriquecimiento
                df_nuevo['Estado_Sede'] = col_k.map(dict_est).fillna('Sin Asignar')
                df_nuevo['Tipo_Sede'] = col_k.map(dict_tip).fillna('Sin Asignar')
                
                df_nuevo.drop(columns=['Oficina_Normalizada'], inplace=True)
                
                # Auditoría (Solo mostramos las NUEVAS que faltan)
                sin_norm = df_nuevo[df_nuevo['Estado_Sede'] == 'Sin Asignar']['Oficina'].unique()
                sin_norm_reales = [x for x in sin_norm if str(x).upper() not in ['NONE', 'SIN ASIGNAR', 'NAN', '', 'NONE']]
                
                if sin_norm_reales:
                    console.print(f"[yellow]⚠ Atención: {len(sin_norm_reales)} oficinas NUEVAS no están en el CSV:[/]")
                    console.print(sin_norm_reales[:10])

        except Exception as e:
            console.print(f"[red]❌ Error aplicando Dim Oficinas: {e}[/]")
    else:
        if not df_nuevo.empty: 
            console.print("[yellow]⚠ No se encontró 'Dim Oficinas.csv'. Se saltó la normalización.[/]")

    # -------------------------------------------------------------------------
    # 5. UNIFICACIÓN Y GUARDADO
    # -------------------------------------------------------------------------
    df_nuevo = limpiar_nulos_powerbi(df_nuevo)
    
    df_final = pd.DataFrame()
    
    if not df_historico.empty:
        df_final = pd.concat([df_historico, df_nuevo], ignore_index=True)
        # Deduplicación de seguridad (Keep Last = Prioriza la data nueva si hubo corrección)
        subset_cols = [c for c in ["N° Abonado", "Fecha", "Hora", "Vendedor", "Oficina"] if c in df_final.columns]
        df_final = df_final.drop_duplicates(subset=subset_cols, keep='last')
    else:
        df_final = df_nuevo

    if not df_final.empty:
        guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=len(df_final))
    else:
        console.print("[yellow]⚠️ Resultado vacío (Revisar flujo anterior).[/]")

if __name__ == "__main__":
    ejecutar()
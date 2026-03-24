import pandas as pd
import os
import sys

# --- SETUP DE RUTAS (TRUCO DEL ASCENSOR) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console

@reportar_tiempo
def ejecutar(ruta_silver=None): 
    console.rule("[bold yellow]5. ETL GOLD: NORMALIZACIÓN DE OFICINAS (FULL REFRESH)[/]")

    # 1. Configuración de Rutas
    if not ruta_silver:
        ruta_silver = os.path.join(PATHS["silver"], "Afluencia_Consolidada_Silver.parquet")

    if not os.path.exists(ruta_silver):
        console.print("[red]❌ No se encontró el archivo Silver origen (Afluencia_Consolidada).[/]")
        return

    NOMBRE_GOLD = "Afluencia_Gold.parquet"
    RUTA_GOLD = os.path.join(PATHS["gold"], NOMBRE_GOLD)

    # -------------------------------------------------------------------------
    # 2. LECTURA ATÓMICA
    # -------------------------------------------------------------------------
    try:
        df_silver = pd.read_parquet(ruta_silver)
        console.print(f"[cyan]🚀 Iniciando normalización completa ({len(df_silver)} registros)...[/]")
    except Exception as e:
        console.print(f"[red]❌ Error leyendo archivo Silver: {e}[/]")
        return

    # -------------------------------------------------------------------------
    # 3. NORMALIZACIÓN (APLICADA AL UNIVERSO COMPLETO)
    # -------------------------------------------------------------------------
    path_map = os.path.join(PATHS["silver"], "Dim Oficinas.csv")
    
    if os.path.exists(path_map) and not df_silver.empty:
        try:
            with console.status("[blue]Aplicando Dim Oficinas al universo completo...[/]"):
                # Carga del Diccionario
                df_map = pd.read_csv(path_map, sep=',', encoding='latin-1')
                
                # Blindaje de columnas y datos del mapa
                df_map.columns = df_map.columns.astype(str).str.strip()
                df_map['Input_Original'] = df_map['Input_Original'].astype(str).str.upper().str.strip()
                
                # Creación de diccionarios
                dict_nom = dict(zip(df_map['Input_Original'], df_map['Nombre_Normalizado_Final']))
                dict_est = dict(zip(df_map['Input_Original'], df_map['Estado'])) if 'Estado' in df_map.columns else {}
                dict_tip = dict(zip(df_map['Input_Original'], df_map['Tipo_Sede'])) if 'Tipo_Sede' in df_map.columns else {}
                
                # Normalización de la Data
                col_k = df_silver['Oficina'].fillna('').astype(str).str.upper().str.strip()
                
                # Mapeo y Enriquecimiento
                df_silver['Oficina_Normalizada'] = col_k.map(dict_nom).fillna(df_silver['Oficina'])
                df_silver['Oficina'] = df_silver['Oficina_Normalizada']
                df_silver['Estado_Sede'] = col_k.map(dict_est).fillna('Sin Asignar')
                df_silver['Tipo_Sede'] = col_k.map(dict_tip).fillna('Sin Asignar')
                
                df_silver.drop(columns=['Oficina_Normalizada'], inplace=True)
                
                # Auditoría: Listar las oficinas faltantes (Fix aplicado aquí)
                sin_norm = df_silver[df_silver['Estado_Sede'] == 'Sin Asignar']['Oficina'].unique()
                sin_norm_reales = [x for x in sin_norm if str(x).upper() not in ['NONE', 'SIN ASIGNAR', 'NAN', '', 'NONE']]
                
                if sin_norm_reales:
                    console.print(f"[yellow]⚠ Atención: {len(sin_norm_reales)} oficinas en la historia no están en el CSV:[/]")
                    console.print(sin_norm_reales[:15]) # Muestra hasta las primeras 15 para no saturar la consola

        except Exception as e:
            console.print(f"[red]❌ Error aplicando Dim Oficinas: {e}[/]")
    else:
        if not df_silver.empty: 
            console.print("[yellow]⚠ No se encontró 'Dim Oficinas.csv'. Se saltó la normalización.[/]")

    # -------------------------------------------------------------------------
    # 4. LIMPIEZA Y GUARDADO FINAL
    # -------------------------------------------------------------------------
    df_final = limpiar_nulos_powerbi(df_silver)
    

    if not df_final.empty:
        guardar_parquet(df_final, NOMBRE_GOLD, filas_iniciales=len(df_final))
        console.print(f"[bold green]✅ Gold generado exitosamente: {len(df_final):,} registros listos para Power BI.[/]")
    else:
        console.print("[yellow]⚠️ Resultado vacío (Revisar flujo anterior).[/]")

if __name__ == "__main__":
    ejecutar()
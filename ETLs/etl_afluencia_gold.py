import pandas as pd
import os
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from config import PATHS
from utils import guardar_parquet, reportar_tiempo, limpiar_nulos_powerbi, console

@reportar_tiempo
def ejecutar(ruta_silver): 
    console.rule("[bold yellow]5. ETL GOLD: NORMALIZACIÓN DE OFICINAS[/]")

    if not ruta_silver or not os.path.exists(ruta_silver):
        console.print("[red]❌ No se recibió una ruta Silver válida.[/]")
        return

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(), console=console
    ) as progress:

        # --- TAREA 1: CARGA ---
        task1 = progress.add_task("[cyan]Cargando Silver Enriquecida...", total=1)
        df = pd.read_parquet(ruta_silver)
        progress.update(task1, advance=1)

        # --- TAREA 2: NORMALIZACIÓN CON DIMENSION ---
        task2 = progress.add_task("[blue]Aplicando Dim Oficinas...", total=1)
        
        path_map = os.path.join(PATHS["silver"], "Dim Oficinas.csv")
        
        if os.path.exists(path_map):
            try:
                # Intentamos leer con latin-1 (común en excels guardados como CSV)
                df_map = pd.read_csv(path_map, sep=',', encoding='latin-1')
                
                # --- AQUÍ ESTÁ EL BLINDAJE (SIN CAMBIAR LÓGICA) ---
                # 1. Limpiamos nombres de columnas (quita espacios en cabeceras)
                df_map.columns = df_map.columns.astype(str).str.strip()
                
                # 2. Estandarizamos la llave del CSV (Mayúsculas y sin espacios extra)
                # Esto arregla "RATTAN PLAZA " vs "RATTAN PLAZA"
                df_map['Input_Original'] = df_map['Input_Original'].astype(str).str.upper().str.strip()
                
                # Crear diccionarios
                dict_nom = dict(zip(df_map['Input_Original'], df_map['Nombre_Normalizado_Final']))
                dict_est = dict(zip(df_map['Input_Original'], df_map['Estado'])) if 'Estado' in df_map.columns else {}
                dict_tip = dict(zip(df_map['Input_Original'], df_map['Tipo_Sede'])) if 'Tipo_Sede' in df_map.columns else {}
                
                # Columna clave del Dataframe (Data Sucia)
                # Aplicamos la MISMA limpieza: Mayúsculas y strip
                col_k = df['Oficina'].fillna('').astype(str).str.upper().str.strip()
                
                # Mapeo: Busca la clave limpia en el diccionario limpio
                df['Oficina_Normalizada'] = col_k.map(dict_nom).fillna(df['Oficina'])
                
                # Sobrescribimos la columna Oficina (Tu requerimiento)
                df['Oficina'] = df['Oficina_Normalizada']
                
                # Enriquecimiento
                df['Estado_Sede'] = col_k.map(dict_est).fillna('Sin Asignar')
                df['Tipo_Sede'] = col_k.map(dict_tip).fillna('Sin Asignar')
                
                df.drop(columns=['Oficina_Normalizada'], inplace=True)
                console.print("[green]✔ Normalización aplicada exitosamente.[/]")
                
            except Exception as e:
                console.print(f"[red]❌ Error aplicando Dim Oficinas: {e}[/]")
        else:
            console.print("[yellow]⚠ No se encontró 'Dim Oficinas.csv'. Se mantienen nombres técnicos.[/]")

        progress.update(task2, advance=1)
        df = df.drop_duplicates()
        df = limpiar_nulos_powerbi(df)
        # --- GUARDADO ---
        guardar_parquet(df, "Afluencia_Gold.parquet", filas_iniciales=len(df))
        
        # Reporte final para auditoría
        sin_norm = df[df['Estado_Sede'] == 'Sin Asignar']['Oficina'].unique()
        # Filtramos 'None' y vacíos para no ensuciar el log
        sin_norm_reales = [x for x in sin_norm if str(x).upper() not in ['NONE', 'SIN ASIGNAR', 'NAN', '']]
        
        if len(sin_norm_reales) > 0:
             console.print(f"[yellow]⚠ Oficinas que quedaron sin normalizar (Faltan en el CSV):[/]")
             console.print(sin_norm_reales[:20]) # Muestra máximo 20 para no llenar la pantalla
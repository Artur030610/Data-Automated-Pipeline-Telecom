import pandas as pd
import os
from config import PATHS
from utils import guardar_parquet, console

def generar_dim_franquicias():
    console.rule("[bold cyan]GENERANDO DIMENSIÓN FRANQUICIAS[/]")
    
    ruta_gold = PATHS.get("gold")
    if not ruta_gold:
        return

    # 1. Leer Abonados
    try:
        path_abo = os.path.join(ruta_gold, "Stock_Abonados_Gold_Resumen.parquet")
        df_abo = pd.read_parquet(path_abo)
        lista_abo = df_abo["Franquicia"].unique().tolist()
        console.print(f"   ✅ Abonados: {len(lista_abo)} franquicias.")
    except Exception:
        lista_abo = []
        console.print("[yellow]⚠️ No se encontró Gold de Abonados.[/]")

    # 2. Leer Fallas (CORREGIDO EL NOMBRE DEL ARCHIVO)
    try:
        # El script maestro ahora genera "IDF_Gold.parquet", no el nombre largo anterior
        path_idf = os.path.join(ruta_gold, "IDF_Gold.parquet")
        df_idf = pd.read_parquet(path_idf)
        lista_idf = df_idf["Franquicia"].unique().tolist()
        console.print(f"   ✅ IDF: {len(lista_idf)} franquicias.")
    except Exception:
        lista_idf = []
        console.print(f"[yellow]⚠️ No se encontró Gold de IDF (Buscando: IDF_Gold.parquet).[/]")

    # 3. Unión
    universo = sorted(list(set(lista_abo + lista_idf)))
    universo = [f for f in universo if f and str(f).strip() != "" and f != "NO DEFINIDA"]

    if not universo:
        console.print("[red]❌ No se encontraron franquicias para crear dimensión.[/]")
        return

    # 4. Guardar
    df_dim = pd.DataFrame(universo, columns=["Franquicia"])
    guardar_parquet(df_dim, "Dim_Franquicias.parquet", filas_iniciales=len(df_dim), ruta_destino=ruta_gold)

def ejecutar():
    generar_dim_franquicias()

if __name__ == "__main__":
    ejecutar()
import pandas as pd
import numpy as np
from scipy.stats import normaltest
import os

usuario_path = os.environ['USERPROFILE']
ruta_parquet = os.path.join(usuario_path, "Documents", "A-DataStack", "01-Proyectos", "01-Data_PipelinesFibex", "02_Data_Lake", "silver_data", "Tickets_Silver_Master.parquet")
df = pd.read_parquet(ruta_parquet)

stat, p_valor = normaltest(df['tiempo_resolucion'])

if p_valor > 0.05:
    print("Los datos se comportan como una distribución normal.")
else:
    print("Los datos no son normales.")
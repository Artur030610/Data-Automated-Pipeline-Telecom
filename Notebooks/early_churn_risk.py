import pandas as pd
import numpy as np

# =========================================================================
# 1. CARGA DE DATOS Y PREPARACIÓN DE FECHAS
# =========================================================================
print("⏳ Cargando datos y formateando fechas...")
ruta = r'C:\Users\josperez\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\silver_data\Tickets_Silver_Master.parquet'
df = pd.read_parquet(ruta)

# Convertimos a datetime las columnas con hora exacta para cálculos precisos
df['Fecha Apertura'] = pd.to_datetime(df['Fecha Apertura'])
df['Fecha Cierre'] = pd.to_datetime(df['Fecha Cierre'])
df['Fecha Impresion'] = pd.to_datetime(df['Fecha Impresion'])
print(f"Total de filas brutas leídas: {len(df)}")
print(f"Total de Tickets únicos (N° Orden): {df['N° Orden'].nunique()}")
print(f"Total de tickets cerrados: {df['Fecha Cierre'].notna().sum()}")
# --- NUEVO: CREAR COLUMNA DE ORDENAMIENTO PARA LA QUINCENA ---
meses_map = {"ENE": "01", "FEB": "02", "MAR": "03", "ABR": "04", "MAY": "05", "JUN": "06", 
             "JUL": "07", "AGO": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DIC": "12"}

def crear_orden_quincena(q_str):
    try:
        partes = str(q_str).strip().split()
        if len(partes) >= 3:
            mes, anio, q = partes[0], partes[1], partes[2]
            return f"{anio}-{meses_map.get(mes, '00')}-{q}"
    except:
        pass
    return str(q_str)

df['Orden_Quincena'] = df['Quincena Evaluada'].apply(crear_orden_quincena)

# =========================================================================
# 2. ANÁLISIS 1: RECURRENCIA POR LA MISMA CAUSA (FCR PURO)
# =========================================================================
print("🔍 Calculando Delta de Tiempo y Supervivencia de Soluciones...")
# Ordenamos cronológicamente por contrato y por falla
df = df.sort_values(['N° Contrato', 'Detalle Orden', 'Fecha Apertura'])

# Traemos la fecha de apertura del SIGUIENTE ticket (mismo cliente, misma falla)
df['Siguiente_Apertura'] = df.groupby(['N° Contrato', 'Detalle Orden'])['Fecha Apertura'].shift(-1)

# Supervivencia = Fecha en que volvió a fallar - Fecha en que se "resolvió" la vez anterior
df['Supervivencia_Dias'] = (df['Siguiente_Apertura'] - df['Fecha Cierre']).dt.total_seconds() / 86400.0

# Evitamos que tickets solapados generen días negativos y dañen el promedio
df['Supervivencia_Dias'] = df['Supervivencia_Dias'].clip(lower=0)

# Agrupación base (El truco de las comas para ver el historial en una celda)
df_causas = df.groupby(['Orden_Quincena', 'Quincena Evaluada', 'N° Contrato', 'Detalle Orden']).agg(
    Repeticiones = ('N° Contrato', 'size'),
    Fechas_Atendidas = ('Fecha Apertura Date', lambda x: '; '.join(x.dropna().astype(str).unique())),
    Soluciones = ('Solucion Aplicada', lambda x: '; '.join(x.dropna().astype(str).unique())),
    Promedio_Supervivencia_Dias = ('Supervivencia_Dias', 'mean')
).reset_index()

df_causas['Promedio_Supervivencia_Dias'] = df_causas['Promedio_Supervivencia_Dias'].round(1)

# Aislamos los casos críticos de MISMA CAUSA (Los ~8,000 históricos)
df_recurrentes_detalle = df_causas[df_causas['Repeticiones'] > 1].copy()

# =========================================================================
# 3. EL PARETO ESTRATÉGICO (80/20) PARA LA MISMA CAUSA
# =========================================================================
print("📊 Generando Análisis de Pareto...")
ranking_fallas = df_recurrentes_detalle.groupby('Detalle Orden').agg(
    Casos_Afectados = ('N° Contrato', 'nunique'),
    Total_Tickets_Involucrados = ('Repeticiones', 'sum'), # Nombre claro
    Supervivencia_Promedio_Dias = ('Promedio_Supervivencia_Dias', 'mean')
).sort_values('Total_Tickets_Involucrados', ascending=False)

# Matemáticas del Pareto
total_reaperturas = ranking_fallas['Total_Tickets_Involucrados'].sum()
ranking_fallas['%_Del_Total'] = (ranking_fallas['Total_Tickets_Involucrados'] / total_reaperturas) * 100
ranking_fallas['%_Acumulado_Pareto'] = ranking_fallas['%_Del_Total'].cumsum()

# Limpieza visual de decimales
ranking_fallas['Supervivencia_Promedio_Dias'] = ranking_fallas['Supervivencia_Promedio_Dias'].round(1)
ranking_fallas['%_Del_Total'] = ranking_fallas['%_Del_Total'].round(2)
ranking_fallas['%_Acumulado_Pareto'] = ranking_fallas['%_Acumulado_Pareto'].round(2)

# Resumen Temporal (Evolución por Quincena para la misma causa)
ranking_quincena = df_recurrentes_detalle.groupby(['Orden_Quincena', 'Quincena Evaluada']).agg(
    Casos_Afectados = ('N° Contrato', 'nunique'),
    Total_Tickets_Involucrados = ('Repeticiones', 'sum'),
    Supervivencia_Promedio_Dias = ('Promedio_Supervivencia_Dias', 'mean')
).sort_index(level='Orden_Quincena', ascending=True)

ranking_quincena['Supervivencia_Promedio_Dias'] = ranking_quincena['Supervivencia_Promedio_Dias'].round(1)

# =========================================================================
# 4. ANÁLISIS 2: FRICCIÓN GENERAL (EL UNIVERSO DE LOS 38K)
# =========================================================================
print("🔄 Analizando el Universo de Fricción General (Todos los reclamos múltiples)...")
# Ordenamos solo por contrato y fecha para ver el viaje real del cliente
df_ordenado_cliente = df.sort_values(['N° Contrato', 'Fecha Apertura'])

# Agrupamos por Quincena y Contrato para sacar el Total Real por cliente
df_general = df_ordenado_cliente.groupby(['Orden_Quincena', 'Quincena Evaluada', 'N° Contrato']).agg(
    Total_Tickets = ('N° Contrato', 'size'),
    
    # NUEVO: Viaje de Soluciones (sin duplicados)
    Viaje_Soluciones = ('Solucion Aplicada', lambda x: ' ➔ '.join(x.dropna().astype(str).drop_duplicates())),
    
    Cantidad_Causas_Distintas = ('Detalle Orden', 'nunique'),
    Cantidad_Soluciones_Distintas = ('Solucion Aplicada', 'nunique')
).reset_index()

# EL UNIVERSO: Absolutamente todos los clientes con > 1 ticket (Tus 38,000)
df_friccion_total = df_general[df_general['Total_Tickets'] > 1].copy()

# Ranking general de combinaciones de SOLUCIONES
ranking_soluciones = df_friccion_total.groupby('Viaje_Soluciones').agg(
    Casos_Afectados = ('N° Contrato', 'nunique'),
    Total_Tickets_Involucrados = ('Total_Tickets', 'sum')
).sort_values('Casos_Afectados', ascending=False)


# =========================================================================
# 4.5 NUEVO: TABLA RESUMEN DE VIAJES POR ABONADO
# =========================================================================
print("📝 Creando tabla resumen de viajes por abonado...")
# Ordenamos por quincena para que el join del texto sea cronológico
df_viajes_abonado = df_friccion_total.sort_values('Orden_Quincena').groupby('N° Contrato').agg(
    Viaje_Soluciones_Completo = ('Viaje_Soluciones', ' || '.join),
    Total_Tickets_Periodo = ('Total_Tickets', 'sum'),
    Quincenas_Afectadas = ('Quincena Evaluada', lambda x: ', '.join(x.unique()))
).reset_index().sort_values('Total_Tickets_Periodo', ascending=False)

# =========================================================================
# 5. ANÁLISIS 3: REINCIDENCIA POR GRUPO DE TRABAJO (AUDITORÍA FCR)
# =========================================================================
print("👥 Calculando Tasa de Reincidencia por Grupo de Trabajo...")
# Si un ticket tiene 'Siguiente_Apertura', significa que falló el FCR
df['FCR_Fallido'] = df['Siguiente_Apertura'].notna()

resumen_grupos = df.groupby('Grupo Trabajo').agg(
    Total_Tickets_Atendidos = ('N° Orden', 'count'),
    Tickets_Reincidentes = ('FCR_Fallido', 'sum'),
    Dias_Prom_Reincidencia = ('Supervivencia_Dias', 'mean')
).reset_index()

# Calculamos la Tasa de Reincidencia %
resumen_grupos['Tasa_Reincidencia_%'] = (resumen_grupos['Tickets_Reincidentes'] / resumen_grupos['Total_Tickets_Atendidos']) * 100

# Limpieza y filtrado
resumen_grupos['Tasa_Reincidencia_%'] = resumen_grupos['Tasa_Reincidencia_%'].round(2)
resumen_grupos['Dias_Prom_Reincidencia'] = resumen_grupos['Dias_Prom_Reincidencia'].round(2)

# Excluimos grupos con muy pocos tickets para no sesgar los porcentajes (opcional, ajustado a 20)
resumen_grupos = resumen_grupos[resumen_grupos['Total_Tickets_Atendidos'] >= 20]

# Ordenamos por los que más generan retrabajo
resumen_grupos = resumen_grupos.sort_values(by='Tickets_Reincidentes', ascending=False)


# =========================================================================
# 6. SELECCIÓN DE COLUMNAS PARA AUDITORÍA (Detalle Total Liviano)
# =========================================================================
columnas_esenciales = [
    'Orden_Quincena', 'Quincena Evaluada', 'N° Contrato', 'N° Orden', 'Detalle Orden', 
    'Fecha Apertura', 'Fecha Impresion', 'Fecha Cierre', 
    'Solucion Aplicada', 'Duracion_Horas', 'SLA Resolucion Min', 
    'SLA Impresion Min', 'Cumplio_SLA', 'Franquicia', 'Grupo Trabajo'
]
df_reducido = df[columnas_esenciales].copy()

# =========================================================================
# 6.5 NUEVO: LÍNEA DE TIEMPO CRONOLÓGICA POR ABONADO (El Expediente)
# =========================================================================
print("📅 Generando historial cronológico detallado por abonado...")

# Identificamos a los abonados que sufrieron fricción (los que tienen > 1 ticket)
contratos_con_friccion = df_friccion_total['N° Contrato'].unique()

# Filtramos la base de datos original solo para estos abonados críticos
df_historial_cronologico = df[df['N° Contrato'].isin(contratos_con_friccion)].copy()

# Ordenamos jerárquicamente por Contrato y luego por Fecha
df_historial_cronologico = df_historial_cronologico.sort_values(
    by=['N° Contrato', 'Fecha Apertura'], 
    ascending=[True, True]
)

# Filtramos las columnas que importan para la auditoría (sin viajes)
columnas_expediente = columnas_esenciales
df_historial_cronologico = df_historial_cronologico[columnas_expediente]

# =========================================================================
# 7. EXPORTACIÓN A EXCEL (Multi-hojas)
# =========================================================================
print("💾 Guardando el reporte estratégico en Excel...")
nombre_archivo = r'C:\Users\josperez\Downloads\Analisis_Recurrencia_FCR_Fibex2.xlsx'

with pd.ExcelWriter(nombre_archivo, engine='xlsxwriter') as writer:
    # 1. El Pareto del Subconjunto Crítico (Misma Causa - Los 8k)
    ranking_fallas.to_excel(writer, sheet_name='Pareto_Misma_Causa', index=True)
    
    # 2. Ranking de Viajes de SOLUCIONES (Universo Total)
    ranking_soluciones.to_excel(writer, sheet_name='Ranking_Viaje_Soluciones', index=True)
    
    # 3. Evolución Temporal (Misma Causa)
    ranking_quincena.to_excel(writer, sheet_name='Resumen_por_Quincena', index=True)
    
    # 4. Auditoría por Grupo de Trabajo
    resumen_grupos.to_excel(writer, sheet_name='Auditoria_Grupo_Trabajo', index=False)
    
    # 5. Detalle de los contratos críticos (Misma Causa - Los 8k)
    df_recurrentes_detalle.to_excel(writer, sheet_name='Detalle_Contratos_Misma', index=False)
    
    # 6. Detalle del Universo de contratos múltiples (Los 38k)
    df_friccion_total.to_excel(writer, sheet_name='Detalle_Contratos_Todos', index=False)
    
    # 7. El "Expediente" cronológico exacto para auditoría visual (sin viajes)
    df_historial_cronologico.to_excel(writer, sheet_name='Historial_Cronologico', index=False)
    
    # 8. NUEVO: Resumen de Viajes por Abonado (Toda la historia)
    df_viajes_abonado.to_excel(writer, sheet_name='Resumen_Viajes_Abonado', index=False)
    # =========================================================================

    
    df_reducido.to_excel(writer, sheet_name='Detalle_Total', index=False)

print(f"✅ ¡Éxito! Reporte corporativo generado en: {nombre_archivo}")
print("🚀 Generando archivos Parquet para la Capa Gold (Power BI)...")

ruta_gold = r'C:\Users\josperez\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\gold_data'   

# Guardamos las tablas más importantes para el Dashboard
ranking_fallas.to_parquet(rf'{ruta_gold}\Gold_Pareto_Fallas.parquet', index=True)
ranking_quincena.to_parquet(rf'{ruta_gold}\Gold_Evolucion_Quincena.parquet', index=True)
resumen_grupos.to_parquet(rf'{ruta_gold}\Gold_Auditoria_Grupos.parquet', index=False)
df_recurrentes_detalle.to_parquet(rf'{ruta_gold}\Gold_Detalle_Recurrentes.parquet', index=False)
df_viajes_abonado.to_parquet(rf'{ruta_gold}\Gold_Viajes_Abonado.parquet', index=False)

print("✅ Archivos Parquet generados con éxito.")
# 9. Data origen completa y optimizada
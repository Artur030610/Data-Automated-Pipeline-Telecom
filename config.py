import os
from rich.theme import Theme

# =============================================================================
# 1. CONFIGURACIÓN DE RUTAS BASE
# =============================================================================
# Detectamos la ruta del usuario actual para hacerlo portable
USUARIO = os.path.expanduser("~")

# Ruta raíz del Data Lake
RUTA_BASE = os.path.join(
    USUARIO, 
    "Documents", 
    "A-DataStack", 
    "01-Proyectos", 
    "01-Data_PipelinesFibex", 
    "02_Data_Lake"
)

# =============================================================================
# 2. DICCIONARIO MAESTRO DE RUTAS (PATHS)
# =============================================================================
PATHS = {
    # --- ESTRUCTURA GENERAL (MEDALLION ARCHITECTURE) ---
    "raw":    os.path.join(RUTA_BASE, "raw_data"),
    "silver": os.path.join(RUTA_BASE, "silver_data"), # <--- Agregado
    "gold":   os.path.join(RUTA_BASE, "gold_data"),

    # --- RUTAS DE ENTRADA ESPECÍFICAS (RAW DATA) ---
    "raw_recaudacion":      os.path.join(RUTA_BASE, "raw_data", "1-Recaudación"),
    "raw_ventas_root":      os.path.join(RUTA_BASE, "raw_data", "2-Ventas"),
    "raw_reclamos":         os.path.join(RUTA_BASE, "raw_data", "3-Reclamos"),
    "raw_atencion":         os.path.join(RUTA_BASE, "raw_data", "4-Atencion al cliente"),
    "raw_idf":              os.path.join(RUTA_BASE, "raw_data", "5-Indice de falla","1-IdF"),
    "raw_sla":              os.path.join(RUTA_BASE, "raw_data", "6-SLA"),
    "raw_cobranza":         os.path.join(RUTA_BASE, "raw_data", "7-Operativos Cobranza"),
    "raw_encuestas":        os.path.join(RUTA_BASE, "raw_data", "8-Encuestas de satisfacción"),
    "raw_asesores_univ_9":  os.path.join(RUTA_BASE, "raw_data", "9-Universo de asesores"),
    "raw_visualizaciones":  os.path.join(RUTA_BASE, "raw_data", "10-Visualizaciones"),
    "raw_act_datos":        os.path.join(RUTA_BASE, "raw_data", "11-Act. de Datos"),
    "raw_comeback":         os.path.join(RUTA_BASE, "raw_data", "12-Comebackhome"),
    "raw_puntos_azules":    os.path.join(RUTA_BASE, "raw_data", "13-Puntos azules"),
    
    # Nota: Si usas la carpeta 14
    "raw_asesores_univ_14": os.path.join(RUTA_BASE, "raw_data", "14-Universo de asesores"), 
    "raw_estados":          os.path.join(RUTA_BASE, "raw_data", "15-Estados y Ciudades"),
    "raw_hist_abonados":    os.path.join(RUTA_BASE, "raw_data", "16-Historico de abonados", "1-Historico"),
    "raw_estad_abonados":   os.path.join(RUTA_BASE, "raw_data", "16-Historico de abonados", "2-Estadisticas"),
    "raw_empleados":        os.path.join(RUTA_BASE, "raw_data", "17-Empleados"),
    "raw_referidos":        os.path.join(RUTA_BASE, "raw_data", "18-Referidos"),
    "raw_horaspago":        os.path.join(RUTA_BASE, "raw_data", "1-Recaudación", "1-Horas de pago"),

    # --- SUB-RUTAS ESPECÍFICAS (VENTAS) ---
    "ventas_estatus":   os.path.join(RUTA_BASE, "raw_data", "2-Ventas", "1- Ventas Estatus"),
    "ventas_lis":       os.path.join(RUTA_BASE, "raw_data", "2-Ventas", "2- Ventas LIS"),
    "ventas_abonados":  os.path.join(RUTA_BASE, "raw_data", "2-Ventas", "3- Ventas Listado de abonados"),
    "ventas_archivado": os.path.join(RUTA_BASE, "raw_data", "2-Ventas", "4-Archivado"),
}

# =============================================================================
# 3. CONSTANTES Y FILTROS ESPECÍFICOS
# =============================================================================

# Listas de carpetas internas para iterar en Reclamos
FOLDERS_RECLAMOS_GENERAL = [
    "1-Data-Reclamos por CC", 
    "2-Data-Reclamos por OOCC", 
    "3-Data-Reclamos por RRSS"
]

SUB_RECLAMOS_APP = "4-Data-Reclamos por APP"
SUB_RECLAMOS_BANCO = "5-Data-Reclamos OB"

FOLDERS_ACT_DATOS = [
    "1-CALL CENTER",
    "2-OOCC",
    "3-OBSERVACIONES"
]

# Filtros de exclusión comunes (archivos temporales de Excel)
FILTROS_EXCLUSION_GLOBAL = ["~$", "Consolidado", "Resumen"]

# Mapa de Meses Global
MAPA_MESES = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

# =============================================================================
# 4. CONFIGURACIÓN VISUAL (RICH)
# =============================================================================
THEME_COLOR = Theme({
    "success": "bold green",
    "error": "bold red",
    "warning": "yellow",
    "info": "cyan",
    "title": "bold white on blue"
})

# =============================================================================
# 5. LISTAS DE CLASIFICACIÓN (Para Ventas Listado)
# =============================================================================

LISTA_VENDEDORES_OFICINA = [
    "angelica angulo ofic aragua",
    "marianyeli acosta rodriguez atc ofic turmero aragua",
    "oficina bejuma",
    "gisel haideen becerra gimenez"
]

LISTA_VENDEDORES_PROPIOS = [
    "carlos alberto pereira",
    "carlos javier perez cribas",
    "maria alejandra marquez rivas"
]
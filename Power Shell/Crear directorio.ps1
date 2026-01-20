# Usamos la codificación nativa del sistema (ANSI/Windows-1252)
# Esto suele arreglar los acentos cuando UTF-8 falla
[Console]::OutputEncoding = [System.Text.Encoding]::Default
[System.Console]::InputEncoding = [System.Text.Encoding]::Default

# 1. Ruta Base Universal
$base = "$env:USERPROFILE\Documents\A-DataStack\01-Proyectos\01-Data_PipelinesFibex\02_Data_Lake\raw_data"

# 2. Lista Maestra (Escrita normal, con acentos)
$carpetas = @(
    "1-Recaudación",
    "2-Ventas",
    "2-Ventas\1- Ventas Estatus",
    "2-Ventas\2- Ventas LIS",
    "2-Ventas\3- Ventas Listado de abonados",
    "2-Ventas\4-Archivado",
    "3-Reclamos",
    "3-Reclamos\1-Data-Reclamos por CC",
    "3-Reclamos\2-Data-Reclamos por OOCC",
    "3-Reclamos\3-Data-Reclamos por RRSS",
    "3-Reclamos\4-Data-Reclamos por APP",
    "3-Reclamos\5-Data-Reclamos OB",
    "4-Atencion al cliente",
    "5-Indice de falla",
    "5-Indice de falla\1-IdF",
    "5-Indice de falla\2-Abonados",
    "6-SLA",
    "7-Operativos Cobranza",
    "8-Encuestas de satisfacción",
    "9-Universo de asesores",
    "10-Visualizaciones",
    "11-Act. de Datos",
    "11-Act. de Datos\1-CALL CENTER",
    "11-Act. de Datos\2-OOCC",
    "11-Act. de Datos\3-OBSERVACIONES",
    "12-Comebackhome",
    "13-Puntos azules",
    "14-Universo Asesores",
    "15-Estados y Ciudades",
    "16-Historico de abonados",
    "16-Historico de abonados\1-Historico",
    "16-Historico de abonados\2-Estadisticas",
    "16-Historico de abonados\3-Clientes",
    "17-Empleados",
    "18-Referidos"
)

# 3. Ejecución
foreach ($c in $carpetas) {
    $fullPath = Join-Path -Path $base -ChildPath $c
    New-Item -ItemType Directory -Force -Path $fullPath | Out-Null
}
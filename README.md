# Fibex Telecom - Pipeline de Datos & Analítica

🇬🇧 *[Read this in English](README_en.md)*

**Author:** Jose Arturo Pérez

## 📌 Resumen del Proyecto
Este proyecto implementa un **pipeline ETL** automatizado para procesar, limpiar y consolidar datos operativos para el **Data Lake de Fibex**. Centraliza dominios críticos del negocio, incluyendo:
* 💰 **Financiero:** Recaudación y Horas de Pago.
* 🛠️ **Técnico:** Índice de Falla (IdF) y Reclamos.
* 📈 **Comercial:** Ventas, Estatus y Afluencia de Oficinas.

Reemplaza los flujos de trabajo manuales en Excel por una arquitectura robusta basada en Python, reduciendo el tiempo de procesamiento (gracias a la ejecución de memoria Out-of-Core) y permitiendo actualizaciones diarias para los tableros de Power BI.

## 🛠 Stack Tecnológico
* **Lenguaje:** Python 3.12
* **Automatización RPA:** `playwright` (Extracción de datos web sin intervención humana).
* **Procesamiento y Transformación:** `polars` (Ejecución ultrarrápida Out-of-Core), `pandas` y `python-calamine` (Lectura de Excel basada en Rust).
* **Almacenamiento / Consultas SQL:** DuckDB (Data Warehouse Local) y archivos Parquet.
* **Visualización:** Power BI (Conectores nativos de Parquet).
* **Control de Versiones:** Git

## ⚙️ Instalación y Configuración
Para desplegar este proyecto en un nuevo equipo, sigue estos pasos:
1. Clona el repositorio y crea un entorno virtual (`python -m venv venv`).
2. Instala las dependencias: `pip install -r requirements.txt`.
3. **CRÍTICO - Instalar Navegadores RPA:** Ejecuta `playwright install chromium` para que los robots puedan navegar por el SAE Plus.
4. **Credenciales y Reglas:** Revisa el archivo `config.py` para asegurar que las rutas del Data Lake (`RUTA_BASE`) apunten a la unidad correcta en el nuevo servidor.

## 🚀 Cómo Ejecutar (Orquestación)
Toda la extracción está centralizada. Ejecuta `python extraccion/main.py`. Aparecerá un menú interactivo en la consola (construido con `rich`). Puedes seleccionar ejecutar toda la suite de robots o módulos individuales. Para ejecución desatendida (ej. Programador de Tareas de Windows a las 3 AM), utiliza el flag: `python main.py --auto`.

## 📂 Estructura del Proyecto
* `extraccion/`: Scripts de automatización RPA (Web Scraping con Playwright) y su orquestador `main.py` para descargar reportes del SAE Plus.
* `transformacion/ETLs/`: Scripts de limpieza, consolidación y modelado de datos utilizando Polars, Pandas y DuckDB.
* `main.py`: Orquestador maestro de transformaciones. Controla el flujo de ejecución (Raw -> Bronze -> Silver -> Gold).
* `utils.py` y `extraccion/scraper_utils.py`: Funciones auxiliares modulares (limpieza, ingesta incremental, selectores web, etc.).
* `config.py` y `reglas_negocio.json`: Variables de configuración, rutas dinámicas y reglas de negocio configurables sin tocar código Python.

## 🚀 Características Clave y Lógica

### 1. Lectura de Excel Optimizada
Utiliza el motor **Calamine** para leer archivos Excel pesados, reduciendo los tiempos de carga de minutos a segundos en comparación con la librería estándar `openpyxl`.

### 2. Análisis Inteligente de Fechas
El pipeline extrae automáticamente metadatos de fechas (ej. "ENE Q1") desde los nombres de archivo para aplicar filtros lógicos de negocio específicos (como en el *Índice de Falla*).

### 3. Compatibilidad Nativa con Power BI
Todos los pipelines incluyen una capa de limpieza (`utils.limpiar_nulos_powerbi`) que convierte textos como `NaN`, `NaT` y `"nan"` en objetos `None` reales, asegurando que Power BI los interprete correctamente como `(Blank)` o vacío.

### 4. Arquitectura Medallion e Ingesta Incremental
El pipeline sigue fielmente el patrón de **Arquitectura Medallion (Raw -> Bronze -> Silver -> Gold)**. Mediante Polars y DuckDB, se realiza una ingesta incremental procesando solo archivos nuevos. La capa Bronze actúa como una copia inmutable de los datos crudos (RAW), garantizando la preservación del historial a largo plazo y optimizando el cómputo de las transformaciones posteriores.

### 5. Dimensiones Cambiantes (SCD Tipo 2)
El modelo soporta *Slowly Changing Dimensions* para mantener un registro histórico de los cambios en las entidades. Actualmente implementado en el maestro de empleados (`trans_empleados.py`) para rastrear de forma automatizada los traslados de oficina o ascensos, y con las bases arquitectónicas listas para su próxima activación en la dimensión de clientes (`Dim_Cliente`).

## 🗺️ Roadmap y Próximos Pasos (Hacia la Nube)
Actualmente el Data Lake reside en un entorno local utilizando DuckDB y Parquet. Sin embargo, el proyecto está diseñado y preparado para evolucionar hacia un entorno Cloud-Native en su fase final de Carga (Load):
* **Particionamiento Avanzado:** Implementar particionado lógico de archivos `.parquet` (ej. por `año/mes` o `franquicia`) en la función de guardado para maximizar el rendimiento de lectura.
* **Cloud Storage (Buckets):** Migrar las estructuras de carpetas del Data Lake hacia servicios de almacenamiento de objetos (como AWS S3, Google Cloud Storage o Azure Blob Storage).
* **Centralización en un Data Warehouse:** Inyectar las tablas limpias y consolidadas de la capa Gold hacia un Cloud Data Warehouse corporativo (como Snowflake, BigQuery o Databricks) democratizando el acceso a los datos para toda la organización.

## 🔧 Mantenimiento y Troubleshooting
* **Cambios en el SAE Plus:** Si el portal de SAE cambia su interfaz gráfica (botones o menús), los scrapers fallarán por *Timeout*. Las actualizaciones deben realizarse en `extraccion/scraper_utils.py` (ahí viven todos los selectores CSS y lógica de navegación).
* **Reglas de Negocio:** No es necesario modificar el código Python para agregar nuevos "Vendedores", "Estatus" o "Exclusiones". Simplemente edita el archivo `reglas_negocio.json` y el pipeline adoptará las nuevas reglas en la siguiente ejecución.
* **Capa Gold (Power BI):** El equipo de BI debe conectarse directamente a la carpeta `gold_data` importando los archivos `.parquet`. El modelo de datos está diseñado en Estrella (Star Schema), utilizando IDs enteros subrogados (ej. `Cliente_SK`) para relaciones ultra-rápidas, descartando el uso de strings pesados.

## 🤝 Agradecimientos
Un agradecimiento especial al Sr. **Jonattan Sotillo**, quien fue mi jefe directo y me brindó la oportunidad, el espacio y la confianza para formar parte de este equipo y construir este proyecto. ¡Gracias por el apoyo constante, la visión y el buen humor!
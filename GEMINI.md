# Contexto del Proyecto: Migración de Extracción de Datos - Fibex

## Perfil del Senior User
- Ingeniero Industrial & MBA (Especialista en Data & Business Analytics).
- Foco: Optimización de procesos, Big Data y Data Governance.
- Estilo: Soluciones escalables, modulares y de alto rendimiento.

## Reglas de Oro (INNEGOCIABLES)
1. **Preservación de Identificadores:** No cambiar nombres de variables, diccionarios, listas, tuplas o estructuras de datos existentes. La lógica nueva debe inyectarse respetando el esquema actual.
2. **Eficiencia en el Procesamiento:** Priorizar el uso de **DuckDB** para persistencia y **Polars** para transformación masiva.
3. **Migración Estratégica:** Estamos moviendo la lógica de Power Automate a **Playwright (Python)** para ganar velocidad y estabilidad.

## Stack Tecnológico
- **Automatización:** Playwright (Async/Python).
- **Motores:** DuckDB + Polars.
- **Entorno:** VS Code + Agent Mode.

## Objetivo del Sprint
- Automatizar el flujo completo de ETL (Login -> Navegación -> Descarga -> Ubicacion en el repositorio -> Transformacion -> Carga) sin intervención manual.
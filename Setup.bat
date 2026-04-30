@echo off
title SETUP INICIAL - FIBEX DATA LAKE


echo ==================================================
echo   1. CREANDO ESTRUCTURA DE CARPETAS (DATA LAKE)
echo ==================================================
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0power Shell\Crear directorio.ps1"
echo [OK] Carpetas creadas con exito.
echo.
echo ==================================================
echo   2. INSTALANDO DEPENDENCIAS DE PYTHON
echo ==================================================
set "PY_JEFE=%USERPROFILE%\Documents\A-DataStack\01-Proyectos\00-Toolkits\WPy64-312101\python\python.exe"
set "PY_JOSE=%USERPROFILE%\Documents\A-DataStack\00-Toolkits\WinPython\WPy64-312101\python\python.exe"

set "BASE_PY="
if exist "%PY_JEFE%" set "BASE_PY=%PY_JEFE%"
if not defined BASE_PY if exist "%PY_JOSE%" set "BASE_PY=%PY_JOSE%"

if not defined BASE_PY (
    python --version >nul 2>&1
    if not errorlevel 1 (
        set "BASE_PY=python"
        echo [OK] Python global del sistema detectado.
    ) else (
        echo [!] ERROR: No encuentro Python Portable ni Python instalado en el sistema.
        echo Por favor, copia la carpeta "00-Toolkits" o instala Python 3.12 desde python.org ^(marcando "Add to PATH"^).
        pause
        exit /b
    )
) else (
    echo [OK] Entorno Python detectado.
)

echo ==================================================
echo   3. CREANDO ENTORNO VIRTUAL
echo ==================================================

if exist "%~dp0venv" (
    echo [INFO] Eliminando residuos del entorno virtual anterior...
    rmdir /s /q "%~dp0venv"
)

"%BASE_PY%" -m venv "%~dp0venv"

if not exist "%~dp0venv\Scripts\python.exe" (
    echo [!] ERROR CRITICO: Python no pudo crear la carpeta venv. Verifica permisos o antivirus.
    pause
    exit /b
)

set "FINAL_PY=%~dp0venv\Scripts\python.exe"
echo [OK] Entorno virtual creado en la carpeta venv.
echo.
"%FINAL_PY%" -m pip install --upgrade pip
"%FINAL_PY%" -m pip install -r "%~dp0requirements.txt"
"%FINAL_PY%" -m playwright install chromium
echo.
echo ==================================================
echo [OK] SETUP COMPLETADO. YA TODO ESTA LISTO PARA ARRANCAR.
echo Presione cualquier tecla para cerrar esta ventana...
pause >nul
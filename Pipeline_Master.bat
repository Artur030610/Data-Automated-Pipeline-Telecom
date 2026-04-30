@echo off
title PIPELINE MASTER - FIBEX (EXTRACCION + TRANSFORMACION)
echo ==================================================
echo   PASO 1: Verificando ubicacion de Python...
echo ==================================================

set "PY_JEFE=%USERPROFILE%\Documents\A-DataStack\01-Proyectos\00-Toolkits\WPy64-312101\python\python.exe"
set "PY_JOSE=%USERPROFILE%\Documents\A-DataStack\00-Toolkits\WinPython\WPy64-312101\python\python.exe"
set "PY_VENV=%~dp0venv\Scripts\python.exe"

:: APUNTAMOS A AMBOS MAINS
set "MAIN_EXTRACCION=%~dp0extraccion\main.py"
set "MAIN_TRANSFORMACION=%~dp0main.py"

if exist "%PY_VENV%" (
    set "FINAL_PY=%PY_VENV%"
    echo [OK] Entorno Virtual local detectado.
) else if exist "%PY_JEFE%" (
    set "FINAL_PY=%PY_JEFE%"
    echo [OK] Entorno de JEFE detectado.
) else if exist "%PY_JOSE%" (
    set "FINAL_PY=%PY_JOSE%"
    echo [OK] Entorno de JOSE detectado.
) else (
    python --version >nul 2>&1
    if %errorlevel% equ 0 (
        set "FINAL_PY=python"
        echo [OK] Python global del sistema detectado.
    ) else (
        echo [!] ERROR: No encuentro Python. Ejecuta Setup_Inicial.bat primero o instala Python.
        pause
        exit
    )
)

echo.
echo ==================================================
echo   PASO 2: Ejecutando Extraccion (Modo Auto)
echo ==================================================
:: cmd /c ejecuta y luego devuelve el control al .bat para seguir a la siguiente linea
cmd /c ""%FINAL_PY%" "%MAIN_EXTRACCION%" --auto"

echo.
echo ==================================================
echo   PASO 3: Ejecutando Transformacion (Modo Auto)
echo ==================================================
cmd /c ""%FINAL_PY%" "%MAIN_TRANSFORMACION%" --auto"

echo.
echo [OK] EL PIPELINE COMPLETO HA FINALIZADO.
pause
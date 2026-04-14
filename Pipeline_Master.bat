@echo off
title PIPELINE MASTER - FIBEX (EXTRACCION + TRANSFORMACION)
echo ==================================================
echo   PASO 1: Verificando ubicacion de Python...
echo ==================================================

set "PY_JEFE=%USERPROFILE%\Documents\A-DataStack\01-Proyectos\00-Toolkits\WPy64-312101\python\python.exe"
set "PY_JOSE=%USERPROFILE%\Documents\A-DataStack\00-Toolkits\WinPython\WPy64-312101\python\python.exe"

:: APUNTAMOS A AMBOS MAINS
set "MAIN_EXTRACCION=%~dp0extraccion\main.py"
set "MAIN_TRANSFORMACION=%~dp0main.py"

if exist "%PY_JEFE%" (
    set "FINAL_PY=%PY_JEFE%"
    echo [OK] Entorno de JEFE detectado.
) else if exist "%PY_JOSE%" (
    set "FINAL_PY=%PY_JOSE%"
    echo [OK] Entorno de JOSE detectado.
) else (
    echo [!] ERROR: No encuentro el archivo python.exe
    pause
    exit
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
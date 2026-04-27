@echo off
title EJECUTANDO ORQUESTADOR DE TRANSFORMACION - FIBEX
echo ==================================================
echo   PASO 1: Verificando ubicacion de Python...
echo ==================================================

:: Usamos comillas solo al final para evitar errores de duplicacion
set "PY_JEFE=%USERPROFILE%\Documents\A-DataStack\01-Proyectos\00-Toolkits\WPy64-312101\python\python.exe"
set "PY_JOSE=%USERPROFILE%\Documents\A-DataStack\00-Toolkits\WinPython\WPy64-312101\python\python.exe"
set "MAIN_FILE=%~dp0main.py"

:: --- DETECCION ---
if exist "%PY_JEFE%" (
    set "FINAL_PY=%PY_JEFE%"
    echo [OK] Entorno de JEFE detectado.
) else if exist "%PY_JOSE%" (
    set "FINAL_PY=%PY_JOSE%"
    echo [OK] Entorno de JOSE detectado.
) else (
    echo [!] ERROR: No encuentro el archivo python.exe
    echo Busque en: "%PY_JEFE%"
    echo.
    echo Por favor, verifica que la carpeta 00-Toolkits este ahi.
    pause
    exit
)

echo.
echo ==================================================
echo   PASO 2: Lanzando Orquestador
echo ==================================================
echo Python: "%FINAL_PY%"
echo Script: "%MAIN_FILE%"
echo.

:: REGLA DE ORO PARA CMD /K:
:: Necesita un juego de comillas EXTRAS envolviendo todo el comando 
:: para que no se rompa con los espacios de "Jonattan Sotillo"
cmd /k ""%FINAL_PY%" "%MAIN_FILE%""

:: Si por algun motivo cmd /k fallara, este pause te salvara la vida
pause
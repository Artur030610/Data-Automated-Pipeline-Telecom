@echo off
setlocal
title Orquestador Fibex - Data Stack

:: 1. Carpeta donde está el .bat (02-Scripts)
set SCRIPTS_DIR=%~dp0

:: 2. Salto para llegar al WinPython
:: Subimos 2 niveles (..\..\) para llegar a 'A-DataStack'
set PY_EXE="%SCRIPTS_DIR%..\..\00-Toolkits\WinPython\WPy64-312101\python\python.exe"
set MAIN="%SCRIPTS_DIR%main.py"

echo ==================================================
echo    CARGANDO ENTORNO DESDE TOOLKITS
echo ==================================================
echo.

:: Verificación de seguridad antes de arrancar
if not exist %PY_EXE% (
    echo [!] ERROR: No encuentro el Python en:
    echo %PY_EXE%
    echo.
    echo Revisa si moviste la carpeta WinPython de sitio.
    pause
    exit
)

:: Ejecución
%PY_EXE% %MAIN%

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [!] El script se detuvo con errores.
    pause
) else (
    echo.
    echo [OK] Proceso finalizado.
    timeout /t 5
)

endlocal
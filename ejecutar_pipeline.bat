
@echo off

cd /d "%~dp0"

if not exist "logs" mkdir "logs"

echo ===== INICIO: %date% %time% ===== >> logs\pipeline.log

"C:\Users\Kevin\AppData\Local\Python\pythoncore-3.14-64\python.exe" ejecutar_pipeline.py >> logs\pipeline.log 2>&1

set RESULTADO=%ERRORLEVEL%

echo ===== FIN: %date% %time% - Codigo: %RESULTADO% ===== >> logs\pipeline.log

exit /b %RESULTADO%

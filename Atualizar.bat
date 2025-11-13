@echo off
REM Arquivo de lote para executar o pipeline Python com um único comando.

echo.
echo =========================================
echo INICIANDO O PIPELINE DE DADOS CNPJ...
echo =========================================
echo.

REM Chama o interpretador Python para executar o script principal
python run_pipeline.py

echo.
echo =========================================
echo EXECUÇÃO FINALIZADA.
echo =========================================
echo.

pause

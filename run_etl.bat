@echo off
REM Script batch simplificado para executar o ETL dos KPIs

REM Definindo variáveis padrão
set FARMER_ID=
set MONTHS_BACK=11
set LOG_LEVEL=INFO
set KPI=all

REM Processando argumentos
:arg_loop
if "%1"=="" goto execute_etl

if "%1"=="-f" (
    set FARMER_ID=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="--farmer-id" (
    set FARMER_ID=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="-m" (
    set MONTHS_BACK=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="--months-back" (
    set MONTHS_BACK=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="-l" (
    set LOG_LEVEL=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="--log-level" (
    set LOG_LEVEL=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="-k" (
    set KPI=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="--kpi" (
    set KPI=%2
    shift
    shift
    goto arg_loop
)
if "%1"=="-h" (
    goto show_help
)
if "%1"=="--help" (
    goto show_help
)

echo Opção desconhecida: %1
goto show_help

:show_help
echo Uso: run_etl.bat [opcoes]
echo.
echo Opcoes:
echo   -f, --farmer-id    ID do farmer para filtrar (default: todos)
echo   -m, --months-back  Numero de meses para tras (default: 11)
echo   -l, --log-level    Nivel de log: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
echo   -k, --kpi          KPI especifico: all, receitas, folha (default: all)
echo   -h, --help         Mostra esta mensagem de ajuda
echo.
exit /b 0

:execute_etl
REM Construindo os comandos base com parâmetros
set CMD_RECEITAS=python kpis\kpi_receitas\main.py --log-level %LOG_LEVEL%
set CMD_FOLHA=python kpis\kpi_folha_pagamento\main.py --log-level %LOG_LEVEL%

REM Adicionando parâmetros opcionais
if not "%FARMER_ID%"=="" (
    set CMD_RECEITAS=%CMD_RECEITAS% --farmer-id %FARMER_ID%
    set CMD_FOLHA=%CMD_FOLHA% --farmer-id %FARMER_ID%
)

if not "%MONTHS_BACK%"=="" (
    set CMD_RECEITAS=%CMD_RECEITAS% --months-back %MONTHS_BACK%
)

REM Executando o(s) ETL(s) conforme o parâmetro KPI
if "%KPI%"=="receitas" (
    echo Executando ETL de Receitas: %CMD_RECEITAS%
    %CMD_RECEITAS%
    goto end
)

if "%KPI%"=="folha" (
    echo Executando ETL de Folha de Pagamento: %CMD_FOLHA%
    %CMD_FOLHA%
    goto end
)

REM Se KPI é "all" ou qualquer outro valor, executa ambos
echo Executando ETL de Receitas: %CMD_RECEITAS%
%CMD_RECEITAS%
echo.

echo Executando ETL de Folha de Pagamento: %CMD_FOLHA%
%CMD_FOLHA%

:end
exit /b
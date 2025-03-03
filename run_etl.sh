@echo off
REM Script batch para executar o ETL do KPI Financeiro Mensal

REM Definindo variáveis padrão
set EMPLOYEE="1. Gamma Capital"
set MONTHS_BACK=11
set LOG_LEVEL=INFO
set ONLY_RECEITA=false
set ONLY_FOLHA=false

REM Processando argumentos da linha de comando
:parse_args
if "%~1"=="" goto execute
if "%~1"=="-e" (
    set EMPLOYEE=%~2
    shift & shift
    goto parse_args
)
if "%~1"=="--employee" (
    set EMPLOYEE=%~2
    shift & shift
    goto parse_args
)
if "%~1"=="-m" (
    set MONTHS_BACK=%~2
    shift & shift
    goto parse_args
)
if "%~1"=="--months-back" (
    set MONTHS_BACK=%~2
    shift & shift
    goto parse_args
)
if "%~1"=="-l" (
    set LOG_LEVEL=%~2
    shift & shift
    goto parse_args
)
if "%~1"=="--log-level" (
    set LOG_LEVEL=%~2
    shift & shift
    goto parse_args
)
if "%~1"=="-r" (
    set ONLY_RECEITA=true
    shift
    goto parse_args
)
if "%~1"=="--only-receita" (
    set ONLY_RECEITA=true
    shift
    goto parse_args
)
if "%~1"=="-f" (
    set ONLY_FOLHA=true
    shift
    goto parse_args
)
if "%~1"=="--only-folha" (
    set ONLY_FOLHA=true
    shift
    goto parse_args
)
if "%~1"=="-h" goto help
if "%~1"=="--help" goto help

echo Opção desconhecida: %~1
goto help

:help
echo Uso: run_etl.bat [opções]
echo.
echo Opções:
echo   -e, --employee    Nome do funcionário ou grupo (default: '1. Gamma Capital')
echo   -m, --months-back Número de meses para trás (default: 11)
echo   -l, --log-level   Nível de log: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
echo   -r, --only-receita Processa apenas dados de receita e comissão
echo   -f, --only-folha   Processa apenas dados de folha de pagamento
echo   -h, --help        Mostra esta mensagem de ajuda
echo.
exit /b 0

:execute
REM Construindo o comando base
set CMD=python kpis\kpi_financeiro_mensal\main.py --employee %EMPLOYEE% --months-back %MONTHS_BACK% --log-level %LOG_LEVEL%

REM Adicionando flags opcionais
if %ONLY_RECEITA%==true (
    set CMD=%CMD% --only-receita
)

if %ONLY_FOLHA%==true (
    set CMD=%CMD% --only-folha
)

REM Executando o ETL
echo Executando: %CMD%
%CMD%

REM Verificando o resultado
if %ERRORLEVEL% EQU 0 (
    echo ETL concluído com sucesso!
    exit /b 0
) else (
    echo ETL falhou com erro!
    exit /b 1
)
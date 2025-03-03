# run_etls.ps1
param (
    [string]$FarmerId,
    [int]$MonthsBack = 11,
    [string]$LogLevel = "INFO"
)

$cmdFarmer = "python kpis\farmer\kpi_receita_farmer\main.py --log-level $LogLevel"
$cmdCliente = "python kpis\farmer\kpi_receita_cliente\main.py --log-level $LogLevel"

if ($FarmerId) {
    $cmdFarmer += " --farmer-id $FarmerId"
    $cmdCliente += " --farmer-id $FarmerId"
}

if ($MonthsBack) {
    $cmdFarmer += " --months-back $MonthsBack"
    $cmdCliente += " --months-back $MonthsBack"
}

Write-Host "Executando ETL de Receita por Farmer: $cmdFarmer"
Invoke-Expression $cmdFarmer

Write-Host "`nExecutando ETL de Receita por Cliente: $cmdCliente"
Invoke-Expression $cmdCliente
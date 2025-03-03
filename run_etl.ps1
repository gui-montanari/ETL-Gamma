# run_etl.ps1
param (
    [string]$FarmerId,
    [int]$MonthsBack = 11,
    [string]$LogLevel = "INFO",
    [string]$Kpi = "all"  # "all", "receita_farmer", "receita_cliente", "receita_produto", "comissao_farmer", "fechamento_passado", "fechamento_atual"
)

$cmdReceitaFarmer = "python kpis\farmer\receita\kpi_receita_farmer\main.py --log-level $LogLevel"
$cmdReceitaCliente = "python kpis\farmer\receita\kpi_receita_cliente\main.py --log-level $LogLevel"
$cmdReceitaProduto = "python kpis\farmer\receita\kpi_receita_produto\main.py --log-level $LogLevel"
$cmdComissaoFarmer = "python kpis\farmer\comissao\kpi_comissao_farmer\main.py --log-level $LogLevel"
$cmdFechamentoPassadoFarmer = "python kpis\farmer\comissao\kpi_fechamento_passado\main.py --log-level $LogLevel"
$cmdFechamentoAtualFarmer = "python kpis\farmer\comissao\kpi_fechamento_atual\main.py --log-level $LogLevel"

if ($FarmerId) {
    $cmdReceitaFarmer += " --farmer-id $FarmerId"
    $cmdReceitaCliente += " --farmer-id $FarmerId"
    $cmdReceitaProduto += " --farmer-id $FarmerId"
    $cmdComissaoFarmer += " --farmer-id $FarmerId"
    $cmdFechamentoPassadoFarmer += " --farmer-id $FarmerId"
    $cmdFechamentoAtualFarmer += " --farmer-id $FarmerId"
}

if ($MonthsBack) {
    $cmdReceitaFarmer += " --months-back $MonthsBack"
    $cmdReceitaCliente += " --months-back $MonthsBack"
    $cmdReceitaProduto += " --months-back $MonthsBack"
    $cmdComissaoFarmer += " --months-back $MonthsBack"
    $cmdFechamentoPassadoFarmer += " --months-back $MonthsBack"
}

if ($Kpi -eq "receita_farmer") {
    Write-Host "Executando ETL de Receitas por Farmer: $cmdReceitaFarmer"
    Invoke-Expression $cmdReceitaFarmer
}
elseif ($Kpi -eq "receita_cliente") {
    Write-Host "Executando ETL de Receita por Cliente: $cmdReceitaCliente"
    Invoke-Expression $cmdReceitaCliente
}
elseif ($Kpi -eq "receita_produto") {
    Write-Host "Executando ETL de Receita por Produto: $cmdReceitaProduto"
    Invoke-Expression $cmdReceitaProduto
}
elseif ($Kpi -eq "comissao_farmer") {
    Write-Host "Executando ETL de Comissão por Farmer: $cmdComissaoFarmer"
    Invoke-Expression $cmdComissaoFarmer
}
elseif ($Kpi -eq "fechamento_passado_farmer") {
    Write-Host "Executando ETL de Fechamento de Comissão (Meses Passados): $cmdFechamentoPassadoFarmer"
    Invoke-Expression $cmdFechamentoPassadoFarmer
}
elseif ($Kpi -eq "fechamento_atual_farmer") {
    Write-Host "Executando ETL de Fechamento de Comissão (Mês Atual): $cmdFechamentoAtualFarmer"
    Invoke-Expression $cmdFechamentoAtualFarmer
}
else {
    Write-Host "Executando ETL de Receitas por Farmer: $cmdReceitaFarmer"
    Invoke-Expression $cmdReceitaFarmer
    
    Write-Host "`nExecutando ETL de Receita por Cliente: $cmdReceitaCliente"
    Invoke-Expression $cmdReceitaCliente
    
    Write-Host "`nExecutando ETL de Receita por Produto: $cmdReceitaProduto"
    Invoke-Expression $cmdReceitaProduto
    
    Write-Host "`nExecutando ETL de Comissão por Farmer: $cmdComissaoFarmer"
    Invoke-Expression $cmdComissaoFarmer
    
    Write-Host "`nExecutando ETL de Fechamento de Comissão (Meses Passados): $cmdFechamentoPassadoFarmer"
    Invoke-Expression $cmdFechamentoPassadoFarmer
    
    Write-Host "`nExecutando ETL de Fechamento de Comissão (Mês Atual): $cmdFechamentoAtualFarmer"
    Invoke-Expression $cmdFechamentoAtualFarmer
}
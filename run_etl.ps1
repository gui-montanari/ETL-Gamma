# run_etl.ps1
param (
    [string]$FarmerId,
    [int]$MonthsBack = 11,
    [string]$LogLevel = "INFO",
    [string]$Kpi = "all"  # "all", "receita_farmer", "receita_cliente", "receita_produto", "comissao_farmer", "fechamento_passado", "fechamento_atual"
)

$cmdReceitaFarmerMPassado = "python kpis\farmer\receita\kpi_receita_farmer_m_passado\main.py --log-level $LogLevel"
$cmdReceitaFarmerMPresente = "python kpis\farmer\receita\kpi_receita_farmer_m_presente\main.py --log-level $LogLevel"
$cmdReceitaCliente = "python kpis\farmer\receita\kpi_receita_cliente\main.py --log-level $LogLevel"
$cmdReceitaProdutoFMPassado = "python kpis\farmer\receita\kpi_receita_produto_f_m_passado\main.py --log-level $LogLevel"
$cmdFechamentoFarmerMPassado = "python kpis\farmer\comissao\kpi_fechamento_m_passado\main.py --log-level $LogLevel"
$cmdFechamentoFarmerMPresente = "python kpis\farmer\comissao\kpi_fechamento_m_presente\main.py --log-level $LogLevel"

if ($FarmerId) {
    $cmdReceitaFarmerMPassado += " --farmer-id $FarmerId"
    $cmdReceitaFarmerMPresente += " --farmer-id $FarmerId"
    $cmdReceitaCliente += " --farmer-id $FarmerId"
    $cmdReceitaProdutoFMPassado += " --farmer-id $FarmerId"
    $cmdFechamentoFarmerMPassado += " --farmer-id $FarmerId"
    $cmdFechamentoFarmerMPresente += " --farmer-id $FarmerId"
}

if ($MonthsBack) {
    # Não adicionar --months-back para o ETL do mês atual
    $cmdReceitaFarmerMPassado += " --months-back $MonthsBack"
    $cmdReceitaCliente += " --months-back $MonthsBack"
    $cmdReceitaProdutoFMPassado += " --months-back $MonthsBack"
    $cmdFechamentoFarmerMPassado += " --months-back $MonthsBack"
    # Não adicionar --months-back para o ETL do mês atual
}

if ($Kpi -eq "receita_farmer_m_passado") {
    Write-Host "Executando ETL de Receitas por Farmer (Meses Anteriores): $cmdReceitaFarmerMPassado"
    Invoke-Expression $cmdReceitaFarmerMPassado
}
elseif ($Kpi -eq "receita_farmer_m_presente") {
    Write-Host "Executando ETL de Receita por Farmer (Mês Atual): $cmdReceitaFarmerMPresente"
    Invoke-Expression $cmdReceitaFarmerMPresente
}
elseif ($Kpi -eq "receita_cliente") {
    Write-Host "Executando ETL de Receita por Cliente: $cmdReceitaCliente"
    Invoke-Expression $cmdReceitaCliente
}
elseif ($Kpi -eq "receita_produto_f_m_passado") {
    Write-Host "Executando ETL de Receita por Produto (Meses Anteriores): $cmdReceitaProdutoFMPassado"
    Invoke-Expression $cmdReceitaProdutoFMPassado
}
elseif ($Kpi -eq "fechamento_farmer_m_passado") {
    Write-Host "Executando ETL de Comissão por Farmer (Mês Passado): $cmdFechamentoFarmerMPassado"
    Invoke-Expression $cmdFechamentoFarmerMPassado
}
elseif ($Kpi -eq "fechamento_farmer_m_presente") {
    Write-Host "Executando ETL de Comissão por Farmer (Mês Atual): $cmdFechamentoFarmerMPresente"
    Invoke-Expression $cmdFechamentoFarmerMPresente
}
else {
    Write-Host "Executando ETL de Receitas M PASSADO por Farmer: $cmdReceitaFarmerMPassado"
    Invoke-Expression $cmdReceitaFarmerMPassado

    Write-Host "`nExecutando ETL de Receita M PRESENTE por Farmer: $cmdReceitaFarmerMPresente"
    Invoke-Expression $cmdReceitaFarmerMPresente
    
    Write-Host "`nExecutando ETL de Receita por Cliente: $cmdReceitaCliente"
    Invoke-Expression $cmdReceitaCliente
    
    Write-Host "`nExecutando ETL de Receita por Produto (Meses Anteriores): $cmdReceitaProdutoFMPassado"
    Invoke-Expression $cmdReceitaProdutoFMPassado
    
    Write-Host "`nExecutando ETL de Comissão M PASSADO por Farmer: $cmdFechamentoFarmerMPassado"
    Invoke-Expression $cmdFechamentoFarmerMPassado
    
    Write-Host "`nExecutando ETL de Comissão M PRESENTE por Farmer: $cmdFechamentoFarmerMPresente"
    Invoke-Expression $cmdFechamentoFarmerMPresente
}
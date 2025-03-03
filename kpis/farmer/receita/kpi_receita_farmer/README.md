# ETL Financeiro Mensal - Gamma Capital

Este projeto implementa um ETL para processar dados financeiros mensais da Gamma Capital, incluindo receitas, comissões e folha de pagamento. A solução foi estruturada seguindo o padrão de arquitetura Extract-Transform-Load (ETL) para facilitar a manutenção e escalabilidade.

## Estrutura do Projeto

```
etl/
│   .env
│   .gitignore
│   requirements.txt
│   run_etl.sh
│
├───config/
│       .env.example
│
├───kpis/
│   ├───kpi_folha_pgto/
│   │       extract.py
│   │       load.py
│   │       transform.py
│   │
│   ├───kpi_receita_comissao/
│   │       extract.py
│   │       load.py
│   │       transform.py
│   │
│   └───kpi_financeiro_mensal/
│           extract.py
│           load.py
│           main.py
│           transform.py
│
├───logs/
│
├───tests/
│       test_kpi_financeiro_mensal.py
│
└───utils/
        db_connection.py
```

## Componentes Principais

1. **Extract (extract.py)**
   - Responsável por extrair dados das tabelas de origem no banco de dados.
   - Implementa consultas SQL modularizadas para cada fonte de dados.
   - Gerencia conexões com o banco de dados.

2. **Transform (transform.py)**
   - Processa e transforma os dados extraídos.
   - Implementa lógicas de negócio para cálculos de receitas, comissões e folha de pagamento.
   - Prepara os dados para carregamento.

3. **Load (load.py)**
   - Carrega os dados transformados nas tabelas de destino no banco de dados.
   - Cria o schema e tabelas se não existirem.
   - Gerencia a atualização de registros existentes.

4. **Main (main.py)**
   - Coordena a execução do processo ETL completo.
   - Processa argumentos da linha de comando.
   - Configura o sistema de logging.

## Tabelas de Destino

O ETL cria e atualiza as seguintes tabelas no schema `analysis`:

1. **financeiro_mensal**
   - Tabela consolidada com todos os dados financeiros mensais.
   - Contém receitas, comissões e folha de pagamento.

2. **receita_comissao**
   - Tabela detalhada com dados de receita e comissão.
   - Inclui a fonte dos dados (histórico ou atual).

3. **folha_pagamento**
   - Tabela específica para dados de folha de pagamento.

## Pré-requisitos

- Python 3.8+
- PostgreSQL
- Bibliotecas Python conforme requirements.txt

## Instalação

1. Clone o repositório
2. Crie um ambiente virtual:
   ```
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```
3. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```
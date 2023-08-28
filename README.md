# datapipeline
 Pipeline de dados para unificar arquivos de diferentes origens.

Compatibilidade de entrada do pipeline: CSV e PostgreSQL

Instruções de compilação:

1. Baixe os arquivos de dados e jogue em uma pasta dentro do repositório, chamada "data". Esses arquivos são o "northwind.sql" e o "order_details.csv"
2. Utilize o "docker-compose.yml" para montar o banco de dados Postgre antes de compilar.
3. Digite "python pipeline.py" para compilar via terminal.

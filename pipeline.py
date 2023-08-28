import asyncio
import datetime
import ler_csv
import ler_postgres
import conexao
import saida
import relacionamentos

async def main():

    #Armazenando a data atual
    data_atual = datetime.datetime.now()
    data_atual = data_atual.date()

    #Data anterior
    data_anterior = data_atual - datetime.timedelta(days=1)

    #Conectar ao banco de dados
    conn = conexao.connect()

    # Entrada de dados para o banco de dados PostgreSQL
    PG = ler_postgres.PG(data_atual, conn)
    PG_anterior = ler_postgres.PG(data_anterior, conn) #Processar dados da data anterior
    nomestabelas = await PG.read_PG()
    await PG_anterior.read_PG() # O await é necessário para que o código espere a execução da função

    # Entrada de dados para o CSV
    CSV = ler_csv.CSV(data_atual)
    CSV_anterior = ler_csv.CSV(data_anterior) #Processar dados da data anterior
    await CSV.read_csv()
    await CSV_anterior.read_csv()

    # Saida de dados para o banco de dados SQLITE
    uniao = saida.Saida(data_atual, nomestabelas)
    uniao.banco()

    # Consultar no banco de dados SQLITE na tabela order_details todas as compras do cliente Wellington Importadora
    relacionamentos.relacionamentos('Wellington Importadora')
        
    # Fechar a conexão com o banco de dados
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
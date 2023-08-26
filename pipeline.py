import datetime
import ler_csv
import ler_postgres
import conexao

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
PG.read_PG()
PG_anterior.read_PG()

# Entrada de dados para o CSV
CSV = ler_csv.CSV(data_atual)
CSV_anterior = ler_csv.CSV(data_anterior) #Processar dados da data anterior
CSV.read_csv()
CSV_anterior.read_csv()

conn.close()
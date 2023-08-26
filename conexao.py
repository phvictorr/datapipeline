import psycopg2

# Informações do banco de dados
db_params = {
    'host': 'localhost',
    'database': 'northwind',
    'user': 'northwind_user',
    'password': 'thewindisblowing'
}

# Função para conectar ao banco de dados
def connect():
    conn = psycopg2.connect(**db_params)
    # Verificar se a conexão foi bem sucedida

    if conn:
        print('1. Conexão SQL bem sucedida!')
    else:
        print('Erro: Conexão falhou! Revise os dados e tente novamente!')

    return conn
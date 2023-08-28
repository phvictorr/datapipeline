import sqlite3
import json

class Saida:
    def __init__(self, data, tabelas) -> None:
        self.data = data
        self.tabelas = tabelas

    def banco(self):
        # Criar banco de dados SQLITE caso ele não exista ou atualizar caso exista
        conn = sqlite3.connect('data/resultado.db')
        cursor = conn.cursor()
        
        for tabela in self.tabelas:
            caminho_arquivo_json = f'data/postgres/{tabela}/{str(self.data)}/{tabela}.json'
            caminho_arquivo_colunas = f'data/postgres/{tabela}/{str(self.data)}/{tabela}_colunas.json'
            caminho_arquivo_tipos = f'data/postgres/{tabela}/{str(self.data)}/{tabela}_tipos.json'
            
            with open(caminho_arquivo_json, 'r') as f:
                dados = json.load(f)
                if dados:  # Verifique se há dados no JSON
                    tabela_nome = dados["tabela"]  # Obter o nome da tabela
                    registros = dados["registros"]  # Obter a lista de registros
                    
                    with open(caminho_arquivo_colunas, 'r') as colunas_file:
                        colunas = json.load(colunas_file)

                    with open(caminho_arquivo_tipos, 'r') as tipos_file:
                        tipos = json.load(tipos_file)
                    
                    self.criar_tabela(cursor, tabela_nome, colunas, tipos)
                    self.inserir_dados(cursor, tabela_nome, registros, colunas)
        
         # Inserir dados do CSV na tabela order_details
        caminho_arquivo_json = f'data/csv/{str(self.data)}/order_details.json'
        with open(caminho_arquivo_json, 'r') as f:
            dados = json.load(f)
            if dados:
                tabela_nome = dados["tabela"]  # Obter o nome da tabela
                registros = dados["registros"]  # Obter a lista de registros
                colunas = dados["colunas"]  # Obter a lista de colunas
                tipos = ['integer', 'integer', 'numeric', 'integer', 'numeric']
                
                self.criar_tabela(cursor, tabela_nome, colunas, tipos)  # Cria a tabela se não existir
                self.inserir_dados_orderdt(cursor, tabela_nome, colunas, registros)  # Insere os registros

                    
        conn.commit()  # Salvar as alterações
        conn.close()  # Fechar a conexão após todas as operações

    def criar_tabela(self, cursor, tabela, colunas, tipos):
        colunas_sql = ', '.join([f'{coluna} {self.tipo_sqlite(tipos[indice])}' for indice, coluna in enumerate(colunas)])
        consulta_create = f'CREATE TABLE IF NOT EXISTS {tabela} ({colunas_sql})'
        cursor.execute(consulta_create)

    def tipo_sqlite(self, tipo):
        if "integer" in tipo:
            return 'INTEGER'
        elif "character" in tipo or "text" in tipo:
            return 'TEXT'
        else:
            return 'BLOB'  # Use BLOB para outros tipos de dados

    def inserir_dados(self, cursor, tabela, registros, colunas):
        for registro in registros:
            valores = [str(value) if value is not None else '' for value in registro]
            placeholders = ', '.join(['?' for _ in valores])
            
            # Verificar se o registro já existe na tabela
            consulta_verificar = f'SELECT COUNT(*) FROM {tabela} WHERE {colunas[0]} = ?'
            cursor.execute(consulta_verificar, (valores[0],))
            if cursor.fetchone()[0] == 0:
                consulta_insert = f'INSERT INTO {tabela} VALUES ({placeholders})'
                cursor.execute(consulta_insert, valores)

    def inserir_dados_orderdt(self, cursor, tabela, colunas, registros):
        for registro in registros:
            valores = [str(value) if value is not None else '' for value in registro.values()]
            placeholders = ', '.join(['?' for _ in valores])

            # Verificar se o registro já existe na tabela
            consulta_verificar = f'SELECT COUNT(*) FROM {tabela} WHERE {colunas[0]} = ?'
            cursor.execute(consulta_verificar, (valores[0],))
            if cursor.fetchone()[0] == 0:
                consulta_insert = f'INSERT INTO {tabela} ({", ".join(colunas)}) VALUES ({placeholders})'
                cursor.execute(consulta_insert, valores)


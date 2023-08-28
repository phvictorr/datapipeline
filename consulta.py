class Consulta:

    def __init__(self, tabela, conn):
        self.conn = conn
        self.tabela = tabela
        self.cursor = self.conn.cursor()

    def consulta(self):
        consulta = f"SELECT * FROM {self.tabela};"
        if consulta == None:
            print('Erro: Consulta SQL falhou! Revise os dados e tente novamente!')
        else:
            self.cursor.execute(consulta)
            dados = self.cursor.fetchall()
            return dados
    
    def get_colunas(self):
        consulta_sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;"
        self.cursor.execute(consulta_sql, (self.tabela,))
        
        # Obter os nomes e tipos das colunas
        colunas_info = self.cursor.fetchall()
        nomes_colunas = [desc[0] for desc in colunas_info]
        tipos_colunas = [desc[1] for desc in colunas_info]
        return nomes_colunas, tipos_colunas
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
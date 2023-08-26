import os
import json
import pandas as pd
import datetime

import conexao
import consulta
import conexao

class PG:
    def __init__(self, data, conn):
        self.data = data
        self.conn = conn
    
    def read_PG(self):
        # Consulta SQL para obter os nomes das tabelas
        cursor = self.conn.cursor()
        consulta_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        
        if consulta_sql == None:
            print('Consulta SQL falhou! Revise os dados e tente novamente!')
        else:
            cursor.execute(consulta_sql)
            nomes_tabelas = [registro[0] for registro in cursor.fetchall()]

            if nomes_tabelas == []:
                print('A consulta de nomes de tabelas falhou! Revise os dados e tente novamente!')
            else:
                # Para cada tabela, ler os dados e salvar o arquivo no computador
                for tabela in nomes_tabelas:
                    data_consulta = consulta.Consulta(tabela, self.conn)
                    dados = data_consulta.consulta()

                    # Para o PostgreSQL, verificar se existe pasta com a data atual e criar caso não exista
                    caminho_pasta = os.path.join('data/postgres', tabela, str(self.data))
                    if not os.path.exists(caminho_pasta):
                        os.makedirs(caminho_pasta)

                        # Converter as datas, memoryview e Timestamp para string
                        dados_serializaveis = []
                        for linha in dados:
                            linha_serializavel = [str(col) if isinstance(col, datetime.date) else col for col in linha]
                            linha_serializavel = [list(col) if isinstance(col, memoryview) else col for col in linha_serializavel]
                            linha_serializavel = [list(col) if isinstance(col, pd._libs.tslibs.timestamps.Timestamp) else col for col in linha_serializavel]
                            dados_serializaveis.append(linha_serializavel)

                        # Caminho do arquivo
                        caminho_arquivo_json = os.path.join(caminho_pasta, f"{tabela}.json")

                        # Serializar para JSON
                        with open(caminho_arquivo_json, 'w') as f:
                            json.dump(dados_serializaveis, f)
                        
                        print(f'Arquivo {tabela}.json salvo com sucesso!')
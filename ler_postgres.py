import os
import json
import pandas as pd
import datetime
import consulta

class PG:
    def __init__(self, data, conn):
        self.data = data
        self.conn = conn
    
    async def read_PG(self):
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
                return
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

                        # Obter os nomes das colunas
                        nomes_colunas, tipos_colunas = data_consulta.get_colunas()

                        if nomes_colunas == [] or tipos_colunas == []:
                            print('A consulta de nomes ou tipos de colunas falhou! Revise os dados e tente novamente!')
                            return
                        
                        #Salvar nome de colunas em um arquivo json
                        caminho_arquivo_json = os.path.join(caminho_pasta, f"{tabela}_colunas.json")
                        with open(caminho_arquivo_json, 'w') as f:
                            json.dump(nomes_colunas, f, default=str)  # Use default=str para serializar tipos não padrão
                        
                        if caminho_arquivo_json == None:
                            print('Erro: Caminho do arquivo JSON não encontrado! Revise os dados e tente novamente!')
                            return

                        #Salvar o tipo de colunas em um arquivo json
                        caminho_arquivo_json = os.path.join(caminho_pasta, f"{tabela}_tipos.json")
                        with open(caminho_arquivo_json, 'w') as f:
                            json.dump(tipos_colunas, f, default=str)  # Use default=str para serializar tipos não padrão
                        
                        if caminho_arquivo_json == None:
                            print('Erro: Caminho do arquivo JSON não encontrado! Revise os dados e tente novamente!')
                            return

                        # Manter relacionamentos entre tabelas
                        relacionamentos = {
                            "tabela": tabela,
                            "colunas": nomes_colunas,
                            "registros": dados_serializaveis
                        }

                        # Caminho do arquivo
                        caminho_arquivo_json = os.path.join(caminho_pasta, f"{tabela}.json")

                        if caminho_arquivo_json == None:
                            print('Erro: Caminho do arquivo JSON não encontrado! Revise os dados e tente novamente!')
                            return

                        # Serializar para JSON
                        with open(caminho_arquivo_json, 'w') as f:
                            json.dump(relacionamentos, f, default=str)  # Use default=str para serializar tipos não padrão

                        if caminho_arquivo_json == None:
                            print('Erro: Caminho do arquivo JSON não encontrado! Revise os dados e tente novamente!')
                            return
                        else:
                            print(f'Arquivo {tabela}.json salvo com sucesso!')
            return nomes_tabelas

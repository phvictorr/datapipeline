import os
import json
import pandas as pd

class CSV:
    def __init__(self, data):
        self.data = data

    async def read_csv(self):
        caminho_pasta = os.path.join('data/csv/' + str(self.data))

        if not os.path.exists(caminho_pasta):
            os.makedirs(caminho_pasta)
            
            # Lê o arquivo CSV
            df_data = pd.read_csv('data/order_details.csv')

            if df_data.empty:
                print('Erro: Arquivo CSV não encontrado! Revise os dados e tente novamente!')
                return
            
            # Converte os tipos de dados para strings (necessário para serialização JSON)
            df_data = df_data.astype(str)
            
            # Salva os registros em formato JSON
            registros = df_data.to_dict(orient='records')
            tabela = 'order_details'
            colunas = list(df_data.columns)
            
            relacionamentos = {
                "tabela": tabela,
                "colunas": colunas,
                "registros": registros
            }
            
            with open(os.path.join(caminho_pasta, f'{tabela}.json'), 'w') as f:
                json.dump(relacionamentos, f, default=str)
            
            print('Arquivo JSON do CSV salvo com sucesso!')
            
            # Salva as colunas em formato JSON
            with open(os.path.join(caminho_pasta, f'{tabela}_colunas.json'), 'w') as f:
                json.dump(colunas, f, default=str)
            
            # Salva os tipos de colunas em formato JSON
            tipos_comuns = ['integer' for _ in colunas]
            with open(os.path.join(caminho_pasta, f'{tabela}_tipos.json'), 'w') as f:
                json.dump(tipos_comuns, f, default=str)
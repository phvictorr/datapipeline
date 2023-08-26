import pandas as pd
import os
import json
import numpy as np

class CSV:
    def __init__(self, data):
        self.data = data
    
    def read_csv(self):
        caminho_pasta = os.path.join('data/csv/' + str(self.data))
        if not os.path.exists(caminho_pasta):
            os.makedirs(caminho_pasta)
            df_data = pd.read_csv('data/order_details.csv')
            df_data.to_json('data/csv/' + str(self.data) + '/order_details.json', orient='records')
            print('Arquivo json do CSV salvo com sucesso!')
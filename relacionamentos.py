import sqlite3
import json

def relacionamentos(ship_name):

        conn = sqlite3.connect('data/resultado.db')
        cursor = conn.cursor()

        # Buscar compra com o valor order_ID imprimindo os campos da consulta SQL
        consulta_sql = "SELECT od.product_id, od.unit_price, od.quantity, od.discount, p.product_name, c.category_name, o.order_date, o.ship_name, o.ship_address, o.ship_city, o.ship_region, o.ship_postal_code, o.ship_country, o.shipped_date, o.required_date FROM order_details od INNER JOIN products p ON od.product_id = p.product_id INNER JOIN categories c ON p.category_id = c.category_id INNER JOIN orders o ON od.order_id = o.order_id WHERE o.ship_name = ?;"
        cursor.execute(consulta_sql, (ship_name,))
        resultado = cursor.fetchall()
        print(f'Compra do cliente {ship_name}:')
        
        # Imprimir todas as compras do cliente por produto e data
        for registro in resultado:
            print(f'Product ID: {registro[0]}')
            print(f'Unit Price: {registro[1]}')
            print(f'Quantity: {registro[2]}')
            print(f'Discount: {registro[3]}')
            print(f'Product Name: {registro[4]}')
            print(f'Category Name: {registro[5]}')
            print(f'Order Date: {registro[6]}')
            print(f'Ship Name: {registro[7]}')
            print(f'Ship Address: {registro[8]}')
            print(f'Ship City: {registro[9]}')
            print(f'Ship Region: {registro[10]}')
            print(f'Ship Postal Code: {registro[11]}')
            print(f'Ship Country: {registro[12]}')
            print(f'Shipped Date: {registro[13]}')
            print(f'Required Date: {registro[14]}')
            print('--------------------------------------')
        
        #Salvar resultado em um arquivo json
        caminho_arquivo_json = 'data/resultado_consulta.json'
        with open(caminho_arquivo_json, 'w') as f:
            json.dump(resultado, f, default=str)  # Use default=str para serializar tipos não padrão

        # Fechar a conexão com o banco de dados
        conn.close()
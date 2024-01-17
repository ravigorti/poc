import sqlite3
import json

def generate_query_from_json(json_file_path):
    with open(json_file_path, 'r') as json_file:
        config = json.load(json_file)

    columns = config['columns']
    tables = config['tables']

    columns_string = ', '.join(columns)

    query_parts = [f"SELECT {columns_string}\nFROM {tables[0]['name']} {tables[0]['alias']}"]

    for table in tables[1:]:
        if 'join_condition' in table:
            query_parts.append(f"JOIN {table['name']} {table['alias']} ON {table['join_condition']}")

    select_query = ' '.join(query_parts) + ';'

    return select_query

json_file_path = r'/Users/ravitejagorti/Desktop/DAL_poc/sample1.json'
sql_query = generate_query_from_json(json_file_path)
print(sql_query)

conn = sqlite3.connect(r'/Users/ravitejagorti/Desktop/DAL_poc/mysqlite.sqlite')

cursor = conn.cursor()

cursor.execute(sql_query)

results = cursor.fetchall()
print(*results ,sep='\n')

cursor.close()
conn.close()
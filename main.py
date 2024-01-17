import sqlite3
import json

def generate_query_from_json(json_file_path):
    with open(json_file_path, 'r') as json_file:
        config = json.load(json_file)

    columns = config['columns']
    tables = config['tables']

    select_query = f"SELECT {', '.join(columns)}\nFROM "

    for i, table in enumerate(tables):
        table_name = table['name']
        table_alias = table['alias']

        select_query += f"{table_name} {table_alias}"

        if i > 0:
            join_condition = table.get('join_condition')
            if join_condition:
                select_query += f" ON {join_condition}"

        if i < len(tables) - 1:
            select_query += ', '

    return select_query

json_file_path = r'/Users/ravitejagorti/Desktop/DAL_poc/sample2.json'
sql_query = generate_query_from_json(json_file_path)
print(sql_query)



conn = sqlite3.connect(r'/Users/ravitejagorti/Desktop/DAL_poc/mysqlite.sqlite')

cursor = conn.cursor()

cursor.execute(sql_query)

results = cursor.fetchall()
print(*results ,sep='\n')

cursor.close()
conn.close()
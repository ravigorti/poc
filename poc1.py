import json
import pandas as pd
import sqlite3

def generate_sql_query(json_file_path, csv_file_path):
    with open(json_file_path, 'r') as json_file:
        config = json.load(json_file)

    relationships_df = pd.read_csv(csv_file_path)
    print(relationships_df)
    json_tables = set(config['columns'].keys())

    relevant_relationships_df = relationships_df[
        (relationships_df['Table1'].isin(json_tables)) & (relationships_df['Table2'].isin(json_tables))
    ]
    relevant_relationships_df = relevant_relationships_df[relevant_relationships_df.Table2 != relevant_relationships_df.iloc[0]['Table1']]
    relevant_relationships_df = relevant_relationships_df.reset_index(drop='index')
    print(relevant_relationships_df)
    select_query = "SELECT "

    for table, columns in config['columns'].items():
        select_query += ', '.join([f"{table}.{column}" for column in columns]) + ', '

    select_query = select_query[:-2]

    from_query = f" FROM {relevant_relationships_df.iloc[0]['Table1']}"

    join_conditions = []

    for _, relationship in relevant_relationships_df.iterrows():
            join_conditions.append(f" JOIN {relationship['Table2']} ON {relationship['Condition']}")

    final_query = select_query + from_query + ''.join(join_conditions) + ';'

    return final_query

json_file_path = r'/Users/ravitejagorti/Desktop/DAL_poc/sample3.json'  # Replace with the actual path to your JSON file
csv_file_path = r'/Users/ravitejagorti/Desktop/DAL_poc/tables_relationships.csv'  # Replace with the actual path to your CSV file

sql_query = generate_sql_query(json_file_path, csv_file_path)

print(sql_query)


conn = sqlite3.connect(r'/Users/ravitejagorti/Desktop/DAL_poc/mysqlite.sqlite')

cursor = conn.cursor()

cursor.execute(sql_query)

results = cursor.fetchall()
print(*results ,sep='\n')

cursor.close()
conn.close()
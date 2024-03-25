import psycopg2
import mysql.connector
import pandas as pds
import traceback

# Define function to create DataFrame to count table rows
def count_rows(dbms,db_params,input_df,header_labels):
    
    # Unique list of databases
    databases = set(input_df['database'].values)

    # Array to insert values to
    data = []

    # For each database create connection
    for db in databases:

        db_params['database'] = db
    
        # Connect to database
        conn = dbms.connect(**db_params)

        # Create cursor
        cur = conn.cursor()

        # MAGIC for each db execute query to count rows for each table from input_file, if table not found, add table anyway with count blank, don't break transaction 
        for row in input_df.index:
            if input_df['database'][row]==db:
                schema = input_df['schema'][row]
                table = input_df['table'][row]
                if dbms==postgreSQL: 
                    query = f'SELECT count(1) from {schema}."{table}";'
                elif dbms==mySQL:
                    query = f'SELECT count(1) from {schema}.{table};'
                try:
                    cur.execute(query)
                    result = cur.fetchone()[0]
                    print("Rows in "+table+":",result)
                    data.append([db, schema, table, 'yes', result])
                except Exception:
                    print(traceback.format_exc())
                    data.append([db, schema, table, 'no', ''])
                    conn.rollback()

    # Convert results to a DataFrame
    results_df = pds.DataFrame(data, columns = header_labels)

    # End cursor and connection
    cur.close()
    conn.close()

    return results_df

# Set input file with lists of tables to search
input_file = "##CHANGEME##.xlsx"

# Read the XLSX file into a DataFrame
model_tables = pds.read_excel(input_file)

# List unique databases
databases = set(model_tables['database'].values)

# Set labels in output df
header_labels_prod = ['database','schema','table','is_found','rows_count_prod']
header_labels_dev = ['database','schema','table','is_found','rows_count_dev']

# Set database parameters for PostgreSQL
postgreSQL = psycopg2

db_params_psql_prod = {
    'host': '##CHANGEME##',
    'database': '',
    'user': '##CHANGEME##',
    'password': '##CHANGEME##',
    'port':'##CHANGEME##'
}

db_params_psql_dev = {
    'host': '##CHANGEME##',
    'database': '',
    'user': '##CHANGEME##',
    'password': '##CHANGEME##',
    'port':'##CHANGEME##'
}

# Set database parameters for MySQL
mySQL = mysql.connector

db_params_mysql_prod = {
    'host': '##CHANGEME##',
    'database': '',
    'user': '##CHANGEME##',
    'password': '##CHANGEME##',
    'port':'##CHANGEME##'
}

db_params_mysql_dev = {
    'host': '##CHANGEME##',
    'database': '',
    'user': '##CHANGEME##',
    'password': '##CHANGEME##',
    'port':'##CHANGEME##'
}

# Convert results to a DataFrame PROD
results_df_prod = count_rows('##mySQL/postgreSQL##','##db_params_**##',model_tables,header_labels_prod) #set database and connection parameters

# Convert results to a DataFrame DEV
results_df_dev = count_rows('##mySQL/postgreSQL##','##db_params_**##',model_tables,header_labels_dev) #set database and connection parameters

# Merge dev and prod DataFrames
results_df = pds.merge(results_df_prod,results_df_dev[['table','rows_count_dev']], on='table')
print(results_df)

# List only tables with difference between dev and prod and export DataFrame to an Excel file
results_df_diff = results_df[(results_df['rows_count_prod'] != results_df['rows_count_dev'])]
results_df_diff.to_excel('##CHANGEME##.xlsx', index=False)
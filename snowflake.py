import os, argparse
import pandas as pd
import snowflake.connector as snowflake
from snowflake.connector.pandas_tools import write_pandas


def connect(user, password, account, warehouse):
    conn = snowflake.connect(
        user = user,
        password = password,
        account = account,
        warehouse = warehouse
    )
    return conn


def database_config(conn, database, schema, table, file):
    cs = conn.cursor()

    # Create a Database & Schema if not exists
    cs.execute("CREATE DATABASE IF NOT EXISTS " + database)
    cs.execute("USE DATABASE " + database)
    cs.execute("CREATE SCHEMA IF NOT EXISTS " + schema)
    cs.execute("USE SCHEMA " + schema)

    # Getting filename in UPPERCASE and removing .csv
    filename = os.path.splitext(os.path.basename(file))[0].upper()
    if table == '':
        table = f'{filename}_table'

    # Create a CREATE TABLE SQL-statement
    create_table = "CREATE TABLE IF NOT EXISTS " + table + " (\n"

    # Reading csv file
    df = pd.read_csv(file)
    # Converting columns names to upper case
    df.columns = df.columns.str.upper()

    for col in df.columns:
        column_name = col.upper()
        # Creating query based on column data type
        if (df[col].dtype.name == "int" or df[col].dtype.name == "int64"):
            create_table = create_table + column_name + " int"
        elif df[col].dtype.name == "object":
            create_table = create_table + column_name + " varchar(16777216)"
        elif df[col].dtype.name == "datetime64[ns]":
            create_table = create_table + column_name + " datetime"
        elif df[col].dtype.name == "float64":
            create_table = create_table + column_name + " float8"
        elif df[col].dtype.name == "bool":
            create_table = create_table + column_name + " boolean"
        else:
            create_table = create_table + column_name + " varchar(16777216)"

        # Deciding next steps. Either column is not the last column (add comma) else end create_table
        if df[col].name != df.columns[-1]:
            create_table = create_table + ",\n"
        else:
            create_table = create_table + ")"

            # Execute the SQL statement to create the table
            cs.execute(create_table)

    cs.execute('TRUNCATE TABLE IF EXISTS ' + table)

    return cs, df


def upload_snowflake(user, password, account,
        warehouse, database, schema, table, file):
    try:
        conn = connect(
            user,
            password,
            account,
            warehouse
        )
        cs, df = database_config(
            conn, database, schema,
            table, file
        )

        write_pandas(
            conn = conn,
            df = df,
            table_name = table.upper(),
            database = database,
            schema = schema
        )

        cs.close()
        conn.close()
    except Exception as e:
        print(e)
        return e


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description = 'Loading data into snowflake database'
    )
    parser.add_argument(
        'user',
        help = 'Snowflake Username',
        type = str
    )
    parser.add_argument(
        'password',
        help = 'Snowflake Password',
        type = str
    )
    parser.add_argument(
        'account',
        help = 'Snowflake account identifier - ex company.snowflakecomputing.com \
            would just be "company" as the ACCOUNT name',
        type = str
    )
    parser.add_argument(
        'warehouse',
        help = 'Snowflake warehouse',
        type = str
    )
    parser.add_argument(
        'database',
        help = 'Database name',
        type = str
    )
    parser.add_argument(
        'schema',
        help = 'Schema name',
        type = str
    )
    parser.add_argument(
        'table',
        help = 'Table name',
        type = str
    )
    parser.add_argument(
        'file',
        help = 'Only CSV file path',
        type = str
    )

    args = parser.parse_args()
    upload_snowflake(
        args.user, args.password, args.account,
        args.warehouse, args.database, args.schema,
        args.table, args.file
    )

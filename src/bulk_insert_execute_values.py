#------------------------------------------------------------------------------
# Complete example of how to convert a csv to
# a pandas dataframe, and then to PostgreSQL
# Method: Bulk Insert using execute_values()
#------------------------------------------------------------------------------
# Author: Naysan Saran, May 2020
# License: GPL V3.0
#------------------------------------------------------------------------------
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras

# Connection parameters
param_dic = {
    "host"      : "localhost",
    "database"  : "globaldata",
    "user"      : "myuser",
    "password"  : "Passw0rd"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def execute_query(conn, query):
    """ Execute a single query """

    ret = 0 # Return value
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    # If this was a select query, return the result
    if 'select' in query.lower():
        ret = cursor.fetchall()
    cursor.close()
    return ret

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def read_dataframe(csv_file):
    """
    Load and prepare the dataframe for insertion
    into the database
    """
    df = pd.read_csv(csv_file)
    df = df.rename(columns={
    "Source": "source",
    "Date": "datetime",
    "Mean": "mean_temp"
    })
    return df


def main():
    

    # Read the csv file
    csv_file = "../data/global-temp-monthly.csv"
    df = read_dataframe(csv_file)
    print(df.head(5))

    # Connect to the database
    conn = connect(param_dic)

    # Run the execute_many strategy
    execute_values(conn, df, 'MonthlyTemp')

    # Check that the values were indeed inserted
    n_rows = execute_query(conn, "select count(*) from MonthlyTemp;")
    print("Number of rows in the table = %s" % n_rows)

    # Optional - Clear the table
    execute_query(conn, "delete from MonthlyTemp where true;")

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()


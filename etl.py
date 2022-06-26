import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Copy data from S3 bucket into staging tables (stg_events and stg_songs)
    """
    for query in copy_table_queries:   
        try:
            cur.execute(query)
            conn.commit()
            print(f"Query {query} executed and committed to database")
        except psycopg2.Error as e:
            print(f"Error: Issue dropping table. Query {query} was not correctly executed.")
            print(e)


def insert_tables(cur, conn):
    """
    - Insert data from staging tables into the final analytical tables
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f"Query {query} executed and committed to database")
        except psycopg2.Error as e:
            print(f"Error: Issue dropping table. Query {query} was not correctly executed.")
            print(e)


def main():
    """
    - Parse and read info from the config file
    
    - Establishes connection with the Redshift cluster and gets
    cursor to it.  
    
    - Load data into staging tables.  
    
    - Insert data into analytical tables. 
    
    - Closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to cluster
    try: 
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except psycopg2.Error as e:
        print("Error: Could not connect to Redshift cluster")
        print(e)
    
    # get cursor
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to default database")
        print(e)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

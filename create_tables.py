import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Drop, if exists, all tables to restart the setup the ETL process from beginning
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f"Query {query} executed and committed to database")
        except psycopg2.Error as e:
            print(f"Error: Issue dropping table. Query {query} was not correctly executed.")
            print(e)
        


def create_tables(cur, conn):
    """
    - Create two staging tables and the five final tables for the database
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f"Query {query} executed and committed to database")
        except psycopg2.Error as e:
            print("Error: Issue creating table. Query {query} was not correctly executed.")
            print(e)
        


def main():
    """
    - Parse and read info from the config file
    
    - Establishes connection with the Redshift cluster and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

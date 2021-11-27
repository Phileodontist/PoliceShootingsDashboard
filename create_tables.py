"""
This script can be used to manually create/drop staging/prod tables
otherwise, the create_tables, drop_tables functions
can be used in other scripts to run queries defined in
the sql_queries.py file
"""
import sys
import configparser
import psycopg2
from sql_queries import create_stage_table_queries, drop_stage_table_queries, \
                        create_prod_table_queries, drop_prod_table_queries


def drop_tables(cur, conn, queries):
    """
    Drops all tables within RDS instance
    """
    for query in queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, queries):
    """
    Creates tables within the RDS instance
    """
    for query in queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Manually runs create/drop queries
    """
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    if (len(sys.argv) == 2 and sys.argv[1] in ('Prod', 'Stage')):
        table_type = sys.argv[1]
    else:
        print("Example: python3 create_table.py (Prod, Stage)")
        return

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['postgres'].values()))
    cur = conn.cursor()

    if (table_type == 'Stage'):
        drop_tables(cur, conn, drop_stage_table_queries)
        create_tables(cur, conn, create_stage_table_queries)
    else:
        drop_tables(cur, conn, drop_prod_table_queries)
        create_tables(cur, conn, create_prod_table_queries)
        
    conn.close()
    
if __name__ == "__main__":
    main()

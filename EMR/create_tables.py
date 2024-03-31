"""
This script can be used to manually create/drop staging/prod tables
otherwise, the create_tables, drop_tables functions
can be used in other scripts to run queries defined in
the sql_queries.py file
"""
import sys
import argparse
import configparser
import psycopg2
from sql_queries import create_stage_table_queries, drop_stage_table_queries, \
                        create_prod_table_queries, drop_prod_table_queries

parser = argparse.ArgumentParser(description='')
parser.add_argument('-t', '--table', dest='table_type',
                    help='Defines which set of tables to operate on. [prod, stage]', required=True)
parser.add_argument('-e', '--env', dest='environ',
                    help='Defines what environment to operate on. [local, aws]', required=True)
args = parser.parse_args()

"""
python3 create_tables.py --table stage --env local
python3 create_tables.py --table prod  --env local
python3 create_tables.py -t prod -e aws
"""


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

    table = args.table_type.lower()
    env= args.environ.lower()

    if table in ('prod', 'stage'):
        table_type = table
    else:
        print("Example: python3 create_table.py -t [prod, stage] -e [local, aws]")
        return

    # Change config['RDS'] to config['postgres'] to connect to local
    if env == 'local':
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['postgres'].values()))
    else:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['RDS'].values()))
    cur = conn.cursor()

    if (table_type == 'stage'):
        drop_tables(cur, conn, drop_stage_table_queries)
        create_tables(cur, conn, create_stage_table_queries)
    else:
        drop_tables(cur, conn, drop_prod_table_queries)
        create_tables(cur, conn, create_prod_table_queries)
        
    conn.close()
    
if __name__ == "__main__":
    main()

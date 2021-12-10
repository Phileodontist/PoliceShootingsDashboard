import os
import psycopg2
import configparser
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from sql_queries import stage_tables, prod_tables, transformation_queries
# from sql_queries import create_prod_table_queries, drop_prod_table_queries

# Used within jupyter notebook
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'

config = configparser.ConfigParser()
config.read('config.ini')

"""
How to run this script via command line: 
spark-submit --packages org.postgresql:postgresql:42.1.1 ETL_Prod.py
"""

def create_spark_session():
    """
    Creates a Spark Sessions
    """
    spark = SparkSession.builder \
                        .config("spark.driver.extraClassPath", os.environ['PYSPARK_SUBMIT_ARGS']) \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .master('local[*]') \
                        .appName('ETL')\
                        .getOrCreate()
    return spark

def create_temp_view(spark, table_name):
    """
    Creates a spark temp view for each of the stage tables
    """
    
    '''
    df = spark.read \
      .format("jdbc") \
      .option('driver', 'org.postgresql.Driver') \
      .option("url", config.get('postgres', 'url')) \
      .option("dbtable", table_name) \
      .option("user", config.get('postgres', 'user')) \
      .option("password", config.get('postgres', 'password')) \
      .load()
     ''''
    
    df = spark.read \
      .format("jdbc") \
      .option('driver', 'org.postgresql.Driver') \
      .option("url", config.get('RDS', 'url')) \
      .option("dbtable", table_name) \
      .option("user", config.get('RDS', 'user')) \
      .option("password", config.get('RDS', 'password')) \
      .load()    
    
    df.createOrReplaceTempView(table_name)
    
def write_to_prod(spark, query, table_name, db_properties):
    """
    Writes records into prod tables
    """
    prod_data = spark.sql(query)
    prod_data.write \
             .option('driver', 'org.postgresql.Driver') \
             .jdbc(url=config.get('EDS', 'url'), \
                   table=table_name, \
                   mode='overwrite', \
                   properties=db_properties)
    
def main():
    spark = create_spark_session()
    
    # Set up db connections
    db_properties = {}
    #db_properties['username'] = config.get('postgres','user')
    #db_properties['password'] = config.get('postgres','password')
    db_properties['username'] = config.get('RDS','user')
    db_properties['password'] = config.get('RDS','password')    
    
    for stage_table, prod_table, transformation in zip(stage_tables, prod_tables, transformation_queries):
        create_temp_view(spark, stage_table)
        write_to_prod(spark, transformation, prod_table, db_properties)
    
if __name__ == '__main__':
    main()
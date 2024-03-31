import os
import psycopg2
import argparse
import configparser
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from sql_queries import stage_tables, prod_tables, transformation_queries

parser = argparse.ArgumentParser(description='')
parser.add_argument('-m', '--mode', dest='mode', help='options: [local, aws]', required=True)
args = parser.parse_args()

config = configparser.ConfigParser()
config.read('config.ini')

# Configure to run on cluster mode
# conf = (SparkConf().set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
#                    .set("spark.driver.extraJavaOptions",   "-Dcom.amazonaws.services.s3.enableV4=true"))
        
# sc = SparkContext(conf=conf)
# sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")        

# # Configure in order to use s3a, for local execution
# hadoopConf = sc._jsc.hadoopConfiguration()
# hadoopConf.set("fs.s3a.awsAccessKeyId", config['S3']['aws_access_key_id'])
# hadoopConf.set("fs.s3a.awsSecretAccessKey", config['S3']['aws_secret_access_key'])
# hadoopConf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
# hadoopConf.set("com.amazonaws.services.s3a.enableV4", "true")
# hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 

"""
How to run this script via command line: 
spark-submit --packages org.postgresql:postgresql:42.1.1 \
 --class org.apache.hadoop:hadoop-aws:2.7.7 \
 --class com.amazonaws:aws-java-sdk:1.7.4 ETL_Prod.py -m 'local'
"""

"""
    .config("spark.driver.extraClassPath", os.environ['PYSPARK_SUBMIT_ARGS']) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
"""

def create_spark_session():
    """
    Creates a Spark Sessions
    """
    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName('Prod_Script')\
                        .getOrCreate()
    return spark

def create_temp_view(spark, table_name):
    """
    Creates a spark temp view for each of the stage tables
    """
    if args.mode == 'local':
        # Reading from local Postgres
        df = spark.read \
          .format("jdbc") \
          .option('driver', 'org.postgresql.Driver') \
          .option("url", config.get('postgres', 'url')) \
          .option("dbtable", table_name) \
          .option("user", config.get('postgres', 'user')) \
          .option("password", config.get('postgres', 'password')) \
          .load()
    else:
        # Reading from RDS
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
    
    if args.mode == 'local':
        # Writing to local Postgres
        prod_data.write \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("truncate", "true") \
                 .jdbc(url=config.get('postgres', 'url'), 
                       table=table_name, \
                       mode='append', \
                       properties=db_properties)
    else:
        # Writing to RDS
        prod_data.write \
                 .option('driver', 'org.postgresql.Driver') \
                 .option("truncate", "true") \
                 .jdbc(url=config.get('RDS', 'url'), \
                       table=table_name, \
                       mode='append', \
                       properties=db_properties)
    
def main():
    spark = create_spark_session()
    
    # Set up db connections
    db_properties = {}
    
    if args.mode == 'local':
        db_properties['username'] = config.get('postgres','user')
        db_properties['password'] = config.get('postgres','password')
    else:
        db_properties['username'] = config.get('RDS','user')
        db_properties['password'] = config.get('RDS','password')    
    
    for stage_table, prod_table, transformation in zip(stage_tables, prod_tables, transformation_queries):
        create_temp_view(spark, stage_table)
        write_to_prod(spark, transformation, prod_table, db_properties)
    
if __name__ == '__main__':
    main()

import os
import requests
import argparse
import configparser
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from sql_queries import create_stage_table_queries, drop_stage_table_queries
from schemas import stage_police_shootings_schema, stage_us_cities_schema, \
                    stage_us_demographics_schema, stage_unemployment_schema, raw_unemployment_schema

parser = argparse.ArgumentParser(description='Collections metadata of bgpstream users')
parser.add_argument('-m', '--mode', dest='mode', help='Local or AWS', required=True)
args = parser.parse_args()

config = configparser.ConfigParser()
config.read('config.ini')

# # Configure to run on cluster mode
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
- org.postgresql:postgresql:42.1.1 :   Enables the use of postgres
- com.amazonaws:aws-java-sdk:1.7.4 :   Set of Java libraries that make using AWS easier  
- org.apache.hadoop:hadoop-aws:2.7.7 : Version of hadoop to run Spark on

# Local
spark-submit --packages org.postgresql:postgresql:42.1.1 --class org.apache.hadoop:hadoop-aws:2.7.7 --class com.amazonaws:aws-java-sdk:1.7.4 ETL_Staging.py -m 'local'

# Cluster
spark-submit --packages org.postgresql:postgresql:42.1.1 ETL_Staging.py

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
                        .appName('Staging_Script')\
                        .getOrCreate()
    return spark

def request_unemployment_data(state_id):
    """
    Requests data for each state
    
    Return response from unemployment API
    """
    endPointTemplate = 'https://api.careeronestop.org/v1/unemployment/{}/{}/{}'
    
    headersAuth = {
        'Authorization': 'Bearer '+ config.get('unemploymentAPI', 'unemployment_api_key')
    }
    
    url = endPointTemplate.format(config.get('unemploymentAPI', 'unemployment_userID'), state_id, 'county')
    response = requests.get(url, headers=headersAuth, verify=True)
    counties = response.json()['CountyList']
    
    results = []
    for county in counties:
        county['StateID'] = state_id
        results.append(county)
        
    return results

def retrieve_unemployment_data(spark, request_unemployment_data):
    """
    Retrieve unemployment data from API
    
    Returns a df containing unemployment data
    """
    
    if args.mode == 'local':
        # Read from local file
        with open(config.get('pathways', 'state_ids'), 'r') as f:
            ids = f.readline()
            state_ids = ids.split(',')
            rdd = spark.sparkContext.parallelize(state_ids)
            # Request unemployment data for each state & flattens each result into one list
            unemployment_data = rdd.flatMap(request_unemployment_data)
    else:
        # Read from S3 bucket  
        state_ids = spark.read.csv('s3://police-shootings-data/state_ids.txt')
        rdd = state_ids.rdd
        unemployment_data = rdd.flatMap(request_unemployment_data)
        
    # Convert into dataframe and rename columns
    df = spark.createDataFrame(unemployment_data, raw_unemployment_schema) \
              .selectExpr('AreaID as area_id', \
                          'AreaName as area_name', \
                          'AreaType as area_type', \
                          'Stfips as stfips', \
                          'UnEmpCount as unemployment_count', \
                          'UnEmpRate as unemployment_rate', \
                          'StateID as state_id')
        
    return df

def write_to_stage(df, db_properties, table_name):
    """
    Write raw data into staging tables
    """
    if args.mode == 'local':
        # Write to local postgres database
        df.write.option('driver', 'org.postgresql.Driver') \
          .jdbc(url=config.get('postgres', 'url'), table=table_name, \
                mode='overwrite', properties=db_properties)    
    else:
        # Connect to AWS RDS
        df.write.option('driver', 'org.postgresql.Driver') \
          .jdbc(url=config.get('RDS', 'url'), table=table_name, \
                mode='overwrite', properties=db_properties) 
    
def retrieve_data(spark, schema, file_name):
    """
    Read data from files in S3
    
    Return df containing data from files
    """
    if args.mode == 'local':
        # For local file access
        df = spark.read.option('header', 'true').option('sep', ',').schema(schema) \
                  .csv('file://' + config.get('pathways', file_name))
    else:
        # For S3 access 
        df = spark.read.option('header', 'true').option('sep', ',').schema(schema) \
                  .csv('s3://police-shootings-data/' + file_name + '.csv')        
    return df

def main():
    # Initialize session
    spark = create_spark_session()
    
    # Set up db connections
    db_properties = {}
    
    if args.mode == 'local':
        db_properties['username'] = config.get('postgres','user')
        db_properties['password'] = config.get('postgres','password')
    else:
        db_properties['username'] = config.get('RDS','user')
        db_properties['password'] = config.get('RDS','password') 
    
    # Retrieve data
    unemployment_data = retrieve_unemployment_data(spark, request_unemployment_data)
    police_shootings_data = retrieve_data(spark, stage_police_shootings_schema, 'police_shootings')
    us_demographics_data = retrieve_data(spark, stage_us_demographics_schema, 'us_demographics')
    us_cities_data = retrieve_data(spark, stage_us_cities_schema, 'us_cities')
    
    # Write date to staging tables
    write_to_stage(unemployment_data, db_properties, 'stage_unemployment')
    write_to_stage(police_shootings_data, db_properties, 'stage_police_shootings')
    write_to_stage(us_demographics_data, db_properties, 'stage_us_demographics')
    write_to_stage(us_cities_data, db_properties, 'stage_us_cities')
    
    spark.stop()
    
if __name__ == '__main__':
    main()
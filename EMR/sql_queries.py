import os
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('config.ini')

# Global variables
# policeShootings = config.get("S3", "police_shootings")
# usCities = config.get("S3", "us_cities")
# usDemographics = config.get("S3", "us_demographics")
# stateIDs = config.get("S3", "state_ids")
# IAM_ROLE = config.get("IAM_ROLE","ARN")

policeShootings = config.get("pathways", "police_shootings")
usCities = config.get("pathways", "us_cities")
usDemographics = config.get("pathways", "us_demographics")
stateIDs = config.get("pathways", "state_ids")

# DROP TABLES
stage_police_shootings_table_drop = "DROP TABLE IF EXISTS stage_police_shootings"
stage_police_agencies_table_drop = "DROP TABLE IF EXISTS stage_police_agencies"
stage_unemployment_table_drop = "DROP TABLE IF EXISTS stage_unemployment"
prod_police_shootings_table_drop = "DROP TABLE IF EXISTS prod_police_shootings"
prod_police_agencies_table_drop = "DROP TABLE IF EXISTS prod_police_agencies"
prod_unemployment_table_drop = "DROP TABLE IF EXISTS prod_unemployment"

# CREATE TABLES
############## Raw data ##############

stage_police_shootings_table_create = ("""CREATE TABLE IF NOT EXISTS stage_police_shootings
(
    id                   int,
    name                 varchar(100), 
    date                 date,
    armed_with           varchar(50),
    age                  int,
    gender               varchar(10),
    race                 varchar(30), 
    race_source          varchar(30),
    city                 varchar(100),
    state                varchar(10),
    mental_illness       boolean,
    threat_level         varchar(50),
    flee                 varchar(50),
    body_camera          boolean,
    longitude            float,
    latitude             float,
    location_precision   varchar(50),
    agency_ids           varchar(50)
)
""")

stage_police_agencies_table_create = ("""CREATE TABLE IF NOT EXISTS stage_police_agencies
(
    id                   int,
    name                 varchar(100),
    type                 varchar(50),
    state                varchar(10),
    oricodes             varchar(50),
    total_shootings      int
)
""")

stage_us_cities_table_create = ("""CREATE TABLE IF NOT EXISTS stage_us_cities 
(
    state_id             varchar(50),
    state_name           varchar(50), 
    county               varchar(50),
    city                 varchar(50),   
    city_ascii           varchar(50),
    county_fips          int,
    county_name          varchar(50),
    latitude             float, 
    longitude            float,
    population           int,
    density              int,
    source               varchar(50),
    military             boolean,
    incorporated         boolean,
    timezone             varchar(50),
    ranking              int,
    zips                 varchar(50),
    id                   int
)
""")

stage_demographics_table_create = ("""CREATE TABLE IF NOT EXISTS stage_us_demographics 
(
    county_name    varchar(50),
    max_age        int,
    min_age        int,
    population     int,
    race           varchar(50),
    sex            varchar(10),
    state_name     varchar(50),
    year           varchar(50)
)
""")

stage_unemployment_table_create = ("""CREATE TABLE IF NOT EXISTS stage_unemployment 
(
    area_id                varchar(50),    
    area_name              varchar(50),
    area_type              varchar(50), 
    stfips                 varchar(10),    
    unemployment_count     int,
    unemployment_rate      float,
    state_id               varchar(50)
)
""")

############## Raw data ##############

############## Prod data ##############

prod_police_shootings_table_create = ("""CREATE TABLE IF NOT EXISTS prod_police_shootings 
(
    id                       int,
    name                     varchar(100),
    date                     date,
    armed_with               varchar(50),
    age                      int,
    gender                   varchar(10),
    race                     varchar(30),
    state_id                 varchar(10),
    county                   varchar(100),
    city                     varchar(100),
    mental_illness           boolean,
    threat_level             varchar(50),
    flee                     varchar(50),
    body_camera              boolean,
    agency_ids               varchar(50),
    PRIMARY KEY(id),
    CONSTRAINT fk_location
      FOREIGN KEY(state_id, county, city)
      REFERENCES prod_us_cities(state_id, county, city)
)
""")

prod_police_agencies_table_create = ("""CREATE TABLE IF NOT EXISTS prod_police_agencies
(
    id                   int,
    name                 varchar(100), 
    type                 varchar(50),
    state                varchar(10),
    total_shootings      int,
    PRIMARY KEY(id)
)
""")

prod_us_cities_table_create = ("""CREATE TABLE IF NOT EXISTS prod_us_cities 
 (
    state_id       varchar(10),
    state_name     varchar(50), 
    county         varchar(50),
    city           varchar(50),
    PRIMARY KEY(state_id, county, city)
)
""")

prod_demographics_table_create = ("""CREATE TABLE IF NOT EXISTS prod_us_demographics 
(
    state_id       varchar(10),
    state_name     varchar(50),
    county         varchar(50),
    race           varchar(50),
    gender         varchar(10),
    population     int,
    year           varchar(50),
    PRIMARY KEY(state_id, county, race, gender, year)
)
""")

prod_unemployment_table_create = ("""CREATE TABLE IF NOT EXISTS prod_unemployment 
(
    state_id               varchar(50),
    county                 varchar(50),
    area_type              varchar(10),
    unemployment_count     int,
    unemployment_rate      float,
    PRIMARY KEY(state_id, county, area_type)
)
""")

############## Prod data ##############

############## Prod Transform Statements ##############
prod_police_shootings_transformation = ("""
SELECT
    id,
    name,
    date,
    CASE
        WHEN armed_with IS NULL THEN 'unknown'
        ELSE armed_with
    END AS armed_with,
    age,
    CASE
        WHEN gender = 'male' THEN 'Male'
        ELSE 'Female'
    END AS gender,
    CASE
        WHEN race = 'A' THEN 'Asian'
        WHEN race = 'B' THEN 'Black'
        WHEN race = 'N' THEN 'Native'
        WHEN race = 'H' THEN 'Hispanic'
        WHEN race = 'W' THEN 'White'
        WHEN race = 'O' THEN 'Other'
        ELSE 'Not Documented'
    END AS race,
    city,
    state AS state_id,
    mental_illness,
    threat_level,
    CASE
        WHEN flee IS NULL THEN 'N/A'
        ELSE flee
    END AS flee,
    body_camera,
    agency_ids
FROM    
  stage_police_shootings
""")

prod_police_agencies_transformation = ("""
SELECT
    id,
    name,
    type,
    state,
    total_shootings
FROM stage_police_agencies
""")

prod_us_cities_transformation = ("""
SELECT 
    state_id,
    state_name,
    county_name AS county,
    city
FROM stage_us_cities
""")

prod_us_demographics_transformation = ("""
SELECT 
    DISTINCT
    suc.state_id, 
    suc.state_name,
    county,
    race,
    gender,
    sub.population,
    year
FROM (
    SELECT 
        state_name,
        regexp_replace(county_name, 'County', '') as county,                              
        CASE 
            WHEN race like 'AMERICAN INDIAN%' then 'American Indian'
            WHEN race like 'SOME OTHER RACE%' then 'Other'
            WHEN race like 'WHITE%' then 'White'
            WHEN race like 'ASIAN%' then 'Asian'
            WHEN race like 'NATIVE HAWAIIAN%' then 'Native Hawaiian'
            WHEN race like 'TWO OR MORE%' then 'Mixed'
            WHEN race like 'BLACK%' then 'African American'
        END as race,
        sex as gender,
        sum(CAST (population AS integer)) AS population,
        year
        FROM stage_us_demographics AS sud
        WHERE
             race is NOT NULL 
             AND sex is NOT NULL 
             AND min_age is NOT NULL 
             AND max_age is NOT NULL
             AND state_name NOT in ('Puerto Rico')
        GROUP BY state_name, county_name, sex, race, year
) AS sub
JOIN stage_us_cities AS suc
ON sub.state_name = suc.state_name  
""")

prod_unemployment_transformation = ("""
SELECT 
    DISTINCT
    state_id, 
    REPLACE(area_name, ' County', '') AS county,
    area_type,
    CAST(unemployment_count AS int)   AS unemployment_count,
    ROUND(CAST(unemployment_rate AS float), 1) AS unemployment_rate
FROM stage_unemployment
""")

############## Prod Insert Statements ##############

# QUERY LISTS
create_stage_table_queries = [stage_police_shootings_table_create, stage_police_agencies_table_create, stage_us_cities_table_create, stage_demographics_table_create, stage_unemployment_table_create]

## Fact table should be the last one to be created
create_prod_table_queries = [prod_police_agencies_table_create, prod_us_cities_table_create, prod_demographics_table_create, prod_unemployment_table_create, prod_police_shootings_table_create]

drop_stage_table_queries = [stage_police_shootings_table_drop, stage_police_agencies_table_drop, stage_unemployment_table_drop]

## Fact table should be the last one to be dropped
drop_prod_table_queries = [prod_police_agencies_table_drop, prod_unemployment_table_drop, prod_police_shootings_table_drop]

## Order should match the order of prod_tables
stage_tables=['stage_police_agencies','stage_us_cities', 'stage_us_demographics', 'stage_unemployment', 'stage_police_shootings']

## Fact table should be last to be populated
prod_tables=['prod_police_agencies', 'prod_us_cities', 'prod_us_demographics', 'prod_unemployment', 'prod_police_shootings']

## Order should match the order of prod_tables
transformation_queries = [prod_police_agencies_transformation, prod_us_cities_transformation, prod_us_demographics_transformation, prod_unemployment_transformation, prod_police_shootings_transformation]

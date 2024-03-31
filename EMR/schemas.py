from pyspark.sql.types import StructType, StructField, StringType, \
                              DateType, IntegerType, BooleanType, FloatType

stage_police_shootings_schema = StructType([
    StructField('id',                 IntegerType(), False),
    StructField('date',               DateType(),    True),
    StructField('threat_level',       StringType(),  True),
    StructField('flee',               StringType(),  True),
    StructField('armed_with',         StringType(),  True),
    StructField('city',               StringType(),  True),
    StructField('county',             StringType(),  True),
    StructField('state',              StringType(),  True),
    StructField('longitude',          FloatType(),   True),
    StructField('latitude',           FloatType(),   True),
    StructField('location_precision', StringType(),  True),
    StructField('name',               StringType(),  True),
    StructField('age',                IntegerType(), True),
    StructField('gender',             StringType(),  True),
    StructField('race',               StringType(),  True),
    StructField('race_source',        StringType(),  True),
    StructField('mental_illness',     BooleanType(), True),
    StructField('body_camera',        BooleanType(), True),
    StructField('agency_ids',         StringType(),  True)
])

stage_police_agencies_schema = StructType([
    StructField('id',              IntegerType(), False),
    StructField('name',            StringType(),  True),
    StructField('type',            StringType(),  True),
    StructField('state',           StringType(),  True),
    StructField('oricodes',        StringType(),  True),
    StructField('total_shootings', IntegerType(), True)

])
stage_us_cities_schema = StructType([
    StructField('city',          StringType(), True),
    StructField('city_ascii',    StringType(), True),    
    StructField('state_id',      StringType(), True),
    StructField('state_name',    StringType(), True),
    StructField('county_fips',   StringType(), True),
    StructField('county_name',   StringType(), True),
    StructField('lat',           StringType(), True),
    StructField('lng',           StringType(), True),
    StructField('population',    StringType(), True),
    StructField('density',       StringType(), True),
    StructField('source',        StringType(), True),
    StructField('military',      StringType(), True),
    StructField('incorporated',  StringType(), True),
    StructField('timezone',      StringType(), True),
    StructField('ranking',       StringType(), True),
    StructField('zips',          StringType(), True),
    StructField('id',            StringType(), True)
])

stage_us_demographics_schema = StructType([
    StructField('state_name',  StringType(),  True),
    StructField('county_name', StringType(),  True),
    StructField('population',  StringType(),  True),
    StructField('race',        StringType(),  True),
    StructField('sex',         StringType(),  True),
    StructField('min_age',     StringType(),  True),    
    StructField('max_age',     StringType(),  True), 
    StructField('year',        StringType(),  True)
])

stage_unemployment_schema = StructType([
    StructField('area_id',            StringType(),  True),    
    StructField('area_name',          StringType(),  True),
    StructField('area_type',          StringType(),  True),    
    StructField('stfips',             StringType(),  True),    
    StructField('unemployment_count', StringType(),  True),
    StructField('unemployment_rate',  StringType(),  True),
    StructField('state_id',           StringType(),  True)
])

raw_unemployment_schema = StructType([
    StructField('AreaID',            StringType(),  True),
    StructField('AreaName',          StringType(),  True),
    StructField('AreaType',          StringType(),  True),
    StructField('Stfips',            StringType(),  True),    
    StructField('UnEmpCount',        StringType(),  True),
    StructField('UnEmpRate',         StringType(),  True),
    StructField('StateID',           StringType(),  True)
])

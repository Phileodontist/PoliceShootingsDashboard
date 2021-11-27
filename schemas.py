from pyspark.sql.types import StructType, StructField, StringType, \
                              DateType, IntegerType, BooleanType, FloatType

stage_police_shootings_schema = StructType([
    StructField('id',                StringType(), True),
    StructField('name',              StringType(),  True),
    StructField('date',              StringType(),    True),
    StructField('manner_of_death',   StringType(),  True),
    StructField('armed',             StringType(),  True),
    StructField('age',               StringType(), True),
    StructField('gender',            StringType(),  True),
    StructField('race',              StringType(),  True),
    StructField('city',              StringType(),  True),
    StructField('state',             StringType(),  True),
    StructField('signs_of_mental_illness', StringType(), True),
    StructField('threat_level',      StringType(),  True),
    StructField('flee',              StringType(),  True),
    StructField('body_camera',       StringType(), True),
    StructField('longitude',         StringType(),   True),
    StructField('latitude',          StringType(),   True),
    StructField('is_geocoding_exact', StringType(), True)
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
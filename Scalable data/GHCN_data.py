
###########################################################################################################################################################
#code_schema_loading

# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
import pyspark.sql.functions as F


# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
##Processing Q2 (a) and (b) and (c)

##########################################################################
#to create and schema and load the daily data.
schema_daily = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("element", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("measurement_flag", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("source_flag", StringType(), True),
    StructField("obs_time", StringType(), True),
    
])

daily = (
    spark.read.format("com.databricks.spark.csv")
	.option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")	
	.limit(1000)
)

daily.cache()
daily.show()

#+-----------+--------+-------+-----+----------------+------------+-----------+--------+
#|         id|    date|element|value|measurement_flag|quality_flag|source_flag|obs_time|
#+-----------+--------+-------+-----+----------------+------------+-----------+--------+
#|US1FLSL0019|20200101|   PRCP|  0.0|            null|        null|          N|    null|
#|US1FLSL0019|20200101|   SNOW|  0.0|            null|        null|          N|    null|
#|US1NVNY0012|20200101|   PRCP|  0.0|            null|        null|          N|    null|
#|US1NVNY0012|20200101|   SNOW|  0.0|            null|        null|          N|    null|
#|US1ILWM0012|20200101|   PRCP|  0.0|            null|        null|          N|    null|
#|USS0018D08S|20200101|   TMAX| 46.0|            null|        null|          T|    null|
#|USS0018D08S|20200101|   TMIN|  6.0|            null|        null|          T|    null|
#|USS0018D08S|20200101|   TOBS|  6.0|            null|        null|          T|    null|
#|USS0018D08S|20200101|   PRCP| 76.0|            null|        null|          T|    null|
#|USS0018D08S|20200101|   SNWD|  0.0|            null|        null|          T|    null|
#|USS0018D08S|20200101|   TAVG| 23.0|            null|        null|          T|    null|
#|USS0018D08S|20200101|   WESD|127.0|            null|        null|          T|    null|
#|USC00141761|20200101|   TMAX| 39.0|            null|        null|          7|    0700|
#|USC00141761|20200101|   TMIN|-50.0|            null|        null|          7|    0700|
#|USC00141761|20200101|   TOBS|-17.0|            null|        null|          7|    0700|
#|USC00141761|20200101|   PRCP|  0.0|            null|        null|          7|    0700|
#|USC00141761|20200101|   SNOW|  0.0|            null|        null|          7|    null|
#|USC00141761|20200101|   SNWD|  0.0|            null|        null|          7|    0700|
#|USC00141761|20200101|   WESD|  0.0|            null|        null|          7|    null|
#|RQC00660061|20200101|   TMAX|289.0|            null|        null|          7|    0800|
#+-----------+--------+-------+-----+----------------+------------+-----------+--------+
#only showing top 20 rows


##########################################################################
#to create and schema and load the countries data.


countries = (
	spark.read.format('text')
	.option("header", "false")
    .option("inferSchema", "false")
    #.schema(schema_countries)
    .load("hdfs:///data/ghcnd/countries")
)
schema_countries = countries.select(
    countries.value.substr(1,2).alias('code'),
    countries.value.substr(4,47).alias('countries_name'),
    
)
schema_countries.cache()
schema_countries.show()

#+----+--------------------+
#|code|      countries_name|
#+----+--------------------+
#|  AC|Antigua and Barbuda |
#|  AE|United Arab Emira...|
#|  AF|         Afghanistan|
#|  AG|            Algeria |
#|  AJ|         Azerbaijan |
#|  AL|             Albania|
#|  AM|            Armenia |
#|  AO|             Angola |
#|  AQ|American Samoa [U...|
#|  AR|          Argentina |
#|  AS|          Australia |
#|  AU|            Austria |
#|  AY|         Antarctica |
#|  BA|            Bahrain |
#|  BB|           Barbados |
#|  BC|           Botswana |
#|  BD|Bermuda [United K...|
#|  BE|            Belgium |
#|  BF|       Bahamas, The |
#|  BG|          Bangladesh|
#+----+--------------------+
#only showing top 20 rows
#

##########################################################################
#to create and schema and load the states data.


states = (
	spark.read.format('text')
	.option("header", "false")
    .option("inferSchema", "false")
    #.schema(schema_states)
    .load("hdfs:///data/ghcnd/states")
)
schema_states = states.select(
    states.value.substr(1,2).alias('code'),
    states.value.substr(4,47).alias('state_name'),
    
)
schema_states.cache()
schema_states.show()
#+----+--------------------+
#|code|          state_name|
#+----+--------------------+
#|  AB|             ALBERTA|
#|  AK|              ALASKA|
#|  AL|ALABAMA          ...|
#|  AR|            ARKANSAS|
#|  AS|      AMERICAN SAMOA|
#|  AZ|             ARIZONA|
#|  BC|    BRITISH COLUMBIA|
#|  CA|          CALIFORNIA|
#|  CO|            COLORADO|
#|  CT|         CONNECTICUT|
#|  DC|DISTRICT OF COLUMBIA|
#|  DE|            DELAWARE|
#|  FL|             FLORIDA|
#|  FM|          MICRONESIA|
#|  GA|             GEORGIA|
#|  GU|                GUAM|
#|  HI|              HAWAII|
#|  IA|                IOWA|
#|  ID|               IDAHO|
#|  IL|            ILLINOIS|
#+----+--------------------+
#only showing top 20 rows


##########################################################################
#to create and schema and load the states data.

stations = (
	spark.read.format('text')
	.option("header", "false")
    .option("inferSchema", "false")
    #.schema(schema_stations)
    .load("hdfs:///data/ghcnd/stations")
)
schema_stations = stations.select(
    stations.value.substr(1,11).alias('id'),
    stations.value.substr(13,8).cast('float').alias('latitude'),
	stations.value.substr(22,9).cast('float').alias('longitude'),
	stations.value.substr(32,6).cast('float').alias('elevation'),
	stations.value.substr(39,2).alias('state'),
	stations.value.substr(42,30).alias('name'),
	stations.value.substr(73,3).alias('gsn_flag'),
	stations.value.substr(77,3).alias('hcn_crn_flag'),
	stations.value.substr(81,5).alias('wmo_id'),
	
	
    
)
schema_stations.cache()
schema_stations.show()

#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
#|         id|latitude|longitude|elevation|state|                name|gsn_flag|hcn_crn_flag|wmo_id|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
#|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |
#|ACW00011647| 17.1333| -61.7833|     19.2|     |ST JOHNS         ...|        |            |      |
#|AE000041196|  25.333|   55.517|     34.0|     |SHARJAH INTER. AI...|     GSN|            | 41196|
#|AEM00041194|  25.255|   55.364|     10.4|     |DUBAI INTL       ...|        |            | 41194|
#|AEM00041217|  24.433|   54.651|     26.8|     |ABU DHABI INTL   ...|        |            | 41217|
#|AEM00041218|  24.262|   55.609|    264.9|     |AL AIN INTL      ...|        |            | 41218|
#|AF000040930|  35.317|   69.017|   3366.0|     |NORTH-SALANG     ...|     GSN|            | 40930|
#|AFM00040938|   34.21|   62.228|    977.2|     |HERAT            ...|        |            | 40938|
#|AFM00040948|  34.566|   69.212|   1791.3|     |KABUL INTL       ...|        |            | 40948|
#|AFM00040990|    31.5|    65.85|   1010.0|     |KANDAHAR AIRPORT ...|        |            | 40990|
#|AG000060390| 36.7167|     3.25|     24.0|     |ALGER-DAR EL BEID...|     GSN|            | 60390|
#|AG000060590| 30.5667|   2.8667|    397.0|     |EL-GOLEA         ...|     GSN|            | 60590|
#|AG000060611|   28.05|   9.6331|    561.0|     |IN-AMENAS        ...|     GSN|            | 60611|
#|AG000060680|    22.8|   5.4331|   1362.0|     |TAMANRASSET      ...|     GSN|            | 60680|
#|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |
#|AGE00147704|   36.97|     7.79|    161.0|     |ANNABA-CAP DE GAR...|        |            |      |
#|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |
#|AGE00147706|    36.8|     3.03|    344.0|     |ALGIERS-BOUZAREAH...|        |            |      |
#|AGE00147707|    36.8|     3.04|     38.0|     |ALGIERS-CAP CAXIN...|        |            |      |
#|AGE00147708|   36.72|     4.05|    222.0|     |TIZI OUZOU       ...|        |            | 60395|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+


##########################################################################
#to create and schema and load the inventory data.


inventory = (
	spark.read.format('text')
	.option("header", "false")
    .option("inferSchema", "false")
    #.schema(schema_inventory)
    .load("hdfs:///data/ghcnd/inventory")
)
schema_inventory = inventory.select(
    inventory.value.substr(1,11).alias('id'),
    inventory.value.substr(13,8).cast('float').alias('latitude'),
	inventory.value.substr(22,9).cast('float').alias('longitude'),
	inventory.value.substr(32,4).alias('element'),
	inventory.value.substr(37,4).cast('integer').alias('firstyear'),
	inventory.value.substr(42,4).cast('integer').alias('lastyear'),

    
)
schema_inventory.cache()
schema_inventory.show()
#
#+-----------+--------+---------+-------+---------+--------+
#|         id|latitude|longitude|element|firstyear|lastyear|
#+-----------+--------+---------+-------+---------+--------+
#|ACW00011604| 17.1167| -61.7833|   TMAX|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   TMIN|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   PRCP|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   SNOW|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   SNWD|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   PGTM|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   WDFG|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   WSFG|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   WT03|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   WT08|     1949|    1949|
#|ACW00011604| 17.1167| -61.7833|   WT16|     1949|    1949|
#|ACW00011647| 17.1333| -61.7833|   TMAX|     1961|    1961|
#|ACW00011647| 17.1333| -61.7833|   TMIN|     1961|    1961|
#|ACW00011647| 17.1333| -61.7833|   PRCP|     1957|    1970|
#|ACW00011647| 17.1333| -61.7833|   SNOW|     1957|    1970|
#|ACW00011647| 17.1333| -61.7833|   SNWD|     1957|    1970|
#|ACW00011647| 17.1333| -61.7833|   WT03|     1961|    1961|
#|ACW00011647| 17.1333| -61.7833|   WT16|     1961|    1966|
#|AE000041196|  25.333|   55.517|   TMAX|     1944|    2019|
#|AE000041196|  25.333|   55.517|   TMIN|     1944|    2020|
#+-----------+--------+---------+-------+---------+--------+
#only showing top 20 rows



##########################################################################

schema_inventory.registerTempTable('inventory_tbl')
schema_stations.registerTempTable('stations_tbl')
schema_states.registerTempTable('states_tbl')
schema_countries.registerTempTable('countries_tbl')
daily.registerTempTable('daily_tbl')


#(c)How many rows are in each metadata table? How many stations do not have a WMO ID?

schema_countries.count()
#219
schema_inventory.count()
#687141
schema_states.count()
#74
schema_stations.count()
#115081





count_wmo= schema_stations.filter(F.col('wmo_id')=='     ')
count_wmo.show()
count_wmo.count()
#
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+
#|         id|latitude|longitude|elevation|state|                name|gsn_flag|hcn_crn_flag|wmo_id|countrycode|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+
#|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |         AC|
#|ACW00011647| 17.1333| -61.7833|     19.2|     |ST JOHNS         ...|        |            |      |         AC|
#|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |         AG|
#|AGE00147704|   36.97|     7.79|    161.0|     |ANNABA-CAP DE GAR...|        |            |      |         AG|
#|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |         AG|
#|AGE00147706|    36.8|     3.03|    344.0|     |ALGIERS-BOUZAREAH...|        |            |      |         AG|
#|AGE00147707|    36.8|     3.04|     38.0|     |ALGIERS-CAP CAXIN...|        |            |      |         AG|
#|AGE00147709|   36.63|      4.2|    942.0|     |FORT NATIONAL    ...|        |            |      |         AG|
#|AGE00147711| 36.3697|     6.62|    660.0|     |CONSTANTINE      ...|        |            |      |         AG|
#|AGE00147712|   36.17|     1.34|    112.0|     |ORLEANSVILLE (CHL...|        |            |      |         AG|
#|AGE00147713|   36.18|      5.4|   1081.0|     |SETIF            ...|        |            |      |         AG|
#|AGE00147714|   35.77|      0.8|     78.0|     |ORAN-CAP FALCON  ...|        |            |      |         AG|
#|AGE00147715|   35.42|   8.1197|    863.0|     |TEBESSA          ...|        |            |      |         AG|
#|AGE00147717|    35.2|     0.63|    476.0|     |SIDI-BEL-ABBES   ...|        |            |      |         AG|
#|AGE00147720|   33.68|      1.0|   1320.0|     |GERYVILLE (EL-BAY...|        |            |      |         AG|
#|AGE00147780|   37.08|     6.47|    195.0|     |SKIKDA-CAP BOUGAR...|        |            |      |         AG|
#|AGE00147794|   36.78|      5.1|    225.0|     |BEJAIA-CAP CARBON...|        |            |      |         AG|
#|ALE00100939| 41.3331|  19.7831|     89.0|     |TIRANA           ...|        |            |      |         AL|
#|ALE00108905|    42.1|  19.5331|     43.0|     |SHKODRA          ...|        |            |      |         AL|
#|AQC00914000|-14.3167|-170.7667|    408.4|   AS|AASUFOU          ...|        |            |      |         AQ|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+
#only showing top 20 rows
#
#106988

##########################################################################
##########################################################################

###########################################################################################################################################################
#Processing 3

#3a) Extract the two character country code from each station code in stations and store the output as a new column using the withColumn command.

schema_stations =schema_stations.withColumn("countrycode",F.substring(F.col("id"),1,2))
schema_stations.show()

#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+
#|         id|latitude|longitude|elevation|state|                name|gsn_flag|hcn_crn_flag|wmo_id|countrycode|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+
#|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |         AC|
#|ACW00011647| 17.1333| -61.7833|     19.2|     |ST JOHNS         ...|        |            |      |         AC|
#|AE000041196|  25.333|   55.517|     34.0|     |SHARJAH INTER. AI...|     GSN|            | 41196|         AE|
#|AEM00041194|  25.255|   55.364|     10.4|     |DUBAI INTL       ...|        |            | 41194|         AE|
#|AEM00041217|  24.433|   54.651|     26.8|     |ABU DHABI INTL   ...|        |            | 41217|         AE|
#|AEM00041218|  24.262|   55.609|    264.9|     |AL AIN INTL      ...|        |            | 41218|         AE|
#|AF000040930|  35.317|   69.017|   3366.0|     |NORTH-SALANG     ...|     GSN|            | 40930|         AF|
#|AFM00040938|   34.21|   62.228|    977.2|     |HERAT            ...|        |            | 40938|         AF|
#|AFM00040948|  34.566|   69.212|   1791.3|     |KABUL INTL       ...|        |            | 40948|         AF|
#|AFM00040990|    31.5|    65.85|   1010.0|     |KANDAHAR AIRPORT ...|        |            | 40990|         AF|
#|AG000060390| 36.7167|     3.25|     24.0|     |ALGER-DAR EL BEID...|     GSN|            | 60390|         AG|
#|AG000060590| 30.5667|   2.8667|    397.0|     |EL-GOLEA         ...|     GSN|            | 60590|         AG|
#|AG000060611|   28.05|   9.6331|    561.0|     |IN-AMENAS        ...|     GSN|            | 60611|         AG|
#|AG000060680|    22.8|   5.4331|   1362.0|     |TAMANRASSET      ...|     GSN|            | 60680|         AG|
#|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |         AG|
#|AGE00147704|   36.97|     7.79|    161.0|     |ANNABA-CAP DE GAR...|        |            |      |         AG|
#|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |         AG|
#|AGE00147706|    36.8|     3.03|    344.0|     |ALGIERS-BOUZAREAH...|        |            |      |         AG|
#|AGE00147707|    36.8|     3.04|     38.0|     |ALGIERS-CAP CAXIN...|        |            |      |         AG|
#|AGE00147708|   36.72|     4.05|    222.0|     |TIZI OUZOU       ...|        |            | 60395|         AG|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+



#schema_stations.registerTempTable('stations_tbl')


#3b LEFT JOIN stations with countries using your output from part (a)
#spark.sql("select * from stations_tbl a left join countries_tbl b on a.countrycode= b.code ").show()

schema_stations_country= (schema_stations.join(schema_countries, schema_stations.countrycode == schema_countries.code,how='left').drop("code"))
schema_stations_country.show()
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+
#|         id|latitude|longitude|elevation|state|                name|gsn_flag|hcn_crn_flag|wmo_id|countrycode|      countries_name|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+
#|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |         AC|Antigua and Barbuda |
#|ACW00011647| 17.1333| -61.7833|     19.2|     |ST JOHNS         ...|        |            |      |         AC|Antigua and Barbuda |
#|AE000041196|  25.333|   55.517|     34.0|     |SHARJAH INTER. AI...|     GSN|            | 41196|         AE|United Arab Emira...|
#|AEM00041194|  25.255|   55.364|     10.4|     |DUBAI INTL       ...|        |            | 41194|         AE|United Arab Emira...|
#|AEM00041217|  24.433|   54.651|     26.8|     |ABU DHABI INTL   ...|        |            | 41217|         AE|United Arab Emira...|
#|AEM00041218|  24.262|   55.609|    264.9|     |AL AIN INTL      ...|        |            | 41218|         AE|United Arab Emira...|
#|AF000040930|  35.317|   69.017|   3366.0|     |NORTH-SALANG     ...|     GSN|            | 40930|         AF|         Afghanistan|
#|AFM00040938|   34.21|   62.228|    977.2|     |HERAT            ...|        |            | 40938|         AF|         Afghanistan|
#|AFM00040948|  34.566|   69.212|   1791.3|     |KABUL INTL       ...|        |            | 40948|         AF|         Afghanistan|
#|AFM00040990|    31.5|    65.85|   1010.0|     |KANDAHAR AIRPORT ...|        |            | 40990|         AF|         Afghanistan|
#|AG000060390| 36.7167|     3.25|     24.0|     |ALGER-DAR EL BEID...|     GSN|            | 60390|         AG|            Algeria |
#|AG000060590| 30.5667|   2.8667|    397.0|     |EL-GOLEA         ...|     GSN|            | 60590|         AG|            Algeria |
#|AG000060611|   28.05|   9.6331|    561.0|     |IN-AMENAS        ...|     GSN|            | 60611|         AG|            Algeria |
#|AG000060680|    22.8|   5.4331|   1362.0|     |TAMANRASSET      ...|     GSN|            | 60680|         AG|            Algeria |
#|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |         AG|            Algeria |
#|AGE00147704|   36.97|     7.79|    161.0|     |ANNABA-CAP DE GAR...|        |            |      |         AG|            Algeria |
#|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |         AG|            Algeria |
#|AGE00147706|    36.8|     3.03|    344.0|     |ALGIERS-BOUZAREAH...|        |            |      |         AG|            Algeria |
#|AGE00147707|    36.8|     3.04|     38.0|     |ALGIERS-CAP CAXIN...|        |            |      |         AG|            Algeria |
#|AGE00147708|   36.72|     4.05|    222.0|     |TIZI OUZOU       ...|        |            | 60395|         AG|            Algeria |
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+
#only showing top 20 rows



#3c LEFT JOIN stations and states , allowing for the fact that state codes are only provided for stations in the US.

stations_Country_states= (schema_stations_country.join(schema_states, schema_stations_country.state == schema_states.code,how='left').drop("code"))
stations_Country_states.show()
stations_Country_states.count()


#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+----------+
#|         id|latitude|longitude|elevation|state|                name|gsn_flag|hcn_crn_flag|wmo_id|countrycode|      countries_name|state_name|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+----------+
#|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |         AC|Antigua and Barbuda |      null|
#|ACW00011647| 17.1333| -61.7833|     19.2|     |ST JOHNS         ...|        |            |      |         AC|Antigua and Barbuda |      null|
#|AE000041196|  25.333|   55.517|     34.0|     |SHARJAH INTER. AI...|     GSN|            | 41196|         AE|United Arab Emira...|      null|
#|AEM00041194|  25.255|   55.364|     10.4|     |DUBAI INTL       ...|        |            | 41194|         AE|United Arab Emira...|      null|
#|AEM00041217|  24.433|   54.651|     26.8|     |ABU DHABI INTL   ...|        |            | 41217|         AE|United Arab Emira...|      null|
#|AEM00041218|  24.262|   55.609|    264.9|     |AL AIN INTL      ...|        |            | 41218|         AE|United Arab Emira...|      null|
#|AF000040930|  35.317|   69.017|   3366.0|     |NORTH-SALANG     ...|     GSN|            | 40930|         AF|         Afghanistan|      null|
#|AFM00040938|   34.21|   62.228|    977.2|     |HERAT            ...|        |            | 40938|         AF|         Afghanistan|      null|
#|AFM00040948|  34.566|   69.212|   1791.3|     |KABUL INTL       ...|        |            | 40948|         AF|         Afghanistan|      null|
#|AFM00040990|    31.5|    65.85|   1010.0|     |KANDAHAR AIRPORT ...|        |            | 40990|         AF|         Afghanistan|      null|
#|AG000060390| 36.7167|     3.25|     24.0|     |ALGER-DAR EL BEID...|     GSN|            | 60390|         AG|            Algeria |      null|
#|AG000060590| 30.5667|   2.8667|    397.0|     |EL-GOLEA         ...|     GSN|            | 60590|         AG|            Algeria |      null|
#|AG000060611|   28.05|   9.6331|    561.0|     |IN-AMENAS        ...|     GSN|            | 60611|         AG|            Algeria |      null|
#|AG000060680|    22.8|   5.4331|   1362.0|     |TAMANRASSET      ...|     GSN|            | 60680|         AG|            Algeria |      null|
#|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |         AG|            Algeria |      null|
#|AGE00147704|   36.97|     7.79|    161.0|     |ANNABA-CAP DE GAR...|        |            |      |         AG|            Algeria |      null|
#|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |         AG|            Algeria |      null|
#|AGE00147706|    36.8|     3.03|    344.0|     |ALGIERS-BOUZAREAH...|        |            |      |         AG|            Algeria |      null|
#|AGE00147707|    36.8|     3.04|     38.0|     |ALGIERS-CAP CAXIN...|        |            |      |         AG|            Algeria |      null|
#|AGE00147708|   36.72|     4.05|    222.0|     |TIZI OUZOU       ...|        |            | 60395|         AG|            Algeria |      null|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+----------+
#only showing top 20 rows
#
#115081

#3d Based on inventory , what was the first and last year that each station was active and collected any element at all?

#spark.sql("select id , min(firstyear), max(lastyear) from inventory_tbl group by id order by id desc").show()
(schema_inventory.groupby('id').agg(F.min('firstyear').alias('firstyear'),F.max('lastyear').alias('lastyear')).sort(F.col('id'),ascending =False)).show()

#+-----------+---------+--------+
#|         id|firstyear|lastyear|
#+-----------+---------+--------+
#|ZI000067991|     1951|    1990|
#|ZI000067983|     1951|    2019|
#|ZI000067977|     1968|    1990|
#|ZI000067975|     1951|    2020|
#|ZI000067969|     1962|    1990|
#|ZI000067965|     1963|    1990|
#|ZI000067964|     1962|    1968|
#|ZI000067889|     1951|    1990|
#|ZI000067867|     1951|    1989|
#|ZI000067865|     1951|    1990|
#|ZI000067861|     1963|    1990|
#|ZI000067853|     1962|    1990|
#|ZI000067843|     1968|    1990|
#|ZI000067789|     1962|    1990|
#|ZI000067781|     1951|    1990|
#|ZI000067779|     1962|    1990|
#|ZI000067775|     1956|    2020|
#|ZI000067765|     1951|    1990|
#|ZI000067761|     1963|    1990|
#|ZI000067755|     1964|    1990|
#+-----------+---------+--------+
#only showing top 20 rows

#How many different elements has each station collected overall?

spark.sql("select count(distinct(element)) as ELEMENT_COUNT, id from inventory_tbl group by id order by count(distinct(element)) desc").show()

schema_inventory.groupby('id').agg(F.countDistinct('element').alias('countofDistinct_Element')).sort(F.col('countofDistinct_Element'),ascending =False).show()

#+-----------+-----------------------+
#|         id|countofDistinct_Element|
#+-----------+-----------------------+
#|USW00014607|                     62|
#|USW00013880|                     61|
#|USW00023066|                     60|
#|USW00013958|                     59|
#|USW00093817|                     58|
#|USW00024121|                     58|
#|USW00093058|                     57|
#|USW00014944|                     57|
#|USW00024127|                     56|
#|USW00024157|                     56|
#|USW00024156|                     56|
#|USW00094849|                     54|
#|USW00025309|                     54|
#|USW00093822|                     54|
#|USW00094908|                     54|
#|USW00026510|                     54|
#|USW00014914|                     54|
#|USW00003813|                     53|
#|USW00013722|                     53|
#|USW00024232|                     52|
#+-----------+-----------------------+
#only showing top 20 rows

#the element mostly collected
schema_inventory.groupBy("element").count().orderBy("count", ascending=False).show(5)

#+-------+------+
#|element| count|
#+-------+------+
#|   PRCP|113070|
#|   SNOW| 66536|
#|   MDPR| 60775|
#|   SNWD| 58844|
#|   DAPR| 53484|
#+-------+------+
#only showing top 5 rows


#Further, count separately the number of core elements and the number of ”other” elements that each station has collected overall
(schema_inventory.filter(F.col("element").isin({'TMIN', 'TMAX','PRCP','SNOW', 'SNWD'})).groupby('id').agg(F.count('element').alias('count_CoreElement')).sort(F.col('count_CoreElement'),ascending =False)).show()
#
#+-----------+-----------------+
#|         id|count_CoreElement|
#+-----------+-----------------+
#|CA00613P001|                5|
#|CA007024320|                5|
#|CA006143069|                5|
#|CA006026852|                5|
#|CA006143083|                5|
#|CA006044298|                5|
#|CA006143722|                5|
#|CA006068158|                5|
#|CA006147188|                5|
#|CA006075594|                5|
#|CA006148120|                5|
#|CA00610FC98|                5|
#|CA006151064|                5|
#|CA006113329|                5|
#|CA006151689|                5|
#|CA006133121|                5|
#|CA0061519JM|                5|
#|CA006137161|                5|
#|CA006154142|                5|
#|CA007014160|                5|
#+-----------+-----------------+
#only showing top 20 rows

#count-115024


#other element
(schema_inventory.filter(~F.col("element").isin({'TMIN', 'TMAX','PRCP','SNOW', 'SNWD'})).groupby('id').agg(F.count('element').alias('count_OtherElement')).sort(F.col('count_OtherElement'),ascending =False)).show()

#+-----------+------------------+
#|         id|count_OtherElement|
#+-----------+------------------+
#|USW00014607|                57|
#|USW00013880|                56|
#|USW00023066|                55|
#|USW00013958|                54|
#|USW00093817|                53|
#|USW00024121|                53|
#|USW00014944|                52|
#|USW00093058|                52|
#|USW00024156|                51|
#|USW00024157|                51|
#|USW00024127|                51|
#|USW00094849|                49|
#|USW00094908|                49|
#|USW00025309|                49|
#|USW00093822|                49|
#|USW00026510|                49|
#|USW00014914|                49|
#|USW00013722|                48|
#|USW00003813|                48|
#|USW00014942|                47|
#+-----------+------------------

#How many stations collect all five core elements? How many only collected temperature?

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
invent = (schema_inventory.select(['id','firstyear','element','lastyear']).groupby('id').agg(F.collect_set('element').alias('set_of_element'),F.min('firstyear').alias('firstyear'),F.max('lastyear').alias('lastyear')))
invent.show()

#+-----------+--------------------+---------+--------+
#|         id|      set_of_element|firstyear|lastyear|
#+-----------+--------------------+---------+--------+
#|AGE00147719|[TMAX, TMIN, PRCP...|     1888|    2020|
#|AGM00060445|[TMAX, TMIN, PRCP...|     1957|    2020|
#|AJ000037679|              [PRCP]|     1959|    1987|
#|AJ000037831|              [PRCP]|     1955|    1987|
#|AJ000037981|              [PRCP]|     1959|    1987|
#|AJ000037989|[TMAX, TMIN, PRCP...|     1936|    2017|
#|ALE00100939|        [TMAX, PRCP]|     1940|    2000|
#|AM000037719|[TMAX, TMIN, PRCP...|     1912|    1992|
#|AM000037897|[TMAX, TMIN, PRCP...|     1936|    2020|
#|AQC00914873|[WT03, TMAX, TMIN...|     1955|    1967|
#|AR000000002|              [PRCP]|     1981|    2000|
#|AR000087007|[TMAX, TMIN, PRCP...|     1956|    2020|
#|AR000087374|[TMAX, TMIN, PRCP...|     1956|    2020|
#|AR000875850|[TMAX, TMIN, PRCP...|     1908|    2020|
#|ARM00087022|[TMAX, TMIN, PRCP...|     1973|    2020|
#|ARM00087480|[TMAX, TMIN, PRCP...|     1965|    2020|
#|ARM00087509|[TMAX, TMIN, PRCP...|     1973|    2020|
#|ARM00087532|[TMAX, TMIN, PRCP...|     1973|    2020|
#|ARM00087904|[TMAX, TMIN, PRCP...|     2003|    2020|
#|ASN00001003|              [PRCP]|     1909|    1940|
#+-----------+--------------------+---------+--------+
#only showing top 20 rows

##count of id with all 5 core elements
def func(x):
	count = 0
	for i in x:
		if (i == 'TMAX' or i == 'TMIN' or i == 'PRCP' or i == 'SNOW' or i == 'SNWD'):
			count = count+1
	if(count == 5 ):
		return 1
	else:
		return 0


udf1 = udf(lambda x: func(x),IntegerType())
invent_five = invent.withColumn('Element_Choice',udf1('set_of_element'))
invent_five.select('id').filter(invent_five.Element_Choice == 1).count()
invent_five.select('id','set_of_element').filter(invent_five.Element_Choice == 1).show()


#+-----------+--------------------+
#|         id|      set_of_element|
#+-----------+--------------------+
#|AQC00914873|[WT03, TMAX, TMIN...|
#|AYW00067402|[TMAX, TMIN, WT09...|
#|CA001013051|[TMAX, MDSF, TMIN...|
#|CA0010130MN|[TMAX, MDSF, TMIN...|
#|CA001017559|[TMAX, MDSF, TMIN...|
#|CA001025370|[TMAX, MDSF, TMIN...|
#|CA001025375|[TMAX, TMIN, PRCP...|
#|CA001025915|[TMAX, MDSF, TMIN...|
#|CA001025C70|[TMAX, MDSF, TMIN...|
#|CA001026270|[TMAX, TMIN, PRCP...|
#|CA00102PG75|[TMAX, TMIN, PRCP...|
#|CA001030605|[TMAX, MDSF, TMIN...|
#|CA001031110|[TMAX, TMIN, PRCP...|
#|CA001047670|[TMAX, MDSF, TMIN...|
#|CA0010551R8|[TMAX, MDSF, TMIN...|
#|CA001062251|[TMAX, TMIN, WSFG...|
#|CA001062544|[TMAX, TMIN, PRCP...|
#|CA001064321|[TMAX, MDSF, TMIN...|
#|CA001072692|[TMAX, MDSF, TMIN...|
#|CA001073L4F|[TMAX, MDSF, TMIN...|
#+-----------+--------------------+
#only showing top 20 rows
#count 20266


#count with onlt temp element


def func(x):
	count = 0
	for i in x:
		if (i != 'TMAX' and i != 'TMIN'):
			count = count+1
	if(count >0 ):
		return 0
	else:
		return 1


udf1 = udf(lambda x: func(x),IntegerType())
invent_temp = invent.withColumn('Element_Choice',udf1('set_of_element'))

invent_temp.select('id','set_of_element').filter(invent_temp.Element_Choice == 1).show()
invent_temp.select('id','set_of_element').filter(invent_temp.Element_Choice == 1).count()

#+-----------+--------------+
#|         id|set_of_element|
#+-----------+--------------+
#|CA002400571|  [TMAX, TMIN]|
#|CA006017400|  [TMAX, TMIN]|
#|FIE00142495|  [TMAX, TMIN]|
#|FIE00143270|  [TMAX, TMIN]|
#|FIE00143370|  [TMAX, TMIN]|
#|FIE00145606|  [TMAX, TMIN]|
#|FIE00146482|  [TMAX, TMIN]|
#|GLE00146928|  [TMAX, TMIN]|
#|GRE00105242|  [TMAX, TMIN]|
#|NLE00100542|  [TMAX, TMIN]|
#|NLE00113682|  [TMAX, TMIN]|
#|NOE00133990|  [TMAX, TMIN]|
#|NOE00134350|  [TMAX, TMIN]|
#|SWE00137023|  [TMAX, TMIN]|
#|SWE00137470|  [TMAX, TMIN]|
#|ASN00071031|  [TMAX, TMIN]|
#|CA001100001|  [TMAX, TMIN]|
#|CA002402352|  [TMAX, TMIN]|
#|CA006012500|  [TMAX, TMIN]|
#|FIE00143020|  [TMAX, TMIN]|
#+-----------+--------------+
#only showing top 20 rows
#
#count : 301





##e)LEFT JOIN stations and your output from part (d)
enrich_station = (
stations_Country_states.join(invent,on ='id',how='left')) 
enrich_station.show()

#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+--------------+--------------------+---------+--------+
#|         id|latitude|longitude|elevation|state|                name|gsn_flag|hcn_crn_flag|wmo_id|countrycode|      countries_name|    state_name|      set_of_element|firstyear|lastyear|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+--------------+--------------------+---------+--------+
#|AGE00147719| 33.7997|     2.89|    767.0|     |LAGHOUAT         ...|        |            | 60545|         AG|            Algeria |          null|[TMAX, TMIN, PRCP...|     1888|    2020|
#|AGM00060445|  36.178|    5.324|   1050.0|     |SETIF AIN ARNAT  ...|        |            | 60445|         AG|            Algeria |          null|[TMAX, TMIN, PRCP...|     1957|    2020|
#|AJ000037679|    41.1|     49.2|    -26.0|     |SIASAN'          ...|        |            | 37679|         AJ|         Azerbaijan |          null|              [PRCP]|     1959|    1987|
#|AJ000037831|    40.4|     47.0|    160.0|     |MIR-BASHIR       ...|        |            | 37831|         AJ|         Azerbaijan |          null|              [PRCP]|     1955|    1987|
#|AJ000037981|    38.9|     48.2|    794.0|     |JARDIMLY         ...|        |            | 37981|         AJ|         Azerbaijan |          null|              [PRCP]|     1959|    1987|
#|AJ000037989|    38.5|     48.9|    -22.0|     |ASTARA           ...|     GSN|            | 37989|         AJ|         Azerbaijan |          null|[TMAX, TMIN, PRCP...|     1936|    2017|
#|ALE00100939| 41.3331|  19.7831|     89.0|     |TIRANA           ...|        |            |      |         AL|             Albania|          null|        [TMAX, PRCP]|     1940|    2000|
#|AM000037719|    40.6|    45.35|   1834.0|     |CHAMBARAK        ...|        |            | 37719|         AM|            Armenia |          null|[TMAX, TMIN, PRCP...|     1912|    1992|
#|AM000037897|  39.533|   46.017|   1581.0|     |SISIAN           ...|        |            | 37897|         AM|            Armenia |          null|[TMAX, TMIN, PRCP...|     1936|    2020|
#|AQC00914873|  -14.35|-170.7667|     14.9|   AS|TAPUTIMU TUTUILA ...|        |            |      |         AQ|American Samoa [U...|AMERICAN SAMOA|[WT03, TMAX, TMIN...|     1955|    1967|
#|AR000000002|  -29.82|   -57.42|     75.0|     |BONPLAND         ...|        |            |      |         AR|          Argentina |          null|              [PRCP]|     1981|    2000|
#|AR000087007|   -22.1|    -65.6|   3479.0|     |LA QUIACA OBSERVA...|     GSN|            | 87007|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1956|    2020|
#|AR000087374| -31.783|  -60.483|     74.0|     |PARANA AERO      ...|     GSN|            | 87374|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1956|    2020|
#|AR000875850| -34.583|  -58.483|     25.0|     |BUENOS AIRES OBSE...|        |            | 87585|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1908|    2020|
#|ARM00087022|  -22.62|  -63.794|    449.0|     |GENERAL ENRIQUE M...|        |            | 87022|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1973|    2020|
#|ARM00087480| -32.904|  -60.785|     25.9|     |ROSARIO          ...|        |            | 87480|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1965|    2020|
#|ARM00087509| -34.588|  -68.403|    752.9|     |SAN RAFAEL       ...|        |            | 87509|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1973|    2020|
#|ARM00087532| -35.696|  -63.758|    139.9|     |GENERAL PICO     ...|        |            | 87532|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     1973|    2020|
#|ARM00087904| -50.267|   -72.05|    204.0|     |EL CALAFATE AERO ...|        |            | 87904|         AR|          Argentina |          null|[TMAX, TMIN, PRCP...|     2003|    2020|
#|ASN00001003|-14.1331| 126.7158|      5.0|     |PAGO MISSION     ...|        |            |      |         AS|          Australia |          null|              [PRCP]|     1909|    1940|
#+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+-----------+--------------------+--------------+--------------------+---------+--------+
#only showing top 20 rows
#
#
#In [81]: enrich_station.count()
#count:115081

enrich_station.write.mode('overwrite').parquet("hdfs:///user/sjs252/output/ghcnd/enriched_station.parquet")		  
	
##f()LEFT JOIN your 1000 rows subset of daily and your output from part (e).  Are there any stations in your subset of daily that are not in stations at all?

daily_sta = (daily.join(enrich_station,on = 'id' ,how='left'))	
daily_sta.show()

#registering tables to perform spaek sql command.

daily_sta.registerTempTable('daily_sta_tbl')
enrich_station.registerTempTable('stations_tbl')


spark.sql("select distinct(id) from daily_tbl MINUS select distinct(id) from stations_tbl").show()
#0





###########################################################################################################################################################
#Analysis 1


#a)How many stations are there in total?
#spark.sql("select count(distinct(id)) from stations_tbl").show()
(enrich_station.select('id').agg(F.countDistinct('id').alias('Count_id'))).show()

#115081

#How many stations have been active in 2020?
#spark.sql("select count(distinct(id)) from stations_tbl where lastyear = 2020 ").show()
(enrich_station.select('id').filter(F.col('lastyear') ==2020).agg(F.countDistinct('id').alias('Count_id'))).show()
#32850

##How many stations are in each of the GCOS Surface Network (GSN), 

#spark.sql("select count(id) as Count_id from stations_tbl where gsn_flag = 'GSN'").show()
(enrich_station.select('id').filter(F.col('gsn_flag') =='GSN').agg(F.count('id').alias('Count_id'))).show()
#991

#the US Historical Climatology Network (HCN)
#spark.sql("select count(id) as Count_id  from stations_tbl where hcn_crn_flag = 'HCN' ").show()
(enrich_station.select('id').filter(F.col('hcn_crn_flag') =='HCN').agg(F.count('id').alias('Count_id'))).show()
#1218

#and the US Climate Reference Network (CRN)?
#spark.sql("select count(id) as Count_id  from stations_tbl where hcn_crn_flag = 'CRN' ").show()
(enrich_station.select('id').filter(F.col('hcn_crn_flag') =='CRN').agg(F.count('id').alias('Count_id'))).show()
#233

#Are there any stations that are in more than one of these networks
#spark.sql("select count(id) as Count_id from stations_tbl where hcn_crn_flag in  ('HCN','CRN') and gsn_flag = 'GSN' ").show()
(enrich_station.select('id').filter((F.col('hcn_crn_flag').isin({'HCN','CRN'})) & (F.col('gsn_flag')=='GSN' )).agg(F.count('id').alias('Count_id'))).show()
#14





#(b)Count the total number of stations in each country, and store the output in countries using the withColumnRenamed command.



df_countries = schema_countries.join(schema_stations.groupby(F.col('countrycode')).agg(F.count('id').alias('Total_stations')),schema_countries.code==enrich_station.countrycode,how ="left")
df_countries = df_countries.withColumnRenamed("Total_stations","Number_of_STATIONS").sort(F.col('Number_of_STATIONS'),ascending =False)
df_countries.show()

#+----+--------------------+-----------+------------------+
#|code|      countries_name|countrycode|Number_of_STATIONS|
#+----+--------------------+-----------+------------------+
#|  US|      United States |         US|             61867|
#|  AS|          Australia |         AS|             17088|
#|  CA|             Canada |         CA|              8818|
#|  BR|              Brazil|         BR|              5989|
#|  MX|             Mexico |         MX|              5249|
#|  IN|              India |         IN|              3807|
#|  SW|             Sweden |         SW|              1721|
#|  SF|       South Africa |         SF|              1166|
#|  GM|            Germany |         GM|              1130|
#|  RS|             Russia |         RS|              1123|
#|  FI|            Finland |         FI|               922|
#|  NO|             Norway |         NO|               461|
#|  NL|        Netherlands |         NL|               386|
#|  KZ|         Kazakhstan |         KZ|               329|
#|  WA|            Namibia |         WA|               283|
#|  CH|             China  |         CH|               228|
#|  RQ|Puerto Rico [Unit...|         RQ|               211|
#|  SP|              Spain |         SP|               207|
#|  UP|            Ukraine |         UP|               204|
#|  JA|              Japan |         JA|               202|
#+----+--------------------+-----------+------------------+
#only showing top 20 rows



df_states = schema_states.join(schema_stations.groupby(F.col('state')).agg(F.count('id').alias('Total_stations')),schema_states.code==enrich_station.state,how ="left")
df_states = df_states.withColumnRenamed("Total_stations","Number_of_STATIONS").sort(F.col('Number_of_STATIONS'),ascending =False)
df_states.show()

#+----+----------------+-----+------------------+
#|code|      state_name|state|Number_of_STATIONS|
#+----+----------------+-----+------------------+
#|  TX|           TEXAS|   TX|              5037|
#|  CO|        COLORADO|   CO|              4176|
#|  CA|      CALIFORNIA|   CA|              2798|
#|  NC|  NORTH CAROLINA|   NC|              2159|
#|  NE|        NEBRASKA|   NE|              2090|
#|  KS|          KANSAS|   KS|              1900|
#|  NM|      NEW MEXICO|   NM|              1890|
#|  ON|         ONTARIO|   ON|              1818|
#|  IL|        ILLINOIS|   IL|              1783|
#|  OR|          OREGON|   OR|              1764|
#|  FL|         FLORIDA|   FL|              1744|
#|  BC|BRITISH COLUMBIA|   BC|              1683|
#|  IN|         INDIANA|   IN|              1618|
#|  NY|        NEW YORK|   NY|              1594|
#|  TN|       TENNESSEE|   TN|              1477|
#|  WA|      WASHINGTON|   WA|              1457|
#|  AZ|         ARIZONA|   AZ|              1453|
#|  MO|        MISSOURI|   MO|              1434|
#|  AB|         ALBERTA|   AB|              1420|
#|  PA|    PENNSYLVANIA|   PA|              1326|
#+----+----------------+-----+------------------+
#only showing top 20 rows


df_countries.write.mode('overwrite').parquet("hdfs:///user/sjs252/output/ghcnd/enriched_countries.parquet")		  
df_states.write.mode('overwrite').parquet("hdfs:///user/sjs252/output/ghcnd/enriched_states.parquet")		  


#c()How many stations are there in the Northern Hemisphere only
(enrich_station.filter(F.col('latitude') > 0).agg(F.countDistinct('id'))).show()
#89745


#Some of the countries in the database are territories of the United States as indicated by the name of the country. How many stations are there in total in the territories of the United States around the world?
df_countries_territory = df_countries.select('countries_name','Number_of_STATIONS').filter(F.col('countries_name').like("%[United States]%"))
df_countries_territory.show()
#+--------------------+------------------+
#|      countries_name|Number_of_STATIONS|
#+--------------------+------------------+
#|Puerto Rico [Unit...|               211|
#|Virgin Islands [U...|                43|
#|Guam [United Stat...|                21|
#|American Samoa [U...|                20|
#|Northern Mariana ...|                11|
#|Johnston Atoll [U...|                 4|
#|Palmyra Atoll [Un...|                 3|
#|Wake Island [Unit...|                 1|
#+--------------------+------------------+

df_countries_territory.groupby().sum('Number_of_STATIONS').show()
#314
###########################################################################################################################################################
#Analysis 2
#Q2
#(a)
#Write a Spark function that computes the geographical distance between two stations using their latitude and longitude as arguments. You can test this function by using CROSS JOIN on a small subset ofstations to generate a table with two stations in each row



from pyspark.sql import DataFrameWriter as W
from math import radians, cos, sin, asin, sqrt
spark = (SparkSession.builder
                      .appName("HDFS_Haversine_Fun")
                      .getOrCreate())
					  

def get_distance(longit_a, latit_a, longit_b, latit_b):
	# Transform to radians
	longit_a, latit_a, longit_b, latit_b = map(radians,[longit_a,latit_a,longit_b,latit_b])
	dist_longit = longit_b-longit_a
	dist_latit = latit_b-latit_a
	
	# Calculate area
	area = sin(dist_latit/2)**2 + cos(latit_a)*cos(latit_b)*sin(dist_longit/2)**2
	# Calculate the central angle
	central_angle = 2*asin(sqrt(area))
	radius = 6371
	# Calculate Distanced
	distance = central_angle*radius
	return abs(round(distance, 2))
	

udf_get_distance = F.udf(get_distance)

stations_20 = enrich_station.select('id','name','latitude','longitude').limit(20)
station_pairs = (stations_20.crossJoin(stations_20).toDF(
"STATION_ID_A", "STATION_NAME_A", "LATITUDE_A", "LONGITUDE_A","STATION_ID_B", "STATION_NAME_B", "LATITUDE_B", "LONGITUDE_B"))

station_pairs = (station_pairs.filter(
station_pairs.STATION_ID_A != station_pairs.STATION_ID_B))

station_pairs_distance = (station_pairs.withColumn("ABS_DISTANCE", udf_get_distance(station_pairs.LONGITUDE_A, station_pairs.LATITUDE_A,station_pairs.LONGITUDE_B, station_pairs.LATITUDE_B).cast(FloatType())))
station_pairs_distance.show()

#+------------+--------------------+----------+-----------+------------+--------------------+----------+-----------+------------+
#|STATION_ID_A|      STATION_NAME_A|LATITUDE_A|LONGITUDE_A|STATION_ID_B|      STATION_NAME_B|LATITUDE_B|LONGITUDE_B|ABS_DISTANCE|
#+------------+--------------------+----------+-----------+------------+--------------------+----------+-----------+------------+
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AGE00147718|BISKRA           ...|     34.85|       5.72|     5524.17|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AGM00060417|BOUIRA           ...|    36.383|      3.883|     5644.45|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AGM00060421|OUM EL BOUAGHI   ...|    35.867|      7.117|      5374.7|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AGM00060531|ZENATA           ...|    35.017|      -1.45|     6148.95|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AGM00060555|SIDI MAHDI       ...|    33.068|      6.089|     5541.85|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AJ000037756|MARAZA           ...|    40.533|     48.933|     1818.13|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AJ000037895|KHANKANDY        ...|    39.983|      46.75|     1959.08|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AJ000037923|ALAT             ...|    39.967|       49.4|     1753.74|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AM000037782|AMBERD (KOSHABULA...|      40.4|       44.3|     2169.65|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AM000037791|FANTAN           ...|      40.4|     44.683|     2139.29|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AM000037801|GAVAR            ...|     40.35|    45.1331|     2101.72|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AMM00037717|SEVAN OZERO      ...|    40.567|       45.0|     2121.07|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AO000066447|MAVINGA          ...|   -15.833|      20.35|     7158.77|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AQC00914594|MALAELOA         ...|  -14.3333|  -170.7667|    13980.03|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AQW00061705|PAGO PAGO WSO AP ...|  -14.3306|  -170.7136|     13984.9|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AR000087828|TRELEW AERO      ...|     -43.2|    -65.266|    15570.77|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| AR000087925|RIO GALLEGOS AERO...|   -51.617|    -69.283|    15756.54|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| ARM00087046|JUJUY            ...|   -24.393|    -65.098|    15171.91|
#| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85| ARM00087178|POSADAS          ...|   -27.386|    -55.971|    14428.26|
#| AGE00147718|BISKRA           ...|     34.85|       5.72| AFM00040990|KANDAHAR AIRPORT ...|      31.5|      65.85|     5524.17|
#+------------+--------------------+----------+-----------+------------+--------------------+----------+-----------+------------+
#only showing top 20 rows



#b()Apply this function to compute the pairwise distances between all stations in New Zealand, and save the result to your output directory. 
nz_stations = enrich_station.select('id','name','latitude','longitude').filter(F.col('countrycode') == 'NZ')
nz_station_pairs = (nz_stations.crossJoin(nz_stations).toDF(
"STATION_ID_A", "STATION_NAME_A", "LATITUDE_A", "LONGITUDE_A","STATION_ID_B", "STATION_NAME_B", "LATITUDE_B", "LONGITUDE_B"))

nz_station_pairs = (nz_station_pairs.filter(
nz_station_pairs.STATION_ID_A != nz_station_pairs.STATION_ID_B))

nz_station_pairs_distance = (nz_station_pairs.withColumn("ABS_DISTANCE", udf_get_distance(nz_station_pairs.LONGITUDE_A, nz_station_pairs.LATITUDE_A,nz_station_pairs.LONGITUDE_B, nz_station_pairs.LATITUDE_B).cast(FloatType())))
nz_station_pairs_distance.sort(F.col('ABS_DISTANCE'),ascending =False).show()

nz_station_pairs_distance.write.mode('overwrite').parquet("hdfs:///user/sjs252/output/ghcnd/nz_station_pairs_distance.parquet")		  

#What two stations are the geographically furthest apart in New Zealand?

#+------------+--------------------+----------+-----------+------------+--------------------+----------+-----------+------------+
#|STATION_ID_A|      STATION_NAME_A|LATITUDE_A|LONGITUDE_A|STATION_ID_B|      STATION_NAME_B|LATITUDE_B|LONGITUDE_B|ABS_DISTANCE|
#+------------+--------------------+----------+-----------+------------+--------------------+----------+-----------+------------+
#| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167|     2799.18|
#| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917|     2799.18|
#| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917| NZM00093929|ENDERBY ISLAND AW...|   -50.483|      166.3|     2705.42|
#| NZM00093929|ENDERBY ISLAND AW...|   -50.483|      166.3| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917|     2705.42|
#| NZ000093844|INVERCARGILL AIRP...|   -46.417|    168.333| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917|     2251.34|
#| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917| NZ000093844|INVERCARGILL AIRP...|   -46.417|    168.333|     2251.34|
#| NZ000937470|TARA HILLS       ...|   -44.517|      169.9| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917|     2008.89|
#| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917| NZ000937470|TARA HILLS       ...|   -44.517|      169.9|     2008.89|
#| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167| NZ000093012|KAITAIA          ...|     -35.1|    173.267|     1967.22|
#| NZ000093012|KAITAIA          ...|     -35.1|    173.267| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167|     1967.22|
#| NZM00093929|ENDERBY ISLAND AW...|   -50.483|      166.3| NZ000093012|KAITAIA          ...|     -35.1|    173.267|     1800.52|
#| NZ000093012|KAITAIA          ...|     -35.1|    173.267| NZM00093929|ENDERBY ISLAND AW...|   -50.483|      166.3|     1800.52|
#| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917| NZM00093781|CHRISTCHURCH INTL...|   -43.489|    172.532|     1796.56|
#| NZM00093781|CHRISTCHURCH INTL...|   -43.489|    172.532| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917|     1796.56|
#| NZ000936150|HOKITIKA AERODROM...|   -42.717|    170.983| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917|     1796.36|
#| NZ000093994|RAOUL ISL/KERMADE...|    -29.25|   -177.917| NZ000936150|HOKITIKA AERODROM...|   -42.717|    170.983|     1796.36|
#| NZM00093110|AUCKLAND AERO AWS...|     -37.0|      174.8| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167|     1783.96|
#| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167| NZM00093110|AUCKLAND AERO AWS...|     -37.0|      174.8|     1783.96|
#| NZ000093292|GISBORNE AERODROM...|    -38.65|    177.983| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167|     1687.99|
#| NZ000939450|CAMPBELL ISLAND A...|    -52.55|    169.167| NZ000093292|GISBORNE AERODROM...|    -38.65|    177.983|     1687.99|
#+------------+--------------------+----------+-----------+------------+--------------------+----------+-----------+------------+
#only showing top 20 rows

#RAOUL ISL/KERMADEC and CAMPBELL ISLAND AWS with a distance of 2799.18

###########################################################################################################################################################

#Analysis 3


#start_pyspark_shell -e 4 -c 2 -w 4 -m 4 
 
schema_daily_2020 = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("element", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("measurement_flag", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("source_flag", StringType(), True),
    StructField("obs_time", StringType(), True),
    
])

daily_2020 = (
    spark.read.format("com.databricks.spark.csv")
	.option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily_2020)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")	
)


daily_2020.count()





schema_daily_2015 = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("element", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("measurement_flag", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("source_flag", StringType(), True),
    StructField("obs_time", StringType(), True),
    
])

daily_2015 = (
    spark.read.format("com.databricks.spark.csv")
	.option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily_2015)
    .load("hdfs:///data/ghcnd/daily/2015.csv.gz")	
)
daily_2015.cache()

daily_2015.count()




schema_daily_2015_2020 = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("element", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("measurement_flag", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("source_flag", StringType(), True),
    StructField("obs_time", StringType(), True),
    
])

daily_2015_2020 = (
    spark.read.format("com.databricks.spark.csv")
	.option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily_2015_2020)
    .load("hdfs:///data/ghcnd/daily/{201[5-9],2020}.csv.gz")	
)
daily_2015_2020.cache()
daily_2015_2020.count()


daily_2015_2020.rdd.getNumPartitions()
#6

###########################################################################################################################################################
#Analysis 4


schema_daily = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("element", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("measurement_flag", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("source_flag", StringType(), True),
    StructField("obs_time", StringType(), True),
    
])

daily_1768_2020 = (
    spark.read.format("com.databricks.spark.csv")
	.option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/{*}.csv.gz")	
)


daily_1768_2020.count()
#2928664523

#b
daily_1768_2020_core = (daily_1768_2020.filter(F.col("element").isin({"TMIN", "TMAX","SNOW","PRCP","SNWD"})))

daily_1768_2020_core.count()
#2509690508

(daily_1768_2020_core.groupby(F.col("element")).agg(F.count("element"))).show()

#
#+-------+--------------+
#|element|count(element)|
#+-------+--------------+
#|   SNOW|     332430532|
#|   SNWD|     283572167|
#|   PRCP|    1021682210|
#|   TMIN|     435296249|
#|   TMAX|     436709350|
#+-------+--------------+
##PRCP has more 1021682210
#
#


#c


#Determine how many observations of TMIN do not have a corresponding observation of TMAX.
daily_1768_2020_c=(daily_1768_2020.filter(F.col("element").isin({"TMIN", "TMAX"})).groupby('id','date').agg(F.collect_set('element').alias('set_of_element')))

##function
def func(x):
	
	if(len(x) >1 ):
		return 0
	else:
		return 1


udf1 = udf(lambda x: func(x),IntegerType())
daily_1768_2020_c = daily_1768_2020_c.withColumn('Element_Count',udf1('set_of_element'))

daily_1768_2020_ca= daily_1768_2020_c.filter(daily_1768_2020_c.Element_Count == 1).groupby('set_of_element').agg(F.count('set_of_element'))
daily_1768_2020_ca.show()

#
#+--------------+---------------------+
#|set_of_element|count(set_of_element)|
#+--------------+---------------------+
#|        [TMIN]|              8428801|
#|        [TMAX]|              9841902|
#

#How many different stations contributed to these observations?


daily_1768_2020_cb= daily_1768_2020_c.filter(daily_1768_2020_c.Element_Count == 1).groupby('set_of_element').agg(F.countDistinct('id'))
daily_1768_2020_cb.show()




#In [34]: daily_1768_2020_cb.show()
#    ...:
#+--------------+------------------+
#|set_of_element|count(DISTINCT id)|
#+--------------+------------------+
#|        [TMIN]|             27526|
#|        [TMAX]|             28277|
#


#d)Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand, and save the result to your output directory



daily_1768_2020_d= (daily_1768_2020.filter(F.col("element").isin({"TMIN", "TMAX"}) &(F.substring(F.col("id"),1,2)=='NZ')))

daily_1768_2020_d.count()
# 458892

daily_1768_2020_d.write.mode('overwrite').option('header', 'false').csv("hdfs:///user/sjs252/output/ghcnd/daily_1768_2020_NZ.csv")


##[sjs252@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -copyToLocal /user/sjs252/output/ghcnd/daily_1768_2020_NZ.csv/


#	years covered  
daily_1768_2020_de=(daily_1768_2020_d.select(F.countDistinct(F.substring(F.col('date'),1,4)).alias('COUNT_YEAR')))
daily_1768_2020_de.show()
#81

#tmin#tmax joined with stations to get the station name.
daily_temp_NZ = (daily_1768_2020_d.select(['id', 'date', 'element', 'value']).join(enrich_station.select(['id','name']),daily_1768_2020_d.id == enrich_station.id, how = 'left_outer').drop(enrich_station.id))

daily_temp_NZ.write.mode('overwrite').option('header', 'false').csv("hdfs:///user/sjs252/output/ghcnd/daily_temp_NZ.csv")



##e
daily_prcp = (daily_1768_2020.filter(F.col("element")=="PRCP"))
daily_prcp_station = (daily_prcp.select(['id', 'date', 'element', 'value']).join(enrich_station.select(['id','countrycode','countries_name']),daily_prcp.id == enrich_station.id, how = 'left_outer').drop(daily_prcp.id))
daily_prcp_station = daily_prcp_station.withColumn("year",F.substring(F.col("date"),1,4))
daily_prcp_temp = daily_prcp_station.groupby('year','countries_name').agg(F.avg('value').alias('Average_rainfall')).sort(F.col('Average_rainfall'),ascending =False)
daily_prcp_temp.show()

#+----+--------------------+------------------+
#|year|      countries_name|  Average_rainfall|
#+----+--------------------+------------------+
#|2000|   Equatorial Guinea|            4361.0|
#|1975| Dominican Republic |            3414.0|
#|1974|               Laos |            2480.5|
#|1978|              Belize| 2244.714285714286|
#|1979|        Sint Maarten|            1967.0|
#|1974|         Costa Rica |            1820.0|
#|1979|              Belize|1755.5454545454545|
#|1973|            Suriname|            1710.0|
#|1978|             Curacao|1675.0384615384614|
#|1977|              Belize|1541.7142857142858|
#|1978|           Honduras |1469.6122448979593|
#|1977|             Curacao|1442.5384615384614|
#|1978|        Sint Maarten|1292.8695652173913|
#|1977|           Honduras | 1284.138888888889|
#|1978|Trinidad and Tobago |            1265.0|
#|1976|             Guyana |1213.3333333333333|
#|1979|             Curacao|            1168.2|
#|1973|            Tunisia |            1162.0|
#|2006|               Burma|            1152.0|
#|2001|   Equatorial Guinea|            1100.0|
#+----+--------------------+------------------+
#only showing top 20 rows

#to check the consistency of Equatorial Guinea

(daily_prcp_temp.filter(F.col('countries_name') == 'Equatorial Guinea')).show(10)
#+----+-----------------+------------------+
#|year|   countries_name|  Average_rainfall|
#+----+-----------------+------------------+
#|2000|Equatorial Guinea|            4361.0|
#|2001|Equatorial Guinea|            1100.0|
#|1997|Equatorial Guinea| 709.3636363636364|
#|1996|Equatorial Guinea| 576.1111111111111|
#|2016|Equatorial Guinea|124.85353535353535|
#|2011|Equatorial Guinea|          123.6875|
#|2017|Equatorial Guinea| 100.0909090909091|
#|2018|Equatorial Guinea| 91.25603864734299|
#|2012|Equatorial Guinea| 89.61842105263158|
#|2013|Equatorial Guinea| 89.27027027027027|
#+----+-----------------+------------------+

## to check the data for the year 2020 for Equatorial  Guinea

(daily_prcp_station.filter((F.col('year') =='2000') & (F.col('countries_name') =='Equatorial Guinea'))).show()
#
#+--------+-------+------+-----------+-----------+-----------------+----+
#|    date|element| value|         id|countrycode|   countries_name|year|
#+--------+-------+------+-----------+-----------+-----------------+----+
#|20000622|   PRCP|4361.0|EKM00064810|         EK|Equatorial Guinea|2000|
#+--------+-------+------+-----------+-----------+-----------------+----+


daily_prcp_temp.write.mode('overwrite').option('header', 'false').csv("hdfs:///user/sjs252/output/ghcnd/daily_prcp_temp.csv")

###########################################################################################################################################################
###########################################################################################################################################################
#Reference
##
#https://medium.com/@nikolasbielski/using-a-custom-udf-in-pyspark-to-compute-haversine-distances-d877b77b4b18
#https://stackoverflow.com/
#https://medium.com/the-die-is-forecast/animating-choropleth-maps-with-ggnaimate-visualizing-coup-risk-and-rainfall-forecasts-every-month-4c525a589567
#

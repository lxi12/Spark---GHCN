### Processing ###
#
## Q1 
#
# How is the data structed? Draw a directory tree
hdfs dfs -ls -h /data/ghcnd      #shared data directory
hdfs dfs -ls -R /data/ghcnd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
 
# How many years in daily total?     260
hdfs dfs -count -h /data/ghcnd/daily    
# How does the size of data change?
hdfs dfs -ls -R -h /data/ghcnd

# What is the total size of all data?
hdfs dfs -du -s -h -v -x /data/ghcnd
# How much of that is daily?
hdfs dfs -du -h /data/ghcnd	
 
 
# 
## Q2
#
start_pyspark_shell

import sys
import glob
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row, DataFrame, Window, functions as F
from pyspark.sql.types import * 

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
sc.getConf().getAll()


# (a) Define the schema
daily_schema = StructType([
  StructField("ID", StringType(), True),
  StructField("DATE", StringType(), True),
  StructField("ELEMENT", StringType(), True),
  StructField("VALUE", DoubleType(), True),
  StructField("MEASUREMENT_FLAG", StringType(), True),
  StructField("QUALITY_FLAG", StringType(), True),
  StructField("SOURCE_FLAG", StringType(), True),
  StructField("OBSERVATION_TIME", StringType(), True)
])


# (b) Load
daily1000 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:///data/ghcnd/daily/2022.csv.gz")
    .limit(1000)
)
daily1000.show()
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+
# |         ID|    DATE|ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+
# |AE000041196|20220101|   TAVG|204.0|               H|        null|          S|            null|
# |AEM00041194|20220101|   TAVG|211.0|               H|        null|          S|            null|
# |AEM00041218|20220101|   TAVG|207.0|               H|        null|          S|            null|
# |AEM00041217|20220101|   TAVG|209.0|               H|        null|          S|            null|
# |AG000060390|20220101|   TAVG|121.0|               H|        null|          S|            null|
# |AG000060590|20220101|   TAVG|151.0|               H|        null|          S|            null|
# |AG000060611|20220101|   TAVG|111.0|               H|        null|          S|            null|
# |AGE00147708|20220101|   TMIN| 73.0|            null|        null|          S|            null|
# |AGE00147708|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGE00147708|20220101|   TAVG|133.0|               H|        null|          S|            null|
# |AGE00147716|20220101|   TMIN|107.0|            null|        null|          S|            null|
# |AGE00147716|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGE00147716|20220101|   TAVG|133.0|               H|        null|          S|            null|
# |AGE00147718|20220101|   TMIN| 90.0|            null|        null|          S|            null|
# |AGE00147718|20220101|   TAVG|152.0|               H|        null|          S|            null|
# |AGE00147719|20220101|   TMAX|201.0|            null|        null|          S|            null|
# |AGE00147719|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGE00147719|20220101|   TAVG|119.0|               H|        null|          S|            null|
# |AGM00060351|20220101|   PRCP|  0.0|            null|        null|          S|            null|
# |AGM00060351|20220101|   TAVG|126.0|               H|        null|          S|            null|
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+


# (c)
# station
station = spark.read.text('hdfs:///data/ghcnd/ghcnd-stations.txt')
station = station.select(
    F.trim(station.value.substr(1,11)).alias('ID'),
    F.trim(station.value.substr(13,8)).cast('double').alias('LATITUDE'),
    F.trim(station.value.substr(22,9)).cast('double').alias('LONGITUDE'),
    F.trim(station.value.substr(32,6)).cast('double').alias('ELEVATION'),
    F.trim(station.value.substr(39,2)).alias('STATE'),
    F.trim(station.value.substr(42,30)).alias('NAME'),
    F.trim(station.value.substr(73,3)).alias('GSN_FLAG'),
    F.trim(station.value.substr(77,3)).alias('HCN/CRN_FLAG'),
    F.trim(station.value.substr(81,5)).alias('WMO_ID'))
    
station.cache()
station.show()
# +-----------+--------+---------+---------+-----+--------------------+--------+----------+------+
# |         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCNRN_FLAG|WMO_ID|
# +-----------+--------+---------+---------+-----+--------------------+--------+----------+------+
# |ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |          |      |
# |ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |          |      |
# |AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|          | 41196|
# |AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |          | 41194|
# |AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |          | 41217|
# |AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |          | 41218|
# |AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|          | 40930|
# |AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |          | 40938|
# |AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |          | 40948|
# |AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |          | 40990|
# |AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|          | 60390|
# |AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|          | 60590|
# |AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|          | 60611|
# |AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|          | 60680|
# |AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |          |      |
# |AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |          |      |
# |AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |          |      |
# |AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |          |      |
# |AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |          |      |
# |AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |          | 60395|
# +-----------+--------+---------+---------+-----+--------------------+--------+----------+------+


# country
country = spark.read.text("hdfs:///data/ghcnd/ghcnd-countries.txt")
country = country.select(
    F.trim(country.value.substr(1,2)).alias("CODE"),
    F.trim(country.value.substr(4,47)).alias("NAME"))

country.cache()
country.show()
# +----+--------------------+
# |CODE|                NAME|
# +----+--------------------+
# |  AC| Antigua and Barbuda|
# |  AE|United Arab Emirates|
# |  AF|         Afghanistan|
# |  AG|             Algeria|
# |  AJ|          Azerbaijan|
# |  AL|             Albania|
# |  AM|             Armenia|
# |  AO|              Angola|
# |  AQ|American Samoa [U...|
# |  AR|           Argentina|
# |  AS|           Australia|
# |  AU|             Austria|
# |  AY|          Antarctica|
# |  BA|             Bahrain|
# |  BB|            Barbados|
# |  BC|            Botswana|
# |  BD|Bermuda [United K...|
# |  BE|             Belgium|
# |  BF|        Bahamas, The|
# |  BG|          Bangladesh|
# +----+--------------------+


# state
state = spark.read.text("hdfs:///data/ghcnd/ghcnd-states.txt")
state = state.select(
    F.trim(state.value.substr(1,2)).alias("CODE"),
    F.trim(state.value.substr(4,47)).alias("NAME"))

state.cache()
state.show()
# +----+--------------------+
# |CODE|                NAME|
# +----+--------------------+
# |  AB|             ALBERTA|
# |  AK|              ALASKA|
# |  AL|             ALABAMA|
# |  AR|            ARKANSAS|
# |  AS|      AMERICAN SAMOA|
# |  AZ|             ARIZONA|
# |  BC|    BRITISH COLUMBIA|
# |  CA|          CALIFORNIA|
# |  CO|            COLORADO|
# |  CT|         CONNECTICUT|
# |  DC|DISTRICT OF COLUMBIA|
# |  DE|            DELAWARE|
# |  FL|             FLORIDA|
# |  FM|          MICRONESIA|
# |  GA|             GEORGIA|
# |  GU|                GUAM|
# |  HI|              HAWAII|
# |  IA|                IOWA|
# |  ID|               IDAHO|
# |  IL|            ILLINOIS|
# +----+--------------------+


# inventory
inventory = spark.read.text('hdfs:///data/ghcnd/ghcnd-inventory.txt')
inventory = inventory.select(
    F.trim(inventory.value.substr(1,11)).alias('ID'),
    F.trim(inventory.value.substr(13,8)).cast('double').alias('LATITUDE'),
    F.trim(inventory.value.substr(22,9)).cast('double').alias('LONGITUDE'), 
    F.trim(inventory.value.substr(32,4)).alias('ELEMENT'),
    F.trim(inventory.value.substr(37,4)).cast('integer').alias('FIRSTYEAR'),
    F.trim(inventory.value.substr(42,4)).cast('integer').alias('LASTYEAR'))
    
inventory.cache()
inventory.show()
# +-----------+--------+---------+-------+---------+--------+
# |         ID|LATITUDE|LONGITUDE|ELEMENT|FIRSTYEAR|LASTYEAR|
# +-----------+--------+---------+-------+---------+--------+
# |ACW00011604| 17.1167| -61.7833|   TMAX|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   TMIN|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   PRCP|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   SNOW|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   SNWD|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   PGTM|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   WDFG|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   WSFG|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   WT03|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   WT08|     1949|    1949|
# |ACW00011604| 17.1167| -61.7833|   WT16|     1949|    1949|
# |ACW00011647| 17.1333| -61.7833|   TMAX|     1961|    1961|
# |ACW00011647| 17.1333| -61.7833|   TMIN|     1961|    1961|
# |ACW00011647| 17.1333| -61.7833|   PRCP|     1957|    1970|
# |ACW00011647| 17.1333| -61.7833|   SNOW|     1957|    1970|
# |ACW00011647| 17.1333| -61.7833|   SNWD|     1957|    1970|
# |ACW00011647| 17.1333| -61.7833|   WT03|     1961|    1961|
# |ACW00011647| 17.1333| -61.7833|   WT16|     1961|    1966|
# |AE000041196|  25.333|   55.517|   TMAX|     1944|    2021|
# |AE000041196|  25.333|   55.517|   TMIN|     1944|    2021|
# +-----------+--------+---------+-------+---------+--------+


# How many rows are in each metadata table?
station.count()    #118493
country.count()    #219
state.count()      #74
inventory.count()  #704963

# How many stations do not have a WMO ID? 110407
station.filter(station.WMO_ID == '').count()


# 
## Q3
#

#(a)
stations = station.withColumn('COUNTRY_CODE', station.ID.substr(1,2))
stations.show()
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+------------+
# |         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|COUNTRY_CODE|
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+------------+
# |ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |          AC|
# |ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |          AC|
# |AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|          AE|
# |AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|          AE|
# |AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|          AE|
# |AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|          AE|
# |AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|          AF|
# |AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|          AF|
# |AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|          AF|
# |AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|          AF|
# |AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|          AG|
# |AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|          AG|
# |AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|          AG|
# |AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|          AG|
# |AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |          AG|
# |AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |          AG|
# |AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |          AG|
# |AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |          AG|
# |AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |          AG|
# |AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|          AG|
# +-----------+--------+---------+---------+-----+--------------------+--------+------------+------+------------+


#(b)
stations_country = (
  stations
  .join(
    country.withColumnRenamed("CODE", "COUNTRY_CODE").withColumnRenamed("NAME", "COUNTRY_NAME"),
    on="COUNTRY_CODE",
    how="left"
  )
)
stations_country.show()
# +------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# |COUNTRY_CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|        COUNTRY_NAME|
# +------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
# |          AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|
# |          AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|
# |          AE|AE000041196|  25.333|   55.517|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|
# |          AE|AEM00041194|  25.255|   55.364|     10.4|     |          DUBAI INTL|        |            | 41194|United Arab Emirates|
# |          AE|AEM00041217|  24.433|   54.651|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|
# |          AE|AEM00041218|  24.262|   55.609|    264.9|     |         AL AIN INTL|        |            | 41218|United Arab Emirates|
# |          AF|AF000040930|  35.317|   69.017|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|
# |          AF|AFM00040938|   34.21|   62.228|    977.2|     |               HERAT|        |            | 40938|         Afghanistan|
# |          AF|AFM00040948|  34.566|   69.212|   1791.3|     |          KABUL INTL|        |            | 40948|         Afghanistan|
# |          AF|AFM00040990|    31.5|    65.85|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|
# |          AG|AG000060390| 36.7167|     3.25|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|
# |          AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|
# |          AG|AG000060611|   28.05|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|             Algeria|
# |          AG|AG000060680|    22.8|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|             Algeria|
# |          AG|AGE00135039| 35.7297|     0.65|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |             Algeria|
# |          AG|AGE00147704|   36.97|     7.79|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |             Algeria|
# |          AG|AGE00147705|   36.78|     3.07|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |             Algeria|
# |          AG|AGE00147706|    36.8|     3.03|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|
# |          AG|AGE00147707|    36.8|     3.04|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |             Algeria|
# |          AG|AGE00147708|   36.72|     4.05|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|
# +------------+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
 
 
# (c)
stations_country_state = (
  stations_country
  .join(
    state.withColumnRenamed("CODE", "STATE").withColumnRenamed("NAME", "STATE_NAME"),
    on="STATE",
    how="left"
  )
)
stations_country_state.show() 
# +-----+------------+-----------+--------+---------+---------+--------------------+--------+------------+------+--------------------+----------+
# |STATE|COUNTRY_CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|        COUNTRY_NAME|STATE_NAME|
# +-----+------------+-----------+--------+---------+---------+--------------------+--------+------------+------+--------------------+----------+
# |     |          AC|ACW00011604| 17.1167| -61.7833|     10.1|ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|      null|
# |     |          AC|ACW00011647| 17.1333| -61.7833|     19.2|            ST JOHNS|        |            |      | Antigua and Barbuda|      null|
# |     |          AE|AE000041196|  25.333|   55.517|     34.0| SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|      null|
# |     |          AE|AEM00041194|  25.255|   55.364|     10.4|          DUBAI INTL|        |            | 41194|United Arab Emirates|      null|
# |     |          AE|AEM00041217|  24.433|   54.651|     26.8|      ABU DHABI INTL|        |            | 41217|United Arab Emirates|      null|
# |     |          AE|AEM00041218|  24.262|   55.609|    264.9|         AL AIN INTL|        |            | 41218|United Arab Emirates|      null|
# |     |          AF|AF000040930|  35.317|   69.017|   3366.0|        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|      null|
# |     |          AF|AFM00040938|   34.21|   62.228|    977.2|               HERAT|        |            | 40938|         Afghanistan|      null|
# |     |          AF|AFM00040948|  34.566|   69.212|   1791.3|          KABUL INTL|        |            | 40948|         Afghanistan|      null|
# |     |          AF|AFM00040990|    31.5|    65.85|   1010.0|    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|      null|
# |     |          AG|AG000060390| 36.7167|     3.25|     24.0|  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|      null|
# |     |          AG|AG000060590| 30.5667|   2.8667|    397.0|            EL-GOLEA|     GSN|            | 60590|             Algeria|      null|
# |     |          AG|AG000060611|   28.05|   9.6331|    561.0|           IN-AMENAS|     GSN|            | 60611|             Algeria|      null|
# |     |          AG|AG000060680|    22.8|   5.4331|   1362.0|         TAMANRASSET|     GSN|            | 60680|             Algeria|      null|
# |     |          AG|AGE00135039| 35.7297|     0.65|     50.0|ORAN-HOPITAL MILI...|        |            |      |             Algeria|      null|
# |     |          AG|AGE00147704|   36.97|     7.79|    161.0| ANNABA-CAP DE GARDE|        |            |      |             Algeria|      null|
# |     |          AG|AGE00147705|   36.78|     3.07|     59.0|ALGIERS-VILLE/UNI...|        |            |      |             Algeria|      null|
# |     |          AG|AGE00147706|    36.8|     3.03|    344.0|   ALGIERS-BOUZAREAH|        |            |      |             Algeria|      null|
# |     |          AG|AGE00147707|    36.8|     3.04|     38.0|  ALGIERS-CAP CAXINE|        |            |      |             Algeria|      null|
# |     |          AG|AGE00147708|   36.72|     4.05|    222.0|          TIZI OUZOU|        |            | 60395|             Algeria|      null|
# +-----+------------+-----------+--------+---------+---------+--------------------+--------+------------+------+--------------------+----------+
 

# (d) 
# What is station active years and How many elements, different elements each station collect overall?
station_Element = (
    inventory
    .groupBy('ID')
    .agg(F.min(F.col('FIRSTYEAR')).alias('STARTYEAR'),
         F.max(F.col('LASTYEAR')).alias('ENDYEAR'),
         F.count(F.col('ELEMENT')).alias('SUM_ELEMENT'),
         F.countDistinct(F.col('ELEMENT')).alias('SUM_DisctELEMENT'))
    .sort(F.col('ID')))
station_Element.show()
# +-----------+---------+-------+-----------+----------------+
# |         ID|STARTYEAR|ENDYEAR|SUM_ELEMENT|SUM_DisctELEMENT|
# +-----------+---------+-------+-----------+----------------+
# |ACW00011604|     1949|   1949|         11|              11|
# |ACW00011647|     1957|   1970|          7|               7|
# |AE000041196|     1944|   2021|          4|               4|
# |AEM00041194|     1983|   2021|          4|               4|
# |AEM00041217|     1983|   2021|          4|               4|
# |AEM00041218|     1994|   2021|          4|               4|
# |AF000040930|     1973|   1992|          5|               5|
# |AFM00040938|     1973|   2021|          5|               5|
# |AFM00040948|     1966|   2021|          5|               5|
# |AFM00040990|     1973|   2020|          5|               5|
# |AG000060390|     1940|   2021|          5|               5|
# |AG000060590|     1892|   2021|          4|               4|
# |AG000060611|     1958|   2021|          5|               5|
# |AG000060680|     1940|   2004|          4|               4|
# |AGE00135039|     1852|   1966|          3|               3|
# |AGE00147704|     1909|   1937|          3|               3|
# |AGE00147705|     1877|   1938|          3|               3|
# |AGE00147706|     1893|   1920|          3|               3|
# |AGE00147707|     1878|   1879|          3|               3|
# |AGE00147708|     1879|   2021|          5|               5|
# +-----------+---------+-------+-----------+----------------+
 
    
# Count separately core elements and ”other” elements each station collect overall
coreElements = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]
station_coreElement = (
    inventory
    .filter(F.col('ELEMENT').isin(coreElements))
    .groupBy('ID')
    .agg(F.count(F.col('ELEMENT')).alias('SUM_CoreELEMENT')) 
    .sort(F.col('ID')))
station_coreElement.show()
# +-----------+---------------+
# |         ID|SUM_CoreELEMENT|
# +-----------+---------------+
# |ACW00011604|              5|
# |ACW00011647|              5|
# |AE000041196|              3|
# |AEM00041194|              3|
# |AEM00041217|              3|
# |AEM00041218|              3|
# |AF000040930|              4|
# |AFM00040938|              4|
# |AFM00040948|              4|
# |AFM00040990|              4|
# |AG000060390|              4|
# |AG000060590|              3|
# |AG000060611|              4|
# |AG000060680|              3|
# |AGE00135039|              3|
# |AGE00147704|              3|
# |AGE00147705|              3|
# |AGE00147706|              3|
# |AGE00147707|              3|
# |AGE00147708|              4|
# +-----------+---------------+

    
station_otherElement = (
    inventory
    .filter(~F.col('ELEMENT').isin(coreElements))
    .groupBy('ID')
    .agg(F.count(F.col('ELEMENT')).alias('SUM_OtherELEMENT')) 
    .sort(F.col('ID')))
station_otherElement.show()
# +-----------+----------------+
# |         ID|SUM_OtherELEMENT|
# +-----------+----------------+
# |ACW00011604|               6|
# |ACW00011647|               2|
# |AE000041196|               1|
# |AEM00041194|               1|
# |AEM00041217|               1|
# |AEM00041218|               1|
# |AF000040930|               1|
# |AFM00040938|               1|
# |AFM00040948|               1|
# |AFM00040990|               1|
# |AG000060390|               1|
# |AG000060590|               1|
# |AG000060611|               1|
# |AG000060680|               1|
# |AGE00147708|               1|
# |AGE00147710|               1|
# |AGE00147716|               1|
# |AGE00147718|               1|
# |AGE00147719|               1|
# |AGM00060351|               1|
# +-----------+----------------+


# How many stations collect all 5 core elements?  20289 
station_coreElement.filter(F.col('SUM_CoreELEMENT') == 5).count()

# How many stations only collected precipitation?  16136
temp = (
    inventory
    .groupBy('ID')
    .agg(F.collect_set('ELEMENT').cast('string').alias('ELEMENT_SET'))
    .sort(F.col('ID')))
    
temp.filter(F.col('ELEMENT_SET')=='[PRCP]').count()

 
# (e)
# Join outputs from (d)
temp1 = stations_country_state.join(station_Element,'ID','left')
temp2 = temp1.join(station_coreElement,'ID','left')
stations_super = temp2.join(station_otherElement,'ID','left')

stations_super.cache()
stations_super.show()
# +-----------+-----+------------+--------+---------+---------+--------------------+--------+------------+------+--------------------+--------------+---------+-------+-----------+----------------+---------------+----------------+
# |         ID|STATE|COUNTRY_CODE|LATITUDE|LONGITUDE|ELEVATION|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|        COUNTRY_NAME|    STATE_NAME|STARTYEAR|ENDYEAR|SUM_ELEMENT|SUM_DisctELEMENT|SUM_CoreELEMENT|SUM_OtherELEMENT|
# +-----------+-----+------------+--------+---------+---------+--------------------+--------+------------+------+--------------------+--------------+---------+-------+-----------+----------------+---------------+----------------+
# |AGE00147719|     |          AG| 33.7997|     2.89|    767.0|            LAGHOUAT|        |            | 60545|             Algeria|          null|     1888|   2021|          4|               4|              3|               1|
# |AGM00060445|     |          AG|  36.178|    5.324|   1050.0|     SETIF AIN ARNAT|        |            | 60445|             Algeria|          null|     1957|   2021|          5|               5|              4|               1|
# |AJ000037679|     |          AJ|    41.1|     49.2|    -26.0|             SIASAN'|        |            | 37679|          Azerbaijan|          null|     1959|   1987|          1|               1|              1|            null|
# |AJ000037831|     |          AJ|    40.4|     47.0|    160.0|          MIR-BASHIR|        |            | 37831|          Azerbaijan|          null|     1955|   1987|          1|               1|              1|            null|
# |AJ000037981|     |          AJ|    38.9|     48.2|    794.0|            JARDIMLY|        |            | 37981|          Azerbaijan|          null|     1959|   1987|          1|               1|              1|            null|
# |AJ000037989|     |          AJ|    38.5|     48.9|    -22.0|              ASTARA|     GSN|            | 37989|          Azerbaijan|          null|     1936|   2017|          5|               5|              4|               1|
# |ALE00100939|     |          AL| 41.3331|  19.7831|     89.0|              TIRANA|        |            |      |             Albania|          null|     1940|   2000|          2|               2|              2|            null|
# |AM000037719|     |          AM|    40.6|    45.35|   1834.0|           CHAMBARAK|        |            | 37719|             Armenia|          null|     1912|   1992|          5|               5|              4|               1|
# |AM000037897|     |          AM|  39.533|   46.017|   1581.0|              SISIAN|        |            | 37897|             Armenia|          null|     1936|   2021|          5|               5|              4|               1|
# |AQC00914873|   AS|          AQ|  -14.35|-170.7667|     14.9|    TAPUTIMU TUTUILA|        |            |      |American Samoa [U...|AMERICAN SAMOA|     1955|   1967|         12|              12|              5|               7|
# |AR000000002|     |          AR|  -29.82|   -57.42|     75.0|            BONPLAND|        |            |      |           Argentina|          null|     1981|   2000|          1|               1|              1|            null|
# |AR000087007|     |          AR|   -22.1|    -65.6|   3479.0| LA QUIACA OBSERVATO|     GSN|            | 87007|           Argentina|          null|     1956|   2021|          5|               5|              4|               1|
# |AR000087374|     |          AR| -31.783|  -60.483|     74.0|         PARANA AERO|     GSN|            | 87374|           Argentina|          null|     1956|   2021|          5|               5|              4|               1|
# |AR000875850|     |          AR| -34.583|  -58.483|     25.0| BUENOS AIRES OBSERV|        |            | 87585|           Argentina|          null|     1908|   2021|          5|               5|              4|               1|
# |ARM00087022|     |          AR|  -22.62|  -63.794|    449.0|GENERAL ENRIQUE M...|        |            | 87022|           Argentina|          null|     1973|   2021|          4|               4|              3|               1|
# |ARM00087480|     |          AR| -32.904|  -60.785|     25.9|             ROSARIO|        |            | 87480|           Argentina|          null|     1965|   2021|          5|               5|              4|               1|
# |ARM00087509|     |          AR| -34.588|  -68.403|    752.9|          SAN RAFAEL|        |            | 87509|           Argentina|          null|     1973|   2021|          5|               5|              4|               1|
# |ARM00087532|     |          AR| -35.696|  -63.758|    139.9|        GENERAL PICO|        |            | 87532|           Argentina|          null|     1973|   2021|          5|               5|              4|               1|
# |ARM00087904|     |          AR| -50.267|   -72.05|    204.0|    EL CALAFATE AERO|        |            | 87904|           Argentina|          null|     2003|   2021|          5|               5|              4|               1|
# |ASN00001003|     |          AS|-14.1331| 126.7158|      5.0|        PAGO MISSION|        |            |      |           Australia|          null|     1909|   1940|          1|               1|              1|            null|
# +-----------+-----+------------+--------+---------+---------+--------------------+--------+------------+------+--------------------+--------------+---------+-------+-----------+----------------+---------------+----------------+

    
# Save stations_super to csv.gz
( stations_super.write
    .option('compression','gzip')  
    .mode('overwrite')
    .csv('hdfs:///user/lxi12/outputs/ghcnd/stations_super.csv.gz')
)


#(f)
# Joing 1000 rows subset of daily with stations_super
daily1000_stations = daily1000.join(stations_super, 'ID', 'left')
daily1000_stations.show()
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----+------------+--------+---------+---------+-------------------+--------+------------+------+--------------------+----------+---------+-------+-----------+----------------+---------------+----------------+
# |         ID|    DATE|ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|STATE|COUNTRY_CODE|LATITUDE|LONGITUDE|ELEVATION|               NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|        COUNTRY_NAME|STATE_NAME|STARTYEAR|ENDYEAR|SUM_ELEMENT|SUM_DisctELEMENT|SUM_CoreELEMENT|SUM_OtherELEMENT|
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----+------------+--------+---------+---------+-------------------+--------+------------+------+--------------------+----------+---------+-------+-----------+----------------+---------------+----------------+
# |AE000041196|20220101|   TAVG|204.0|               H|        null|          S|            null|     |          AE|  25.333|   55.517|     34.0|SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|      null|     1944|   2021|          4|               4|              3|               1|
# |AEM00041194|20220101|   TAVG|211.0|               H|        null|          S|            null|     |          AE|  25.255|   55.364|     10.4|         DUBAI INTL|        |            | 41194|United Arab Emirates|      null|     1983|   2021|          4|               4|              3|               1|
# |AEM00041218|20220101|   TAVG|207.0|               H|        null|          S|            null|     |          AE|  24.262|   55.609|    264.9|        AL AIN INTL|        |            | 41218|United Arab Emirates|      null|     1994|   2021|          4|               4|              3|               1|
# |AEM00041217|20220101|   TAVG|209.0|               H|        null|          S|            null|     |          AE|  24.433|   54.651|     26.8|     ABU DHABI INTL|        |            | 41217|United Arab Emirates|      null|     1983|   2021|          4|               4|              3|               1|
# |AG000060390|20220101|   TAVG|121.0|               H|        null|          S|            null|     |          AG| 36.7167|     3.25|     24.0| ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|      null|     1940|   2021|          5|               5|              4|               1|
# |AG000060590|20220101|   TAVG|151.0|               H|        null|          S|            null|     |          AG| 30.5667|   2.8667|    397.0|           EL-GOLEA|     GSN|            | 60590|             Algeria|      null|     1892|   2021|          4|               4|              3|               1|
# |AG000060611|20220101|   TAVG|111.0|               H|        null|          S|            null|     |          AG|   28.05|   9.6331|    561.0|          IN-AMENAS|     GSN|            | 60611|             Algeria|      null|     1958|   2021|          5|               5|              4|               1|
# |AGE00147708|20220101|   TMIN| 73.0|            null|        null|          S|            null|     |          AG|   36.72|     4.05|    222.0|         TIZI OUZOU|        |            | 60395|             Algeria|      null|     1879|   2021|          5|               5|              4|               1|
# |AGE00147708|20220101|   PRCP|  0.0|            null|        null|          S|            null|     |          AG|   36.72|     4.05|    222.0|         TIZI OUZOU|        |            | 60395|             Algeria|      null|     1879|   2021|          5|               5|              4|               1|
# |AGE00147708|20220101|   TAVG|133.0|               H|        null|          S|            null|     |          AG|   36.72|     4.05|    222.0|         TIZI OUZOU|        |            | 60395|             Algeria|      null|     1879|   2021|          5|               5|              4|               1|
# |AGE00147716|20220101|   TMIN|107.0|            null|        null|          S|            null|     |          AG|    35.1|    -1.85|     83.0|NEMOURS (GHAZAOUET)|        |            | 60517|             Algeria|      null|     1878|   2021|          4|               4|              3|               1|
# |AGE00147716|20220101|   PRCP|  0.0|            null|        null|          S|            null|     |          AG|    35.1|    -1.85|     83.0|NEMOURS (GHAZAOUET)|        |            | 60517|             Algeria|      null|     1878|   2021|          4|               4|              3|               1|
# |AGE00147716|20220101|   TAVG|133.0|               H|        null|          S|            null|     |          AG|    35.1|    -1.85|     83.0|NEMOURS (GHAZAOUET)|        |            | 60517|             Algeria|      null|     1878|   2021|          4|               4|              3|               1|
# |AGE00147718|20220101|   TMIN| 90.0|            null|        null|          S|            null|     |          AG|   34.85|     5.72|    125.0|             BISKRA|        |            | 60525|             Algeria|      null|     1880|   2021|          4|               4|              3|               1|
# |AGE00147718|20220101|   TAVG|152.0|               H|        null|          S|            null|     |          AG|   34.85|     5.72|    125.0|             BISKRA|        |            | 60525|             Algeria|      null|     1880|   2021|          4|               4|              3|               1|
# |AGE00147719|20220101|   TMAX|201.0|            null|        null|          S|            null|     |          AG| 33.7997|     2.89|    767.0|           LAGHOUAT|        |            | 60545|             Algeria|      null|     1888|   2021|          4|               4|              3|               1|
# |AGE00147719|20220101|   PRCP|  0.0|            null|        null|          S|            null|     |          AG| 33.7997|     2.89|    767.0|           LAGHOUAT|        |            | 60545|             Algeria|      null|     1888|   2021|          4|               4|              3|               1|
# |AGE00147719|20220101|   TAVG|119.0|               H|        null|          S|            null|     |          AG| 33.7997|     2.89|    767.0|           LAGHOUAT|        |            | 60545|             Algeria|      null|     1888|   2021|          4|               4|              3|               1|
# |AGM00060351|20220101|   PRCP|  0.0|            null|        null|          S|            null|     |          AG|  36.795|    5.874|     11.0|              JIJEL|        |            | 60351|             Algeria|      null|     1981|   2021|          4|               4|              3|               1|
# |AGM00060351|20220101|   TAVG|126.0|               H|        null|          S|            null|     |          AG|  36.795|    5.874|     11.0|              JIJEL|        |            | 60351|             Algeria|      null|     1981|   2021|          4|               4|              3|               1|
# +-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----+------------+--------+---------+---------+-------------------+--------+------------+------+--------------------+----------+---------+-------+-----------+----------------+---------------+----------------+

 
# Any stations in daily1000 that are not in stations_super?  0
daily1000.select('ID').subtract(stations_super.select('ID')).count()  



### Analysis ###

start_pyspark_shell -e 4 -c 2 -w 4 -m 4

#
## Q1
#

# (a)
# How many stations are there in total?  118493  
stations_super.select('ID').distinct().count()   

# How many stations were active in 2021?  38284 
stations_super.filter(F.col('ENDYEAR') >= 2021).count() 

# How many stations are in GSN?  991
stations_super.filter(F.col('GSN_FLAG')== 'GSN').count() 

# How many stations are in HCN? 1218 
stations_super.filter(F.col('HCN/CRN_FLAG')== 'HCN').count() 

# How many stations are in CRN?  0
stations_super.filter(F.col('HCN/CRN_FLAG')== 'CRN').count()

# Are there any stations that are in more than one of these networks?  14
stations_super.filter((F.col('GSN_FLAG')== 'GSN') & (F.col('HCN/CRN_FLAG')== 'HCN')).count()


# (b)
# Count the sum stations in each country, and store the output in country
temp_c = stations_super.groupBy(F.col('COUNTRY_CODE')).count()        #dataframe  

country_sumstation = (
  country
  .join(
    temp_c.withColumnRenamed("COUNT", "COUNTRY_SUMSTATION").withColumnRenamed("COUNTRY_CODE", "CODE"),
    on="CODE",
    how="left"
  )
)
country_sumstation.show()
country_sumstation.write.csv(f"hdfs:///user/lxi12/outputs/ghcnd/country_sumstation",mode='overwrite', header=True)
# +----+--------------------+------------------+
# |CODE|                NAME|COUNTRY_SUMSTATION|
# +----+--------------------+------------------+
# |  AC| Antigua and Barbuda|                 2|
# |  AE|United Arab Emirates|                 4|
# |  AF|         Afghanistan|                 4|
# |  AG|             Algeria|                87|
# |  AJ|          Azerbaijan|                66|
# |  AL|             Albania|                 3|
# |  AM|             Armenia|                53|
# |  AO|              Angola|                 6|
# |  AQ|American Samoa [U...|                21|
# |  AR|           Argentina|               101|
# |  AS|           Australia|             17088|
# |  AU|             Austria|                13|
# |  AY|          Antarctica|               102|
# |  BA|             Bahrain|                 1|
# |  BB|            Barbados|                 1|
# |  BC|            Botswana|                21|
# |  BD|Bermuda [United K...|                 2|
# |  BE|             Belgium|                 1|
# |  BF|        Bahamas, The|                40|
# |  BG|          Bangladesh|                10|
# +----+--------------------+------------------+


# Count the sum stations in each state and store the output in state 
temp_s = stations_super.groupBy(F.col('STATE')).count()

state_sumstation = (
  state
  .join(
    temp_s.withColumnRenamed("COUNT", "STATE_SUMSTATION").withColumnRenamed("STATE", "CODE"),
    on="CODE",
    how="left"
  )
)
state_sumstation.show()
state_sumstation.write.csv(f"hdfs:///user/lxi12/outputs/ghcnd/state_sumstation",mode='overwrite', header=True)

# +----+--------------------+----------------+
# |CODE|                NAME|STATE_SUMSTATION|
# +----+--------------------+----------------+
# |  AB|             ALBERTA|            1428|
# |  AK|              ALASKA|            1003|
# |  AL|             ALABAMA|            1012|
# |  AR|            ARKANSAS|             885|
# |  AS|      AMERICAN SAMOA|              21|
# |  AZ|             ARIZONA|            1534|
# |  BC|    BRITISH COLUMBIA|            1688|
# |  CA|          CALIFORNIA|            2879|
# |  CO|            COLORADO|            4310|
# |  CT|         CONNECTICUT|             350|
# |  DC|DISTRICT OF COLUMBIA|              15|
# |  DE|            DELAWARE|             124|
# |  FL|             FLORIDA|            1877|
# |  FM|          MICRONESIA|              38|
# |  GA|             GEORGIA|            1261|
# |  GU|                GUAM|              21|
# |  HI|              HAWAII|             752|
# |  IA|                IOWA|             905|
# |  ID|               IDAHO|             794|
# |  IL|            ILLINOIS|            1889|
# +----+--------------------+----------------+

 
# (c)
# How many stations are there in the Northern Hemisphere only? 93156
stations_super.filter(F.col('LATITUDE') > 0).count()

# How many stations are there in total in the territories of the US, excluding the US itself? 339
US_territories = (country_sumstation
                    .filter((F.col('NAME').contains('United States')) & (~F.col('NAME').startswith('United States'))))

US_territories.select(F.sum('COUNTRY_SUMSTATION')).show()
# +-----------------------+
# |sum(COUNTRY_SUMSTATION)|
# +-----------------------+
# |                    339|
# +-----------------------+


#
## Q2
#
# (a)
# udf to compute the distance
# pip install geopy distance       #run outside pyspark shell  
# from geopy.distance import geodesic   #couldn't add module to pyspark shell 

#@F.udf(returnType = DoubleType())
#def get_distance(loc1, loc2):
#    """loc(latitude, longtitude), distance in km"""
#    return geodesic(loc1, loc2).km
 
from math import radians, sin, cos, sqrt, asin

@F.udf(returnType=DoubleType())
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0    # Earth mean radius in kilometers
    
    dlon = radians(lon2) - radians(lon1)
    dlat = radians(lat2) - radians(lat1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
 
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    return R * c
    
# test the udf    
stations_s1 = (stations_super.filter(F.col('LATITUDE')>45).limit(5)
    .select(F.col('ID').alias('ID1'),F.col('NAME').alias('NAME1'),F.col('LATITUDE').alias('LAT1'),F.col('LONGITUDE').alias('LON1'))
    )
stations_s1.show()
# +-----------+---------+------+------+
# |        ID1|    NAME1|  LAT1|  LON1|
# +-----------+---------+------+------+
# |BOM00026653|  POLOTSK|55.467|28.767|
# |BOM00026846| STOLBTSY|  53.5|  26.7|
# |BOM00026864|  KLICHEV|  53.5|29.333|
# |BOM00026938|IVACEVICI|52.717| 25.35|
# |BOM00026966|  ZHLOBIN|  52.9|30.033|
# +-----------+---------+------+------+


stations_s2 =(stations_super.filter(F.col('GSN_FLAG')=='GSN').limit(5)
    .select(F.col('ID').alias('ID2'),F.col('NAME').alias('NAME2'),F.col('LATITUDE').alias('LAT2'),F.col('LONGITUDE').alias('LON2'))
    )
stations_s2.show()
# +-----------+------------+--------+--------+
# |        ID2|       NAME2|    LAT2|    LON2|
# +-----------+------------+--------+--------+
# |ALM00013615|TIRANA RINAS|  41.415|  19.721|
# |AO000066160|      LUANDA|   -8.85|  13.233|
# |AR000087344|CORDOBA AERO| -31.317| -64.217|
# |ARM00087506|    MALARGUE| -35.494| -69.574|
# |ASN00011052|     FORREST|-30.8453|128.1092|
# +-----------+------------+--------+--------+

    
stations_sub = stations_s1.crossJoin(stations_s2)
stations_distance = stations_sub.withColumn('DISTANCE/KM', haversine(F.col('LAT1'), F.col('LON1'), F.col('LAT2'), F.col('LON2')))
stations_distance.show()
# +-----------+-------------------+------+------+-----------+------------+--------+--------+------------------+
# |        ID1|              NAME1|  LAT1|  LON1|        ID2|       NAME2|    LAT2|    LON2|       DISTANCE/KM|
# +-----------+-------------------+------+------+-----------+------------+--------+--------+------------------+
# |AUM00011231|KLAGENFURT(CIV/MIL)| 46.65|14.333|ALM00013615|TIRANA RINAS|  41.415|  19.721| 723.7546317482938|
# |AUM00011231|KLAGENFURT(CIV/MIL)| 46.65|14.333|AO000066160|      LUANDA|   -8.85|  13.233| 6172.284696420912|
# |AUM00011231|KLAGENFURT(CIV/MIL)| 46.65|14.333|AR000087344|CORDOBA AERO| -31.317| -64.217|11693.482202930723|
# |AUM00011231|KLAGENFURT(CIV/MIL)| 46.65|14.333|ARM00087506|    MALARGUE| -35.494| -69.574|12373.509252350901|
# |AUM00011231|KLAGENFURT(CIV/MIL)| 46.65|14.333|ASN00011052|     FORREST|-30.8453|128.1092|14190.895972961902|
# |BOM00026643|      SHARCOVSCHINA|55.367|27.467|ALM00013615|TIRANA RINAS|  41.415|  19.721|1651.0212907682187|
# |BOM00026643|      SHARCOVSCHINA|55.367|27.467|AO000066160|      LUANDA|   -8.85|  13.233| 7262.031315426936|
# |BOM00026643|      SHARCOVSCHINA|55.367|27.467|AR000087344|CORDOBA AERO| -31.317| -64.217|12923.944901661414|
# |BOM00026643|      SHARCOVSCHINA|55.367|27.467|ARM00087506|    MALARGUE| -35.494| -69.574|13599.924311597988|
# |BOM00026643|      SHARCOVSCHINA|55.367|27.467|ASN00011052|     FORREST|-30.8453|128.1092|13431.889626639391|
# |BOM00026666|            VITEBSK|55.167|30.217|ALM00013615|TIRANA RINAS|  41.415|  19.721|1710.6980630454875|
# |BOM00026666|            VITEBSK|55.167|30.217|AO000066160|      LUANDA|   -8.85|  13.233| 7291.692916474411|
# |BOM00026666|            VITEBSK|55.167|30.217|AR000087344|CORDOBA AERO| -31.317| -64.217|13084.210841822754|
# |BOM00026666|            VITEBSK|55.167|30.217|ARM00087506|    MALARGUE| -35.494| -69.574| 13761.10732068377|
# |BOM00026666|            VITEBSK|55.167|30.217|ASN00011052|     FORREST|-30.8453|128.1092|13256.835448364036|
# |BOM00026951|             SLUTSK|  53.0|  27.7|ALM00013615|TIRANA RINAS|  41.415|  19.721| 1420.150194229638|
# |BOM00026951|             SLUTSK|  53.0|  27.7|AO000066160|      LUANDA|   -8.85|  13.233| 7012.888517904904|
# |BOM00026951|             SLUTSK|  53.0|  27.7|AR000087344|CORDOBA AERO| -31.317| -64.217|12855.700249676958|
# |BOM00026951|             SLUTSK|  53.0|  27.7|ARM00087506|    MALARGUE| -35.494| -69.574|13534.439151036297|
# |BOM00026951|             SLUTSK|  53.0|  27.7|ASN00011052|     FORREST|-30.8453|128.1092|13364.245618195093|
# +-----------+-------------------+------+------+-----------+------------+--------+--------+------------------+

# compare the results with online geographic distance calculation website is nearly the same, means the udf is working fine


# (b)
NZ_stations = stations_super.filter(F.col('COUNTRY_CODE') == 'NZ')     
NZ_stations.count()     # 15

NZ_stationspair = (
    NZ_stations.alias('l').join(NZ_stations.alias('r'), on='COUNTRY_CODE')
    .select('l.COUNTRY_CODE',
            F.col('l.ID').alias('ID1'), F.col('l.NAME').alias('NAME1'), F.col('l.LATITUDE').alias('LAT1'), F.col('l.LONGITUDE').alias('LON1'),
            F.col('r.ID').alias('ID2'), F.col('r.NAME').alias('NAME2'), F.col('r.LATITUDE').alias('LAT2'), F.col('r.LONGITUDE').alias('LON2'))
    .where(F.col('ID1') < F.col('ID2')))

NZ_stationspair.count()   # 105 unqiue pairs, it equals to 15*14/2            
NZ_stationspair.show()             
# +------------+-----------+------------------+-------+--------+-----------+-------------------+-------+--------+
# |COUNTRY_CODE|        ID1|             NAME1|   LAT1|    LON1|        ID2|              NAME2|   LAT2|    LON2|
# +------------+-----------+------------------+-------+--------+-----------+-------------------+-------+--------+
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093439|WELLINGTON AERO AWS|-41.333|   174.8|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093929| ENDERBY ISLAND AWS|-50.483|   166.3|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZ000939450|CAMPBELL ISLAND AWS| -52.55| 169.167|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093781|  CHRISTCHURCH INTL|-43.489| 172.532|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZ000939870|CHATHAM ISLANDS AWS| -43.95|-176.567|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZ000937470|         TARA HILLS|-44.517|   169.9|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093678|           KAIKOURA|-42.417|   173.7|
# |          NZ|NZ000936150|HOKITIKA AERODROME|-42.717| 170.983|NZM00093110|  AUCKLAND AERO AWS|  -37.0|   174.8|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000933090|   NEW PLYMOUTH AWS|-39.017| 174.183|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093439|WELLINGTON AERO AWS|-41.333|   174.8|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093929| ENDERBY ISLAND AWS|-50.483|   166.3|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000939450|CAMPBELL ISLAND AWS| -52.55| 169.167|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093781|  CHRISTCHURCH INTL|-43.489| 172.532|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000939870|CHATHAM ISLANDS AWS| -43.95|-176.567|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000937470|         TARA HILLS|-44.517|   169.9|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093678|           KAIKOURA|-42.417|   173.7|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZM00093110|  AUCKLAND AERO AWS|  -37.0|   174.8|
# |          NZ|NZ000093994|RAOUL ISL/KERMADEC| -29.25|-177.917|NZ000936150| HOKITIKA AERODROME|-42.717| 170.983|
# |          NZ|NZ000093012|           KAITAIA|  -35.1| 173.267|NZ000933090|   NEW PLYMOUTH AWS|-39.017| 174.183|
# |          NZ|NZ000093012|           KAITAIA|  -35.1| 173.267|NZM00093439|WELLINGTON AERO AWS|-41.333|   174.8|
# +------------+-----------+------------------+-------+--------+-----------+-------------------+-------+--------+

    
nzstations_distance = NZ_stationspair.withColumn('DISTANCE/KM', haversine(F.col('LAT1'), F.col('LON1'), F.col('LAT2'), F.col('LON2')))

nzstations_distance.write.csv(f"hdfs:///user/lxi12/outputs/ghcnd/nzstations_distance", mode='overwrite', header=True)

# closest two stations in NZ
nzstations_distance.sort(F.col('DISTANCE/KM')).show()   
# +------------+-----------+-------------------+-------+-------+-----------+-------------------+-------+-------+------------------+
# |COUNTRY_CODE|        ID1|              NAME1|   LAT1|   LON1|        ID2|              NAME2|   LAT2|   LON2|       DISTANCE/KM|
# +------------+-----------+-------------------+-------+-------+-----------+-------------------+-------+-------+------------------+
# |          NZ|NZ000093417|    PARAPARAUMU AWS|  -40.9|174.983|NZM00093439|WELLINGTON AERO AWS|-41.333|  174.8| 50.52902648213865|
# |          NZ|NZM00093439|WELLINGTON AERO AWS|-41.333|  174.8|NZM00093678|           KAIKOURA|-42.417|  173.7|151.07143500970233|
# |          NZ|NZ000936150| HOKITIKA AERODROME|-42.717|170.983|NZM00093781|  CHRISTCHURCH INTL|-43.489|172.532| 152.2583566829735|
# |          NZ|NZM00093678|           KAIKOURA|-42.417|  173.7|NZM00093781|  CHRISTCHURCH INTL|-43.489|172.532|152.45897065797334|
# |          NZ|NZ000093417|    PARAPARAUMU AWS|  -40.9|174.983|NZM00093678|           KAIKOURA|-42.417|  173.7| 199.5296235578923|
# |          NZ|NZ000936150| HOKITIKA AERODROME|-42.717|170.983|NZ000937470|         TARA HILLS|-44.517|  169.9|218.30901921694397|
# |          NZ|NZ000093417|    PARAPARAUMU AWS|  -40.9|174.983|NZ000933090|   NEW PLYMOUTH AWS|-39.017|174.183|220.19979430792796|
# |          NZ|NZ000936150| HOKITIKA AERODROME|-42.717|170.983|NZM00093678|           KAIKOURA|-42.417|  173.7|224.98127858624963|
# |          NZ|NZ000933090|   NEW PLYMOUTH AWS|-39.017|174.183|NZM00093110|  AUCKLAND AERO AWS|  -37.0|  174.8| 230.7008604393771|
# |          NZ|NZ000937470|         TARA HILLS|-44.517|  169.9|NZM00093781|  CHRISTCHURCH INTL|-43.489|172.532|239.53012978726045|
# |          NZ|NZ000093844|INVERCARGILL AIRPOR|-46.417|168.333|NZ000937470|         TARA HILLS|-44.517|  169.9|244.05296643063028|
# |          NZ|NZ000093012|            KAITAIA|  -35.1|173.267|NZM00093110|  AUCKLAND AERO AWS|  -37.0|  174.8|252.23867325454387|
# |          NZ|NZ000933090|   NEW PLYMOUTH AWS|-39.017|174.183|NZM00093439|WELLINGTON AERO AWS|-41.333|  174.8|262.80637803404636|
# |          NZ|NZM00093439|WELLINGTON AERO AWS|-41.333|  174.8|NZM00093781|  CHRISTCHURCH INTL|-43.489|172.532|303.52422163509766|
# |          NZ|NZ000939450|CAMPBELL ISLAND AWS| -52.55|169.167|NZM00093929| ENDERBY ISLAND AWS|-50.483|  166.3|303.56667741780706|
# |          NZ|NZ000093292| GISBORNE AERODROME| -38.65|177.983|NZ000933090|   NEW PLYMOUTH AWS|-39.017|174.183| 331.6421027650992|
# |          NZ|NZ000093292| GISBORNE AERODROME| -38.65|177.983|NZM00093110|  AUCKLAND AERO AWS|  -37.0|  174.8|  334.360818792576|
# |          NZ|NZ000936150| HOKITIKA AERODROME|-42.717|170.983|NZM00093439|WELLINGTON AERO AWS|-41.333|  174.8| 350.7960221353078|
# |          NZ|NZ000093417|    PARAPARAUMU AWS|  -40.9|174.983|NZM00093781|  CHRISTCHURCH INTL|-43.489|172.532| 351.5964188686495|
# |          NZ|NZ000093292| GISBORNE AERODROME| -38.65|177.983|NZ000093417|    PARAPARAUMU AWS|  -40.9|174.983|358.18054651496413|
# +------------+-----------+-------------------+-------+-------+-----------+-------------------+-------+-------+------------------+


#
## Q3
#
# (a)
!hdfs getconf -confKey 'dfs.blocksize'        #the default blocksize of hdfs is 134217728B, 128M

!hdfs fsck /data/ghcnd/daily/2022.csv.gz -files -blocks     #1 small block, as 2022 is partial year
#/data/ghcnd/daily/2022.csv.gz 25985757 bytes, replicated: replication=8, 1 block(s):  OK
#0. BP-700027894-132.181.129.68-1626517177804:blk_1073769759_28939 len=25985757 Live_repl=8

!hdfs fsck /data/ghcnd/daily/2021.csv.gz -files -blocks     #2 blocks, one full, one partial
# /data/ghcnd/daily/2021.csv.gz 146936025 bytes, replicated: replication=8, 2 block(s):  OK
# 0. BP-700027894-132.181.129.68-1626517177804:blk_1073769757_28937 len=134217728 Live_repl=8
# 1. BP-700027894-132.181.129.68-1626517177804:blk_1073769758_28938 len=12718297 Live_repl=8
 

# (b)
daily2021 = spark.read.csv('hdfs:///data/ghcnd/daily/2021.csv.gz')
# 1 job, 1 stage, 1 task, scan text. execute 64KB
#This job has one stage and as the HDFS file has 2 blocks, I expect the Spark partion to be 2 as well, 
#however both the below result and Spark UI tell there is only 1 partition therefor 1 task. ??
# In [5]: daily2021.rdd.getNumPartitions()
# Out[5]: 1

daily2021.count()       # 34657282
# job 1, 2 stages, 2 tasks, scan csv, shuffling, count   
# task 1 executed 141m, task2 executed 59B, not correspond to the number of blocks in each input. 


daily2022 = spark.read.csv('hdfs:///data/ghcnd/daily/2022.csv.gz')
# 1 job, 1 stage, 1 task, scan text. execute 64KB

daily2022.count()       # 5971307
# job 1, 2 stages, 2 tasks, scan csv, shuffling, count   
# task 1 executed 24m, task2 executed 59B, not correspond to the number of blocks in each input. 


# (c)
daily2014_2022 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:///data/ghcnd/daily/20{1[4-9],2[0-2]}*")
)
daily2014_2022.count()     # 284918108

# job 1, 2 stages, stage1, 9tasks, 1386.8M;   stage2, 1task, 531B
# stage1 task numbers are exactly the same as input files number
# Explain how Spark partitions input files that are compressed ?
# it doesn't affect block size, it is influenced by the size of the data in the block 


# (d)
# depends on input data, the task would be input files +1 
# think about repartitioning, the cost of repartitioning, and the potential benefits, tasks vs cores. 


#
## Q4
#
# (a)
daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", False)
    .option("inferSchema", False)
    .schema(daily_schema )
    .load("hdfs:///data/ghcnd/daily/")
)
# Count number of rows in daily
daily_all.count()         # 3000243596


# (b)
daily_sub = daily_all.filter(F.col('ELEMENT').isin(coreElements))

# How many observations are there for each core elements?
daily_sub.groupBy(F.col('ELEMENT')).count().show()
# +-------+----------+
# |ELEMENT|     count|
# +-------+----------+
# |   SNOW| 341985067|
# |   SNWD| 289981374|
# |   PRCP|1043785667|
# |   TMIN| 444271327|
# |   TMAX| 445623712|
# +-------+----------+


# (c)
daily_t = (daily_sub
    .filter((F.col('ELEMENT')=='TMIN') | (F.col('ELEMENT')=='TMAX'))
    .groupBy('ID', 'DATE').agg(F.collect_set('ELEMENT').cast('string').alias('SET'))
    )

daily_t.show()
# +-----------+--------+------------+
# |         ID|    DATE|         SET|
# +-----------+--------+------------+
# |ACW00011604|19490121|[TMAX, TMIN]|
# |ACW00011604|19490314|[TMAX, TMIN]|
# |ACW00011604|19490316|[TMAX, TMIN]|
# |ACW00011604|19490401|[TMAX, TMIN]|
# |ACW00011604|19490409|[TMAX, TMIN]|
# |ACW00011604|19490422|[TMAX, TMIN]|
# |ACW00011604|19490506|[TMAX, TMIN]|
# |ACW00011604|19490510|[TMAX, TMIN]|
# |ACW00011604|19490515|[TMAX, TMIN]|
# |ACW00011604|19490520|[TMAX, TMIN]|
# |ACW00011604|19490522|[TMAX, TMIN]|
# |ACW00011604|19490626|[TMAX, TMIN]|
# |ACW00011604|19490701|[TMAX, TMIN]|
# |ACW00011604|19490722|[TMAX, TMIN]|
# |ACW00011604|19490726|[TMAX, TMIN]|
# |AE000041196|19440325|[TMAX, TMIN]|
# |AE000041196|19440328|[TMAX, TMIN]|
# |AE000041196|19440402|[TMAX, TMIN]|
# |AE000041196|19440404|[TMAX, TMIN]|
# |AE000041196|19440410|[TMAX, TMIN]|
# +-----------+--------+------------+


# How many observations of TMIN not respond to TMAX?    8808805
daily_tt = daily_t.filter(F.col('SET') == '[TMIN]')
daily_tt.count()
# How many different stations contributed to these observations?   27650
daily_tt.select('ID').distinct().count()

## Another way

#daily_tmin = daily_sub.filter(F.col('ELEMENT')=='TMIN')
#daily_tmax = daily_sub.filter(F.col('ELEMENT')=='TMAX')      
#temp_d = daily_tmin.select('ID','DATE').subtract(daily_tmax.select('ID','DATE')) 
#temp_d.count()
#temp_d.select('ID').distinct().count()


# (d)
daily_NZ = daily_all.filter((F.substring(F.col('ID'),1,2)=='NZ') & ((F.col('ELEMENT')=='TMIN')| (F.col('ELEMENT')=='TMAX')))
daily_NZ.sort('ID','DATE')
 

daily_avNZ = daily_NZ.groupBy('ID','DATE','ELEMENT').agg(F.avg('VALUE').alias('AVERAGE'))
daily_avNZ = daily_avNZ.withColumn('YEAR', F.substring(F.col('DATE'), 1, 4))
daily_avNZ.show()
# +-----------+--------+-------+-------+----+
# |         ID|    DATE|ELEMENT|AVERAGE|YEAR|
# +-----------+--------+-------+-------+----+
# |NZM00093781|20110102|   TMAX|  311.0|2011|
# |NZ000933090|20110104|   TMAX|  233.0|2011|
# |NZM00093678|20110106|   TMAX|  309.0|2011|
# |NZM00093781|20110107|   TMAX|  258.0|2011|
# |NZ000093292|20110108|   TMAX|  223.0|2011|
# |NZ000936150|20110109|   TMIN|   65.0|2011|
# |NZM00093110|20110109|   TMAX|  248.0|2011|
# |NZ000093292|20110110|   TMIN|  136.0|2011|
# |NZ000093844|20110111|   TMAX|  190.0|2011|
# |NZ000093844|20110111|   TMIN|   99.0|2011|
# |NZ000093417|20110111|   TMAX|  234.0|2011|
# |NZM00093439|20110113|   TMAX|  164.0|2011|
# |NZ000933090|20110114|   TMAX|  236.0|2011|
# |NZ000093844|20110116|   TMAX|  190.0|2011|
# |NZ000093844|20110118|   TMAX|  242.0|2011|
# |NZ000093844|20110121|   TMAX|  186.0|2011|
# |NZ000093292|20110122|   TMAX|  178.0|2011|
# |NZ000093844|20110125|   TMAX|  192.0|2011|
# |NZ000936150|20110126|   TMIN|   88.0|2011|
# |NZ000933090|20110126|   TMIN|  103.0|2011|
# +-----------+--------+-------+-------+----+


# Write output to HDFS
daily_avNZ.coalesce(1).write.csv('hdfs:///user/lxi12/outputs/ghcnd/daily_avNZ', mode='overwrite', header=True)

# How many observations?     472271
daily_avNZ.count()
# How many years are covered ?  83
daily_avNZ.select('YEAR').distinct().count() 

# Copy to local and count the rows use bash and to plot
!hdfs dfs -copyToLocal hdfs:///user/lxi12/outputs/ghcnd/daily_avNZ/ /users/home/lxi12
!wc -l `find /users/home/lxi12/daily_avNZ/*.csv -type f`      # 472272, because of header line, so is 1 over 472271


# (e)
rainfall = daily_all.filter(F.col('ELEMENT') == 'PRCP')

rainfall = (rainfall
    .select('ID', 'DATE', 'ELEMENT', 'VALUE')
    .withColumn('COUNTRY_CODE', F.substring(F.col('ID'), 1, 2))
    .withColumn('YEAR',  F.substring(F.col('DATE'), 1, 4))
    )

rainfall = (rainfall.groupBy('YEAR', 'COUNTRY_CODE').agg({'VALUE':'mean'}))

rainfall = (
  rainfall
  .join(
    country.withColumnRenamed('CODE', 'COUNTRY_CODE'),
    on="COUNTRY_CODE",
    how="left"
  )
)

rainfall.show()
# +------------+----+------------------+--------------------+
# |COUNTRY_CODE|YEAR|        avg(VALUE)|                NAME|
# +------------+----+------------------+--------------------+
# |          MC|2009| 46.30769230769231|         Macau S.A.R|
# |          GQ|2009| 70.71777003484321|Guam [United States]|
# |          BK|2009|37.044673539518904|Bosnia and Herzeg...|
# |          JO|2009|  47.3421052631579|              Jordan|
# |          ID|2009|163.48736462093862|           Indonesia|
# |          MZ|2009| 67.22142121524202|          Mozambique|
# |          LE|2009|  36.1336032388664|             Lebanon|
# |          CW|2009|35.091603053435115|Cook Islands [New...|
# |          VE|2009|52.972093023255816|           Venezuela|
# |          GB|2009|114.67282321899737|               Gabon|
# |          NF|2009|2.8666666666666667|Norfolk Island [A...|
# |          NL|2007|26.566130872457176|         Netherlands|
# |          SP|2007|14.262157902278263|               Spain|
# |          TZ|2007| 68.71515892420538|            Tanzania|
# |          IR|2007|            9.1738|                Iran|
# |          KG|2007|2.5158878504672897|          Kyrgyzstan|
# |          FJ|2007| 88.54520037278658|                Fiji|
# |          GR|2007|15.019373018668546|              Greece|
# |          BK|2007|29.926621160409557|Bosnia and Herzeg...|
# |          EG|2007|38.037974683544306|               Egypt|
# +------------+----+------------------+--------------------+


# Write output to HDFS
rainfall.coalesce(1).write.csv('hdfs:///user/lxi12/outputs/ghcnd/rainfall/', mode = 'overwrite', header = True)

# Which country has the highest average rainfall in a single year? 
rainfall.orderBy(F.col('avg(VALUE)').desc()).show(1)              #EK
# +------------+----+----------+-----------------+
# |COUNTRY_CODE|YEAR|avg(VALUE)|             NAME|
# +------------+----+----------+-----------------+
# |          EK|2000|    4361.0|Equatorial Guinea|
# +------------+----+----------+-----------------+


# Copy to local to plot
!hdfs dfs -copyToLocal hdfs:///user/lxi12/outputs/ghcnd/rainfall/ /users/home/lxi12
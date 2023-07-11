# Databricks notebook source
# dbutils.fs.rm('/FileStore/Downloads', recurse=True)

# COMMAND ----------

display(dbutils.fs.ls('FileStore/yelp_data'))

# COMMAND ----------

display(dbutils.fs.ls('FileStore/yelp_dataset'))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# MAGIC %sh mkdir /dbfs/FileStore/yelp_dataset
# MAGIC

# COMMAND ----------

# MAGIC %sh ls /dbfs/FileStore

# COMMAND ----------

# MAGIC %sh cp -rf yelp_data/* /dbfs/FileStore/yelp_dataset

# COMMAND ----------

# dbutils.fs.cp(f"file:/databricks/driver/{my_filename}", file_path)
display(dbutils.fs.ls('dbfs:/FileStore/yelp_dataset'))

# COMMAND ----------

display(dbutils.fs.ls('.'))

# COMMAND ----------

data_file_path = '/FileStore/yelp_dataset/'
df = {}

# COMMAND ----------

for datafile in dbutils.fs.ls(data_file_path):
    print(datafile)
    print(datafile.path.split('/')[-1].split('.')[0].split('_')[-1])
# dbutils.fs.ls(data_file_path)[0].path.split('/')[-1].split('.')[0].split('_')[-1]


# COMMAND ----------

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master('local').getOrCreate()
df = {}

# COMMAND ----------

df['business'] = spark.read.json('dbfs:/FileStore/yelp_dataset/yelp_academic_dataset_business.json')

# COMMAND ----------

df_bus_cat = df['business'].select(['business_id', 'categories'])

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

dir(F)

# COMMAND ----------

dir(T)

# COMMAND ----------

df_bus_cat.display()

# COMMAND ----------

import pyspark.sql.window as W

# COMMAND ----------

dir(W)

# COMMAND ----------

df_bus_cat\
    .withColumn('category', F.explode(F.split(F.col('categories'), ',')))\
    .withColumn('category_mapping_id', F.row_number().over(W.Window.partitionBy('business_id').orderBy(F.col('business_id'))))\
    .show(100)

# COMMAND ----------

att_schema = T.ArrayType(T.StructType(
    [
        T.StructField("AcceptsInsurance", T.BooleanType(), nullable=True), 
        T.StructField("AgesAllowed", T.BooleanType(), nullable=True), 
        T.StructField("Alcohol", T.BooleanType(), nullable=True), 
        T.StructField("Ambience", T.BooleanType(), nullable=True), 
        T.StructField("BYOB", T.BooleanType(), nullable=True), 
        T.StructField("BYOBCorkage", T.BooleanType(), nullable=True), 
        T.StructField("BestNights", T.BooleanType(), nullable=True), 
        T.StructField("BikeParking", T.BooleanType(), nullable=True), 
        T.StructField("BusinessAcceptsBitcoin", T.BooleanType(), nullable=True), 
        T.StructField("BusinessAcceptsCreditCards", T.BooleanType(), nullable=True), 
        T.StructField("ByAppointmentOnly", T.BooleanType(), nullable=True), 
        T.StructField("Caters", T.BooleanType(), nullable=True), 
        T.StructField("CoatCheck", T.BooleanType(), nullable=True), 
        T.StructField("Corkage", T.BooleanType(), nullable=True), 
        T.StructField("DietaryRestrictions", T.BooleanType(), nullable=True), 
        T.StructField("DogsAllowed", T.BooleanType(), nullable=True), 
        T.StructField("DriveThru", T.BooleanType(), nullable=True), 
        T.StructField("GoodForDancing", T.BooleanType(), nullable=True), 
        T.StructField("GoodForKids", T.BooleanType(), nullable=True), 
        T.StructField("GoodForMeal", T.BooleanType(), nullable=True), 
        T.StructField("HairSpecializesIn", T.BooleanType(), nullable=True), 
        T.StructField("HappyHour", T.BooleanType(), nullable=True), 
        T.StructField("HasTV", T.BooleanType(), nullable=True), 
        T.StructField("Music", T.BooleanType(), nullable=True), 
        T.StructField("NoiseLevel", T.BooleanType(), nullable=True), 
        T.StructField("Open24Hours", T.BooleanType(), nullable=True), 
        T.StructField("OutdoorSeating", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsAttire", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsCounterService", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsDelivery", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsGoodForGroups", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsPriceRange2", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsReservations", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsTableService", T.BooleanType(), nullable=True), 
        T.StructField("RestaurantsTakeOut", T.BooleanType(), nullable=True), 
        T.StructField("Smoking", T.BooleanType(), nullable=True), 
        T.StructField("WheelchairAccessible", T.BooleanType(), nullable=True), 
        T.StructField("WiFi", T.BooleanType(), nullable=True)
    ]
))


# COMMAND ----------

df_attributes = df['business'].select(['business_id', 'attributes'])

# COMMAND ----------

df_attributes.withColumn('att_json', F.from_json(F.col('attributes'),schema))


# COMMAND ----------

df_attributes\
    .withColumn('Attributes.BusinessParking', F.col('attributes').getItem('BusinessParking'))\
    .withColumn('Attributes.ByAppointmentOnly', F.col('attributes').getItem('ByAppointmentOnly'))\
    .display()

# COMMAND ----------

display(df_attributes.filter(df_attributes.attributes.isNotNull()))

# COMMAND ----------

df_attributes.schema[1].dataType[0].name

# COMMAND ----------

# https://medium.com/@thomaspt748/how-to-flatten-json-files-dynamically-using-apache-pyspark-c6b1b5fd4777
from typing import Final, Dict, Tuple
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame as SDF
from pyspark.sql import functions as F
from pyspark.sql import types as T

def rename_dataframe_cols(df: SDF, col_names: Dict[str, str]) -> SDF:
    """
    rename columns
    """
    return df.select(*[F.col(col_name).alias(col_names.get(col_name, col_name)) for col_name in df.columns])

def update_column_names(df: SDF, index: int) -> SDF:
    df_temp = df
    all_cols = df_temp.columns
    new_cols = dict((column, f"{column}*{index}") for column in all_cols)
    df_temp = df_temp.transform(lambda df_x: rename_dataframe_cols(df_x, new_cols))

    return df_temp

def flatten_json(df_arg: SDF, index: int = 1) -> SDF:
    """
    flatten json in a spark dataframe with recursion
    """
    df = update_column_names(df_arg, index) if index == 1 else df_arg

    fields = df.schema.fields

    for field in fields:
        data_type = str(field.dataType)
        column_name = field.name
        first_10_chars = data_type[:10]

        if first_10_chars == "ArrayType(":
            # Explode Array
            df_temp = df.withColumn(column_name, F.explode_outer(F.col(column_name)))
            return flatten_json(df_temp, index + 1)
        
        elif first_10_chars == "StructType":
            current_col = column_name

            append_str = current_col

            data_type_str = str(df.schema[current_col].dataType)

            df_temp = df.withColumnRenamed(column_name, column_name + "#1") \
                if column_name in data_type_str else df
            current_col = current_col + "#1" if column_name in data_type_str else current_col

            df_before_expanding = df_temp.select(f"{current_col}.*")
            newly_gen_cols = df_before_expanding.columns

            begin_index = append_str.rfind('*')
            end_index = len(append_str)
            level = append_str[begin_index + 1: end_index]
            next_level = int(level) + 1

            custom_cols = dict((field, f"{append_str}->{field}*{next_level}") for field in newly_gen_cols)
            df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
            df_temp3 = df_temp2.transform(lambda df_x: rename_dataframe_cols(df_x, custom_cols))
            return flatten_json(df_temp3, index + 1)
    return df

# COMMAND ----------

df_attributes_fl = flatten_json(df_attributes)
df_attributes_fl.display()

# COMMAND ----------

df_attributes_fl \
    .withColumn('test', F.regexp_replace('attributes*1->Alcohol*2', 'u\'', '')) \
    .withColumn('testt', F.regexp_replace('test', '\'', '')) \
    .display()

# COMMAND ----------



# COMMAND ----------

df['tip'] = spark.read.json('dbfs:/FileStore/yelp_dataset/yelp_academic_dataset_tip.json')
display(df['tip'])

# COMMAND ----------

df['tip'].printSchema()

# COMMAND ----------

from datetime import datetime
max_datetime = datetime(9999, 12, 31, 23, 59, 59)

# COMMAND ----------

df_tip = df['tip'] \
    .withColumn("tip_row_num", F.row_number().over(W.Window.partitionBy([F.col('business_id'), F.col('user_id')]).orderBy(F.col('date')))) \
    .withColumn("start_datetime", F.to_timestamp(F.col('date'))) \
    .withColumn("end_datetime_stg", F.lead(F.col('start_datetime'), 1).over(W.Window.partitionBy([F.col('business_id'), F.col('user_id')]).orderBy(F.col('date'))) + F.expr('INTERVAL -1 seconds')) \
    .withColumn("max_datetime", F.lit(max_datetime).cast(T.TimestampType())) \
    .withColumn("end_datetime", F.when(F.isnull(F.col('end_datetime_stg')), F.col('max_datetime')).otherwise(F.col('end_datetime_stg'))) \
    .withColumn("is_current", F.when(F.isnull(F.col('end_datetime_stg')), F.lit(True)).otherwise(F.lit(False))) \
    .drop('end_datetime_stg', 'max_datetime') \
    # .display()
    

# COMMAND ----------

permanent_table_name = "df_tip"
df_tip.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table df_tip

# COMMAND ----------

df_tip.write.parquet(f'dbfs:/FileStore/tables/{permanent_table_name}')

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/'))

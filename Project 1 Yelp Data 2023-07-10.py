# Databricks notebook source
# MAGIC %md
# MAGIC # Download dataset

# COMMAND ----------

from urllib.request import urlopen
from shutil import copyfileobj
# https://stackoverflow.com/a/74032774/7876126

my_url = '''https://drive.google.com/uc?export=download&id=1ZJksDgxUTp3dG2GOPlB2vvz3UPknil6_&confirm=t&uuid=68c9d3ff-81ed-48bf-96fa-69254cfae279&at=AKKF8vy3IYHK4s-kB_NS6s58mohz:1688974748736'''
my_filename = "yelp_data.zip"

with urlopen(my_url) as in_stream, open(my_filename, 'wb') as out_file:
    copyfileobj(in_stream, out_file)

# COMMAND ----------

# MAGIC %md check local folder and unzip file

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %sh unzip yelp_data -d yelp_data_unzip

# COMMAND ----------

# MAGIC %sh ls -lah yelp_data_unzip/*

# COMMAND ----------

# MAGIC %sh rm -rf yelp_data_unzip/__MACOSX

# COMMAND ----------

# MAGIC %sh rm -rf yelp_data_unzip/yelp_data/.DS_Store

# COMMAND ----------

# MAGIC %md
# MAGIC Prep the raw file and upload to dbfs

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

file_local_path = "yelp_data_unzip/yelp_data"
dbfs_file_path = "dbfs:/FileStore/yelp_data"
dbutils.fs.mkdirs(dbfs_file_path)


# COMMAND ----------

dbutils.fs.cp(f"file:/databricks/driver/{file_local_path}", dbfs_file_path, recurse=True)

# COMMAND ----------

display(dbutils.fs.ls(dbfs_file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Load dbfs raw files

# COMMAND ----------

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master('local').appName("yelp_data").config("test", "forExample").getOrCreate()

# COMMAND ----------

print(dbfs_file_path)
df = {}

for datafile in dbutils.fs.ls(dbfs_file_path):
    datafile_name = datafile.path.split('/')[-1].split('.')[0].split('_')[-1]
    df[datafile_name] = spark.read.format('json').option("inferSchema", True).load(datafile.path)
    print(datafile.path)

print("------------------")
print(df.keys())

# COMMAND ----------

for df_name, df_data in df.items():
    print(f"------------ [df_{df_name}] Schema ------------")
    df_data.printSchema()
    print(f"---------------------------------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data exploration and Cleansing

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


# COMMAND ----------

for df_name in df.keys():
    distinct_cnt = df[df_name].distinct().count()
    cnt = df[df_name].count()
    print(f"[df_{df_name}] Table Duplicates Check:")
    print(f"Distinct Cnt: {distinct_cnt}; Cnt: {cnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC df_tip -> SCD ii

# COMMAND ----------

from datetime import datetime
max_datetime = datetime(9999, 12, 31, 23, 59, 59)

# COMMAND ----------

df_tip_SCD2 = df['tip'] \
    .withColumn("tip_row_num", F.row_number().over(Window.partitionBy([F.col('business_id'), F.col('user_id')]).orderBy(F.col('date')))) \
    .withColumn("start_datetime", F.to_timestamp(F.col('date'))) \
    .withColumn("end_datetime_stg", F.lead(F.col('start_datetime'), 1).over(Window.partitionBy([F.col('business_id'), F.col('user_id')]).orderBy(F.col('date'))) + F.expr('INTERVAL -1 seconds')) \
    .withColumn("max_datetime", F.lit(max_datetime).cast(T.TimestampType())) \
    .withColumn("end_datetime", F.when(F.isnull(F.col('end_datetime_stg')), F.col('max_datetime')).otherwise(F.col('end_datetime_stg'))) \
    .withColumn("is_current", F.when(F.isnull(F.col('end_datetime_stg')), F.lit(True)).otherwise(F.lit(False))) \
    .drop('end_datetime_stg', 'max_datetime')

# display(df_tip_SCD2)

# COMMAND ----------

df['tip'] = df_tip_SCD2
df['tip'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC df_business ->
# MAGIC - df_business
# MAGIC - df_business_categories
# MAGIC - df_business_attributes

# COMMAND ----------

# MAGIC %md
# MAGIC df_business_categories

# COMMAND ----------

df_business_cat_exploded = df['business'].select(['business_id', 'categories']).withColumn('category', F.explode(F.split(F.col('categories'), ',')))

# COMMAND ----------

# Save 
df['business_cat_exploded'] = df_business_cat_exploded
df['business_cat_exploded'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC df_business_attributes

# COMMAND ----------

df_business_attributes = df['business'].select(['business_id', 'attributes']).filter(F.col('attributes').isNotNull())

# COMMAND ----------

# https://medium.com/@thomaspt748/how-to-flatten-json-files-dynamically-using-apache-pyspark-c6b1b5fd4777
from typing import Final, Dict, Tuple
# from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame as SDF
# from pyspark.sql import functions as F
# from pyspark.sql import types as T

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

df_business_attributes_flatten = flatten_json(df_business_attributes)

# COMMAND ----------

# Save
df['business_attributes_flatten'] = df_business_attributes_flatten
df['business_attributes_flatten'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC df_business

# COMMAND ----------

# Clean and save
df['business'] = df['business'].drop('attributes', 'categories')
df['business'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC user table ->
# MAGIC - user
# MAGIC - user_friends_map

# COMMAND ----------

df_user_friends = df['user'].select(['user_id', 'friends'])
df_user_friends = df_user_friends.withColumn("friend", F.split(F.col('friends'), ","))
df['user_friends'] = df_user_friends

df['user_friends'].show()

# COMMAND ----------

df['user'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC review

# COMMAND ----------

df['review'].select('business_id','review_id','user_id').count()

# COMMAND ----------

df['review'].select('business_id','review_id','user_id').distinct().count()

# COMMAND ----------

df['review'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC checkin

# COMMAND ----------

df_checkin = df['checkin'].withColumn("last_date", F.element_at(F.split(F.col('date'), ','), -1))
df['checkin'] = df_checkin
df['checkin'].show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Persist into parquet file on dbfs

# COMMAND ----------

display(dbutils.fs.ls('./FileStore'))

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/yelp_data_parquet/')

# COMMAND ----------

print(df.keys())
for df_name in df:
    print(f'------------------ [ df_{df_name} ] -----------------')
    persisted_file_path = f'dbfs:/FileStore/tables/yelp_PersistedParquet_{df_name}'
    df[df_name].write.mode("overwrite").parquet(persisted_file_path)
    print(f'---------- [ {persisted_file_path} ] ------------')
    df[df_name].show()

# COMMAND ----------

print("DONE")

# COMMAND ----------



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



# COMMAND ----------

for df_name in df.keys():
    distinct_cnt = df[df_name].distinct().count()
    cnt = df[df_name].count()
    print(f"[df_{df_name}] Table Duplicates Check:")
    print(f"Distinct Cnt: {distinct_cnt}; Cnt: {cnt}")

# COMMAND ----------

df['tip'].withColumn("rn", F.row_number().over())

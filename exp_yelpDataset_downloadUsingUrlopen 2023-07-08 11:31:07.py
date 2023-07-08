# Databricks notebook source
import os
os.path.abspath(os.path.curdir)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

from urllib.request import urlopen
from shutil import copyfileobj
# https://stackoverflow.com/a/74032774/7876126

my_url = '''https://yelp-dataset.s3.amazonaws.com/YDC22/yelp_dataset.tgz?AWSAccessKeyId=ASIAQ2DWR7AO5OIJF4U2&Signature=QqD1mn7yKE0dJ%2BtiPpIdCRtdpiY%3D&x-amz-security-token=FwoGZXIvYXdzEBUaDF2GHj%2B%2BPOxlnpjl2CKuBMEXqpMNtYszEbrJIFQ8W9PsH%2FAquGxZtrhY%2F8gy6godYG51YSqY4EfRcsoFUNTQm9Wz4A95%2FDtoO5xsEV3hWuQ0wmJeQHfWW7qyOpzH2Qmwt9Utd6DwtYKqzRuWgwtHuG12mpqr98XTNWMVL8EklQhkEjpgCNGKLCXFavobkmZV2czPnJP9dZQd4GYt6MUU%2BxL0DoEny0IE7gr%2FF4k0R%2Ba%2BVBTbakzh9NT1PNtum0l3EcNuaGdNUcTMx3wRTNMR5VUI6VAbNRF76Q8RZ7bAUMD%2Flt%2BfUQcjRHkXkrkAvQzfWmzq7DNfyt69RbwbdSCB%2Fi9%2Fu1WitSTeibxbjb1ls8qguDNm7ChuQOqYZfkDpbPaHlh%2F5wKrAA8QyTl31oYqh%2Fh%2BFgL3SOsbVmnXWOgRvh9auyoYlS8fVVaEtg7RbF2PrL5L8A2yQv6QAiv%2BMCDvod0RY6VaZQu9Q7J7UE%2FZqBZIy9HHpNrs1%2FZcxaaIpuGC1l1tVC1VmmPatJFEQoYXMyRrvl%2FC3lewFiS%2BbRrTul12BzKYEprFH4inA2fCt7FEVz1lcUk9I3HT3ixRJjM0apVOk6LpUYMt6jFbaf5AcnqyvYs7Wrie3KcAYuTqwxiyUgo6F4gJYwU3BNbqSpE%2BXlTb9Tlz8FCGhkGv%2FmsQPww9mBUj8gQXBGFSxyhB4wBBlZGWXcrQ9sZsccmoY6rZs30ZqiEV%2B6%2FobKIMrmbITFjHYPi7Xtei0qiFmdVKpyj4%2FqalBjImJcHnwNGIBdLBnuzuhJbjNCTx%2FXSO0JQO7pNrihcI73xSzuXNXZg%3D&Expires=1688846230'''
my_filename = "yelp_dataset.tar"
file_path = "/FileStore/Downloads"
dbutils.fs.mkdirs(file_path)

with urlopen(my_url) as in_stream, open(my_filename, 'wb') as out_file:
    copyfileobj(in_stream, out_file)


# COMMAND ----------

dbutils.fs.cp(f"file:/databricks/driver/{my_filename}", file_path)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/Downloads"))

# COMMAND ----------

display(dbutils.fs.ls('file:/databricks/driver/yelp_dataset.tar'))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/Downloads"))

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls -lah

# COMMAND ----------

# MAGIC %sh mkdir yelp_dataset_extract

# COMMAND ----------

# MAGIC %sh tar -xvf yelp_dataset.tar --directory yelp_dataset_extract

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls /dbfs/FileStore/

# COMMAND ----------

# MAGIC %sh mv yelp_dataset_extract /dbfs/FileStore/Downloads/yelp_dataset_extract

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/Downloads/yelp_dataset_extract"))

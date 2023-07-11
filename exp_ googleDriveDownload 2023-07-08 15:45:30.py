# Databricks notebook source
my_url = '''https://drive.google.com/uc?export=download&id=1ZJksDgxUTp3dG2GOPlB2vvz3UPknil6_&confirm=t&uuid=db92237c-800e-41c6-b811-38395ee01d15&at=AKKF8vxaKu5ABZ7aCr8Ou1efBd3y:1688848988289'''
my_url

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from urllib.request import urlopen
from shutil import copyfileobj
# https://stackoverflow.com/a/74032774/7876126

print(my_url)
my_filename = "yelp_data.zip"
# file_path = "/FileStore/Downloads"
# dbutils.fs.mkdirs(file_path)

with urlopen(my_url) as in_stream, open(my_filename, 'wb') as out_file:
    copyfileobj(in_stream, out_file)


# COMMAND ----------

# MAGIC %sh ls -lah

# COMMAND ----------

# MAGIC %sh unzip yelp_data.zip

# COMMAND ----------

# MAGIC %sh ls -lah

# COMMAND ----------



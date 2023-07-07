# Databricks notebook source
# MAGIC %sh ls

# COMMAND ----------

import os

# COMMAND ----------

df=spark.read.csv(f"file:{os.getcwd()}/data/winequality-red.csv", header=True) # "file:" prefix and absolute file path are required for PySpark
display(df)

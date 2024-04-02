# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Date Format in All Tables

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

# COMMAND ----------


# find and store all tables
tables = []
for t in dbutils.fs.ls("mnt/bronze/SalesLT/"):
    tables.append(t.name.split('/')[0])

# read tables as dataframes
for t in tables:
    path = "/mnt/bronze/SalesLT/" + t + "/" + t + ".parquet"
    df = spark.read.format('parquet').load(path)
    columns = df.columns

    for col in columns:
        # transform only columns with dates
        if "Date" in col or "date" in col:
            df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
    # write the transformed tables to the silver layer
    output_path = "/mnt/silver/SalesLT/" + t + "/"
    df.write.format('delta').mode('overwrite').save(output_path)

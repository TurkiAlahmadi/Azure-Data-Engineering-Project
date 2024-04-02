# Databricks notebook source
# MAGIC %md
# MAGIC ## Add Underscore Between Words in Column Names

# COMMAND ----------

# find and store all tables
tables = []
for t in dbutils.fs.ls("mnt/silver/SalesLT/"):
    tables.append(t.name.split('/')[0])

# read tables as dataframes
for t in tables:
    path = "/mnt/silver/SalesLT/" + t
    df = spark.read.format('delta').load(path)
    # transform columns
    columns = df.columns
    for prev_col in columns:
        new_col = "".join(["_" + char if char.isupper() and not prev_col[i-1].isupper() else char for i, char in enumerate(prev_col)]).lstrip("_")
        df = df.withColumnRenamed(prev_col, new_col)
    
    # write the transformed tables to the gold layer
    output_path = "/mnt/gold/SalesLT/" + t + "/"
    df.write.format('delta').mode('overwrite').save(output_path)

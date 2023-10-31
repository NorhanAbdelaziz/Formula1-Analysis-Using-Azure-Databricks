# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId" , IntegerType() , False),
    StructField("raceId" , IntegerType() , True),
    StructField("driverId" , IntegerType() , True),
    StructField("constructorId" , IntegerType() , True),
    StructField("number" , IntegerType() , True),
    StructField("position" , IntegerType() , True),
    StructField("q1" , StringType() , True),
    StructField("q2" , StringType() , True),
    StructField("q3" , StringType() , True)
])

# COMMAND ----------

qualifying_df = spark.read \
  .schema(qualifying_schema) \
    .option("multiline" , True) \
    .json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , concat , col , lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df \
    .withColumnRenamed("driverId" , "driver_id") \
        .withColumnRenamed("raceId" , "race_id") \
            .withColumnRenamed("qualifyId" , "qualify_id") \
                .withColumnRenamed("constructorId" , "constructor_id") \
                    .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC Add ingestion date column

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying',processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

# dbutils.notebook.exit("success!")
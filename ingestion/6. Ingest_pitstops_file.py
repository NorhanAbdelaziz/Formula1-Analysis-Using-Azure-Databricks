# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.csv file 

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

pitstops_schema = StructType(fields=[
    StructField("raceId" , IntegerType() , False),
    StructField("driverId" , IntegerType() , True),
    StructField("stop" , StringType() , True),
    StructField("lap" , IntegerType() , True),
    StructField("time" , StringType() , True),
    StructField("duration" , StringType() , True),
    StructField("milliseconds" , IntegerType() , True),
])

# COMMAND ----------

pitstops_df = spark.read \
  .schema(pitstops_schema) \
    .option("multiline", True) \
    .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , concat , col , lit

# COMMAND ----------

pitstops_renamed_df = pitstops_df \
    .withColumnRenamed("driverId" , "driver_id") \
        .withColumnRenamed("raceId" , "race_id") \
            .withColumn("data_source", lit(v_data_source))\
                .withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add ingestion date column

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pitstops_final_df, 'f1_processed', 'pit_stops',processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("success!")
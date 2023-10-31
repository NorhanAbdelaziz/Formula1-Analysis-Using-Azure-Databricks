# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


# COMMAND ----------

laptime_schema = StructType(fields=[
    StructField("raceId" , IntegerType() , False),
    StructField("driverId" , IntegerType() , True),
    StructField("lap" , IntegerType() , True),
    StructField("position" , IntegerType() , True),
    StructField("time" , StringType() , True),
    StructField("milliseconds" , IntegerType() , True),
])


# COMMAND ----------

laptime_df = spark.read \
  .schema(laptime_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , concat , col , lit

# COMMAND ----------

laptime_renamed_df = laptime_df \
    .withColumnRenamed("driverId" , "driver_id") \
        .withColumnRenamed("raceId" , "race_id") \
            .withColumn("data_source", lit(v_data_source))\
                .withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add ingestion date column

# COMMAND ----------

laptime_final_df = add_ingestion_date(laptime_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(laptime_final_df, 'f1_processed', 'lap_time',processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("success!")
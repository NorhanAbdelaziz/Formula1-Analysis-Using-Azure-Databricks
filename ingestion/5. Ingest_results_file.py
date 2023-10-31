# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.csv file 

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
# MAGIC ##### Step 1 - Read json file using the spark dataframe reader

# COMMAND ----------

results_schema = "resultId INT , raceId INT , driverId INT , constructorId INT , number INT , grid INT , position INT , positionText STRING , positionOrder INT , points INT , laps INT , time STRING , milliseconds INT , fastestLap INT , rank INT , fastestLapTime STRING , fastestLapSpeed STRING , statusId INT"

# COMMAND ----------

results_df = spark.read \
  .schema(results_schema) \
    .json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , concat , col , lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId" , "result_id") \
    .withColumnRenamed("raceId" , "race_id") \
        .withColumnRenamed("constructorId" , "constructor_id") \
            .withColumnRenamed("driverId" , "driver_id") \
                .withColumnRenamed("positionText" , "position_text") \
                    .withColumnRenamed("positionOrder" , "position_order") \
                        .withColumnRenamed("fastestLap" , "fastest_lap") \
                            .withColumnRenamed("fastestLapTime" , "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed" , "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                        .withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop unwanted columns

# COMMAND ----------

results_dropped_df = results_renamed_df.drop('statusId')

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##### De-dupe the dataframe

# COMMAND ----------

results_final_df = results_dropped_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add ingestion date column

# COMMAND ----------

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

merge_condition = "src.result_id = tgt.result_id AND src.race_id = tgt.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results',processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("success!")
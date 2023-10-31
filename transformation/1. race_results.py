# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name" , "race_name") \
        .withColumnRenamed("date" , "race_date")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location" , "circuit_location")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
        .withColumnRenamed("time" , "race_time") \
            .withColumnRenamed("race_id", "results_race_id") \
                .withColumnRenamed("file_date", "results_file_date") 

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name" , "driver_name") \
        .withColumnRenamed("nationality" , "driver_nationality") \
            .withColumnRenamed("number" , "driver_number") \
            .withColumnRenamed("driver_id" , "isdriver_id")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name" , "team")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id , "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_result_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.isdriver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number" ,"isdriver_id", "driver_nationality","team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date") \
    .withColumn("created_date", current_timestamp()) \
        .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id "
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')



# COMMAND ----------


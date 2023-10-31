# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file 

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType , DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
.option("header" , True) \
.schema(races_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col , lit

# COMMAND ----------

races_selected_df = races_df.select(col("raceId") , col("year") , col("round") , col("circuitId") , col("name") , col("date") , col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId" , "race_id") \
.withColumnRenamed("circuitId" , "circuit_id") \
.withColumnRenamed("year" , "race_year") \
    .withColumn("data_source", lit(v_data_source)) \
         .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add Ingetion Date column to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , concat , lit , to_timestamp


# COMMAND ----------

races_added_df = races_renamed_df \
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) 

# COMMAND ----------

races_final_df = add_ingestion_date(races_added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

races_final_df.write \
    .mode("overwrite") \
        .partitionBy("race_year") \
            .format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("success!")
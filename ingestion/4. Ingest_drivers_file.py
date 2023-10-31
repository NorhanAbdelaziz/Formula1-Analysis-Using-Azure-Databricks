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

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------


drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True) 
])

# COMMAND ----------

drivers_df = spark.read \
  .schema(drivers_schema) \
    .json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , concat , col , lit

# COMMAND ----------

drivers_renamed_df = drivers_df \
    .withColumnRenamed("driverId" , "driver_id") \
        .withColumnRenamed("driverRef" , "driver_ref") \
            .withColumn("name" , concat(col("name.forename") , lit(" "), col("name.surname"))) \
                .withColumn("data_source", lit(v_data_source)) \
                     .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop unwanted columns

# COMMAND ----------

drivers_dropped_df = drivers_renamed_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add ingestion date column

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite") \
    .format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("success!")
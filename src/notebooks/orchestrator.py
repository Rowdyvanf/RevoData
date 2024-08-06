# Databricks notebook source
# MAGIC %run ./configs

# COMMAND ----------

# MAGIC %run ../utils/all_utils

# COMMAND ----------

# DBTITLE 1,imports


# COMMAND ----------

# DBTITLE 1,injest data
configs = Config()
databases = configs.get_config("databases")
data_inputs = configs.get_config("inputs")

injest_obj = Injest(databases = databases ,data_inputs = data_inputs)
injest_obj.create_databases()
injest_obj.execute_injest()

# COMMAND ----------

# DBTITLE 1,Calculate avarage revenue
transform_obj = Transform(databases = databases)
transform_obj.calculate_airbnb()
transform_obj.calculate_rentals()
transform_obj.calculate_total_avarages()

# COMMAND ----------

# DBTITLE 1,load parquet files
load_obj = Load(databases = databases, output_path = configs.get_config("output_path"))
load_obj.load_tables()

# Databricks notebook source
# MAGIC %sql
# MAGIC -- select * from final_db.airbnb
# MAGIC -- select * from final_db.rentals
# MAGIC select * from final_db.average_prices

# COMMAND ----------

# MAGIC %sql
# MAGIC select zipcode, count(*) from source_clean.airbnb group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct room_type from source_clean.airbnb

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

file_path = "/Volumes/test_databricks_v01/default/default/raw_data/airbnb.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

display(df)

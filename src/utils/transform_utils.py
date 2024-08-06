# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

class Transform:
    def __init__(self, databases: dict):
        self.databases = databases

    def calculate_airbnb(self):
        silver_db = self.databases["silver"]
        gold_db = self.databases["gold"]
        table_name = f"{silver_db}.airbnb"
        gold_table_name = f"{gold_db}.airbnb"
        airbnb_data = spark.sql(f"""
                                select * from {table_name}
                                where room_type = 'Entire home/apt'
                                """)
        #select zipcodes with avarage prices
        df_avg_price = airbnb_data.groupBy("zipcode").agg(F.round(F.avg("price"), 2).alias("average_price"))
        # save as table in gold
        df_avg_price.write.mode("overwrite").saveAsTable(gold_table_name)

    def calculate_rentals(self):
        silver_db = self.databases["silver"]
        gold_db = self.databases["gold"]
        table_name = f"{silver_db}.rentals"
        gold_table_name = f"{gold_db}.rentals"
        rentals_data = spark.sql(f"""
                                 select postalCode as zipcode, rent + additionalCostsRaw AS price
                                 from {table_name}
                                 """)
        #select zipcodes with avarage prices
        df_avg_price = rentals_data.groupBy("zipcode").agg(F.round(F.avg("price"), 2).alias("average_price"))
        # save as table in gold
        df_avg_price.write.mode("overwrite").saveAsTable(gold_table_name)

    def calculate_total_avarages(self):
        gold_db = self.databases["gold"]
        airbnb_data = spark.sql(f"""
                                select * from {gold_db}.airbnb
                                """)
        rentals_data = spark.sql(f"""
                                select * from {gold_db}.rentals
                                """)
        total_df = airbnb_data.unionAll(rentals_data).groupBy("zipcode").agg(F.round(F.avg("average_price"), 2).alias("average_price"))
        total_df.write.mode("overwrite").saveAsTable(f"{gold_db}.average_prices")
        

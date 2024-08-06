# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

class Injest:
    def __init__(self, databases: dict, data_inputs: dict):
        self.databases = databases
        self.data_inputs = data_inputs

    def create_databases(self):
        for key, value in self.databases.items():
            if not value.isidentifier():
                raise ValueError(f"Invalid database name: {value}")
            spark.sql(f"""
                      CREATE DATABASE IF NOT EXISTS {value}
                      """)
            print(f"Database {key} = {value} created or already exists")

    def execute_injest(self):
        bronze_db = self.databases['bronze']
        silver_db = self.databases['silver']
        for key, value in self.data_inputs.items():
            # Ingest raw data into bronze db
            raw_df = self.create_raw_df(value)
            raw_df.cache()
            bronze_table_name = f"{bronze_db}.{key}"
            raw_df.write.mode("overwrite").saveAsTable(bronze_table_name)
            
            # Clean up data and write to silver db
            clean_df = self.cleanup_df(raw_df,key)
            silver_table_name = f"{silver_db}.{key}"
            clean_df.write.mode("overwrite").saveAsTable(silver_table_name)
        
    @staticmethod
    def create_raw_df(input: dict) -> DataFrame:
        file_type = input.get('file_type')
        file_path = input.get('file_path')
        
        if file_type == 'csv':
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        elif file_type == 'json':
            df = spark.read.json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        return df
    
    @staticmethod
    def cleanup_df(df: DataFrame,df_name: str) -> DataFrame:
        for column in df.columns:
            # Get the datatype of the column
            column_type = dict(df.dtypes)[column]
            
            if column_type == 'string':  # Apply cleanup only to string columns
                df = df.withColumn(column, F.trim(F.col(column)))
                df = df.withColumn(column, F.regexp_replace(F.col(column), '[^\w\s/]', ''))
        
        if df_name == 'airbnb':
            # Convert 'price' column to double
            df = df.withColumn("price", F.col("price").cast("double"))
            # select only rows with zipcode and Entire home/apt
            df = df.filter(F.col("zipcode").isNotNull())
            # remove space from zipcode and select only rows where zipcode contains letters and numbers
            df = df.withColumn("zipcode", F.regexp_replace(F.col("zipcode"), " ", "")).filter(F.col("zipcode").rlike("^[0-9]{4}[A-Za-z]{2}$")).withColumn("zipcode", F.upper(F.col("zipcode")))

        elif df_name == 'rentals':
            # remove letters from rent column
            df = df.withColumn("rent", F.regexp_extract(F.col("rent"), r"(\d+)", 0).cast("double"))
            df = df.filter(F.col("rent").isNotNull())
            #remove letters from additionalCostsRaw column
            df = df.withColumn("additionalCostsRaw", F.regexp_extract(F.col("additionalCostsRaw"), r"(\d+)", 0).cast("double"))
            # zipcode filters
            df = df.filter(F.col("postalCode").isNotNull())
            df = df.withColumn("postalCode", F.regexp_replace(F.col("postalCode"), " ", "")).filter(F.col("postalCode").rlike("^[0-9]{4}[A-Za-z]{2}$")).withColumn("postalCode", F.upper(F.col("postalCode")))

        else:
            print(f"df: {df_name} not defined in cleanup_df()")
        # Remove duplicates
        df = df.dropDuplicates()
        return df

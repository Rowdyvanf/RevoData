# Databricks notebook source
class Load:
    def __init__(self,databases: dict, output_path: str):
        self.databases = databases
        self.output_path = output_path

    def load_tables(self):
        gold_db = self.databases["gold"]
        tables = spark.catalog.listTables(gold_db)
        for table in tables:

            table_name = table.name
            df = spark.table(f"{gold_db}.{table_name}")
            parquet_path = f"{self.output_path}/{table_name}.parquet"
            df.write.mode("overwrite").parquet(parquet_path)
            
            df.write.mode("overwrite").parquet(f"/Workspace/Repos/rowdyvanfrederikslust@gmail.com/RevoData/data/output{table_name}.parquet")
            
            print(f"Table {table_name} saved as: {parquet_path}")

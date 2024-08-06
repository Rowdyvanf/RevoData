# Databricks notebook source
class Config:
    def __init__(self):
        self.settings = {
            "databases": {
                "bronze": "source_raw",
                "silver": "source_clean",
                "gold": "final_db",
            },
            "inputs": {
                "airbnb": {
                    "file_path":"/Volumes/test_databricks_v01/default/default/raw_data/airbnb.csv",
                    "file_type":"csv",
                },
                "rentals":{
                    "file_path":"/Volumes/test_databricks_v01/default/default/raw_data/rentals.json",
                    "file_type":"json",
                }
            },
            "output_path":"/Volumes/test_databricks_v01/default/default/output/"
        }

    def get_config(self, key: str) -> dict:
        return self.settings.get(key, "Key not found")

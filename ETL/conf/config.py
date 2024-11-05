# Databricks notebook source
# MAGIC %md
# MAGIC ## Configurations:
# MAGIC - Centralize the configs in one place
# MAGIC - Make the config dynamic whereever it is possible

# COMMAND ----------

class Config():    
    def __init__(self):
        # Dynamicaly get the external location path urls
        self.base_dir_data = spark.sql("describe external location `unmanaged_data`").select("url").collect()[0][0]
        self.base_dir_checkpoint = spark.sql("describe external location `unmanaged_checkpoint`").select("url").collect()[0][0]

        # Since it is a small project only 1 databse is kept for bronze/silver/gold
        self.db_name = "kore_vital"
        self.maxFilesPerTrigger = 1000


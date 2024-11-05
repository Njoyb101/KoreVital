# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

class ConfigInitializer: 
    def __init__(self, env): 
        Conf = Config() 
        self.landing_zone = Conf.base_dir_data + "/raw" 
        self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints" 
        self.catalog = env 
        self.db_name = Conf.db_name 


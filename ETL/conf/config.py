# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = spark.sql("describe external location `data`").select("url").collect()[0][0]
        self.base_dir_checkpoint = spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.db_name = "kore_vital"
        self.maxFilesPerTrigger = 1000

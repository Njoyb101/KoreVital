# Databricks notebook source
# MAGIC %run ./conf/set-up

# COMMAND ----------

Sh = SetupHelper("dev")
Sh.setup()
print("="*30)
Sh.validate()

# COMMAND ----------

Sh.cleanup()

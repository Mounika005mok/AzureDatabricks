# Databricks notebook source
landing_zone = "dbfs:/FileStore/retail_data"
orders_data = landing_zone + "/orders_data"
checkpoint_path = landing_zone + "/orders_checkpoint"

# COMMAND ----------

orders_df = spark.readStream \
.format("cloudFiles") \
.option("cloudFiles.format","csv") \
.option("cloudFiles.inferSchema","true") \
.option("cloudFiles.inferColumnTypes","true") \
.option("cloudFiles.schemaLocation", checkpoint_path) \
.load(orders_data)

# COMMAND ----------

orders_df.display()

# COMMAND ----------

orders_df.writeStream \
.format("delta") \
.option("checkpointLocation",checkpoint_path) \
.outputMode("append") \
.toTable("orderdelta")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orderdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe orderdelta

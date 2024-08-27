# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions", "3")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/streaming_input/input",True)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/streaming_input/input1")

# COMMAND ----------

orders_df = spark \
.readStream \
.format("json") \
.schema(orders_schema) \
.option("path","dbfs:/FileStore/streaming_input/input1") \
.load()

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")

# COMMAND ----------

exploded_orders = spark.sql("""select order_id,customer_id,city,state,
pincode,explode(line_items) lines from orders""")

# COMMAND ----------

exploded_orders.createOrReplaceTempView("exploded_orders")

# COMMAND ----------

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

# COMMAND ----------

flattened_orders.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

def myfunction(flattened_orders,batch_id):
    flattened_orders.createOrReplaceTempView("orders_flattened")
    aggregated_orders = flattened_orders._jdf.sparkSession().sql("""select customer_id, approx_count_distinct(order_id) as orders_placed, count(item_id) as products_purchased,sum(subtotal) as amount_spent from orders_flattened group by customer_id""")
    
    aggregated_orders.createOrReplaceTempView("orders_result")
    merge_statement = """merge into orders_final_result1 t using orders_result s
    on t.customer_id == s.customer_id
    when matched then
    update set t.products_purchased = t.products_purchased + s.products_purchased, t.orders_placed = t.orders_placed + s.orders_placed,
    t.amount_spent = t.amount_spent + s.amount_spent
    when not matched then
    insert *
    """

    flattened_orders._jdf.sparkSession().sql(merge_statement)

# COMMAND ----------

streaming_query = flattened_orders \
.writeStream \
.format("delta") \
.outputMode("update") \
.option("checkpointLocation","checkpointdir109") \
.foreachBatch(myfunction) \
.start()

# COMMAND ----------

spark.sql("create table orders_final_result1 (customer_id long, orders_placed long, products_purchased long, amount_spent float)")

# COMMAND ----------

spark.sql("select * from orders_final_result1").show()

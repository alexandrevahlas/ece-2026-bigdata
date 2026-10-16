# Lab: Introduction to Spark's RDD API
# Run from the project folder, after downloading users.csv and orders.csv from the bronze layer

import re
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# RDD creation and partitions
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")

print(users_lines.getNumPartitions())
print(orders_lines.getNumPartitions())

orders_repartitioned = orders_lines.repartition(8)
print(orders_repartitioned.getNumPartitions())

# Lazy evaluation and lineage
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())

print(orders_data.count())

# The cost of no schema: the multiline address
for line in users_lines.take(6):
    print(line)

print(users_lines.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print(users_records.count())


# Manual parsing of orders
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
print(orders_rdd.take(3))

# Key-value RDDs: total quantity per user
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print(user_quantity.take(5))

user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print(user_quantity_grouped.take(5))


# Manual deduplication
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)

print(orders_rdd.count(), orders_dedup.count())


# Joining orders with users
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
print(enriched.take(3))

spark.stop()

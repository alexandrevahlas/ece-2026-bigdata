# Lab Spark RDD - Questions

The code of the lab is in `spark/lab_rdd.py`.

## RDD creation and partitions

**What decides the number of partitions `textFile()` picks by default, for a file this size?**

`textFile()` uses `minPartitions`, which is `sc.defaultMinPartitions` by default, that is `min(defaultParallelism, 2)`. Our files are very small compared to the size of an input split, so Spark just splits them in this minimum number: 2 partitions for `users.csv` and 2 for `orders.csv`. For a big file it would be one partition per split, around one per block.

**`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**

`repartition()` always shuffles, it is actually `coalesce(n, shuffle=True)`, we can see a `ShuffledRDD` in the lineage of `orders_repartitioned`. `coalesce()` without shuffle only merges existing partitions together on the same executor, so it does not move data, but it can only reduce the number of partitions.

## Lazy evaluation and lineage

**Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**

Yes, a new job with a new stage appears and it reads `orders.csv` again, 427928 bytes of input like the first time. `orders_data` is not cached, Spark only keeps the lineage, so every action recomputes everything from the file: reading, then the filter.

**At which point in this notebook would `.cache()` change that answer?**

If we call `.cache()` on `orders_data` right after the filter, before the first `count()`. Then the first count stores the partitions in memory and the second count reads them from the cache instead of reading and filtering the file again. We can see it in the Storage tab.

## The cost of no schema: the multiline address

**Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**

`spark.read.csv` is a real CSV parser. With `multiLine=True` it understands the quotes, so a line break inside a quoted field is part of the value and the record continues on the next line. `textFile()` knows nothing about CSV, it just cuts the file at every line break, so each address gives 2 elements: 101 lines for 50 users plus the header.

**In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?**

There must be exactly one record per line, with no line break inside a value, or the line breaks must be escaped. JSON Lines or a CSV without multiline fields are fine for example.

## Manual parsing of orders

**What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?**

`line.split(",")` would return 6 values instead of 5, so the unpacking into 5 variables raises a `ValueError: too many values to unpack`. Since the RDD is lazy, nothing fails in the cell itself, the error happens at the first action that processes this line and the whole job fails.

**Where does the DataFrame API perform the equivalent check, and when?**

In the CSV reader, when the data is read during an action, using the schema given or inferred at read time. The reader handles quotes so a comma inside a quoted value is not a problem, and the `mode` option decides what happens with a bad row: `PERMISSIVE` puts nulls and keeps the line in a corrupt record column, `DROPMALFORMED` drops it, and `FAILFAST` stops the job.

## Key-value RDDs: total quantity per user

**`reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?**

`groupByKey` shuffles every `(user_id, quantity)` pair, one per order, and only sums after the shuffle. More data goes through the network and the disk, and all the values of a key have to fit in memory on one executor. In my run the `reduceByKey` stage wrote 2513 bytes of shuffle against 6230 bytes for `groupByKey`, so about 2.5 times more for the same result, and the gap grows with the number of orders per user.

**For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?**

When we need all the values of a key at the same time and the result cannot be built by merging partial results two by two. For example the exact median of the quantities of a user, or getting the full list of the orders of each user sorted by date.

## Manual deduplication

**`partitionBy()` in the window function is described as a wide transformation, shuffling rows so every row of a given `order_id` lands on the same partition. What plays that role here?**

The `reduceByKey` on the pairs `(order_id, order)`. It shuffles the data by the hash of the key, so all the orders with the same `order_id` end up in the same partition before `most_recent` is applied. In the Spark UI it is the `reduceByKey` stage with 186001 bytes of shuffle write.

**Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?**

The window function. `row_number() over (partition by order_id order by ordered_at desc) = 1` directly says keep the most recent row of each order. With `reduceByKey` the logic is hidden in the `most_recent` function, and it also only works because the dates are strings in a format that sorts correctly.

## Joining orders with users

**`join()` is a wide transformation on both RDDs. What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?**

Both RDDs would need to be already partitioned by the key with the same partitioner, for example `partitionBy(n)` with the same number of partitions on both, and kept in cache. Then all the rows of a `user_id` are already in the same partition number on both sides and the join does not need a shuffle.

**Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?**

With `join()` the order is simply dropped. With `leftOuterJoin()` it is kept and the username part is `None`, so we get `(user_id, (order, None))`. Here every order has a user, so both give 2829 rows and no `None`.

**`enriched` is not yet filtered or aggregated. What is the risk of calling `.collect()` on it directly, on a dataset much larger than this one?**

`collect()` sends all the rows to the driver and puts them in its memory. On a big dataset the driver runs out of memory and crashes, or it is very slow because everything goes through one machine. It is better to filter or aggregate first, use `take()`, or write the result to storage.

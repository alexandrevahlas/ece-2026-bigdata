# Lab Spark DataFrames - Questions

The code of the lab is in `spark/lab_silver.py`. It was run with `spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, on Spark 4.1.1 with Scala 2.13.

## SparkSession with S3 access

**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**

The job is lost. In `local[*]` mode the driver and the executors run in the JVM of the service, so restarting the service kills the Spark session and everything in memory. The new service gets new credentials in its environment variables, so we have to create a new session and run the job again from the start. Also, even without a restart, a job that runs longer than the validity of the token starts failing with 403 errors on S3 because the key, secret and token given to the session are not refreshed.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**

`SimpleAWSCredentialsProvider` only sends the access key and the secret key. Onyxia gives temporary STS credentials, and they are only valid together with the session token, so S3 refuses the requests with an access denied error. With permanent keys there is no token, which is why it worked before. `TemporaryAWSCredentialsProvider` also sends the session token.

## Silver layer

**Explain the output of the code that was just run.**

The query counts the users per state and how many of them have no city. Most states have 2 or 3 users since we only have 50 random users. The only rows with missing cities are `AE`, 3 users and 3 missing, and `AP`, 2 users and 2 missing. These are military addresses like `DPO AP 09617` that have no comma and no city, so the regex finds nothing and the city becomes NULL. It shows that the parsing works and that the empty strings were correctly turned into NULL. The other states have 0 missing city.

**Compare the DataFrame and SQL cells above. Which one would you choose for a one-off exploration, and which for a model reused by several colleagues? Why?**

For a quick exploration I would use SQL, it is faster to write and everybody can read it. For a model reused by several colleagues I would use the DataFrame API in a Python module, because the code can be split into functions, reused, tested with unit tests and checked by the editor, and column errors are found when the plan is built. Both give the same result here and the same Catalyst plan, so it is mostly a question of readability and maintenance.

**`regexp_extract()` returned an empty string instead of `NULL` on no match. Where else in this notebook could that same behavior silently produce a wrong result if left unhandled?**

`state` and `zip_code` are also built with `regexp_extract()` and were not fixed. If an address does not match the pattern, they get an empty string instead of NULL. A not null check would not see them, and a `groupBy("state")` would show an empty state as a real group, like the problem that was fixed for `city`.

## Write the silver layer as Parquet

**List the objects written under `silver/users.parquet/`. How many part-files are there, and why?**

In my bucket the path is `silver/spark/users.parquet/` and it contains a `_SUCCESS` file and a single part-file, `part-00000-...-c000.snappy.parquet` of 9581 bytes. Spark writes one file per partition of the DataFrame. The CSV is read with `multiLine=True` so it cannot be split and gives only 1 partition, and the transformations keep it. For orders there is a shuffle because of the window, but adaptive query execution merges the small partitions back into 1, so we also get 1 part-file.

**`coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?**

We lose the parallelism. All the data goes through one task on one core, so the write is slow and that single task can run out of memory. `coalesce` can also reduce the parallelism of the previous steps of the same stage. And readers get one huge file instead of several files they can read in parallel.

## Write the silver layer as Iceberg tables

**Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?**

In `silver/spark/orders.parquet/` the data is in the part-file and there is only a `_SUCCESS` file next to it. Nothing describes the table, the schema is only in the footer of the Parquet file. In `warehouse/silver/orders/` the data is in `data/00000-...parquet`, with 2829 rows. The `metadata/` folder describes the table: `v1.metadata.json` has the schema, the partitioning and the list of snapshots, `snap-...avro` is the manifest list of the snapshot, the `...-m0.avro` manifest lists the data files with their statistics, and `version-hint.text` says which metadata version is the current one.

**A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?**

With a plain Parquet directory the overwrite deletes the old files and writes the new ones in place. The reader can fail because a file it listed is gone, or read a mix of old and new files, or see an empty or incomplete table. With Iceberg the new files are written next to the old ones and the commit just switches to a new metadata version at the end. The reader keeps reading the snapshot it started with, so it sees either the old table or the new one, never something in between.

**With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?**

Both jobs can start from `v1`, write their own `v2.metadata.json` and update `version-hint.text`. Object storage has no atomic rename, so one commit can overwrite the other and its changes are lost without any error, or the pointer can end up on a wrong version. A catalog service like a REST catalog, Nessie or a Hive metastore does an atomic compare and swap of the pointer: the second commit is refused and has to retry on top of the first one. It also gives one place to find the tables from several engines and to manage access.

## Compare with the dbt lab

**What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?**

In dbt `order_id` and `user_id` are `UUID` and `ordered_at` is a `TIMESTAMP WITH TIME ZONE`. In the Spark output, DuckDB reads `order_id` and `user_id` as `varchar` and `ordered_at` as a plain `timestamp`. We cast the uuids to strings. For consumers, each id takes 36 bytes instead of 16, nothing checks that the value is a valid uuid anymore, and a join with a real `UUID` column needs a cast. The timestamp also loses the time zone information, so a consumer has to know it is in UTC.

**Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?**

One object per model is easier: one path, and any tool can open it. The directory of part-files scales better because each task writes its own file in parallel, and readers can also read the files in parallel. The consumer just has to read the whole directory or use a glob like `*.parquet`.

**The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?**

This lab only used Python `assert` on row counts, plus a `groupBy("order_id").count()` to look for duplicates. To get the same guarantees I would need a query per rule that must return 0 rows: a `groupBy` with `count > 1` for unique, a filter on `isNull()` for not null, a filter with `~isin(...)` for the accepted values, and a left anti join between orders and users for relationships, and then make the job fail if one of them returns rows. A data quality library like Great Expectations or Deequ can also do it.

**`ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?**

The Python variables and the order of the cells. `stg_users` is built from `users_typed`, which is built from `users_bronze`, and Spark keeps this lineage in the logical plan of each DataFrame. For the SQL part the temporary views like `orders_typed` play this role. But there is no global graph: we have to run the cells in the right order ourselves, and nothing can rebuild only what changed like `dbt build --select`.

**Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?**

dbt with DuckDB built the silver layer in a few seconds, while the Spark script takes about a minute. But it is not the same work. Spark has to start a JVM and download the Iceberg jar, and because it is lazy it reads the CSV from S3 again for almost every `count()` and `show()`. It also writes the data twice, in Parquet and in Iceberg, while the dbt models were only views. And with such small data the startup cost of Spark is much bigger than the actual processing.

**For each of these cases, would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?**

10 GB per day: dbt with DuckDB, it fits on one machine and it is simpler and cheaper. 10 TB per day: Spark on a cluster, because the data has to be split over many machines. A team of analysts who only know SQL: dbt, since everything is written in SQL with tests and documentation. If the volume is big, dbt can also run its SQL on Spark.

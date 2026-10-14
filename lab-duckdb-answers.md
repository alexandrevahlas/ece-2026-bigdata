# Lab DuckDB - Questions

## S3 configuration

**The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?**

The secret is saved in plain text in `~/.duckdb/stored_secrets`. Any person or program that can read this file gets the S3 keys, and the file can also end up in a backup or be pushed to Git by mistake. On Onyxia the risk is limited because the credentials are temporary and expire after some hours, the service runs in my own pod that nobody else uses, and the keys only give access to my own bucket.

**The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?**

I would not give my own credentials to the Job. The Job should have its own credentials with only the permissions it needs, for example read only on the bronze prefix. They are stored in a Secret that is only mounted in the pod of the Job, and with RBAC only the service account of the Job can read this Secret. Even better, the service account can get short lived credentials automatically so no key is stored at all.

## Query the bronze layer

**The dataset contains 50 users, each one with at least one order, and 2829 orders with unique identifiers. Why does `approx_unique` return 40 and 3167?**

`approx_unique` does not count the exact number of distinct values, it estimates it with the HyperLogLog algorithm. This is much faster and uses very little memory on big datasets, but the result has an error margin, and here it is quite big. To get the real value we have to use `count(DISTINCT ...)`.

**The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?**

DuckDB only looks at a sample of rows to guess the type of each column, so it can guess a wrong type. For example if a column only has numbers at the beginning it is read as an integer, and the query fails later when a row contains text. A column that is empty in the sample also becomes VARCHAR. To avoid this we can give the types ourselves or increase `sample_size`.

## Parquet export

**Why is the `uuid` column barely compressed?**

Every uuid is random and different from the others, so there are no repeated values and no patterns. Dictionary encoding does not help because all the values are unique, and Snappy cannot find repetitions to compress.

**Why is the compressed size of some columns larger than their uncompressed size?**

For `date` and `product` the data is already very compact after the encoding, so Snappy cannot make it smaller. But the compression still adds a few bytes of its own, so the result ends up a little bigger than the original.

## Hive partitioning

**Why is partitioning by `uuid` a bad idea?**

Every order has a different uuid, so we would get one directory and one tiny file per order, 2829 files with one row each. This is the small files problem seen in the object storage module: each file needs its own request, listing all the keys becomes slow and costly, and there is more metadata than data. Also queries almost never filter on a single uuid, so the partitioning would not help anyway.

**Which partition column would you choose for a dataset of orders growing every day?**

The date of the order, partitioned by day or by month depending on the volume. The new orders always go in a new partition and most queries filter on a period of time, so only the files of that period are read. The granularity has to be chosen so that the files do not become too small.

## CSV vs. Parquet at scale

**How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?**

My file has 252416 orders split in 3 row groups. The last order is in October 2048, so the max date of every row group is before 2100 and all 3 row groups are skipped. That is why the query only received 16 KiB with 1 GET request: DuckDB only read the footer.

**The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?**

The filter works because each row group covers a different period, so its min and max dates are close. If the orders were shuffled, every row group would have dates from 2020 to 2048 and the same min and max. DuckDB could not skip any row group and would have to read the date column of the whole file.

**Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which part to the parsing of the CSV file?**

The CSV query took 1.12 s and the Parquet query 0.31 s. The CSV query downloaded the whole file, 28.2 MiB, even if it only needs 2 columns, while the Parquet query only downloaded 203.5 KiB, the footer and the 2 columns. So most of the difference comes from the network. The rest comes from parsing: for the CSV, DuckDB has to read text, split the lines and convert every value to the right type, while Parquet is already stored in binary with its types. To measure the two parts separately we could run the same queries on local copies of the files, then there is no network and the difference left is only the parsing.

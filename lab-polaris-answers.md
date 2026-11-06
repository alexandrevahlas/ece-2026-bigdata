# Lab Polaris - Questions

Polaris was deployed with the Helm chart of the deployment lab, the files are in `polaris/deployment/`. The code of the lab is in `spark/lab_polaris.py`. It was run twice with `spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`, once with `POLARIS_PRINCIPAL=dev` and once with `POLARIS_PRINCIPAL=analyst`.

## SparkSession with the Polaris catalog

**Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?**

`SHOW NAMESPACES IN lab` was answered by Spark itself. The `lab` catalog is a Hadoop catalog, there is no server behind it, the Iceberg library just lists the folders under `s3a://user-a-vahlas-ece/warehouse/` with my S3 keys. It returned `silver` because the folder `warehouse/silver/` exists since the Spark lab.

`SHOW NAMESPACES IN polaris` was answered by the Polaris server, through the REST API at `http://polaris:8181/api/catalog`. Polaris answers from its own database in PostgreSQL and not from the bucket. We can tell because it returned an empty list at first, even if the same files are in the bucket, since nothing was created in Polaris yet. Also when I ran it as `analyst` the answer was a `ForbiddenException` with the message "Principal 'analyst' ... is not authorized for op LIST_NAMESPACES", which can only come from Polaris checking the roles.

**Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?**

In a request to Polaris. The credential is given to the Spark session in the config, but the Iceberg REST client sends it to the token endpoint of Polaris `/api/catalog/v1/oauth/tokens` and gets a bearer token back. Every catalog request after that is sent with this token, and Polaris uses it to know the principal and its roles. The credential is never sent to the bucket. The S3 files are read and written with the S3A keys of the session, because there is no credential vending on this storage, and Polaris itself uses the keys of the `polaris-s3` secret.

## Register the Silver table in Polaris

**What did `register_table` write, in Polaris and in the bucket? Compare the number of objects under `warehouse/silver/orders/` before and after.**

In Polaris it created the table entity `silver.orders` in the namespace `silver`, in the PostgreSQL database. This entity only stores a pointer to the current metadata file, `s3a://user-a-vahlas-ece/warehouse/silver/orders/metadata/v1.metadata.json`. In the bucket it wrote nothing. There were 5 objects under `warehouse/silver/orders/` before and still 5 after: the data file, the manifest, the manifest list, `v1.metadata.json` and `version-hint.text`, which still contains `1`. The count through Polaris gave 2829 orders, the same as in the Spark lab. It is not 5,000 like in the lab text because our generated dataset has 2829 orders after deduplication.

**Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?**

Because the two catalogs keep their own pointer and do not know about each other. A commit through `lab` would write `v2.metadata.json` and update `version-hint.text`, but Polaris would still point to `v1.metadata.json`, so everybody reading through Polaris would see old data. If someone then writes through Polaris, the new metadata is built from v1, so the history of the table splits in two and the changes made through `lab` are lost for Polaris. A cleanup like expiring snapshots from one side could also delete files that the other side still uses. And writing through `lab` skips the access control of Polaris completely.

## Create tables as `dev`

**Where did Polaris store the files for `silver.users`, and why?**

In `s3://user-a-vahlas-ece/warehouse/silver/users/`, with a `data/` folder and a `metadata/` folder. The bootstrap job created the catalog with the default base location `s3://user-a-vahlas-ece/warehouse/`, and when a table is created without a location the path is built as base location, then namespace, then table name. That is also why the location starts with `s3://` and we need `spark.hadoop.fs.s3.impl` mapped to S3A. It is the same folder as the `lab.silver.users` table of the Spark lab, so the new data file `00000-3-...parquet` is next to the old `00000-56-...parquet`. The metadata files are named differently: `00000-<uuid>.metadata.json` and `00001-<uuid>.metadata.json`, without a `version-hint.text`, because the pointer is kept in Polaris and not in a file.

**Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the data files?**

For `silver.orders` the data files and the metadata were written before by Spark with the Hadoop catalog. Polaris did not write or check anything, it only references the existing metadata file, and the `lab` catalog can still commit on the same files with its `version-hint.text`. For `silver.users` Polaris chose the location, Spark wrote the data files where Polaris told it to, and every commit goes through Polaris, which saves the new metadata file as the current one. So Polaris owns the life cycle of `silver.users`, for example a drop with purge can delete its files. In both cases the data files are normal Parquet files in my bucket and I access them with my own S3 keys, so Polaris cannot stop someone who has the bucket keys from reading or changing the files directly.

## Schema change as `dev`

**Which metadata file records the change? Is it the same file that `register_table` pointed to?**

The change is recorded in `warehouse/silver/users/metadata/00001-61fc53cc-2f5a-4601-8be0-c062aef3dc21.metadata.json`. The `createOrReplace()` wrote `00000-19fd52cb-...metadata.json`, and the `ALTER` wrote the new file `00001-...` with a new schema with id 1 that has the `country` column. The file `00000-...` is listed in its `metadata-log` and Polaris now points to `00001-...`. There is still only one snapshot, 8874735014659247104, because only the schema changed and not the data. It is not the same file that `register_table` pointed to. `register_table` pointed to `orders/metadata/v1.metadata.json`, of another table, and this file was not modified. Iceberg never modifies an existing metadata file, each commit writes a new one and the catalog moves the pointer.

**Which privilege does Polaris check for this statement?**

The `ALTER TABLE ... ADD COLUMN` is sent as an update of the table metadata, and Polaris checks the operation `ADD_TABLE_SCHEMA` on the table. We can see this name in the error of the analyst. It needs a privilege to write the table metadata, `TABLE_WRITE_PROPERTIES`, which is part of what `CATALOG_MANAGE_CONTENT` gives. `dev` has `CATALOG_MANAGE_CONTENT` through `data_engineer` and `catalog_contributor`, so the statement worked. The analyst only has read privileges, so the same statement was refused.

## Read as the analyst

**Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?**

The missing privilege is `TABLE_WRITE_DATA`. The error was "Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT". Adding a snapshot is how an `INSERT` commits new data, and that needs `TABLE_WRITE_DATA`. `catalog_reader` only provides `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`, I checked the grants with the management API. With these the analyst could run `SHOW TABLES IN polaris.silver` and the `GROUP BY product` on `silver.orders`. But it could not run `SHOW NAMESPACES IN polaris`, refused for `LIST_NAMESPACES`, because there is no `NAMESPACE_LIST` in the role. The `ALTER TABLE ... ADD COLUMN phone` was refused for `ADD_TABLE_SCHEMA`.

**Check the snapshots of `silver.orders` as `dev`. Did the failed `INSERT` create one?**

No. As `dev`, `silver.orders` still has one snapshot, 5945283590285559202 with the operation `overwrite`, and Polaris still points to `v1.metadata.json`. Spark wrote the new data file first and then asked Polaris to commit. Polaris refused the commit, so Spark aborted the write, we can see "IcebergBatchWrite ... aborted" in the log, and deleted the file. There are still 5 objects under `warehouse/silver/orders/`. Polaris refused the operation before any snapshot was committed, but it is worth noticing that it could not stop Spark from writing a file in the bucket, because the analyst session has my S3 keys.

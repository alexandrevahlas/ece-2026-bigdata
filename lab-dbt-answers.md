# Lab dbt - Questions

## Bronze layer

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?**

Since the raw files are never modified, we still have the original data. We only have to fix the SQL of the model and run `dbt build` again to rebuild the silver layer from bronze. We don't need to ask the source system to send the data again, which may not even be possible after six months.

**The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?**

I would store the orders in Parquet instead of CSV, with one new file at each ingestion, partitioned by date, for example by day or by month. Queries would only read the partitions of the period they need and only the columns they use, instead of the whole history every time.

## Build

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**

A view only stores the SQL query. When we query it, DuckDB runs the query again, so here it reads and transforms the CSV files from S3 every time. A view is better when the data is small or changes often and we always want the latest version, and it does not take any storage. A table is better when the data is queried a lot or when the transformation is slow.

**`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?**

The text form of a uuid has 36 characters, 32 hexadecimal characters and 4 dashes, so it takes 36 bytes, more than twice the 16 bytes of the UUID type.

## Data tests

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?**

For the age of the customers I would replace the invalid birthdates with NULL in the silver layer. That way wrong ages are not displayed and these users are just left out of the age statistics. For the sales per region the birthdate is not used at all, so I would keep all the records and keep the test as a warning to monitor it. Rejecting these users would also remove their orders and the sales would be wrong.

**The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?**

A singular test that joins `stg_users` with a reference table of the zip code ranges of each state, and returns the users whose zip code is not in the range of their state. It needs official reference data, for example the zip code list from USPS or the Census Bureau, that could be loaded as a dbt seed.

# Databricks notebook source
import json
import re

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main", "Target catalog")
dbutils.widgets.text("schema", "bakehouse_medallion", "Target schema")
dbutils.widgets.text("volume", "raw_files", "Target volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

identifier_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for name, value in {
    "catalog": catalog,
    "schema": schema,
    "volume": volume,
}.items():
    if not identifier_pattern.fullmatch(value):
        raise ValueError(f"Invalid {name}: {value!r}")

base_path = f"/Volumes/{catalog}/{schema}/{volume}"

sources = [
    {
        "table": "bronze_transactions",
        "format": "json",
        "path": f"{base_path}/transactions/*.json",
        "options": {},
    },
    {
        "table": "bronze_customers",
        "format": "csv",
        "path": f"{base_path}/customers/*.csv",
        "options": {"header": "true"},
    },
    {
        "table": "bronze_franchises",
        "format": "csv",
        "path": f"{base_path}/franchises/*.csv",
        "options": {"header": "true"},
    },
]

written_tables = []

for source in sources:
    dataframe = (
        spark.read.format(source["format"])
        .options(**source["options"])
        .load(source["path"])
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

    table_name = f"`{catalog}`.`{schema}`.`{source['table']}`"

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    written_tables.append((source["table"], table_name))

results = []

for table, table_name in written_tables:
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")
    results.append({"table": table, "row_count": row_count})

dbutils.notebook.exit(json.dumps(results))

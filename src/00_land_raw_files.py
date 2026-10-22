# Databricks notebook source
import json
import re

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

exports = [
    {
        "source": "samples.bakehouse.sales_transactions",
        "path": f"{base_path}/transactions",
        "format": "json",
        "options": {},
    },
    {
        "source": "samples.bakehouse.sales_customers",
        "path": f"{base_path}/customers",
        "format": "csv",
        "options": {"header": "true"},
    },
    {
        "source": "samples.bakehouse.sales_franchises",
        "path": f"{base_path}/franchises",
        "format": "csv",
        "options": {"header": "true"},
    },
]

results = []

for export in exports:
    dataframe = spark.table(export["source"])
    row_count = dataframe.count()

    (
        dataframe.coalesce(1)
        .write.format(export["format"])
        .mode("overwrite")
        .options(**export["options"])
        .save(export["path"])
    )

    results.append(
        {
            "source": export["source"],
            "destination": export["path"],
            "format": export["format"],
            "row_count": row_count,
        }
    )

dbutils.notebook.exit(json.dumps(results))

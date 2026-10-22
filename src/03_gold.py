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

silver = spark.table(
    f"`{catalog}`.`{schema}`.`silver_sales_enriched`"
)

revenue_by_franchise = (
    silver.groupBy(
        "franchise_name",
        "franchise_city",
        "franchise_country",
    )
    .agg(
        F.sum("totalPrice").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("transactionID").alias("transaction_count"),
    )
    .orderBy(F.desc("total_revenue"))
)

daily_product_sales = (
    silver.withColumn("sale_date", F.to_date("dateTime"))
    .groupBy("sale_date", "product")
    .agg(
        F.sum("quantity").alias("total_units"),
        F.sum("totalPrice").alias("total_revenue"),
    )
)

top_customers = (
    silver.groupBy(
        "customerID",
        "customer_first_name",
        "customer_last_name",
        "customer_city",
        "customer_country",
    )
    .agg(
        F.sum("totalPrice").alias("total_revenue"),
        F.countDistinct("transactionID").alias("transaction_count"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
    .drop("customerID")
)

silver_total_revenue = silver.agg(
    F.sum("totalPrice").alias("total_revenue")
).first()["total_revenue"]

franchise_total_revenue = revenue_by_franchise.agg(
    F.sum("total_revenue").alias("total_revenue")
).first()["total_revenue"]

daily_total_revenue = daily_product_sales.agg(
    F.sum("total_revenue").alias("total_revenue")
).first()["total_revenue"]

if franchise_total_revenue != silver_total_revenue:
    raise ValueError(
        "Revenue reconciliation failed for gold_revenue_by_franchise: "
        f"silver={silver_total_revenue}, gold={franchise_total_revenue}"
    )

if daily_total_revenue != silver_total_revenue:
    raise ValueError(
        "Revenue reconciliation failed for gold_daily_product_sales: "
        f"silver={silver_total_revenue}, gold={daily_total_revenue}"
    )

tables = [
    ("gold_revenue_by_franchise", revenue_by_franchise),
    ("gold_daily_product_sales", daily_product_sales),
    ("gold_top_customers", top_customers),
]

results = []

for table, dataframe in tables:
    table_name = f"`{catalog}`.`{schema}`.`{table}`"

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")
    results.append({"table": table, "row_count": row_count})

print(f"Reconciled total revenue: {silver_total_revenue}")

dbutils.notebook.exit(
    json.dumps(
        {
            "tables": results,
            "reconciled_total_revenue": str(silver_total_revenue),
        }
    )
)

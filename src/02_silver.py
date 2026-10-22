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

bronze_transactions = spark.table(
    f"`{catalog}`.`{schema}`.`bronze_transactions`"
)
bronze_customers = spark.table(
    f"`{catalog}`.`{schema}`.`bronze_customers`"
)
bronze_franchises = spark.table(
    f"`{catalog}`.`{schema}`.`bronze_franchises`"
)

transactions = (
    bronze_transactions
    .withColumn("dateTime", F.col("dateTime").cast("timestamp"))
    .withColumn("quantity", F.col("quantity").cast("long"))
    .withColumn("unitPrice", F.col("unitPrice").cast("decimal(18,2)"))
    .withColumn("totalPrice", F.col("totalPrice").cast("decimal(18,2)"))
    .dropDuplicates(["transactionID"])
    .withColumn(
        "cardNumber",
        F.when(
            F.col("cardNumber").isNotNull(),
            F.concat(
                F.lit("****"),
                F.expr("right(CAST(cardNumber AS STRING), 4)"),
            ),
        ),
    )
)

quality_checked = transactions.withColumn(
    "reject_reason",
    F.concat_ws(
        "; ",
        F.when(F.col("transactionID").isNull(), "null_transactionID"),
        F.when(F.col("customerID").isNull(), "null_customerID"),
        F.when(F.col("franchiseID").isNull(), "null_franchiseID"),
        F.when(F.col("dateTime").isNull(), "null_dateTime"),
        F.when(F.col("quantity") <= 0, "non_positive_quantity"),
        F.when(
            F.col("totalPrice") != F.col("quantity") * F.col("unitPrice"),
            "total_price_mismatch",
        ),
    ),
)

rejected = quality_checked.filter(F.col("reject_reason") != "")

valid_transactions = (
    quality_checked
    .filter(F.col("reject_reason") == "")
    .drop("reject_reason")
    .withColumnRenamed("_source_file", "transaction_source_file")
    .withColumnRenamed("_ingested_at", "transaction_ingested_at")
)

customers = bronze_customers.select(
    F.col("customerID").cast("long").alias("customerID"),
    F.col("first_name").alias("customer_first_name"),
    F.col("last_name").alias("customer_last_name"),
    F.col("city").alias("customer_city"),
    F.col("state").alias("customer_state"),
    F.col("country").alias("customer_country"),
    F.col("continent").alias("customer_continent"),
    F.col("postal_zip_code").alias("customer_postal_zip_code"),
    F.col("gender").alias("customer_gender"),
    F.col("_source_file").alias("customer_source_file"),
    F.col("_ingested_at").alias("customer_ingested_at"),
)

franchises = bronze_franchises.select(
    F.col("franchiseID").cast("long").alias("franchiseID"),
    F.col("name").alias("franchise_name"),
    F.col("city").alias("franchise_city"),
    F.col("district").alias("franchise_district"),
    F.col("zipcode").alias("franchise_zipcode"),
    F.col("country").alias("franchise_country"),
    F.col("size").alias("franchise_size"),
    F.col("longitude").cast("double").alias("franchise_longitude"),
    F.col("latitude").cast("double").alias("franchise_latitude"),
    F.col("supplierID").cast("long").alias("franchise_supplierID"),
    F.col("_source_file").alias("franchise_source_file"),
    F.col("_ingested_at").alias("franchise_ingested_at"),
)

enriched = (
    valid_transactions
    .join(customers, on="customerID", how="left")
    .join(franchises, on="franchiseID", how="left")
)

tables = [
    ("silver_sales_enriched", enriched),
    ("silver_sales_rejected", rejected),
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

dbutils.notebook.exit(json.dumps(results))

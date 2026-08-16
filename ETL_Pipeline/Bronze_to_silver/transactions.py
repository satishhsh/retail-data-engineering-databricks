from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver layer: Apply standardization and data quality rules
@dp.table(
    name="retail_p.retail_silver.transactions",
    comment="Standardized transactions with data quality checks applied",
    table_properties={"delta.enableChangeDataFeed": "true"},
    cluster_by=["store_id", "product_id"]
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_store_id", "store_id IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
@dp.expect("valid_selling_price", "selling_price >= 0")
@dp.expect("valid_discount", "discount_amount >= 0")

def transactions_silver():
    return (
        spark.readStream.table("retail_p.blob_bonze.transactions")
        .select(
            # Keep transaction identifiers as-is
            F.col("transaction_id"),
            F.trim(F.col("opportunity_name")).alias("opportunity_name"),
            F.col("product_id"),
            F.col("store_id"),
            
            # Cast numeric fields to proper types
            F.col("quantity").cast("int").alias("quantity"),
            F.col("selling_price").cast("int").alias("selling_price"),
            F.col("discount_amount").cast("int").alias("discount_amount"),
            
            # Cast timestamp field
            F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
            
            # Standardize categorical fields
            F.trim(F.col("payment_mode")).alias("payment_mode"),
            F.trim(F.col("sales_channel")).alias("sales_channel"),
            
            
        )
    )
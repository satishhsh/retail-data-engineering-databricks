from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_p.retail_silver.inventory",
    comment="Cleaned inventory data with basic quality checks"
)
@dp.expect_or_drop("valid_inventory_id", "inventory_id IS NOT NULL AND TRIM(inventory_id) != ''")
@dp.expect("valid_product_id", "product_id IS NOT NULL AND TRIM(product_id) != ''")
@dp.expect("valid_store_id", "store_id IS NOT NULL AND TRIM(store_id) != ''")
@dp.expect("valid_stock_quantity", "stock_quantity IS NOT NULL AND stock_quantity >= 0")
@dp.expect("valid_reorder_level", "reorder_level IS NOT NULL AND reorder_level >= 0")
def inventory():
    """
    Bronze to Silver transformation for inventory.
    
    Standardization operations:
    - Trim whitespace from string columns
    - Uppercase warehouse_location for consistency
    
    Data Quality Rules:
    - Drop records with missing inventory_id, product_id, or store_id
    - Drop records with invalid stock_quantity (null or negative)
    - Warn on invalid reorder_level
    """
    return (
        spark.readStream.table("retail_p.postgres_bronze.inventory")
        .select(
            # Trim key columns
            F.trim(F.col("inventory_id")).alias("inventory_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("store_id")).alias("store_id"),
            
            # Quantity columns
            F.col("stock_quantity"),
            F.col("reorder_level"),
            
            # Inventory status
            F.when(
                F.col("stock_quantity") < F.col("reorder_level"),
                "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
            
            # Standardize warehouse location
            F.trim(F.col("warehouse_location")).alias("warehouse_location"),
            
            # Timestamps
            F.col("last_stock_update"),
            F.current_timestamp().alias("processed_at")
        )
    )
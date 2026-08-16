from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_p.retail_silver.product_catalog",
    comment="Standardized product catalog with data quality rules applied"
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND TRIM(product_id) != ''")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND TRIM(product_name) != ''")
@dp.expect_or_drop("valid_unit_price", "unit_price IS NOT NULL AND unit_price >= 0")
@dp.expect("has_category", "category IS NOT NULL")
@dp.expect("has_brand", "brand IS NOT NULL")
@dp.expect("valid_launch_date", "launch_date IS NULL OR launch_date <= CURRENT_DATE()")

def product_catalog():
    """
    Bronze to Silver transformation for product catalog.
    
    Standardization operations:
    - Trim whitespace from string columns
    - Uppercase category and subcategory for consistency
    - Handle null supplier names with 'Unknown'
    - Ensure active flag defaults to false if null
    - Round unit price to 2 decimal places
    
    Data Quality Rules:
    - Drop records with missing/empty product_id or product_name
    - Drop records with invalid unit_price (null or negative)
    - Warn on missing category, brand
    - Warn on launch dates in the future

    Additional:
    - Add is_active column based on end_at being null
    - Include start_at and end_at columns as is
    """
    return (
        spark.readStream.table("retail_p.postgres_bronze.product_catalog")
        .select(
            # Trim and clean string columns
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            
            # Standardize category fields
            F.upper(F.trim(F.col("category"))).alias("category"),
            F.initcap(F.trim(F.col("subcategory"))).alias("subcategory"),
            
            # Clean brand
            F.trim(F.col("brand")).alias("brand"),
            
            # Round price to 2 decimal places for consistency
            F.round(F.col("unit_price"), 2).alias("unit_price"),
            
            # Handle null supplier names
            F.coalesce(F.trim(F.col("supplier_name")), F.lit("Unknown")).alias("supplier_name"),
            
            # Date fields
            F.col("launch_date"),
            
            # SCD Type 2 columns - rename from double underscore to single
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),
            
            # is_active based on end_at being null
            (F.col("__END_AT").isNull()).alias("is_active"),
            
            # Timestamps
            F.col("updated_at"),
            F.current_timestamp().alias("processed_at")
        )
    )
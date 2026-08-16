from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_p.retail_silver.account",
    comment="Silver layer account data with standardization and data quality rules"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect("valid_name", "customer_name IS NOT NULL AND LENGTH(TRIM(customer_name)) > 0")
def account_silver():
    """
    Bronze to Silver transformation for Account data.
    Applies standardization and core data quality rules.
    """
    return (
        spark.readStream.table("retail_p.salesforce_bronze.account")
        .select(
            # IDs - no transformation needed
            F.col("Id").alias("id"),
            F.col("IsDeleted").alias("is_deleted"),
            # F.col("ParentId").alias("parent_id"),
            
            # Basic info - trim and standardize
            F.upper(F.trim(F.col("Name"))).alias("customer_name"),
            F.trim(F.col("Type")).alias("type"),
            F.when(F.col("Industry").isNull(), F.lit("unknown")).otherwise(F.trim(F.col("Industry"))).alias("industry"),
            F.trim(F.col("Description")).alias("description"),
            
            # Billing address - trim and standardize
            # F.trim(F.col("BillingStreet")).alias("billing_street"),
            F.trim(F.col("BillingCity")).alias("billing_city"),
            F.trim(F.col("BillingState")).alias("billing_state"),
            # F.trim(F.col("BillingPostalCode")).alias("billing_postal_code"),
            F.trim(F.col("BillingCountry")).alias("billing_country"),

            
            # Shipping address - trim and standardize
            # F.trim(F.col("ShippingStreet")).alias("shipping_street"),
            # F.trim(F.col("ShippingCity")).alias("shipping_city"),
            # F.trim(F.col("ShippingState")).alias("shipping_state"),
            # F.trim(F.col("ShippingPostalCode")).alias("shipping_postal_code"),
            # F.trim(F.col("ShippingCountry")).alias("shipping_country"),
            
            # Contact info - trim
            F.trim(F.col("Phone")).alias("phone"),
            # F.trim(F.col("Fax")).alias("fax"),
            F.trim(F.col("Website")).alias("website"),
            
            # Business metrics - no transformation needed
            # F.col("AnnualRevenue").alias("annual_revenue"),
            # F.col("NumberOfEmployees").alias("number_of_employees"),
            
            
            # CDC column - derive is_active based on __END_AT
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active")
        )
    )
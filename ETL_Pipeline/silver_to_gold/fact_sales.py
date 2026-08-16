from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_p.retail_gold.fact_sales",
    comment="Gold layer fact table combining sales transactions with opportunity details"
)
def fact_sales():
    # Read from silver layer tables
    transactions = spark.read.table("retail_p.retail_silver.transactions")
    opportunity = spark.read.table("retail_p.retail_silver.opportunity")
    
    # Left join transactions with opportunity on opportunity_name = name
    result = transactions.alias("t").join(
        opportunity.alias("o"),
        F.col("t.opportunity_name") == F.col("o.name"),
        "left"
    ).select(
        # All columns from transactions
        F.col("t.transaction_id"),
        F.col("t.opportunity_name"),
        F.col("t.product_id"),
        F.col("t.store_id"),
        F.col("t.quantity"),
        F.col("t.selling_price"),
        F.col("t.discount_amount"),
        F.col("t.transaction_timestamp"),
        F.col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        F.col("t.payment_mode"),
        F.col("t.sales_channel"),
        
        # Selected columns from opportunity
        F.col("o.stage_name"),
        F.col("o.amount"),
        F.col("o.account_id").alias("customer_id")
    )
    
    return result
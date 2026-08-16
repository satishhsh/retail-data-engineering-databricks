from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_p.retail_silver.opportunity",
    comment="Silver layer opportunity data with standardization and data quality checks"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect("valid_amount", "amount IS NULL OR amount >= 0")
@dp.expect("valid_probability", "probability IS NULL OR (probability >= 0 AND probability <= 100)")
@dp.expect("valid_close_date", "CloseDate IS NOT NULL")
@dp.expect("valid_stage", "stage_name IS NOT NULL")

def opportunity():
    """
    Bronze to Silver transformation for Salesforce Opportunity data.
    Applies standardization and data quality rules.
    Standardizes existing columns by overwriting them with cleaned values.
    """
    return (
        spark.readStream.table("retail_p.salesforce_bronze.opportunity")
        .select(
            # String standardization - trim whitespace
            F.trim(F.col("Id")).alias("id"),
            F.col("IsDeleted").alias("is_deleted"),
            F.col("AccountId").alias("account_id"),
            F.col("Name").alias("name"),
            F.col("Description").alias("description"),
            F.col("StageName").alias("stage_name"),
            F.col("Amount").alias("amount"),
            F.col("Probability").alias("probability"),
            F.col("CloseDate"),
            F.col("Type").alias("type"),
            F.col("LeadSource").alias("lead_source"),
            F.col("IsClosed").alias("is_closed"),
            F.col("IsWon").alias("is_won"),
            F.col("ForecastCategory").alias("forecast_category"),
            F.trim(F.col("OwnerId")).alias("owner_id"),
            F.col("CreatedDate").alias("created_date"),
            F.when(F.col("Amount") > 10000, "Enterprise")
             .when(F.col("Amount") > 2500, "Mid_Market")
             .otherwise("Small").alias("deal_size")
        )
    )
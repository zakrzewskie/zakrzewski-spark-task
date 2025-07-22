# import dependencies
import pyspark.sql.functions as F

# load data
claim_df = spark.table("zakrzewski_spark_task.raw.claim")
contract_df = spark.table("zakrzewski_spark_task.raw.contract")

# join tables
joined_df = claim_df.join(contract_df, claim_df.CONTRAT_ID == contract_df.CONTRACT_ID, how="left_outer")


joined_df = joined_df.select(
    F.lit("Europe 3").alias("CONTRACT_SOURCE_SYSTEM"),
    contract_df.CONTRACT_ID.alias("CONTRACT_SOURCE_SYSTEM_ID"),
    F.regexp_replace(F.col("CLAIM_ID"), "^[A-Z_]+", "").alias("SOURCE_SYSTEM_ID"),
    F.when(F.col("CLAIM_TYPE") == '2', "Corporate")
     .when(F.col("CLAIM_TYPE") == '1', "Private")
     .otherwise("Unknown").alias("TRANSACTION_TYPE"),
    F.when(F.col("Claim_ID").startswith("CL"), "COINSURANCE")
    .when(F.col("Claim_ID").startswith("RX"), "REINSURANCE")
    .otherwise(None) 
    .alias("TRANSACTION_DIRECTION"),
    claim_df.AMOUNT.alias("CONFORMED_VALUE"),
    claim_df.DATE_OF_LOSS.alias("BUSINESS_DATE"),
    F.current_timestamp().alias("SYSTEM_TIMESTAMP")
)

display(joined_df)
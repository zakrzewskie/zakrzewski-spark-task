import pyspark.sql.functions as F
from data_io.ingest_hashify_api import get_md4_hash_udf

def prepare_transactions(df_claims, df_contracts):
    
    df_contracts = df_contracts.select(
        "CONTRACT_ID"
        )

    df_claims = df_claims.withColumn("NSE_ID", get_md4_hash_udf(F.col("CLAIM_ID")))
    df_claims = df_claims.select(
        "CLAIM_ID",
        "CONTRACT_ID",
        "CLAIM_TYPE",
        "DATE_OF_LOSS",
        "AMOUNT",
        "CREATION_DATE",
        "NSE_ID"
        )
    
    df_joined = df_claims.join(df_contracts, df_claims.CONTRACT_ID == df_contracts.CONTRACT_ID, how="left_outer")

    df_joined = df_joined.select(
        F.lit("Europe 3").alias("CONTRACT_SOURCE_SYSTEM"),
        df_contracts.CONTRACT_ID.alias("CONTRACT_SOURCE_SYSTEM_ID"),
        F.regexp_replace(F.col("CLAIM_ID"), "^[A-Z_]+", "").alias("SOURCE_SYSTEM_ID"),
        F.when(F.col("CLAIM_TYPE") == '2', "Corporate")
         .when(F.col("CLAIM_TYPE") == '1', "Private")
         .otherwise("Unknown").alias("TRANSACTION_TYPE"),
        F.when(F.col("CLAIM_ID").startswith("CL"), "COINSURANCE")
        .when(F.col("CLAIM_ID").startswith("RX"), "REINSURANCE")
        .otherwise(None) 
        .alias("TRANSACTION_DIRECTION"),
        df_claims.AMOUNT.alias("CONFORMED_VALUE"),
        df_claims.DATE_OF_LOSS.alias("BUSINESS_DATE"),
        df_claims.CREATION_DATE.alias("CREATION_DATE"),
        F.current_timestamp().alias("SYSTEM_TIMESTAMP"),
        df_claims.NSE_ID.alias("NSE_ID")
    )

    return df_joined
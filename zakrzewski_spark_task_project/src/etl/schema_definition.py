from pyspark.sql.types import *

# Create metadata for primary key fields
pk_metadata = {"primary_key": True}

raw_claims_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CLAIM_ID", StringType(), True, pk_metadata),
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CONTRAT_ID", StringType(), True, pk_metadata),
    StructField("CLAIM_TYPE", StringType(), True),
    StructField("DATE_OF_LOSS", StringType(), True),
    StructField("AMOUNT", StringType(), True),
    StructField("CREATION_DATE", StringType(), True),
])

raw_contracts_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CONTRACT_ID", StringType(), True, pk_metadata),
    StructField("CONTRACT_TYPE", StringType(), True),
    StructField("INSURED_PERIOD_FROM", StringType(), True),
    StructField("INSURED_PERIOD_TO", StringType(), True),
    StructField("CREATION_DATE", StringType(), True),
])

clean_claims_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CLAIM_ID", StringType(), True, pk_metadata),
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CONTRACT_ID", LongType(), True, pk_metadata),
    StructField("CLAIM_TYPE", StringType(), True),
    StructField("DATE_OF_LOSS", DateType(), True),
    StructField("AMOUNT", DecimalType(16, 5), True),
    StructField("CREATION_DATE", TimestampType(), True),
])

clean_contracts_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CONTRACT_ID", LongType(), True, pk_metadata),
    StructField("CONTRACT_TYPE", StringType(), True),
    StructField("INSURED_PERIOD_FROM", DateType(), True),
    StructField("INSURED_PERIOD_TO", DateType(), True),
    StructField("CREATION_DATE", TimestampType(), True),
])

serving_transactions_schema = StructType([
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True, pk_metadata),
    StructField("CONTRACT_SOURCE_SYSTEM_ID", LongType(), True, pk_metadata),
    StructField("SOURCE_SYSTEM_ID", IntegerType(), True),
    StructField("TRANSACTION_TYPE", StringType(), False),
    StructField("TRANSACTION_DIRECTION", StringType(), True),
    StructField("CONFORMED_VALUE", DecimalType(16, 5), True),
    StructField("BUSINESS_DATE", DateType(), True),
    StructField("CREATION_DATE", TimestampType(), True),
    StructField("SYSTEM_TIMESTAMP", TimestampType(), True),
    StructField("NSE_ID", StringType(), False, pk_metadata),
])

# Composite primary keys for each table
CONTRACTS_PRIMARY_KEY = ["SOURCE_SYSTEM", "CONTRACT_ID"]
RAW_CLAIMS_PRIMARY_KEY = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRAT_ID"]
CLAIMS_PRIMARY_KEY = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID"]
TRANSACTIONS_PRIMARY_KEY = ["CONTRACT_SOURCE_SYSTEM", "CONTRACT_SOURCE_SYSTEM_ID", "NSE_ID"]

def ensure_all_schemas(spark, catalog, schemas):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for schema in schemas.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
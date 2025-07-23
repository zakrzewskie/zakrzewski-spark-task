from pyspark.sql.types import *

clean_claims_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True),
    StructField("CLAIM_ID", StringType(), True),
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
    StructField("CONTRACT_ID", IntegerType(), True),
    StructField("CLAIM_TYPE", StringType(), True),
    StructField("DATE_OF_LOSS", DateType(), True),
    StructField("AMOUNT", DecimalType(16, 5), True),
    StructField("CREATION_DATE", TimestampType(), True),
])

clean_contracts_schema = StructType([
    StructField("SOURCE_SYSTEM", StringType(), True),
    StructField("CONTRACT_ID", LongType(), True),
    StructField("CONTRACT_TYPE", StringType(), True),
    StructField("INSURED_PERIOD_FROM", DateType(), True),
    StructField("INSURED_PERIOD_TO", DateType(), True),
    StructField("CREATION_DATE", TimestampType(), True),
])

serving_transactions_schema = StructType([
    StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
    StructField("CONTRACT_SOURCE_SYSTEM_ID", LongType(), True),
    StructField("SOURCE_SYSTEM_ID", IntegerType(), True),
    StructField("TRANSACTION_TYPE", StringType(), False),
    StructField("TRANSACTION_DIRECTION", StringType(), True),
    StructField("CONFORMED_VALUE", DecimalType(16, 5), True),
    StructField("BUSINESS_DATE", DateType(), True),
    StructField("CREATION_DATE", TimestampType(), True),
    StructField("SYSTEM_TIMESTAMP", TimestampType(), True),
    StructField("NSE_ID", StringType(), False),
])

def ensure_all_schemas(spark, catalog, schemas):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for schema in schemas.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
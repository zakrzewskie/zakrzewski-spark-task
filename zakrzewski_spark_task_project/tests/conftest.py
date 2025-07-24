import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("ETL Pipeline Tests") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "execution_mode": "local",
        "catalog": "test_catalog",
        "schemas": {
            "raw": "raw_schema",
            "clean": "clean_schema", 
            "serving": "serving_schema"
        },
        "tables": {
            "raw_claims": "raw_claims_table",
            "raw_contracts": "raw_contracts_table",
            "clean_claims": "clean_claims_table",
            "clean_contracts": "clean_contracts_table",
            "serving_transactions": "serving_transactions_table"
        }
    }


@pytest.fixture
def sample_claims_data():
    """Sample claims data for testing."""
    return [
        ("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", "97563756", "2", "14.02.2021", "523.21", "17.01.2022 14:45"),
        ("Claim_SR_Europa_3", "CL_962234", "Contract_SR_Europa_4", "408124123", "1", "30.01.2021", "52369.0", "17.01.2022 14:46"),
        ("Claim_SR_Europa_3", "CL_895168", "Contract_SR_Europa_3", "13767503", "", "02.09.2020", "98465", "17.01.2022 14:45"),
        ("Claim_SR_Europa_3", "CX_12066501", "Contract_SR_Europa_3", "656948536", "2", "04.01.2022", "9000", "17.01.2022 14:45"),
        ("Claim_SR_Europa_3", "RX_9845163", "Contract_SR_Europa_3", "656948536", "2", "04.06.2015", "11000", "17.01.2022 14:45")
    ]


@pytest.fixture
def sample_contracts_data():
    """Sample contracts data for testing."""
    return [
        ("Contract_SR_Europa_3", "408124123", "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ("Contract_SR_Europa_3", "46784575", "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ("Contract_SR_Europa_3", "97563756", "", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ("Contract_SR_Europa_3", "13767503", "Reinsurance", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ("Contract_SR_Europa_3", "656948536", "", "01.01.2015", "01.01.2099", "17.01.2022 13:42")
    ]


@pytest.fixture
def claims_schema():
    """Schema for claims data."""
    return StructType([
        StructField("SOURCE_SYSTEM", StringType(), True),
        StructField("CLAIM_ID", StringType(), True),
        StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
        StructField("CONTRACT_ID", StringType(), True),
        StructField("CLAIM_TYPE", StringType(), True),
        StructField("DATE_OF_LOSS", StringType(), True),
        StructField("AMOUNT", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])


@pytest.fixture
def contracts_schema():
    """Schema for contracts data."""
    return StructType([
        StructField("SOURCE_SYSTEM", StringType(), True),
        StructField("CONTRACT_ID", StringType(), True),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])

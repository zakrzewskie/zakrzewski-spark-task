
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from unittest.mock import patch
from etl.pipeline_serving import prepare_transactions

class TestPipelineServing:
    @patch('etl.pipeline_serving.get_md4_hash_udf')
    def test_prepare_transactions_happy_path(self, mock_hash_udf, spark):
        from datetime import date, datetime
        mock_hash_udf.return_value = lit("HASHED")
        # Clean layer input: types already casted
        claims_data = [
            ("CL_1", 1001, "1", date(2021, 1, 1), 1234.56, datetime(2022, 1, 1, 10, 0)),
            ("RX_2", 1002, "2", date(2022, 2, 2), 789.01, datetime(2022, 2, 2, 11, 0)),
        ]
        claims_schema = StructType([
            StructField("CLAIM_ID", StringType()),
            StructField("CONTRACT_ID", LongType()),
            StructField("CLAIM_TYPE", StringType()),
            StructField("DATE_OF_LOSS", DateType()),
            StructField("AMOUNT", DoubleType()),
            StructField("CREATION_DATE", TimestampType()),
        ])
        df_claims = spark.createDataFrame(claims_data, claims_schema)
        contracts_data = [
            (1001,),
            (1002,)
        ]
        contracts_schema = StructType([
            StructField("CONTRACT_ID", LongType()),
        ])
        df_contracts = spark.createDataFrame(contracts_data, contracts_schema)
        result_df = prepare_transactions(df_claims, df_contracts)
        from etl.utils import assert_composite_key_unique
        from etl.schema_definition import TRANSACTIONS_PRIMARY_KEY
        # Assert composite key uniqueness for transformation table
        assert_composite_key_unique(result_df, TRANSACTIONS_PRIMARY_KEY)
        # Sort output by SOURCE_SYSTEM_ID and input by CLAIM_ID with prefix removed
        rows = sorted(result_df.collect(), key=lambda r: r['SOURCE_SYSTEM_ID'])
        claims_data_sorted = sorted(claims_data, key=lambda r: r[0].split('_', 1)[-1])
        assert len(rows) == 2
        for row, orig in zip(rows, claims_data_sorted):
            assert row.CONTRACT_SOURCE_SYSTEM == "Europe 3"
            assert row.CONTRACT_SOURCE_SYSTEM_ID in [1001, 1002]
            assert row.NSE_ID == "HASHED"
            # Transaction type mapping
            if orig[2] == "2":
                assert row.TRANSACTION_TYPE == "Corporate"
            elif orig[2] == "1":
                assert row.TRANSACTION_TYPE == "Private"
            else:
                assert row.TRANSACTION_TYPE == "Unknown"
            # Transaction direction mapping
            if orig[0].startswith("CL"):
                assert row.TRANSACTION_DIRECTION == "COINSURANCE"
            elif orig[0].startswith("RX"):
                assert row.TRANSACTION_DIRECTION == "REINSURANCE"
            else:
                assert row.TRANSACTION_DIRECTION is None
            assert row.CONFORMED_VALUE == orig[4]
            assert row.BUSINESS_DATE == orig[3]
            assert row.CREATION_DATE == orig[5]

    @patch('etl.pipeline_serving.get_md4_hash_udf')
    def test_prepare_transactions_missing_contract(self, mock_hash_udf, spark):
        from datetime import date, datetime
        mock_hash_udf.return_value = lit("HASHED")
        claims_data = [
            ("CL_1", 1001, "1", date(2021, 1, 1), 1234.56, datetime(2022, 1, 1, 10, 0)),
            ("CL_2", 9999, "2", date(2022, 2, 2), 789.01, datetime(2022, 2, 2, 11, 0)),
        ]
        claims_schema = StructType([
            StructField("CLAIM_ID", StringType()),
            StructField("CONTRACT_ID", LongType()),
            StructField("CLAIM_TYPE", StringType()),
            StructField("DATE_OF_LOSS", DateType()),
            StructField("AMOUNT", DoubleType()),
            StructField("CREATION_DATE", TimestampType()),
        ])
        df_claims = spark.createDataFrame(claims_data, claims_schema)
        contracts_data = [
            (1001,)
        ]
        contracts_schema = StructType([
            StructField("CONTRACT_ID", LongType()),
        ])
        df_contracts = spark.createDataFrame(contracts_data, contracts_schema)
        result_df = prepare_transactions(df_claims, df_contracts)
        rows = result_df.collect()
        # Only CL_1 should have a matching contract
        assert len(rows) == 2
        # CONTRACT_SOURCE_SYSTEM_ID will be None for missing join
        for row in rows:
            if row.SOURCE_SYSTEM_ID == "1":
                assert row.CONTRACT_SOURCE_SYSTEM_ID == 1001
            else:
                assert row.CONTRACT_SOURCE_SYSTEM_ID is None

    @patch('etl.pipeline_serving.get_md4_hash_udf')
    def test_prepare_transactions_empty_inputs(self, mock_hash_udf, spark):
        mock_hash_udf.return_value = lit("HASHED")
        claims_schema = StructType([
            StructField("CLAIM_ID", StringType()),
            StructField("CONTRACT_ID", LongType()),
            StructField("CLAIM_TYPE", StringType()),
            StructField("DATE_OF_LOSS", DateType()),
            StructField("AMOUNT", DoubleType()),
            StructField("CREATION_DATE", TimestampType()),
        ])
        contracts_schema = StructType([
            StructField("CONTRACT_ID", LongType()),
        ])
        df_claims = spark.createDataFrame([], claims_schema)
        df_contracts = spark.createDataFrame([], contracts_schema)
        result_df = prepare_transactions(df_claims, df_contracts)
        assert result_df.count() == 0

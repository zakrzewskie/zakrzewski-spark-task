from pyspark.sql.types import *
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.pipeline_clean import rename_columns_contract, rename_columns_claim
from etl.utils import cast_column_types


class TestPipelineClean:
    """Test cases for pipeline_clean module."""
    
    def test_cast_column_types_date_conversion(self, spark):
        """Test cast_column_types with date conversion."""
        # Arrange
        data = [("01.01.2015", "17.01.2022 13:42")]
        schema = StructType([
            StructField("date_col", StringType(), True),
            StructField("timestamp_col", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        target_schema = StructType([
            StructField("date_col", DateType(), True),
            StructField("timestamp_col", TimestampType(), True)
        ])
        
        # Act
        result_df = cast_column_types(df, target_schema)
        
        # Assert
        assert result_df.schema.fields[0].dataType == DateType()
        assert result_df.schema.fields[1].dataType == TimestampType()
        
        # Check that conversion worked
        row = result_df.collect()[0]
        assert row["date_col"] is not None
        assert row["timestamp_col"] is not None
    
    def test_cast_column_types_decimal_conversion(self, spark):
        """Test cast_column_types with decimal conversion."""
        # Arrange
        data = [("123.45", "67.89")]
        schema = StructType([
            StructField("decimal_col", StringType(), True),
            StructField("amount_col", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        target_schema = StructType([
            StructField("decimal_col", DecimalType(10, 2), True),
            StructField("amount_col", DecimalType(15, 2), True)
        ])
        
        # Act
        result_df = cast_column_types(df, target_schema)
        
        # Assert
        assert isinstance(result_df.schema.fields[0].dataType, DecimalType)
        assert isinstance(result_df.schema.fields[1].dataType, DecimalType)
        
        # Check values
        row = result_df.collect()[0]
        assert float(row["decimal_col"]) == 123.45
        assert float(row["amount_col"]) == 67.89
    
    def test_cast_column_types_string_conversion(self, spark):
        """Test cast_column_types with string conversion."""
        # Arrange
        data = [("123", "456")]
        schema = StructType([
            StructField("int_col", StringType(), True),
            StructField("str_col", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        target_schema = StructType([
            StructField("int_col", IntegerType(), True),
            StructField("str_col", StringType(), True)
        ])
        
        # Act
        result_df = cast_column_types(df, target_schema)
        
        # Assert
        assert result_df.schema.fields[0].dataType == IntegerType()
        assert result_df.schema.fields[1].dataType == StringType()
        
        # Check values
        row = result_df.collect()[0]
        assert row["int_col"] == 123
        assert row["str_col"] == "456"
    
    def test_cast_column_types_invalid_date(self, spark):
        """Test cast_column_types with invalid date format."""
        # Arrange
        data = [("invalid_date",)]
        schema = StructType([
            StructField("date_col", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        target_schema = StructType([
            StructField("date_col", DateType(), True)
        ])
        
        # Act
        result_df = cast_column_types(df, target_schema)
        
        # Assert - invalid dates should become null
        row = result_df.collect()[0]
        assert row["date_col"] is None
    
    def test_rename_columns_contract(self, spark):
        """Test rename_columns_contract function."""
        # Arrange
        data = [("Contract_SR_Europa_3", "408124123", "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42")]
        schema = StructType([
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("CONTRACT_ID", StringType(), True),
            StructField("CONTRACT_TYPE", StringType(), True),
            StructField("INSURED_PERIOD_FROM", StringType(), True),
            StructField("INSUDRED_PERIOD_TO", StringType(), True),  # Typo in original
            StructField("CREATION_DATE", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Act
        result_df = rename_columns_contract(df)
        
        # Assert
        expected_columns = ["SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE", 
                          "INSURED_PERIOD_FROM", "INSURED_PERIOD_TO", "CREATION_DATE"]
        assert result_df.columns == expected_columns
        
        # Check that data is preserved
        row = result_df.collect()[0]
        assert row["SOURCE_SYSTEM"] == "Contract_SR_Europa_3"
        assert row["CONTRACT_ID"] == "408124123"
        assert row["INSURED_PERIOD_TO"] == "01.01.2099"  # Renamed column
    
    def test_rename_columns_contract_no_typo_column(self, spark):
        """Test rename_columns_contract when typo column doesn't exist."""
        # Arrange
        data = [("Contract_SR_Europa_3", "408124123")]
        schema = StructType([
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("CONTRACT_ID", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Act - should not fail even if column doesn't exist
        result_df = rename_columns_contract(df)
        
        # Assert - original columns should remain
        assert result_df.columns == ["SOURCE_SYSTEM", "CONTRACT_ID"]
    
    def test_rename_columns_claim(self, spark):
        """Test rename_columns_claim function."""
        # Arrange
        data = [("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", "97563756", "2")]
        schema = StructType([
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("CLAIM_ID", StringType(), True),
            StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
            StructField("CONTRAT_ID", StringType(), True),  # Typo in original
            StructField("CLAIM_TYPE", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Act
        result_df = rename_columns_claim(df)
        
        # Assert
        expected_columns = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", 
                          "CONTRACT_ID", "CLAIM_TYPE"]
        assert result_df.columns == expected_columns
        
        # Check that data is preserved
        row = result_df.collect()[0]
        assert row["SOURCE_SYSTEM"] == "Claim_SR_Europa_3"
        assert row["CLAIM_ID"] == "CL_68545123"
        assert row["CONTRACT_ID"] == "97563756"  # Renamed column
    
    def test_rename_columns_claim_no_typo_column(self, spark):
        """Test rename_columns_claim when typo column doesn't exist."""
        # Arrange
        data = [("Claim_SR_Europa_3", "CL_68545123")]
        schema = StructType([
            StructField("SOURCE_SYSTEM", StringType(), True),
            StructField("CLAIM_ID", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Act - should not fail even if column doesn't exist
        result_df = rename_columns_claim(df)
        
        # Assert - original columns should remain
        assert result_df.columns == ["SOURCE_SYSTEM", "CLAIM_ID"]
    
    def test_cast_column_types_empty_dataframe(self, spark):
        """Test cast_column_types with empty dataframe."""
        # Arrange
        schema = StructType([
            StructField("date_col", StringType(), True),
            StructField("amount_col", StringType(), True)
        ])
        df = spark.createDataFrame([], schema)
        
        target_schema = StructType([
            StructField("date_col", DateType(), True),
            StructField("amount_col", DecimalType(10, 2), True)
        ])
        
        # Act
        result_df = cast_column_types(df, target_schema)
        
        # Assert
        assert result_df.count() == 0
        assert result_df.schema.fields[0].dataType == DateType()
        assert isinstance(result_df.schema.fields[1].dataType, DecimalType)
    
    def test_cast_column_types_mixed_valid_invalid_dates(self, spark):
        """Test cast_column_types with mix of valid and invalid dates."""
        # Arrange
        data = [("01.01.2015",), ("invalid_date",), ("31.12.2020",)]
        schema = StructType([
            StructField("date_col", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        target_schema = StructType([
            StructField("date_col", DateType(), True)
        ])
        
        # Act
        result_df = cast_column_types(df, target_schema)
        
        # Assert
        rows = result_df.collect()
        assert rows[0]["date_col"] is not None  # Valid date
        assert rows[1]["date_col"] is None      # Invalid date becomes null
        assert rows[2]["date_col"] is not None  # Valid date

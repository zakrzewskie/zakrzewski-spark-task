from pyspark.sql.types import StringType, StructType, StructField
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.pipeline_raw import get_raw_data, CONTRACT_csv, CLAIM_csv


class TestPipelineRaw:
    """Test cases for pipeline_raw module."""
    
    def test_get_raw_data_with_contract_csv(self, spark):
        """Test get_raw_data function with CONTRACT_csv data."""
        # Act
        result_df = get_raw_data(CONTRACT_csv, spark)
        
        # Assert
        assert result_df.count() == 5  # 5 data rows (excluding header)
        
        # Check column names
        expected_columns = ["SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE", 
                          "INSURED_PERIOD_FROM", "INSURED_PERIOD_TO", "CREATION_DATE"]
        assert result_df.columns == expected_columns
        
        # Check data types (should all be strings since inferSchema=False)
        for field in result_df.schema.fields:
            assert field.dataType == StringType()
    
    def test_get_raw_data_with_claim_csv(self, spark):
        """Test get_raw_data function with CLAIM_csv data."""
        # Act
        result_df = get_raw_data(CLAIM_csv, spark)
        
        # Assert
        assert result_df.count() == 7  # 7 data rows (excluding header)
        
        # Check column names
        expected_columns = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", 
                          "CONTRAT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"]
        assert result_df.columns == expected_columns
        
        # Check data types (should all be strings since inferSchema=False)
        for field in result_df.schema.fields:
            assert field.dataType == StringType()
    
    def test_get_raw_data_contract_content(self, spark):
        """Test that get_raw_data returns expected contract data content."""
        # Act
        result_df = get_raw_data(CONTRACT_csv, spark)
        
        # Get first row
        first_row = result_df.collect()[0]
        
        # Assert first row content
        assert first_row["SOURCE_SYSTEM"] == "Contract_SR_Europa_3"
        assert first_row["CONTRACT_ID"] == "408124123"
        assert first_row["CONTRACT_TYPE"] == "Direct"
        assert first_row["INSURED_PERIOD_FROM"] == "01.01.2015"
        assert first_row["INSURED_PERIOD_TO"] == "01.01.2099"
        assert first_row["CREATION_DATE"] == "17.01.2022 13:42"
    
    def test_get_raw_data_claim_content(self, spark):
        """Test that get_raw_data returns expected claim data content."""
        # Act
        result_df = get_raw_data(CLAIM_csv, spark)
        
        # Get first row
        first_row = result_df.collect()[0]
        
        # Assert first row content
        assert first_row["SOURCE_SYSTEM"] == "Claim_SR_Europa_3"
        assert first_row["CLAIM_ID"] == "CL_68545123"
        assert first_row["CONTRACT_SOURCE_SYSTEM"] == "Contract_SR_Europa_3"
        assert first_row["CONTRAT_ID"] == "97563756"  # Note: typo in original data
        assert first_row["CLAIM_TYPE"] == "2"
        assert first_row["DATE_OF_LOSS"] == "14.02.2021"
        assert first_row["AMOUNT"] == "523.21"
        assert first_row["CREATION_DATE"] == "17.01.2022 14:45"
    
    def test_get_raw_data_empty_csv(self, spark):
        """Test get_raw_data with empty CSV (only header)."""
        # Arrange
        empty_csv = "COL1,COL2,COL3"
        
        # Act
        result_df = get_raw_data(empty_csv, spark)
        
        # Assert
        assert result_df.count() == 0
        assert result_df.columns == ["COL1", "COL2", "COL3"]
    
    def test_get_raw_data_single_row(self, spark):
        """Test get_raw_data with single data row."""
        # Arrange
        single_row_csv = """COL1,COL2,COL3
value1,value2,value3"""
        
        # Act
        result_df = get_raw_data(single_row_csv, spark)
        
        # Assert
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["COL1"] == "value1"
        assert row["COL2"] == "value2"
        assert row["COL3"] == "value3"
    
    def test_contract_csv_constant(self):
        """Test that CONTRACT_csv constant contains expected structure."""
        lines = CONTRACT_csv.strip().split('\n')
        
        # Should have header + 5 data rows
        assert len(lines) == 6
        
        # Check header
        header = lines[0]
        expected_header = 'SOURCE_SYSTEM,CONTRACT_ID,"CONTRACT_TYPE",INSURED_PERIOD_FROM,INSURED_PERIOD_TO,CREATION_DATE'
        assert header == expected_header
        
        # Check that all data rows have the same number of fields as header
        header_fields = len(header.split(','))
        for line in lines[1:]:
            # Count fields considering quoted values
            fields = line.split(',')
            assert len(fields) == header_fields
    
    def test_claim_csv_constant(self):
        """Test that CLAIM_csv constant contains expected structure."""
        lines = CLAIM_csv.strip().split('\n')
        
        # Should have header + 7 data rows
        assert len(lines) == 8
        
        # Check header
        header = lines[0]
        expected_header = 'SOURCE_SYSTEM,CLAIM_ID,CONTRACT_SOURCE_SYSTEM,CONTRAT_ID,CLAIM_TYPE,DATE_OF_LOSS,AMOUNT,CREATION_DATE'
        assert header == expected_header
        
        # Check that all data rows have the same number of fields as header
        header_fields = len(header.split(','))
        for line in lines[1:]:
            fields = line.split(',')
            assert len(fields) == header_fields

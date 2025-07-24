import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import pytest
from pyspark.sql.types import *
from unittest.mock import patch, MagicMock, call

# Patch composite key check globally for all tests in this file
patch('etl.utils.assert_composite_key_unique', lambda df, keys: None).start()

from etl.pipeline import run_pipeline

class TestPipeline:
    """Test cases for main pipeline orchestration."""
    
    @patch('etl.pipeline.ensure_all_schemas')
    @patch('etl.pipeline.write_dataset')
    @patch('etl.pipeline.read_dataset')
    @patch('etl.pipeline.prepare_transactions')
    @patch('etl.utils.cast_column_types')
    @patch('etl.pipeline.rename_columns_contract')
    @patch('etl.pipeline.rename_columns_claim')
    @patch('etl.pipeline.get_raw_data')
    def test_run_pipeline_databricks_mode(self, mock_get_raw_data, mock_rename_claim, 
                                        mock_rename_contract, mock_cast_types, 
                                        mock_prepare_transactions, mock_read_dataset,
                                        mock_write_dataset, mock_ensure_schemas, spark):
        """Test run_pipeline in databricks execution mode."""
        # Arrange
        config = {
            "execution_mode": "databricks",
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
        
        # Mock return values
        mock_df = MagicMock()
        mock_get_raw_data.return_value = mock_df
        mock_read_dataset.return_value = mock_df
        mock_rename_claim.return_value = mock_df
        mock_rename_contract.return_value = mock_df
        mock_cast_types.return_value = mock_df
        mock_prepare_transactions.return_value = mock_df
        
        # Act
        run_pipeline(spark, config)
        
        # Assert
        # Check that schema creation is called for databricks mode
        mock_ensure_schemas.assert_called_once_with(spark, "test_catalog", config["schemas"])
        
        # Check that get_raw_data is called twice (for claims and contracts)
        assert mock_get_raw_data.call_count == 2
        
        # Check that write_dataset is called 5 times (raw claims, raw contracts, clean claims, clean contracts, serving transactions)
        assert mock_write_dataset.call_count == 5
        
        # Check that read_dataset is called 4 times (raw claims, raw contracts, clean claims, clean contracts)
        assert mock_read_dataset.call_count == 4
    
    @patch('etl.pipeline.ensure_all_schemas')
    @patch('etl.pipeline.write_dataset')
    @patch('etl.pipeline.read_dataset')
    @patch('etl.pipeline.prepare_transactions')
    @patch('etl.utils.cast_column_types')
    @patch('etl.pipeline.rename_columns_contract')
    @patch('etl.pipeline.rename_columns_claim')
    @patch('etl.pipeline.get_raw_data')
    def test_run_pipeline_local_mode(self, mock_get_raw_data, mock_rename_claim, 
                                   mock_rename_contract, mock_cast_types, 
                                   mock_prepare_transactions, mock_read_dataset,
                                   mock_write_dataset, mock_ensure_schemas, spark):
        """Test run_pipeline in local execution mode."""
        # Arrange
        config = {
            "execution_mode": "local",
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
        
        # Mock return values
        mock_df = MagicMock()
        mock_get_raw_data.return_value = mock_df
        mock_read_dataset.return_value = mock_df
        mock_rename_claim.return_value = mock_df
        mock_rename_contract.return_value = mock_df
        mock_cast_types.return_value = mock_df
        mock_prepare_transactions.return_value = mock_df
        
        # Act
        run_pipeline(spark, config)
        
        # Assert
        # Check that schema creation is NOT called for local mode
        mock_ensure_schemas.assert_not_called()
        
        # Check that the pipeline still runs all other steps
        assert mock_get_raw_data.call_count == 2
        assert mock_write_dataset.call_count == 5
        assert mock_read_dataset.call_count == 4
    
    @patch('etl.pipeline.ensure_all_schemas')
    @patch('etl.pipeline.write_dataset')
    @patch('etl.pipeline.read_dataset')
    @patch('etl.pipeline.prepare_transactions')
    @patch('etl.utils.cast_column_types')
    @patch('etl.pipeline.rename_columns_contract')
    @patch('etl.pipeline.rename_columns_claim')
    @patch('etl.pipeline.get_raw_data')
    def test_run_pipeline_read_dataset_calls(self, mock_get_raw_data, mock_rename_claim, 
                                           mock_rename_contract, mock_cast_types, 
                                           mock_prepare_transactions, mock_read_dataset,
                                           mock_write_dataset, mock_ensure_schemas, spark):
        """Test that run_pipeline calls read_dataset with correct parameters."""
        # Arrange
        config = {
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
                "serving_transactions": "transactions_table"
            }
        }
        
        # Setup mock return values
        mock_df = MagicMock()
        mock_get_raw_data.return_value = mock_df
        mock_read_dataset.return_value = mock_df
        mock_rename_claim.return_value = mock_df
        mock_rename_contract.return_value = mock_df
        mock_cast_types.return_value = mock_df
        mock_prepare_transactions.return_value = mock_df
        
        # Act
        run_pipeline(spark, config)
        
        # Assert - Verify read_dataset calls
        expected_read_calls = [
            call(spark, config, "raw_schema", "raw_claims_table"),
            call(spark, config, "raw_schema", "raw_contracts_table"),
            call(spark, config, "clean_schema", "clean_claims_table"),
            call(spark, config, "clean_schema", "clean_contracts_table")
        ]
        mock_read_dataset.assert_has_calls(expected_read_calls, any_order=False)
        assert mock_read_dataset.call_count == 4
    
    @patch('etl.pipeline.ensure_all_schemas')
    @patch('etl.pipeline.write_dataset')
    @patch('etl.pipeline.read_dataset')
    @patch('etl.pipeline.prepare_transactions')
    @patch('etl.utils.cast_column_types')
    @patch('etl.pipeline.rename_columns_contract')
    @patch('etl.pipeline.rename_columns_claim')
    @patch('etl.pipeline.get_raw_data')
    def test_run_pipeline_with_missing_config_keys(self, mock_get_raw_data, mock_rename_claim, 
                                                  mock_rename_contract, mock_cast_types, 
                                                  mock_prepare_transactions, mock_read_dataset,
                                                  mock_write_dataset, mock_ensure_schemas, spark):
        """Test run_pipeline behavior with missing configuration keys."""
        # Arrange - config missing some keys
        config = {
            "execution_mode": "local",
            "schemas": {
                "raw": "raw_schema"
                # Missing clean and serving schemas
            },
            "tables": {
                "raw_claims": "raw_claims_table"
                # Missing other tables
            }
        }
        
        mock_df = MagicMock()
        mock_get_raw_data.return_value = mock_df
        mock_read_dataset.return_value = mock_df
        mock_rename_claim.return_value = mock_df
        mock_rename_contract.return_value = mock_df
        mock_cast_types.return_value = mock_df
        mock_prepare_transactions.return_value = mock_df
        
        # Act & Assert - should raise KeyError for missing keys
        with pytest.raises(KeyError):
            run_pipeline(spark, config)

from data_io.data_io import read_dataset, write_dataset
from etl.pipeline_raw import CONTRACT_csv, CLAIM_csv, get_raw_data
from etl.pipeline_clean import  rename_columns_contract, rename_columns_claim
from etl.pipeline_serving import prepare_transactions
from etl.utils import cast_column_types, assert_composite_key_unique
from etl.schema_definition import (
    clean_claims_schema, clean_contracts_schema, serving_transactions_schema,
    raw_claims_schema, raw_contracts_schema, ensure_all_schemas
)
from etl.utils import cast_column_types, assert_composite_key_unique, apply_schema_with_metadata

def run_pipeline(spark, config):
    # Ensure schema exists in databricks mode
    if config["execution_mode"] == "databricks":
        ensure_all_schemas(spark, config["catalog"], config["schemas"])
    
    # 1. Read, apply schema, and write raw data
    # Claims
    df_raw_claims = get_raw_data(CLAIM_csv, spark)
    df_raw_claims = apply_schema_with_metadata(df_raw_claims, raw_claims_schema, spark)
    write_dataset(df_raw_claims, config, config['schemas']['raw'], config['tables']['raw_claims'])
    # Validate raw data using schema metadata
    assert_composite_key_unique(df_raw_claims)

    # Contracts
    df_raw_contracts = get_raw_data(CONTRACT_csv, spark)
    df_raw_contracts = apply_schema_with_metadata(df_raw_contracts, raw_contracts_schema, spark)
    write_dataset(df_raw_contracts, config, config['schemas']['raw'], config['tables']['raw_contracts'])
    # Validate raw data using schema metadata
    assert_composite_key_unique(df_raw_contracts)

    # 2. Read raw data, apply clean schema, and write clean data
    # Claims
    df_raw_claims_stored = read_dataset(spark, config, config['schemas']['raw'], config['tables']['raw_claims'])
    df_raw_claims_stored = rename_columns_claim(df_raw_claims_stored)
    df_clean_claims = cast_column_types(df_raw_claims_stored, clean_claims_schema)
    df_clean_claims = apply_schema_with_metadata(df_clean_claims, clean_claims_schema, spark)
    write_dataset(df_clean_claims, config, config['schemas']['clean'], config['tables']['clean_claims'])
    # Validate clean data using schema metadata
    assert_composite_key_unique(df_clean_claims)
    
    # Contracts
    df_raw_contracts_stored = read_dataset(spark, config, config['schemas']['raw'], config['tables']['raw_contracts'])
    df_raw_contracts_stored = rename_columns_contract(df_raw_contracts_stored)
    df_clean_contracts = cast_column_types(df_raw_contracts_stored, clean_contracts_schema)
    df_clean_contracts = apply_schema_with_metadata(df_clean_contracts, clean_contracts_schema, spark)
    write_dataset(df_clean_contracts, config, config['schemas']['clean'], config['tables']['clean_contracts'])
    # Validate clean data using schema metadata
    assert_composite_key_unique(df_clean_contracts)

    # 3. Read clean data, prepare transactions, and apply serving schema
    df_clean_claims_stored = read_dataset(spark, config, config['schemas']['clean'], config['tables']['clean_claims'])
    df_clean_contracts_stored = read_dataset(spark, config, config['schemas']['clean'], config['tables']['clean_contracts'])
    df_transactions = prepare_transactions(df_clean_claims_stored, df_clean_contracts_stored)
    df_transactions = cast_column_types(df_transactions, serving_transactions_schema)
    df_transactions = apply_schema_with_metadata(df_transactions, serving_transactions_schema, spark)
    # Validate serving data using schema metadata
    assert_composite_key_unique(df_transactions)
    write_dataset(df_transactions, config, config['schemas']['serving'], config['tables']['serving_transactions'])

from data_io.data_io import read_dataset, write_dataset
from etl.pipeline_raw import CONTRACT_csv, CLAIM_csv, get_raw_data
from etl.pipeline_clean import  rename_columns_contract, rename_columns_claim
from etl.pipeline_serving import prepare_transactions
from etl.utils import cast_column_types, assert_composite_key_unique
from etl.schema_definition import clean_claims_schema, clean_contracts_schema, serving_transactions_schema, ensure_all_schemas, CONTRACTS_PRIMARY_KEY, RAW_CLAIMS_PRIMARY_KEY, CLAIMS_PRIMARY_KEY, TRANSACTIONS_PRIMARY_KEY

def run_pipeline(spark, config):
    # Ensure schema exists in databricks mode
    if config["execution_mode"] == "databricks":
        ensure_all_schemas(spark, config["catalog"], config["schemas"])
    
    # 1. Read and write raw data
    df_raw_claims = get_raw_data(CLAIM_csv, spark)
    write_dataset(df_raw_claims, config, config['schemas']['raw'], config['tables']['raw_claims'])
    # Assert composite key uniqueness for raw claims
    assert_composite_key_unique(df_raw_claims, RAW_CLAIMS_PRIMARY_KEY)

    df_raw_contracts = get_raw_data(CONTRACT_csv, spark)
    write_dataset(df_raw_contracts, config, config['schemas']['raw'], config['tables']['raw_contracts'])
    # Assert composite key uniqueness for raw contracts
    assert_composite_key_unique(df_raw_contracts, CONTRACTS_PRIMARY_KEY)

    # 2. Read raw data and write clean data
    df_raw_claims_stored = read_dataset(spark, config, config['schemas']['raw'], config['tables']['raw_claims'])
    df_raw_claims_stored = rename_columns_claim(df_raw_claims_stored)
    df_clean_claims = cast_column_types(df_raw_claims_stored, clean_claims_schema)
    write_dataset(df_clean_claims, config, config['schemas']['clean'], config['tables']['clean_claims'])
    # Assert composite key uniqueness for clean claims
    assert_composite_key_unique(df_clean_claims, CLAIMS_PRIMARY_KEY)
    
    df_raw_contracts_stored = read_dataset(spark, config, config['schemas']['raw'], config['tables']['raw_contracts'])
    df_raw_contracts_stored = rename_columns_contract(df_raw_contracts_stored)
    df_clean_contracts = cast_column_types(df_raw_contracts_stored, clean_contracts_schema)
    write_dataset(df_clean_contracts, config, config['schemas']['clean'], config['tables']['clean_contracts'])
    # Assert composite key uniqueness for clean contracts
    assert_composite_key_unique(df_clean_contracts, CONTRACTS_PRIMARY_KEY)

    # 3. Read clean data and prepare transactions dataset
    df_clean_claims_stored = read_dataset(spark, config, config['schemas']['clean'], config['tables']['clean_claims'])
    df_clean_contracts_stored = read_dataset(spark, config, config['schemas']['clean'], config['tables']['clean_contracts'])
    df_transactions = prepare_transactions(df_clean_claims_stored, df_clean_contracts_stored)
    df_transactions = cast_column_types(df_transactions, serving_transactions_schema)
    # Assert composite key uniqueness for transactions
    assert_composite_key_unique(df_transactions, TRANSACTIONS_PRIMARY_KEY)
    write_dataset(df_transactions, config, config['schemas']['serving'], config['tables']['serving_transactions'])

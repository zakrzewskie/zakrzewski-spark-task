from data_io.data_io import read_dataset, write_dataset
from etl.pipeline_raw import CONTRACT_csv, CLAIM_csv, get_raw_data
from etl.pipeline_clean import cast_column_types, rename_columns_contract, rename_columns_claim
from etl.pipeline_serving import prepare_transactions
from etl.schema_definition import clean_claims_schema, clean_contracts_schema, serving_transactions_schema, ensure_all_schemas

def run_pipeline(spark, config):
    # Ensure schema exists
    if config["execution_mode"] == "databricks":
        ensure_all_schemas(spark, config["catalog"], config["schemas"])
    
    # read and write raw data
    df_raw_claims = get_raw_data(CLAIM_csv, spark)
    write_dataset(df_raw_claims, config, config['schemas']['raw'], config['tables']['raw_claims'])

    df_raw_contracts = get_raw_data(CONTRACT_csv, spark)
    write_dataset(df_raw_contracts, config, config['schemas']['raw'], config['tables']['raw_contracts'])

    # read raw data and write clean data
    df_raw_claims_stored = read_dataset(spark, config, config['schemas']['raw'], config['tables']['raw_claims'])
    df_raw_claims_stored = rename_columns_claim(df_raw_claims_stored)
    df_clean_claims = cast_column_types(df_raw_claims_stored, clean_claims_schema)
    write_dataset(df_clean_claims, config, config['schemas']['clean'], config['tables']['clean_claims'])
    
    df_raw_contracts_stored = read_dataset(spark, config, config['schemas']['raw'], config['tables']['raw_contracts'])
    df_raw_contracts_stored = rename_columns_contract(df_raw_contracts_stored)
    df_clean_contracts = cast_column_types(df_raw_contracts_stored, clean_contracts_schema)
    write_dataset(df_clean_contracts, config, config['schemas']['clean'], config['tables']['clean_contracts'])

    # read clean data and prepare transactions dataset
    df_clean_claims_stored = read_dataset(spark, config, config['schemas']['clean'], config['tables']['clean_claims'])
    df_clean_contracts_stored = read_dataset(spark, config, config['schemas']['clean'], config['tables']['clean_contracts'])
    df_transactions = prepare_transactions(df_clean_claims_stored, df_clean_contracts_stored)
    df_transactions = cast_column_types(df_transactions, serving_transactions_schema)
    write_dataset(df_transactions, config, config['schemas']['serving'], config['tables']['serving_transactions'])

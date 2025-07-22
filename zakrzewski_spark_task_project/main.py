import os
import sys

try:
    project_root = os.path.dirname(os.path.abspath(__file__))
except NameError:
    project_root = os.getcwd()

src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pyspark.sql import SparkSession
from data_io import ensure_all_schemas, read_dataset, write_dataset
from config_loader import load_config
from utils import get_spark_session
from pipeline_raw import CONTRACT_csv, CLAIM_csv, get_raw_data
from databricks.connect import DatabricksSession
from pyspark.sql import Row
import yaml



    


# def ensure_schema_exists(spark, catalog, schema):
#     """Create catalog and schema if they don't exist."""
#     spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


# def get_or_create_input_table(spark, config):
#     """Load input table or create dummy data if it doesn't exist."""
#     full_table_name = f"{config['catalog']}.{config['schema']}.{config['input_table']}"
# 
#     if not spark._jsparkSession.catalog().tableExists(full_table_name):
#         print(f"Table {full_table_name} not found. Creating dummy data.")
#         sample_df = spark.createDataFrame([
#             Row(claim_id="1", claim_type="1", amount=100.0),
#             Row(claim_id="2", claim_type="2", amount=250.0),
#         ])
#         sample_df.write.mode("overwrite").saveAsTable(full_table_name)
# 
#     return spark.table(full_table_name)
# 
# 
# def transform_data(df):
#     """Sample transformation: uppercase claim_type."""
#     return df.withColumn("claim_type", df["claim_type"].cast("string").alias("claim_type"))
# 
# 
# def write_output(df, config):
#     """Write output DataFrame as a table."""
#     output_table = f"{config['catalog']}.{config['schema']}.{config['output_table']}"
#     df.write.mode("overwrite").saveAsTable(output_table)
#     print(f"✅ Data written to {output_table}")


def main():

    # Load config
    config = load_config("config/dev.yaml")

    raw_claims = config['tables']['raw_claims']
    clean_claims = config['tables']['clean_claims']
    raw_contracts = config['tables']['raw_contracts']
    clean_contracts = config['tables']['clean_contracts']
    serving_transactions = config['tables']['serving_transactions']
    layer_raw = config['schemas']['raw']
    execution_mode = config['execution_mode']

    spark = get_spark_session(execution_mode)

    # Ensure schema exists
    if execution_mode == "databricks":
        ensure_all_schemas(spark, config["catalog"], config["schemas"])
    
    df_raw_claims = get_raw_data(CLAIM_csv, spark)
    write_dataset(df_raw_claims, config, layer_raw, raw_claims)



if __name__ == "__main__":
    main()
    print('Code ran')
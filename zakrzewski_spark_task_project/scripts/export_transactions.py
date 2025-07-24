#!/usr/bin/env python3
"""
Script for exporting TRANSACTIONS data to various formats.
Reuses existing project utilities for consistency.
"""
import os
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import from main.py
from main import load_config, get_spark_session
from src.data_io.data_io import read_dataset

def export_transactions(config, output_format="csv", output_path=None):
    """
    Export transactions to the specified format.
    
    Args:
        config: Dictionary containing configuration
        output_format: Format to export to (parquet, csv, json)
        output_path: Output path (without extension)
    """
    # Get execution mode from config
    execution_mode = config.get("execution_mode", "local")
    
    # Get Spark session using the function from main.py
    spark = get_spark_session(execution_mode)
    
    try:
        # Read the processed transactions using existing function
        df = read_dataset(
            spark=spark,
            config=config,
            layer=config["schemas"]["serving"],
            table_name=config["tables"]["serving_transactions"]
        )
        
        # Set default output path if not provided
        if output_path is None:
            output_path = os.path.join(
                config.get("local_base_path", "data"),
                "export",
                "transactions"
            )
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Get the schema to identify datetime columns
        datetime_cols = [f.name for f in df.schema.fields if str(f.dataType).startswith('Timestamp')]
        
        # Convert datetime columns to formatted strings in Spark
        from pyspark.sql.functions import col, date_format
        
        # Apply formatting to datetime columns
        for col_name in datetime_cols:
            df = df.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd HH:mm:ss"))
        
        # Now convert to pandas - all datetime columns are now strings
        pdf = df.toPandas()
        
        # Export in the requested format
        if output_format.lower() == "csv":
            full_path = f"{output_path}.csv"
            # Write as a single CSV file
            pdf.to_csv(full_path, index=False)
            print(f"Exported to {full_path}")
            
        elif output_format.lower() == "parquet":
            full_path = f"{output_path}.parquet"
            # For parquet, we can use the Spark writer directly
            df.write.parquet(full_path, mode="overwrite")
            print(f"Exported to {full_path}")
            
        elif output_format.lower() == "json":
            full_path = f"{output_path}.json"
            # Write as a single JSON file
            pdf.to_json(full_path, orient='records', lines=True)
            print(f"Exported to {full_path}")
            
        else:
            raise ValueError(f"Unsupported export format: {output_format}")
            
    finally:
        spark.stop()

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Export TRANSACTIONS data to various formats')
    parser.add_argument('--config', type=str, default='config/dev.yaml',
                      help='Path to config file (default: config/dev.yaml)')
    parser.add_argument('--format', type=str, default='csv', 
                       choices=['parquet', 'csv', 'json'],
                       help='Output format (default: csv)')
    parser.add_argument('--output', type=str, 
                       help='Output path (without extension)')
    
    args = parser.parse_args()
    
    try:
        # Load config with default path if not provided
        config = load_config(args.config)
        
        # Export the data
        export_transactions(
            config=config,
            output_format=args.format,
            output_path=args.output
        )
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please make sure the config file exists or provide a valid path with --config")
        sys.exit(1)

if __name__ == "__main__":
    main()

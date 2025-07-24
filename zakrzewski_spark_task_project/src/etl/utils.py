from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import *
import os

def cast_column_types(df_raw, schema):
    casted_columns = []
    for field in schema.fields:
        col_name = field.name
        dtype = field.dataType

        if dtype == DateType():
            # Handle date parsing with format like "dd.MM.yyyy"
            casted_columns.append(to_date(col(col_name), "dd.MM.yyyy").alias(col_name))
        elif dtype == TimestampType():
            # Handle timestamp parsing with format like "dd.MM.yyyy HH:mm"
            casted_columns.append(to_timestamp(col(col_name), "dd.MM.yyyy HH:mm").alias(col_name))
        elif isinstance(dtype, DecimalType):
            # Cast decimals with precision
            casted_columns.append(col(col_name).cast(dtype).alias(col_name))
        else:
            # Default casting
            casted_columns.append(col(col_name).cast(dtype).alias(col_name))

    return df_raw.select(casted_columns)

def get_primary_key_columns(df):
    # Extract primary key column names from DataFrame schema metadata
    return [field.name for field in df.schema.fields 
            if field.metadata and field.metadata.get("primary_key")]

def apply_schema_with_metadata(df, schema, spark_session=None):
    """
    Apply schema with metadata to a DataFrame.
    
    Args:
        df: Input DataFrame
        schema: Target schema with metadata
        spark_session: Active SparkSession (optional, will try to get from SparkSession if not provided)
    """
    from pyspark.sql.types import StructType, StructField
        
    # Create a new schema with metadata
    fields_with_metadata = []
    for field in schema.fields:
        # Copy the field with its metadata
        new_field = StructField(
            name=field.name,
            dataType=field.dataType,
            nullable=field.nullable,
            metadata=field.metadata  # Preserve the metadata
        )
        fields_with_metadata.append(new_field)
    
    # Create a new DataFrame with the schema applied
    result_df = df.select([
        col(field.name).cast(field.dataType).alias(field.name) 
        for field in schema.fields
    ])
    
    # Apply the schema with metadata
    return spark_session.createDataFrame(result_df.rdd, StructType(fields_with_metadata))

def save_schema_with_metadata(df, output_path):
    # Save the schema (with metadata) of a Spark DataFrame to a JSON file.
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(df.schema.json())

def schema_json_path(config):
    return os.path.join(
        config['local_base_path'],
        config['schemas']['serving'],
        f"{config['tables']['serving_transactions']}.schema.json"
    )

def assert_composite_key_unique(df, key_columns=None):
    # Check if key_columns is provided, otherwise use primary key columns from schema metadata
    if key_columns is None:
        key_columns = get_primary_key_columns(df)
        if not key_columns:
            raise ValueError("No primary key columns found in DataFrame schema metadata")
    
    count_total = df.count()
    count_distinct = df.select(key_columns).distinct().count()
    
    assert count_total == count_distinct, (
        f"Composite key {key_columns} is not unique: "
        f"{count_total} rows, {count_distinct} unique keys"
    )
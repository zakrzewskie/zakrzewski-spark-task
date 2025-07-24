from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import *

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

def assert_composite_key_unique(df, key_columns):
    # Validate composite key uniqueness
    count_total = df.count()
    count_distinct = df.select(key_columns).distinct().count()
    assert count_total == count_distinct, (
        f"Composite key {key_columns} is not unique: "
        f"{count_total} rows, {count_distinct} unique keys"
    )
import os



def read_dataset(spark, config, layer, table_name):
    """
    Reads data either from a Databricks table or from a local path,
    depending on the execution environment.
    
    Args:
        spark: SparkSession
        config: dict loaded from YAML or env
        layer: str, e.g. "raw", "clean", "serving"
        table_name: str, table or filename without extension

    Returns:
        DataFrame
    """
    mode = config.get("execution_mode", "databricks")  # or "local"
    catalog = config.get("catalog", "default")

    if mode == "databricks":
        full_table = f"{catalog}.{layer}.{table_name}"
        print(f"Reading table {full_table}")
        return spark.table(full_table)

    elif mode == "local":
        base_path = config.get("local_base_path", "data")
        file_path = os.path.join(base_path, layer, f"{table_name}.parquet")
        print(f"Reading file {file_path}")
        return spark.read.parquet(file_path)

    else:
        raise ValueError(f"Unknown execution mode: {mode}")


def write_dataset(df, config, layer, table_name):
    """
    Writes data either to a Databricks table or to a local path,
    depending on the execution environment.

    Args:
        df: Spark DataFrame
        config: dict loaded from YAML or env
        layer: str, e.g. "raw", "clean", "serving"
        table_name: str, table or filename without extension
    """
    mode = config.get("execution_mode", "databricks")
    catalog = config.get("catalog", "zakrzewski_spark_task")

    if mode == "databricks":
        full_table = f"{catalog}.{layer}.{table_name}"
        print(f"Writing to table {full_table}")
        df.write.mode("overwrite").saveAsTable(full_table)

    elif mode == "local":
        base_path = config.get("local_base_path", "data")
        out_path = os.path.join(base_path, layer, f"{table_name}.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        print(f"Writing to file {out_path}")
        df.write.mode("overwrite").parquet(out_path)

    else:
        raise ValueError(f"Unknown execution mode: {mode}")
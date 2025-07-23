import os
import sys
import zipfile
import yaml
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark import SparkContext

# # Set up venv Python for PySpark
# venv_python = os.path.join(os.path.dirname(__file__), ".venv", "Scripts", "python.exe")
# venv_python = os.path.abspath(venv_python)
# os.environ["PYSPARK_PYTHON"] = venv_python
# os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python
# 
# # Set up path
# try:
#     project_root = os.path.dirname(os.path.abspath(__file__))
# except NameError:
#     project_root = os.getcwd()
# src_path = os.path.join(project_root, 'src')
# src_zip_path = os.path.join(project_root, 'src.zip')
# if src_path not in sys.path:
#     sys.path.insert(0, src_path)
# 
# 
# print(os.listdir(os.path.join(os.path.dirname(__file__), 'src', 'etl')))

# from utils.spark_utils import get_spark_session





def get_project_root():
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return os.getcwd()

def set_python_environment():
    root = get_project_root()
    venv_python = os.path.join(root, ".venv", "Scripts", "python.exe")  # Windows
    venv_python = os.path.abspath(venv_python)

    os.environ["PYSPARK_PYTHON"] = venv_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

def clean_old_spark_temp_dirs():
    spark_temp = tempfile.gettempdir()
    for name in os.listdir(spark_temp):
        if name.startswith("spark-"):
            path = os.path.join(spark_temp, name)
            try:
                shutil.rmtree(path)
            except Exception:
                pass  # Ignore if already deleted or permission denied

def zip_src_folder(src_dir, zip_path):
    if os.path.exists(zip_path):
        os.remove(zip_path)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, _, files in os.walk(src_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, start=src_dir)
                zipf.write(file_path, arcname)

def setup_paths_and_zip():
    root = get_project_root()
    src_path = os.path.join(root, "src")
    zip_path = os.path.join(root, "src.zip")

    # Ensure Python can import from src
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # Zip the src folder for Spark
    zip_src_folder(src_path, zip_path)
    return zip_path



def load_config(config_path="config/dev.yaml"):
    """Load YAML config file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_spark_session(execution_mode):
    if SparkContext._active_spark_context:
        SparkContext.getOrCreate().stop()

    if execution_mode == "local":
        spark = SparkSession.builder \
            .appName("LocalSparkPipeline") \
            .master("local[*]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
    else:
        spark = SparkSession.builder.getOrCreate()

    print(f"Spark master is set to: {spark.sparkContext.master}")

    return spark

if __name__ == "__main__":
    set_python_environment()
    clean_old_spark_temp_dirs()
    src_zip = setup_paths_and_zip()
    
    # Load config
    config = load_config("config/dev.yaml")
    execution_mode = config['execution_mode'].strip().lower()
    print(f"Execution mode from config: {execution_mode}")
    
    spark = get_spark_session(execution_mode)

    spark.sparkContext.addPyFile(src_zip)
    
    from etl.pipeline import run_pipeline

    run_pipeline(spark, config)
    print("Pipeline execution completed.")

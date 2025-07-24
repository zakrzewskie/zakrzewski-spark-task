# Zakrzewski Spark ETL Task

A PySpark ETL (Extract, Transform, Load) pipeline for processing insurance contract and claim data with data quality validations and export capabilities.

## Features

- **Data Processing**: Processes raw contract and claim data through multiple transformation layers
- **Data Quality**: Implements comprehensive data validation and quality checks
- **Schema Management**: Centralized schema definitions with metadata
- **Export Capabilities**: Export processed data to multiple formats (CSV, JSON, Parquet)
- **Testing**: Comprehensive test suite with pytest

## Prerequisites

- Python 3.11.9
- Java 17 (recommended) or Java 8/11
- pip (Python package manager)
- **Windows Users**:
  - Download WinUtils from [Apache Hadoop](https://github.com/steveloughran/winutils) (matching your Hadoop version)
  - Set `HADOOP_HOME` environment variable to the WinUtils directory
  - Add `%HADOOP_HOME%\bin` to your system PATH

## Installation

1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # On Windows
   # or
   source .venv/bin/activate  # On Unix/Linux/MacOS
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The project uses YAML configuration files located in the `config/` directory. The main configuration file is `config/dev.yaml`.

Key configuration options:
- `execution_mode`: Set to "local" for local development or "databricks" for Databricks execution
- `local_base_path`: Base path for local file operations
- Schema and table names for different data layers

## Usage

### Running the ETL Pipeline

To run the complete ETL pipeline:

```bash
python main.py
```

### Exporting Data

Export processed transactions to different formats:

```bash
# Export to CSV (default)
python scripts/export_transactions.py --config config/dev.yaml

# Export to JSON
python scripts/export_transactions.py --config config/dev.yaml --format json

# Export to Parquet
python scripts/export_transactions.py --config config/dev.yaml --format parquet

# Specify custom output path
python scripts/export_transactions.py --config config/dev.yaml --output-path data/exports/custom_export
```

By default, exports are saved to `data/export/transactions.{format}`.

## Project Structure

```
.
├── config/                  # Configuration files
│   └── dev.yaml            # Development configuration
├── data/                   # Data directories (created at runtime)
│   ├── 10_raw/             # Raw input data
│   ├── 20_clean/           # Cleaned data
│   ├── 30_serving/         # Final processed data
│   └── export/             # Export directory for processed data
├── scripts/                # Utility scripts
│   └── export_transactions.py  # Data export utility
├── src/                    # Source code
│   └── etl/                # ETL pipeline code
│       ├── __init__.py
│       ├── pipeline.py     # Main pipeline implementation
│       └── utils.py        # Utility functions
└── tests/                  # Test files
    └── test_pipeline.py    # Pipeline tests
```

## Running Tests

To run the test suite:

```bash
python run_tests.py
```

## Data Processing Pipeline

The ETL pipeline processes data through several stages:

1. **Extract**: Reads raw data from source systems
2. **Clean**: Applies data cleaning and validation
3. **Transform**: Performs business logic transformations

## Data Export

The `export_transactions.py` script provides functionality to export the processed TRANSACTIONS dataset to various formats:

- **CSV**: Human-readable format, good for small to medium datasets
- **JSON**: Structured format, good for web APIs and integrations
- **Parquet**: Columnar format, optimized for big data processing

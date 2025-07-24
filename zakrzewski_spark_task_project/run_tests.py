#!/usr/bin/env python3
"""
Test runner script for ETL Pipeline unit tests.
This script provides an easy way to run all tests or specific test modules.
"""

import subprocess
import sys
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import yaml

venv_python = os.path.abspath(os.path.join(os.path.dirname(__file__), ".venv", "Scripts", "python.exe"))
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))






def run_tests(test_path=None, verbose=True, coverage=False):
    """
    Run pytest with specified options.
    
    Args:
        test_path (str): Specific test file or directory to run
        verbose (bool): Enable verbose output
        coverage (bool): Enable coverage reporting
    """
    cmd = [sys.executable, "-m", "pytest"]
    
    if test_path:
        cmd.append(test_path)
    
    if verbose:
        cmd.append("-v")
    
    if coverage:
        cmd.extend(["--cov=src", "--cov-report=html", "--cov-report=term"])
    
    # Add current directory to Python path
    env = os.environ.copy()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env["PYTHONPATH"] = current_dir + os.pathsep + env.get("PYTHONPATH", "")
    
    print(f"Running command: {' '.join(cmd)}")
    print(f"Working directory: {current_dir}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, cwd=current_dir, env=env)
        return result.returncode
    except FileNotFoundError:
        print("Error: pytest not found. Please install it with: pip install pytest")
        return 1


def main():
    """Main function to handle command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run ETL Pipeline tests")
    parser.add_argument("test_path", nargs="?", help="Specific test file or directory to run")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-c", "--coverage", action="store_true", help="Enable coverage reporting")
    parser.add_argument("--raw", action="store_true", help="Run only pipeline_raw tests")
    parser.add_argument("--clean", action="store_true", help="Run only pipeline_clean tests")
    parser.add_argument("--serving", action="store_true", help="Run only pipeline_serving tests")
    parser.add_argument("--pipeline", action="store_true", help="Run only main pipeline tests")
    
    args = parser.parse_args()
    
    # Determine test path based on flags
    test_path = args.test_path
    if args.raw:
        test_path = "tests/test_pipeline_raw.py"
    elif args.clean:
        test_path = "tests/test_pipeline_clean.py"
    elif args.serving:
        test_path = "tests/test_pipeline_serving.py"
    elif args.pipeline:
        test_path = "tests/test_pipeline.py"
    
    return run_tests(test_path, args.verbose, args.coverage)


if __name__ == "__main__":
    sys.exit(main())

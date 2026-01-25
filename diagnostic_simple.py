#!/usr/bin/env python3
"""
Simple diagnostic script to test SparkSession in Databricks serverless.

This script runs WITHOUT Flyte to establish a baseline:
1. Can we create a SparkSession in a python_file task?
2. Does SparkSession.builder.getOrCreate() work?
3. Can we run a simple Spark operation?

To use:
1. Push this file to your Git repo
2. Create a Databricks job with this as the python_file
3. Run it on serverless compute
4. Check the output logs

This will tell us if the basic Databricks serverless Spark functionality works
before we add Flyte complexity.
"""

import os
import sys


def log(msg):
    print(f"[SIMPLE-DIAG] {msg}")


def main():
    log("=" * 60)
    log("SIMPLE DATABRICKS SERVERLESS SPARK TEST")
    log("=" * 60)
    log(f"Python: {sys.version}")
    log(f"Working directory: {os.getcwd()}")
    
    # Check environment
    log("\n--- Environment Variables ---")
    spark_remote = os.environ.get("SPARK_REMOTE", "NOT SET")
    log(f"SPARK_REMOTE: {spark_remote[:60]}..." if len(spark_remote) > 60 else f"SPARK_REMOTE: {spark_remote}")
    log(f"DATABRICKS_RUNTIME_VERSION: {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'NOT SET')}")
    log(f"SPARK_CONNECT_MODE_ENABLED: {os.environ.get('SPARK_CONNECT_MODE_ENABLED', 'NOT SET')}")
    
    # Check sys.path
    log("\n--- sys.path (first 5) ---")
    for i, p in enumerate(sys.path[:5]):
        log(f"  {i}: {p}")
    
    # Prioritize Databricks path
    log("\n--- Prioritizing Databricks Python path ---")
    db_paths = [
        "/databricks/python/lib/python3.12/site-packages",
        "/databricks/python/lib/python3.11/site-packages", 
        "/databricks/python/lib/python3.10/site-packages",
    ]
    for db_path in db_paths:
        if os.path.exists(db_path):
            if db_path in sys.path:
                sys.path.remove(db_path)
            sys.path.insert(0, db_path)
            log(f"Added to front of sys.path: {db_path}")
            break
    
    # Test 1: Import pyspark
    log("\n--- Test 1: Import pyspark ---")
    try:
        import pyspark
        log(f"SUCCESS: pyspark imported from {pyspark.__file__}")
        log(f"pyspark version: {pyspark.__version__}")
    except Exception as e:
        log(f"FAILED: {e}")
        return
    
    # Test 2: Create SparkSession
    log("\n--- Test 2: Create SparkSession ---")
    try:
        from pyspark.sql import SparkSession
        log(f"SparkSession class from: {SparkSession.__module__}")
        
        # Try getOrCreate
        log("Calling SparkSession.builder.getOrCreate()...")
        spark = SparkSession.builder.getOrCreate()
        log(f"SUCCESS: {spark}")
        log(f"SparkSession type: {type(spark)}")
    except Exception as e:
        log(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        
        # Try alternative method
        log("\n--- Test 2b: Try getActiveSession ---")
        try:
            active = SparkSession.getActiveSession()
            if active:
                log(f"SUCCESS: Got active session: {active}")
                spark = active
            else:
                log("No active session found")
                return
        except Exception as e2:
            log(f"FAILED: {e2}")
            return
    
    # Test 3: Run a simple Spark operation
    log("\n--- Test 3: Run simple Spark operation ---")
    try:
        log("Creating DataFrame with spark.range(100)...")
        df = spark.range(100)
        log(f"DataFrame: {df}")
        
        log("Counting rows...")
        count = df.count()
        log(f"SUCCESS: count = {count}")
        
        log("Computing sum...")
        total = df.agg({"id": "sum"}).collect()[0][0]
        log(f"SUCCESS: sum(0..99) = {total}")
    except Exception as e:
        log(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 4: Store and retrieve SparkSession
    log("\n--- Test 4: Store and retrieve SparkSession ---")
    try:
        import builtins
        import types
        
        # Store in builtins
        builtins.spark = spark
        log(f"Stored in builtins.spark")
        
        # Store in custom module
        spark_module = types.ModuleType('_test_spark_module')
        spark_module.spark = spark
        sys.modules['_test_spark_module'] = spark_module
        log(f"Stored in _test_spark_module")
        
        # Verify
        log(f"builtins.spark: {builtins.spark}")
        log(f"sys.modules['_test_spark_module'].spark: {sys.modules['_test_spark_module'].spark}")
        
        # Try to retrieve
        retrieved_builtin = builtins.spark
        retrieved_module = sys.modules['_test_spark_module'].spark
        
        log(f"Retrieved from builtins: {retrieved_builtin}")
        log(f"Retrieved from module: {retrieved_module}")
        
        if retrieved_builtin is spark and retrieved_module is spark:
            log("SUCCESS: SparkSession persists correctly!")
        else:
            log("FAILED: SparkSession changed after retrieval")
    except Exception as e:
        log(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
    
    log("\n" + "=" * 60)
    log("DIAGNOSTIC COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()

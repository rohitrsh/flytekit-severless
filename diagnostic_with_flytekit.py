#!/usr/bin/env python3
"""
Diagnostic to test if importing flytekit breaks SparkSession.

This simulates what happens in the Flyte execution path:
1. Create SparkSession (like entrypoint does)
2. Store it in builtins and custom module
3. Import flytekit and flytekitplugins.spark (like fast_execute does)
4. Check if SparkSession is still accessible

This will tell us if flytekit imports are causing the problem.
"""

import os
import sys


def log(msg):
    print(f"[FLYTE-DIAG] {msg}")


def main():
    log("=" * 60)
    log("FLYTEKIT IMPORT DIAGNOSTIC")
    log("=" * 60)
    log(f"Python: {sys.version}")
    
    # Step 1: Prioritize Databricks path and create SparkSession
    log("\n--- Step 1: Create SparkSession (like entrypoint) ---")
    
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
            log(f"Prioritized: {db_path}")
            break
    
    from pyspark.sql import SparkSession
    log(f"SparkSession class from: {SparkSession.__module__}")
    
    spark = SparkSession.builder.getOrCreate()
    log(f"Created SparkSession: {spark}")
    log(f"Type: {type(spark)}")
    
    # Verify it works
    count = spark.range(10).count()
    log(f"spark.range(10).count() = {count}")
    
    # Step 2: Store in builtins and custom module
    log("\n--- Step 2: Store SparkSession ---")
    import builtins
    import types
    
    builtins.spark = spark
    log(f"Stored in builtins.spark")
    
    spark_module = types.ModuleType('_flyte_spark_session')
    spark_module.spark = spark
    sys.modules['_flyte_spark_session'] = spark_module
    log(f"Stored in _flyte_spark_session module")
    
    # Verify storage
    log(f"builtins.spark: {builtins.spark}")
    log(f"_flyte_spark_session.spark: {sys.modules['_flyte_spark_session'].spark}")
    
    # Step 3: Check pyspark modules before flytekit import
    log("\n--- Step 3: Check state BEFORE flytekit import ---")
    pyspark_mods = [k for k in sys.modules.keys() if k.startswith('pyspark')]
    log(f"pyspark modules count: {len(pyspark_mods)}")
    log(f"pyspark module location: {sys.modules.get('pyspark', 'NOT LOADED')}")
    
    # Step 4: Import flytekit (this is what fast_execute does)
    log("\n--- Step 4: Import flytekit ---")
    try:
        import flytekit
        log(f"flytekit imported from: {flytekit.__file__}")
    except Exception as e:
        log(f"flytekit import failed: {e}")
    
    # Step 5: Check state AFTER flytekit import
    log("\n--- Step 5: Check state AFTER flytekit import ---")
    pyspark_mods_after = [k for k in sys.modules.keys() if k.startswith('pyspark')]
    log(f"pyspark modules count: {len(pyspark_mods_after)}")
    
    # Check if builtins.spark still exists
    log(f"builtins.spark exists: {hasattr(builtins, 'spark')}")
    if hasattr(builtins, 'spark'):
        log(f"builtins.spark value: {builtins.spark}")
        log(f"builtins.spark is original spark: {builtins.spark is spark}")
    
    # Check if custom module still exists
    log(f"_flyte_spark_session in sys.modules: {'_flyte_spark_session' in sys.modules}")
    if '_flyte_spark_session' in sys.modules:
        mod = sys.modules['_flyte_spark_session']
        log(f"_flyte_spark_session.spark: {mod.spark}")
        log(f"_flyte_spark_session.spark is original spark: {mod.spark is spark}")
    
    # Step 6: Import flytekitplugins.spark (this is what happens when task loads)
    log("\n--- Step 6: Import flytekitplugins.spark ---")
    try:
        import flytekitplugins.spark
        log(f"flytekitplugins.spark imported from: {flytekitplugins.spark.__file__}")
    except Exception as e:
        log(f"flytekitplugins.spark import failed: {e}")
    
    # Step 7: Check state AFTER flytekitplugins.spark import
    log("\n--- Step 7: Check state AFTER flytekitplugins.spark import ---")
    pyspark_mods_final = [k for k in sys.modules.keys() if k.startswith('pyspark')]
    log(f"pyspark modules count: {len(pyspark_mods_final)}")
    
    # Check pyspark module location
    if 'pyspark' in sys.modules:
        log(f"pyspark module location: {sys.modules['pyspark'].__file__}")
    
    # Check if builtins.spark still works
    log(f"\nbuiltins.spark exists: {hasattr(builtins, 'spark')}")
    if hasattr(builtins, 'spark'):
        log(f"builtins.spark value: {builtins.spark}")
        log(f"builtins.spark is None: {builtins.spark is None}")
        
        # Try to use it
        try:
            test_spark = builtins.spark
            count = test_spark.range(5).count()
            log(f"builtins.spark.range(5).count() = {count}")
            log("SUCCESS: builtins.spark still works!")
        except Exception as e:
            log(f"FAILED: builtins.spark broken: {e}")
    
    # Check if custom module still works
    log(f"\n_flyte_spark_session in sys.modules: {'_flyte_spark_session' in sys.modules}")
    if '_flyte_spark_session' in sys.modules:
        mod = sys.modules['_flyte_spark_session']
        log(f"_flyte_spark_session.spark value: {mod.spark}")
        
        # Try to use it
        try:
            test_spark = mod.spark
            count = test_spark.range(5).count()
            log(f"_flyte_spark_session.spark.range(5).count() = {count}")
            log("SUCCESS: _flyte_spark_session.spark still works!")
        except Exception as e:
            log(f"FAILED: _flyte_spark_session.spark broken: {e}")
    
    # Step 8: Try what task.py does - call _get_databricks_serverless_spark_session
    log("\n--- Step 8: Simulate task.py behavior ---")
    try:
        from flytekitplugins.spark.task import PysparkFunctionTask
        log(f"PysparkFunctionTask imported")
        
        # Check what SparkSession the task module sees
        from flytekitplugins.spark import task as task_module
        if hasattr(task_module, 'SparkSession'):
            log(f"task.py SparkSession: {task_module.SparkSession}")
    except Exception as e:
        log(f"Could not import task module: {e}")
    
    log("\n" + "=" * 60)
    log("DIAGNOSTIC COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Serverless-compatible entrypoint for Flyte Databricks tasks.

This entrypoint is designed to work with Databricks Serverless Compute,
which has restrictions on accessing certain directories like /root and
does not provide standard AWS credentials via instance metadata.

Key features:
- Uses /tmp as the working directory (accessible in serverless)
- Configures AWS credentials from Databricks service credentials
- Works with flytekitplugins-spark which has native serverless support

Credential Provider Configuration (in order of precedence):
    1. Command-line argument: --flyte-credential-provider=<name>
    2. Environment variable: DATABRICKS_SERVICE_CREDENTIAL_PROVIDER
    3. Default fallback (if configured in DEFAULT_CREDENTIAL_PROVIDER below)

Optional Environment Variables:
    AWS_DEFAULT_REGION: AWS region (defaults to 'us-east-1')
    FLYTE_INTERNAL_WORK_DIR: Working directory (defaults to '/tmp/flyte')
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


# =============================================================================
# CONFIGURATION: Set your default credential provider here (optional)
# =============================================================================
# If environment variables and command-line arguments don't work in your 
# Databricks serverless setup, you can hardcode the credential provider name 
# here as a fallback. Set to None to disable the fallback.

DEFAULT_CREDENTIAL_PROVIDER = None  # e.g., "my-credential-provider-name"

# =============================================================================


def parse_credential_provider_from_args():
    """
    Parse the credential provider from command-line arguments.
    
    Looks for --flyte-credential-provider=<name> anywhere in sys.argv
    and removes it from the args list.
    
    Returns:
        tuple: (credential_provider, remaining_args)
    """
    credential_provider = None
    remaining_args = []
    
    for arg in sys.argv[1:]:
        if arg.startswith("--flyte-credential-provider="):
            credential_provider = arg.split("=", 1)[1]
            print(f"[Flyte] Credential provider from args: {credential_provider}")
        else:
            remaining_args.append(arg)
    
    if remaining_args:
        print(f"[Flyte] Command: {remaining_args[0]} ...")
    
    return credential_provider, remaining_args


def debug_print_environment():
    """Print relevant environment variables for debugging."""
    print("[Flyte] === Relevant Environment Variables ===")
    relevant_vars = []
    other_count = 0
    
    for key in sorted(os.environ.keys()):
        value = os.environ[key]
        # Mask sensitive values
        if any(secret_word in key.upper() for secret_word in ['SECRET', 'TOKEN', 'PASSWORD', 'KEY', 'CREDENTIAL']):
            if len(value) > 8:
                value = value[:4] + "****" + value[-4:]
        
        # Only show relevant vars
        if any(x in key.upper() for x in ['AWS', 'FLYTE', 'DATABRICKS', 'S3', 'SPARK']):
            relevant_vars.append(f"  {key}={value}")
        else:
            other_count += 1
    
    for v in relevant_vars:
        print(f"[Flyte] {v}")
    
    print(f"[Flyte] + {other_count} other variables")
    print("[Flyte] === END ===")


def setup_aws_credentials_from_databricks(credential_provider: str = None):
    """
    Configure AWS credentials from Databricks service credentials.
    
    In Databricks serverless, standard AWS credential mechanisms (instance metadata,
    environment variables) are not available. Instead, credentials must be obtained
    via dbutils.credentials.getServiceCredentialsProvider().
    
    Args:
        credential_provider: Optional credential provider name (overrides env var)
    
    Returns:
        bool: True if credentials were configured, False otherwise
    """
    # Try to get credential provider from multiple sources (in order of precedence)
    if not credential_provider:
        credential_provider = os.environ.get("DATABRICKS_SERVICE_CREDENTIAL_PROVIDER")
    
    if not credential_provider:
        if DEFAULT_CREDENTIAL_PROVIDER:
            print(f"[Flyte] Using fallback credential provider: {DEFAULT_CREDENTIAL_PROVIDER}")
            credential_provider = DEFAULT_CREDENTIAL_PROVIDER
        else:
            print("[Flyte] WARNING: No credential provider configured")
            print("[Flyte] S3 access may fail. Options to fix:")
            print("[Flyte]   1. Pass --flyte-credential-provider=<name> as command argument")
            print("[Flyte]   2. Set DATABRICKS_SERVICE_CREDENTIAL_PROVIDER env var")
            print("[Flyte]   3. Use databricks_service_credential_provider in DatabricksV2 config")
            return False
    
    print(f"[Flyte] Configuring AWS credentials from: {credential_provider}")
    
    try:
        # Import dbutils - only available in Databricks runtime
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        # Get SparkSession (pre-configured in serverless)
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        
        # Get the botocore session from Databricks service credentials
        print("[Flyte] Calling dbutils.credentials.getServiceCredentialsProvider()...")
        botocore_session = dbutils.credentials.getServiceCredentialsProvider(credential_provider)
        
        # Create a boto3 session with this botocore session
        import boto3
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        session = boto3.Session(botocore_session=botocore_session, region_name=region)
        
        # Get credentials from the session
        credentials = session.get_credentials()
        if credentials is None:
            print("[Flyte] ERROR: No credentials returned from service credential provider")
            return False
        
        # Get frozen credentials (thread-safe snapshot)
        frozen_credentials = credentials.get_frozen_credentials()
        
        # Set environment variables for AWS SDK / s3fs / flytekit
        os.environ["AWS_ACCESS_KEY_ID"] = frozen_credentials.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = frozen_credentials.secret_key
        
        if frozen_credentials.token:
            os.environ["AWS_SESSION_TOKEN"] = frozen_credentials.token
        
        if region:
            os.environ["AWS_DEFAULT_REGION"] = region
        
        # Verify credentials are working
        try:
            sts_client = session.client("sts")
            identity = sts_client.get_caller_identity()
            print(f"[Flyte] ✓ AWS credentials configured successfully")
            print(f"[Flyte]   Account: {identity.get('Account', 'unknown')}")
            print(f"[Flyte]   ARN: {identity.get('Arn', 'unknown')}")
        except Exception as e:
            print(f"[Flyte] WARNING: Could not verify AWS credentials: {e}")
        
        return True
        
    except ImportError as e:
        print(f"[Flyte] WARNING: Required modules not available: {e}")
        print("[Flyte] This entrypoint must run in Databricks serverless environment")
        return False
    except Exception as e:
        print(f"[Flyte] ERROR: Failed to configure AWS credentials: {e}")
        import traceback
        traceback.print_exc()
        return False


def setup_spark_session():
    """
    Pre-initialize SparkSession and inject it into builtins.
    
    This ensures the Flyte plugin can find the SparkSession via builtins.spark,
    which is the standard Databricks way of exposing the pre-configured session.
    """
    import builtins
    
    # Check if spark is already in builtins
    if hasattr(builtins, 'spark'):
        print(f"[Flyte] SparkSession already in builtins: {builtins.spark}")
        return builtins.spark
    
    spark_remote = os.environ.get("SPARK_REMOTE")
    if not spark_remote:
        print("[Flyte] SPARK_REMOTE not set - cannot initialize SparkSession")
        return None
    
    print(f"[Flyte] SPARK_REMOTE: {spark_remote}")
    
    try:
        # Use Databricks' PySpark which understands the Unix socket format
        import sys
        db_paths = [
            "/databricks/python/lib/python3.12/site-packages",
            "/databricks/python/lib/python3.11/site-packages", 
            "/databricks/python/lib/python3.10/site-packages",
        ]
        
        # Insert Databricks paths at the beginning so they take precedence
        for db_path in db_paths:
            if os.path.exists(db_path):
                if db_path in sys.path:
                    sys.path.remove(db_path)
                sys.path.insert(0, db_path)
                print(f"[Flyte] Prioritized Databricks PySpark: {db_path}")
                break
        
        from pyspark.sql import SparkSession
        
        # First try to get an active session
        active = SparkSession.getActiveSession()
        if active:
            print(f"[Flyte] Got active SparkSession: {active}")
            builtins.spark = active
            return active
        
        # Create session using SPARK_REMOTE
        print("[Flyte] Creating SparkSession via SPARK_REMOTE...")
        spark = SparkSession.builder.remote(spark_remote).getOrCreate()
        print(f"[Flyte] ✓ SparkSession created: {spark}")
        
        # Inject into builtins so plugin can find it
        builtins.spark = spark
        print("[Flyte] ✓ SparkSession injected into builtins")
        
        return spark
        
    except Exception as e:
        print(f"[Flyte] WARNING: Could not initialize SparkSession: {e}")
        import traceback
        traceback.print_exc()
        return None


def setup_environment():
    """Set up the working environment for serverless compute."""
    # Mark this as Databricks serverless for plugin compatibility
    # The flytekitplugins-spark plugin checks these env vars
    os.environ["DATABRICKS_SERVERLESS"] = "true"
    os.environ["SPARK_CONNECT_MODE"] = "true"
    
    # Use /tmp for serverless - it's always writable
    # Unlike /root which is not accessible in serverless
    work_dir = os.environ.get("FLYTE_INTERNAL_WORK_DIR", "/tmp/flyte")
    
    try:
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        os.chdir(work_dir)
        print(f"[Flyte] Working directory: {work_dir}")
    except Exception as e:
        # Fallback to temp directory if even /tmp/flyte fails
        work_dir = tempfile.mkdtemp(prefix="flyte_")
        os.chdir(work_dir)
        print(f"[Flyte] Fallback working directory: {work_dir}")
    
    return work_dir


def execute_flyte_command_inprocess(args: list):
    """
    Execute the Flyte command IN-PROCESS (not as subprocess).
    
    This is critical because:
    1. SparkSession is created and injected into builtins by setup_spark_session()
    2. If we use subprocess, the new process won't have access to builtins.spark
    3. Running in-process preserves the SparkSession for Flyte tasks
    """
    if not args:
        print("[Flyte] ERROR: No command provided", file=sys.stderr)
        return 1
    
    cmd = args[0] if args else ""
    cmd_str = " ".join(args)
    print(f"[Flyte] Executing in-process: {cmd_str}")
    
    try:
        # Import Flyte's entrypoint functions (these are Click commands)
        from flytekit.bin.entrypoint import fast_execute_task_cmd, execute_task_cmd
        
        if cmd == "pyflyte-fast-execute":
            # fast_execute_task_cmd is a Click command - invoke with standalone_mode=False
            # to prevent sys.exit() and get proper return values
            # Args format: --additional-distribution <url> --dest-dir <dir> -- pyflyte-execute ...
            click_args = args[1:]  # Remove the command name, keep the rest
            print(f"[Flyte] Invoking fast_execute_task_cmd with args: {click_args}")
            
            result = fast_execute_task_cmd.main(click_args, standalone_mode=False)
            return 0
            
        elif cmd == "pyflyte-execute":
            # execute_task_cmd is also a Click command
            click_args = args[1:]
            print(f"[Flyte] Invoking execute_task_cmd with args: {click_args}")
            
            result = execute_task_cmd.main(click_args, standalone_mode=False)
            return 0
            
        else:
            # Fallback to subprocess for unknown commands
            print(f"[Flyte] Unknown command '{cmd}', falling back to subprocess")
            result = subprocess.run(args, check=False, env=os.environ.copy())
            return result.returncode
            
    except SystemExit as e:
        # Click commands may still call sys.exit() even with standalone_mode=False
        code = e.code if e.code is not None else 0
        if code == 0:
            return 0
        print(f"[Flyte] Command exited with code: {code}")
        return code
    except Exception as e:
        print(f"[Flyte] ERROR: In-process execution failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        # Fallback to subprocess
        print("[Flyte] Falling back to subprocess execution...")
        try:
            result = subprocess.run(args, check=False, env=os.environ.copy())
            return result.returncode
        except Exception as e2:
            print(f"[Flyte] ERROR: Subprocess also failed: {e2}", file=sys.stderr)
            return 1


def is_running_in_ipython():
    """Check if we're running inside IPython/Jupyter/Databricks notebook."""
    try:
        get_ipython()  # noqa: F821
        return True
    except NameError:
        return False


def main():
    """Main entrypoint for Flyte serverless tasks."""
    print("[Flyte] " + "=" * 60)
    print("[Flyte] Serverless Entrypoint")
    print("[Flyte] " + "=" * 60)
    print(f"[Flyte] Python: {sys.version.split()[0]}")
    print(f"[Flyte] Args: {sys.argv[1:]}")
    
    # Parse credential provider from command-line arguments
    credential_provider, remaining_args = parse_credential_provider_from_args()
    
    # Debug: Print relevant environment variables
    debug_print_environment()
    
    # Configure AWS credentials from Databricks service credentials
    # This must happen BEFORE any S3 access attempts
    credentials_configured = setup_aws_credentials_from_databricks(credential_provider)
    if not credentials_configured:
        print("[Flyte] ⚠ WARNING: Running without Databricks-managed AWS credentials")
        print("[Flyte] S3 operations may fail!")
    
    # Set up the working environment (sets DATABRICKS_SERVERLESS=true)
    work_dir = setup_environment()
    
    # Pre-initialize SparkSession and inject into builtins
    # This ensures the Flyte plugin can find it
    spark = setup_spark_session()
    if spark:
        print("[Flyte] SparkSession ready for Flyte tasks")
    else:
        print("[Flyte] ⚠ No SparkSession - Spark operations may fail")
    
    # Execute the Flyte command IN-PROCESS (not subprocess)
    # This preserves the SparkSession in builtins for Flyte tasks
    return_code = execute_flyte_command_inprocess(remaining_args)
    
    print("[Flyte] " + "=" * 60)
    print(f"[Flyte] Task completed with return code: {return_code}")
    print("[Flyte] " + "=" * 60)
    
    # In IPython/Databricks notebook context, sys.exit() raises SystemExit which
    # appears as a traceback and may be interpreted as an error by Databricks.
    if is_running_in_ipython():
        print(f"[Flyte] Running in IPython context, not calling sys.exit()")
        if return_code != 0:
            raise RuntimeError(f"Flyte task failed with return code: {return_code}")
        return return_code
    else:
        sys.exit(return_code)


if __name__ == "__main__":
    main()

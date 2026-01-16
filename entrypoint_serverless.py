#!/usr/bin/env python3
"""
Serverless-compatible entrypoint for Flyte Databricks tasks.

This entrypoint is designed to work with Databricks Serverless Compute,
which has restrictions on accessing certain directories like /root and
does not provide standard AWS credentials via instance metadata.

Key differences from the classic entrypoint:
- Uses /tmp as the working directory (accessible in serverless)
- Handles the restricted serverless environment
- Compatible with Spark Connect APIs
- Configures AWS credentials from Databricks service credentials

Credential Provider Configuration (in order of precedence):
    1. Command-line argument: --flyte-credential-provider=<name>
    2. Environment variable: DATABRICKS_SERVICE_CREDENTIAL_PROVIDER
    3. Hardcoded fallback (if configured below)

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
# CONFIGURATION: Set your default credential provider here
# =============================================================================
# If environment variables don't work in your Databricks serverless setup,
# you can hardcode the credential provider name here as a fallback.
# Set to None to disable the fallback.

DEFAULT_CREDENTIAL_PROVIDER = "egdataplatform-test-flyte-workflow-access-us-east-1"

# =============================================================================


def parse_credential_provider_from_args():
    """
    Parse the credential provider from command-line arguments.
    
    Looks for --flyte-credential-provider=<name> anywhere in sys.argv
    and removes it from the args list.
    
    The argument can be at any position (typically at the end to avoid
    breaking pyflyte-fast-execute which must be the first argument).
    
    Returns:
        tuple: (credential_provider, remaining_args)
    """
    credential_provider = None
    remaining_args = []
    
    for arg in sys.argv[1:]:
        if arg.startswith("--flyte-credential-provider="):
            credential_provider = arg.split("=", 1)[1]
            print(f"[Flyte Serverless] Found credential provider in args: {credential_provider}")
        else:
            remaining_args.append(arg)
    
    if remaining_args:
        print(f"[Flyte Serverless] Command after parsing: {remaining_args[0]} ...")
    
    return credential_provider, remaining_args


def debug_print_environment():
    """Print all environment variables for debugging."""
    print("[Flyte Serverless] === DEBUG: All Environment Variables ===")
    relevant_vars = []
    other_vars = []
    
    for key in sorted(os.environ.keys()):
        value = os.environ[key]
        # Mask sensitive values
        if any(secret_word in key.upper() for secret_word in ['SECRET', 'TOKEN', 'PASSWORD', 'KEY', 'CREDENTIAL']):
            if len(value) > 8:
                value = value[:4] + "****" + value[-4:]
        
        # Categorize
        if any(x in key.upper() for x in ['AWS', 'FLYTE', 'DATABRICKS', 'S3', 'SPARK']):
            relevant_vars.append(f"  {key}={value}")
        else:
            other_vars.append(f"  {key}={value}")
    
    print("[Flyte Serverless] Relevant variables:")
    for v in relevant_vars:
        print(f"[Flyte Serverless] {v}")
    
    print(f"[Flyte Serverless] + {len(other_vars)} other variables")
    print("[Flyte Serverless] === END Environment Variables ===")


def try_get_credential_provider_from_spark_conf():
    """
    Try to get the credential provider name from Spark configuration.
    
    Databricks serverless might pass environment_vars through Spark conf
    instead of OS environment variables.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Try to get all Spark conf and look for our variable
        print("[Flyte Serverless] Checking Spark configuration...")
        
        all_conf = spark.sparkContext.getConf().getAll()
        for key, value in all_conf:
            if 'credential' in key.lower() or 'DATABRICKS' in key.upper():
                print(f"[Flyte Serverless] Spark conf: {key}={value}")
        
        # Try common Spark conf locations
        try:
            provider = spark.conf.get("spark.databricks.service.credential.provider")
            if provider:
                print(f"[Flyte Serverless] Found credential provider in Spark conf: {provider}")
                return provider
        except Exception:
            pass
            
    except Exception as e:
        print(f"[Flyte Serverless] Could not inspect Spark conf: {e}")
    
    return None


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
        print("[Flyte Serverless] Credential provider not in env vars, trying Spark conf...")
        credential_provider = try_get_credential_provider_from_spark_conf()
    
    if not credential_provider:
        if DEFAULT_CREDENTIAL_PROVIDER:
            print(f"[Flyte Serverless] Using hardcoded fallback credential provider: {DEFAULT_CREDENTIAL_PROVIDER}")
            credential_provider = DEFAULT_CREDENTIAL_PROVIDER
        else:
            print("[Flyte Serverless] WARNING: No credential provider found")
            print("[Flyte Serverless] Options to fix:")
            print("[Flyte Serverless]   1. Set DEFAULT_CREDENTIAL_PROVIDER in entrypoint_serverless.py")
            print("[Flyte Serverless]   2. Pass --flyte-credential-provider=<name> in spark_python_task parameters")
            print("[Flyte Serverless]   3. Set DATABRICKS_SERVICE_CREDENTIAL_PROVIDER env var")
            return False
    
    print(f"[Flyte Serverless] Configuring AWS credentials from: {credential_provider}")
    
    try:
        # Import dbutils - only available in Databricks runtime
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        # Get SparkSession (should be available in serverless context)
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        
        # Get the botocore session from Databricks service credentials
        print("[Flyte Serverless] Calling dbutils.credentials.getServiceCredentialsProvider()...")
        botocore_session = dbutils.credentials.getServiceCredentialsProvider(credential_provider)
        
        # Create a boto3 session with this botocore session
        import boto3
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        session = boto3.Session(botocore_session=botocore_session, region_name=region)
        
        # Get credentials from the session
        credentials = session.get_credentials()
        if credentials is None:
            print("[Flyte Serverless] ERROR: No credentials returned from service credential provider")
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
        
        # Verify credentials are working by checking caller identity
        try:
            sts_client = session.client("sts")
            identity = sts_client.get_caller_identity()
            print(f"[Flyte Serverless] ✓ AWS credentials configured successfully")
            print(f"[Flyte Serverless]   Account: {identity.get('Account', 'unknown')}")
            print(f"[Flyte Serverless]   ARN: {identity.get('Arn', 'unknown')}")
        except Exception as e:
            print(f"[Flyte Serverless] WARNING: Could not verify AWS credentials: {e}")
        
        return True
        
    except ImportError as e:
        print(f"[Flyte Serverless] WARNING: Required modules not available: {e}")
        print("[Flyte Serverless] This entrypoint must run in Databricks serverless environment")
        return False
    except Exception as e:
        print(f"[Flyte Serverless] ERROR: Failed to configure AWS credentials: {e}")
        import traceback
        traceback.print_exc()
        return False


def patch_spark_plugin_on_disk():
    """
    Patch the installed flytekitplugins-spark plugin file on disk.
    
    This is necessary because fast_execute_task_cmd spawns a subprocess,
    and in-memory patches don't survive across processes. By patching
    the actual file, subprocesses will import the patched version.
    """
    try:
        import flytekitplugins.spark.task as task_module
        task_file = task_module.__file__
        
        print(f"[Flyte Serverless] Patching spark plugin at: {task_file}")
        
        with open(task_file, 'r') as f:
            content = f.read()
        
        # Check if already patched
        if "SERVERLESS_PATCHED" in content:
            print("[Flyte Serverless] ✓ Spark plugin already patched on disk")
            return True
        
        # Find the pre_execute method and replace it
        # The original code has this pattern in the pre_execute method:
        # sess_builder = ... (building SparkSession with configs)
        # self.sess = sess_builder.getOrCreate()
        
        # We need to inject a check at the beginning of pre_execute
        patch_code = '''
    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        # SERVERLESS_PATCHED: Modified for Databricks serverless compatibility
        import os
        if os.environ.get("DATABRICKS_SERVERLESS") == "true" or os.environ.get("SPARK_CONNECT_MODE_ENABLED") == "1":
            import pyspark as _pyspark
            print("[Flyte Serverless] Using serverless-compatible pre_execute")
            try:
                self.sess = _pyspark.sql.SparkSession.builder.getOrCreate()
                print("[Flyte Serverless] ✓ Got SparkSession successfully")
            except Exception as e:
                print(f"[Flyte Serverless] Warning: Could not get SparkSession: {e}")
                self.sess = None
            return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()
        
        # Original pre_execute logic for non-serverless environments follows'''
        
        # Find the original pre_execute method definition
        import re
        
        # Pattern to find "def pre_execute(self, user_params: ExecutionParameters)"
        pattern = r'(\n    def pre_execute\(self, user_params: ExecutionParameters\)[^:]*:)'
        
        if re.search(pattern, content):
            # Replace the method signature with our patched version + original
            patched_content = re.sub(
                pattern,
                patch_code,
                content,
                count=1
            )
            
            # Write the patched file
            with open(task_file, 'w') as f:
                f.write(patched_content)
            
            print("[Flyte Serverless] ✓ Patched spark plugin on disk successfully")
            
            # Reload the module to pick up changes
            import importlib
            importlib.reload(task_module)
            
            return True
        else:
            print("[Flyte Serverless] WARNING: Could not find pre_execute method to patch")
            return False
            
    except ImportError as e:
        print(f"[Flyte Serverless] Could not find spark plugin to patch: {e}")
        return False
    except PermissionError as e:
        print(f"[Flyte Serverless] Permission denied patching plugin: {e}")
        return False
    except Exception as e:
        print(f"[Flyte Serverless] Error patching spark plugin: {e}")
        import traceback
        traceback.print_exc()
        return False


def patch_spark_plugin_for_serverless():
    """
    Patch the flytekitplugins-spark plugin for Databricks serverless.
    
    First tries to patch the file on disk (for subprocess compatibility),
    then falls back to in-memory patching.
    """
    # Try disk patching first (works across subprocesses)
    if patch_spark_plugin_on_disk():
        return
    
    # Fallback to in-memory patching
    print("[Flyte Serverless] Falling back to in-memory patching")
    try:
        from flytekitplugins.spark.task import PysparkFunctionTask
        from flytekit.core.context_manager import ExecutionParameters
        
        def serverless_pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
            """Serverless-compatible pre_execute that handles Spark Connect properly."""
            import pyspark as _pyspark
            
            print("[Flyte Serverless] Using patched pre_execute for Spark Connect compatibility")
            
            try:
                self.sess = _pyspark.sql.SparkSession.builder.getOrCreate()
                print(f"[Flyte Serverless] Got SparkSession successfully")
            except Exception as e:
                print(f"[Flyte Serverless] Warning: Could not get SparkSession: {e}")
                self.sess = None
            
            return user_params.builder().add_attr("SPARK_SESSION", self.sess).build()
        
        PysparkFunctionTask.pre_execute = serverless_pre_execute
        print("[Flyte Serverless] ✓ Patched flytekitplugins-spark in-memory")
        
    except ImportError as e:
        print(f"[Flyte Serverless] Could not patch spark plugin (not installed?): {e}")
    except Exception as e:
        print(f"[Flyte Serverless] Warning: Failed to patch spark plugin: {e}")


def setup_environment():
    """Set up the working environment for serverless compute."""
    # Mark this as Databricks serverless for plugin compatibility
    os.environ["DATABRICKS_SERVERLESS"] = "true"
    os.environ["SPARK_CONNECT_MODE"] = "true"
    
    # Patch the spark plugin to work with serverless Spark Connect
    patch_spark_plugin_for_serverless()
    
    # Use /tmp for serverless - it's always writable
    # Unlike /root which is not accessible in serverless
    work_dir = os.environ.get("FLYTE_INTERNAL_WORK_DIR", "/tmp/flyte")
    
    try:
        Path(work_dir).mkdir(parents=True, exist_ok=True)
        os.chdir(work_dir)
        print(f"[Flyte Serverless] Working directory: {work_dir}")
    except Exception as e:
        # Fallback to temp directory if even /tmp/flyte fails
        work_dir = tempfile.mkdtemp(prefix="flyte_")
        os.chdir(work_dir)
        print(f"[Flyte Serverless] Fallback working directory: {work_dir}")
    
    return work_dir


def execute_flyte_command_inprocess(args: list):
    """
    Execute the Flyte command in-process (not as subprocess).
    
    This is critical for Databricks serverless because:
    1. Monkey-patches applied in this process persist when running in-process
    2. Subprocess would start fresh Python without our patches
    3. The spark plugin patch must be active when pre_execute runs
    """
    if not args:
        print("[Flyte Serverless] ERROR: No command provided", file=sys.stderr)
        return 1
    
    cmd_str = " ".join(args)
    print(f"[Flyte Serverless] Executing in-process: {cmd_str}")
    
    # Determine which command to run
    cmd = args[0]
    cmd_args = args[1:]
    
    try:
        if cmd == "pyflyte-fast-execute":
            # Run pyflyte-fast-execute in-process
            # The command is defined in flytekit.bin.entrypoint
            from flytekit.bin.entrypoint import fast_execute_task_cmd
            # Set up sys.argv for click
            old_argv = sys.argv
            sys.argv = [cmd] + cmd_args
            try:
                fast_execute_task_cmd(standalone_mode=False)
                return 0
            except SystemExit as e:
                return e.code if e.code is not None else 0
            except Exception as e:
                print(f"[Flyte Serverless] ERROR in fast_execute: {e}")
                import traceback
                traceback.print_exc()
                return 1
            finally:
                sys.argv = old_argv
                
        elif cmd == "pyflyte-execute":
            # Run pyflyte-execute in-process
            # The command is defined in flytekit.bin.entrypoint
            from flytekit.bin.entrypoint import execute_task_cmd
            old_argv = sys.argv
            sys.argv = [cmd] + cmd_args
            try:
                execute_task_cmd(standalone_mode=False)
                return 0
            except SystemExit as e:
                return e.code if e.code is not None else 0
            except Exception as e:
                print(f"[Flyte Serverless] ERROR in execute: {e}")
                import traceback
                traceback.print_exc()
                return 1
            finally:
                sys.argv = old_argv
        else:
            # Unknown command - fall back to subprocess (patch won't work but try anyway)
            print(f"[Flyte Serverless] WARNING: Unknown command '{cmd}', falling back to subprocess")
            result = subprocess.run(args, check=False, env=os.environ.copy())
            return result.returncode
            
    except ImportError as e:
        print(f"[Flyte Serverless] ERROR: Could not import flytekit modules: {e}")
        return 1


def execute_flyte_command(args: list):
    """Execute the Flyte command - delegates to in-process execution."""
    return execute_flyte_command_inprocess(args)


def main():
    """Main entrypoint for Flyte serverless tasks."""
    print("[Flyte Serverless] " + "=" * 60)
    print("[Flyte Serverless] Starting Flyte Serverless Entrypoint")
    print("[Flyte Serverless] " + "=" * 60)
    print(f"[Flyte Serverless] Python version: {sys.version}")
    print(f"[Flyte Serverless] Raw arguments: {sys.argv[1:]}")
    
    # Parse credential provider from command-line arguments
    credential_provider, remaining_args = parse_credential_provider_from_args()
    if credential_provider:
        print(f"[Flyte Serverless] Credential provider from args: {credential_provider}")
    
    # Debug: Print relevant environment variables
    debug_print_environment()
    
    # Configure AWS credentials from Databricks service credentials
    # This must happen BEFORE any S3 access attempts
    credentials_configured = setup_aws_credentials_from_databricks(credential_provider)
    if not credentials_configured:
        print("[Flyte Serverless] ⚠ WARNING: Running without Databricks-managed AWS credentials")
        print("[Flyte Serverless] S3 operations will likely fail!")
    
    # Set up the working environment
    work_dir = setup_environment()
    
    # Execute the Flyte command with remaining arguments
    return_code = execute_flyte_command(remaining_args)
    
    print("[Flyte Serverless] " + "=" * 60)
    print(f"[Flyte Serverless] Task completed with return code: {return_code}")
    print("[Flyte Serverless] " + "=" * 60)
    sys.exit(return_code)


if __name__ == "__main__":
    main()

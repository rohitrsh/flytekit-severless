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

Required Environment Variables:
    DATABRICKS_SERVICE_CREDENTIAL_PROVIDER: Name of the Databricks service credential
        provider to use for AWS access (e.g., 'egdataplatform-test-flyte-workflow-access-us-east-1')

Optional Environment Variables:
    AWS_DEFAULT_REGION: AWS region (defaults to 'us-east-1')
    FLYTE_INTERNAL_WORK_DIR: Working directory (defaults to '/tmp/flyte')

Usage in your Flyte task config:
    DatabricksV2(
        databricks_conf={
            "environment_key": "default",
            "environments": [{
                "environment_key": "default",
                "spec": {
                    "client": "1",
                    "environment_vars": {
                        "DATABRICKS_SERVICE_CREDENTIAL_PROVIDER": "your-credential-provider-name"
                    }
                }
            }],
            "git_source": {
                "git_url": "https://github.com/YOUR_ORG/YOUR_REPO",
                "git_provider": "gitHub",
                "git_branch": "main",
            },
            "python_file": "path/to/entrypoint_serverless.py",
        }
    )
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def setup_aws_credentials_from_databricks():
    """
    Configure AWS credentials from Databricks service credentials.
    
    In Databricks serverless, standard AWS credential mechanisms (instance metadata,
    environment variables) are not available. Instead, credentials must be obtained
    via dbutils.credentials.getServiceCredentialsProvider().
    
    This function:
    1. Gets the credential provider name from environment variable
    2. Uses dbutils to get a botocore session with proper credentials
    3. Extracts the credentials and sets them as environment variables
    4. This allows flytekit (via s3fs/boto3) to access S3 seamlessly
    
    Returns:
        bool: True if credentials were configured, False otherwise
    """
    credential_provider = os.environ.get("DATABRICKS_SERVICE_CREDENTIAL_PROVIDER")
    
    if not credential_provider:
        print("[Flyte Serverless] WARNING: DATABRICKS_SERVICE_CREDENTIAL_PROVIDER not set")
        print("[Flyte Serverless] S3 access may fail without proper AWS credentials")
        return False
    
    print(f"[Flyte Serverless] Configuring AWS credentials from Databricks service credential: {credential_provider}")
    
    try:
        # Import dbutils - only available in Databricks runtime
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        # Get SparkSession (should be available in serverless context)
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        
        # Get the botocore session from Databricks service credentials
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
            print(f"[Flyte Serverless] AWS credentials configured successfully")
            print(f"[Flyte Serverless] AWS Account: {identity.get('Account', 'unknown')}")
            print(f"[Flyte Serverless] AWS ARN: {identity.get('Arn', 'unknown')}")
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


def setup_environment():
    """Set up the working environment for serverless compute."""
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


def download_inputs(work_dir: str):
    """
    Download any required inputs or dependencies.
    This is called before executing the Flyte task.
    """
    # The pyflyte-fast-execute command handles downloading the distribution
    # and setting up the environment, so we don't need to do much here
    pass


def execute_flyte_command(args: list):
    """Execute the Flyte command with the provided arguments."""
    if not args:
        print("[Flyte Serverless] ERROR: No command provided", file=sys.stderr)
        sys.exit(1)
    
    # Log the command being executed
    cmd_str = " ".join(args)
    print(f"[Flyte Serverless] Executing: {cmd_str}")
    
    # Execute the command
    try:
        result = subprocess.run(
            args,
            check=False,
            env=os.environ.copy(),
        )
        return result.returncode
    except Exception as e:
        print(f"[Flyte Serverless] ERROR: Failed to execute command: {e}", file=sys.stderr)
        return 1


def main():
    """Main entrypoint for Flyte serverless tasks."""
    print("[Flyte Serverless] Starting serverless entrypoint...")
    print(f"[Flyte Serverless] Python version: {sys.version}")
    print(f"[Flyte Serverless] Arguments: {sys.argv[1:]}")
    
    # Configure AWS credentials from Databricks service credentials
    # This must happen BEFORE any S3 access attempts
    credentials_configured = setup_aws_credentials_from_databricks()
    if not credentials_configured:
        print("[Flyte Serverless] WARNING: Running without Databricks-managed AWS credentials")
    
    # Set up the working environment
    work_dir = setup_environment()
    
    # Download any required inputs
    download_inputs(work_dir)
    
    # Get command arguments (skip script name)
    args = sys.argv[1:]
    
    # Execute the Flyte command
    return_code = execute_flyte_command(args)
    
    print(f"[Flyte Serverless] Task completed with return code: {return_code}")
    sys.exit(return_code)


if __name__ == "__main__":
    main()

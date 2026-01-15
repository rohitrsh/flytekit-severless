#!/usr/bin/env python3
"""
Serverless-compatible entrypoint for Flyte Databricks tasks.

This entrypoint is designed to work with Databricks Serverless Compute,
which has restrictions on accessing certain directories like /root.

Key differences from the classic entrypoint:
- Uses /tmp as the working directory (accessible in serverless)
- Handles the restricted serverless environment
- Compatible with Spark Connect APIs

Usage in your Flyte task config:
    DatabricksV2(
        databricks_conf={
            "environment_key": "default",
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
    
    # Set up the environment
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


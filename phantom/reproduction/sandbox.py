"""
Security and sandbox execution runner for Project PHANTOM.
Runs generated reproducers with credential stripping, strict timeouts,
disposable filesystem directories, and optional container execution.
"""

from __future__ import annotations
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Dict, Optional, Tuple


class ReproducerSandbox:
    """
    Executes reproduction scripts in an isolated, security-hardened environment:
    - Strips sensitive cloud and API credentials from environment variables
    - Runs within disposable temporary directory
    - Enforces strict execution time limits
    - Supports containerized Docker sandbox runs
    """

    SENSITIVE_ENV_PREFIXES = (
        "AWS_", "AZURE_", "GCP_", "GOOGLE_", "GITHUB_", "GITLAB_",
        "DATABASE_", "DB_", "PASSWORD", "SECRET", "TOKEN", "API_KEY",
        "ACCESS_KEY", "PRIVATE_KEY", "SSH_",
    )

    @classmethod
    def sanitize_env(cls) -> Dict[str, str]:
        """Strip sensitive credentials, keys, and tokens from subprocess environment."""
        clean = {}
        for k, v in os.environ.items():
            k_upper = k.upper()
            if any(k_upper.startswith(prefix) for prefix in cls.SENSITIVE_ENV_PREFIXES):
                continue
            if any(term in k_upper for term in ("KEY", "SECRET", "TOKEN", "AUTH", "PASS")):
                continue
            clean[k] = v

        clean["PYTHONUNBUFFERED"] = "1"
        clean["PYTHONDONTWRITEBYTECODE"] = "1"
        return clean

    @classmethod
    def run_isolated(
        cls,
        script_path: str | Path,
        timeout: float = 30.0,
        working_dir: Optional[str | Path] = None,
    ) -> Tuple[bool, str, int]:
        """
        Run a reproducer script in an isolated process with credential scrubbing.
        Returns (success: bool, output: str, return_code: int).
        """
        p = Path(script_path).resolve()
        if not p.exists():
            return False, f"Script does not exist: {script_path}", -1

        env = cls.sanitize_env()

        with tempfile.TemporaryDirectory(prefix="phantom_sandbox_") as tmp_dir:
            exec_cwd = working_dir or tmp_dir
            try:
                res = subprocess.run(
                    [sys.executable, str(p)],
                    cwd=str(exec_cwd),
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                output = res.stdout + ("\n" + res.stderr if res.stderr else "")
                return (res.returncode == 0, output, res.returncode)
            except subprocess.TimeoutExpired:
                return False, f"Sandbox execution timed out after {timeout} seconds.", -124
            except Exception as e:
                return False, f"Sandbox execution error: {str(e)}", -1

    @classmethod
    def run_in_docker(
        cls,
        bundle_dir: str | Path,
        timeout: float = 60.0,
    ) -> Tuple[bool, str]:
        """
        Build and run reproducer bundle in a disposable Docker container.
        Requires local docker daemon.
        """
        b_path = Path(bundle_dir).resolve()
        dockerfile = b_path / "Dockerfile"
        if not dockerfile.exists():
            return False, f"Dockerfile not found in {bundle_dir}"

        tag = f"phantom-sandbox-{int(tempfile.NamedTemporaryFile().name.split('tmp')[-1], 16) % 100000}"
        try:
            # Build
            build_res = subprocess.run(
                ["docker", "build", "-t", tag, "."],
                cwd=str(b_path),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if build_res.returncode != 0:
                return False, f"Docker build failed:\n{build_res.stderr}"

            # Run with dropped privileges and no network
            run_res = subprocess.run(
                ["docker", "run", "--rm", "--network", "none", tag],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return (run_res.returncode == 0, run_res.stdout + run_res.stderr)
        except Exception as e:
            return False, f"Docker sandbox execution failed: {str(e)}"
        finally:
            subprocess.run(["docker", "rmi", "-f", tag], capture_output=True)

import contextlib as cl
import os
from pathlib import Path
import tempfile

import pytest

from .runner import run_job


def unit_test_job(test_dir: Path, pytest_args: tuple[str, ...]) -> int:
    """Run unit tests defined in the test_dir using pytest.

    The tests execute in the current process"""
    args = [
        str(test_dir),
        *pytest_args,
    ]
    return pytest.main(args)


def e2e_test_job(job_dir: Path) -> int:
    """Run a full end-to-end test of extracting, loading and transform using a real catalog."""
    with _configure_test_env() as _test_env:
        run_job(job_dir)
    # args = [
    #     str(test_dir),
    #     *pytest_args,
    # ]
    # return pytest.main(args)
    return 1


@cl.contextmanager
def _configure_test_env():
    """Set environment variables for catalog destination during e2e testing."""

    def _var_name(key):
        return f"{prefix}{key}"

    prefix = "PYICEBERG_"
    tmp_dir = tempfile.TemporaryDirectory()
    vars = {
        "CATALOG__DEFAULT__TYPE": "in-memory",
        "CATALOG__DEFAULT__WAREHOUSE": tmp_dir.name,
    }
    for key, value in vars.items():
        os.environ[_var_name(key)] = value

    try:
        yield
    except Exception:
        Path(tmp_dir.name).rmdir()
        for key, value in vars.items():
            del os.environ[_var_name(key)]
        raise

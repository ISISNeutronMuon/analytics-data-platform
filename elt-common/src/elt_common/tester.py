from pathlib import Path

import pytest


def unit_test_job(test_dir: Path, pytest_args: tuple[str, ...]) -> int:
    """Run tests defined in the test_dir."""
    args = [
        str(test_dir),
        *pytest_args,
    ]
    return pytest.main(args)

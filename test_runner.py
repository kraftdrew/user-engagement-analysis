import os
import shutil
import sys
import warnings
from pathlib import Path

import pytest

# Suppress interactive shell exit warning
warnings.filterwarnings("ignore", category=UserWarning, module="IPython")

# Setup paths
try:
    repo_root = Path(__file__).resolve().parent
except NameError:
    repo_root = Path.cwd()

tests_dir = repo_root / "tests"
dst_dir = Path("/tmp/tests")
if dst_dir.exists():
    shutil.rmtree(dst_dir)
shutil.copytree(tests_dir, dst_dir)

# Run pytest without exiting the interpreter
try:
    exit_code = pytest.main([
        str(dst_dir),
        "-v", "-rP",
        "-o", "cache_dir=/tmp/pytest_cache",
        "-W", "ignore::DeprecationWarning",
    ])
except SystemExit as e:
    exit_code = e.code

print("OK. Pytest finished with exit code:", exit_code)
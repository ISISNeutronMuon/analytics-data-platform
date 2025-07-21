#!/bin/bash
# Assume uv is available.
# Create a local virtual environment, install build requirements and build
set -euo pipefail

if [ $# = 0 ]; then
  echo "Usage: $0 <build_dir> [...other arguments passed to mkdocs build]"
  exit 1
fi

if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "Ignoring \`VIRTUAL_ENV=$VIRTUAL_ENV\`. Falling back on uv-discovered environments"
  unset VIRTUAL_ENV
fi

# Constants
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VENV_DIR=.venv
DEFAULT_BUILD_ARGS="--clean"

# Argument handling
build_dir=$1
shift 1
project_root=$SCRIPT_DIR
user_build_args=$*

# Create and configure environment
pushd $project_root
uv venv $VENV_DIR
uv pip install -r requirements.txt

# Build
uv run mkdocs build $DEFAULT_BUILD_ARGS --site-dir $build_dir
popd

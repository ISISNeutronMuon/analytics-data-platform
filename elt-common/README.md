# elt-common

A thin package around [`dlt`](https://dlthub.com) to provide functionality common
to each pipeline.

## Development setup

Development requires the following tools:

- [uv](https://docs.astral.uv/uv/): Used to manage both Python installations and dependencies

### Setting up a Python virtual environment

Once `uv` is installed, create an environment and install the `elt-common`
package in editable mode, along with the development dependencies:

```bash
> uv venv
> source .venv/bin/activate
> uv pip install --editable . --group dev
```

## Running the tests

Run the unit tests using `pytest`:

```bash
> pytest tests
```

See ["How to invoke pytest"](https://docs.pytest.org/en/stable/how-to/usage.html#usage) fo
instructions on how to limit the tests that are executed.

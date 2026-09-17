# Contributing to AsAnyPath

Thank you for contributing to AsAnyPath.

## Getting Started

Use Python 3.10 or newer and install the development dependencies:

```bash
uv sync --extra dev
uv pip install --reinstall --no-cache rust/
```

Optionally install the repository's pre-commit hooks to run formatting and
linting checks before each commit:

```bash
uv run pre-commit install
```

Run the focused tests for your change, then the full suite before opening a pull
request:

```bash
uv run --extra dev pytest -vx tests/
uvx ruff check --fix
uvx ruff format
cargo check --manifest-path rust/Cargo.toml
```

## Pull Requests

Keep pull requests focused and include tests for changed behavior. Preserve
compatibility with Python 3.10 through 3.14. Describe user-visible changes and
document new configuration, credentials, or backend behavior in the README.

Do not include credentials, private endpoints, customer data, or other
confidential information in issues, commits, tests, or pull requests. Report
security issues privately as described in [SECURITY.md](SECURITY.md).

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE).
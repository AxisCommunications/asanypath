# Copilot Skill: Async Python Library Development

## Purpose
Guide development of async Python libraries with clean APIs, proper typing, and thorough testing.

## Technical Stack
- **Python**: 3.10+ with full async/await
- **Package Manager**: uv (sync, run, build)
- **Build**: hatchling
- **Lint/Format**: ruff
- **Type Checking**: mypy with strict settings
- **Testing**: pytest + pytest-asyncio (asyncio_mode = "auto")

## Development Commands
- Install: `uv sync --all-extras`
- Test: `uv run pytest tests/ -x -q`
- Lint: `uvx ruff check --fix && uvx ruff format`
- Type check: `uv run mypy src/`
- Build wheel: `uv build --wheel`

## Code Conventions
- All I/O operations must be async (`async def`)
- Use `anyio` over raw `asyncio` where available for backend-agnostic code
- Type every function signature — no `Any` unless unavoidable
- Public API lives in `__init__.py` via `__all__`
- Use `src/` layout (`src/<package>/`)
- Exceptions go in `exceptions.py`, inherit from a single base error

## Async Patterns
- Never call blocking I/O in async functions — use `anyio.to_thread.run_sync()` if needed
- Prefer `async with` for resource management (connections, sessions, file handles)
- Use `asynccontextmanager` for custom async context managers
- Avoid `asyncio.get_event_loop()` — let the framework manage the loop

## Testing
- All async tests use bare `async def test_*()` (pytest-asyncio auto mode)
- Use fixtures for shared setup (`@pytest.fixture`)
- Mock external services (S3, DB) — never hit real infra in unit tests
- Use `tmp_path` for filesystem tests
- Run with `-x -q` for fast feedback, `-v` for debugging

## Library Design
- Keep dependencies minimal — use optional extras for heavy backends
- Expose a clean `__all__` — implementation details stay private
- Prefer composition over inheritance for mixins
- Document breaking changes in commit messages

## What NOT to Do
- Don't add sync wrappers unless the user asks
- Don't add CLI features unless the user asks
- Don't change `__all__` exports without discussing
- Don't add new dependencies without mentioning it

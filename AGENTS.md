# Repository Guidelines

This repository currently contains no source files or build configuration. The guidance below is a starting point for contributors; update the sections once the project structure and tooling are added.

## Project Structure & Module Organization

- **Root**: Keep the primary application entry point in the repository root (e.g., `app.py`) and document it here.
- **Source**: Place reusable modules in a dedicated directory such as `src/`.
- **Tests**: Keep tests in `tests/` and mirror the source layout (e.g., `tests/feature/test_widget.py`).
- **Assets**: Store static assets in `assets/` or `public/` and reference paths consistently.

## Build, Test, and Development Commands

Add a single, authoritative set of commands in this section once tooling exists. Example patterns to adopt:

- `python -m venv .venv` and `source .venv/bin/activate` — local virtual environment.
- `pip install -r requirements.txt` — install dependencies.
- `streamlit run app.py` — run the app locally.
- `pytest` — run tests.

## Coding Style & Naming Conventions

- **Python**: Use 4-space indentation and type hints where feasible.
- **Formatting**: Prefer `black` and `isort` with default configs unless the repo defines alternatives.
- **Naming**: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.

## Testing Guidelines

- Use `pytest` when tests are introduced.
- Name tests `test_*.py` and functions `test_*`.
- Keep fast unit tests separate from slower integration tests (e.g., `tests/unit/` vs `tests/integration/`).

## Commit & Pull Request Guidelines

- **Commits**: Use short, imperative messages (e.g., “Add form validation”).
- **PRs**: Include a clear description, link relevant issues, and add screenshots for UI changes.
- **Quality**: Ensure tests pass and formatting is clean before requesting review.

## Security & Configuration Tips

- Store secrets in environment variables or a `.env` file (never commit secrets).
- Document required configuration keys in `README.md` once defined.

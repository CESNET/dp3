# Repository Guidelines

## Project Structure & Module Organization
- `dp3/`: core Python package (processing engine, API, CLI utilities).
- `modules/`: example app-specific modules/plugins.
- `config/`: example/default configuration.
- `install/` and `docker/`: deployment and container setup.
- `tests/`: unit/integration tests (`tests/test_common`, `tests/test_api`, `tests/test_config`).
- `docs/` and `mkdocs.yml`: documentation sources.

## Build, Test, and Development Commands
- `python -m venv venv && source venv/bin/activate`: local virtualenv.
- `pip install --editable ".[dev]"`: install for platform development.
- `pre-commit install`: enable formatting/linting hooks.
- `docker compose up -d --build`: start MongoDB, RabbitMQ, Redis for local runs/tests.
- `APP_NAME=my_app CONF_DIR=config dp3 api`: run API process.
- `dp3 worker my_app config 0`: run a worker process.
- `dp3 check <config_dir>`: validate configuration with detailed errors.

## Documentation
- When adding or changing user-visible features, configuration, CLI behavior, deployment flow, or extension points, update the relevant docs in `docs/` and `mkdocs.yml` as part of the same change.
- Prefer keeping how-to guides, concept pages, and configuration reference pages aligned with code changes.

## Coding Style & Naming Conventions
- Formatting: Black with 100-char line length.
- Linting: Ruff (with auto-fix in pre-commit).
- Keep module/function names descriptive; prefer `snake_case` for Python identifiers.
- Follow existing package layout under `dp3/` and test naming under `tests/`.

## Testing Guidelines
- Framework: `unittest`.
- Common tests:  
  `python -m unittest discover -s tests/test_common -v`
- API tests (with config):  
  `CONF_DIR=tests/test_config python -m unittest discover -s tests/test_api -v`
- Add tests next to related area (`tests/test_common`, `tests/test_api`, or `tests/modules`).

## Commit & Pull Request Guidelines
- Commit messages use concise scope + summary, often `Scope: message`, e.g.  
  `Docs: update callback registrar docstring` or `DB: fix unpacking of fulltext filters`.
- Use `Breaking` or similar marker when removing/deprecating behavior, e.g.  
  `API - Breaking: Removed deprecated parameter.`
- PRs should include: summary, rationale, test evidence, linked issues/PRs when relevant, and a note on documentation updates when user-visible behavior changed.

## Configuration & Services
- Local runtime depends on MongoDB, RabbitMQ, and Redis (provided via `docker-compose.yml`).
- Use `CONF_DIR` to point to config sets (e.g. `tests/test_config` for test runs).

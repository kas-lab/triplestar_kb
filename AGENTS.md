# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Detailed user documentation lives under `docs/docs/`; keep root and package READMEs as short entry points rather than duplicating reference material.
- Build documentation from `docs/` with `uv sync --locked --all-groups` and `uv run --locked zensical build --clean --strict`.
- Treat `triplestar_core/triplestar_core/config.py` and `triplestar_bringup/bringup_template/` as the authorities for documented configuration and generated package structure.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.

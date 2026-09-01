# TriplestarKB documentation

The Markdown under [`docs/`](docs/) is the canonical source for detailed user documentation. Keep the repository and package READMEs as short entry points that link here rather than copying reference material.

Implementation details must be checked against the ROS packages and the generated bringup template in the repository. In particular, configuration fields are defined in `triplestar_core/triplestar_core/config.py`, and the starter layout lives under `triplestar_bringup/bringup_template/`.

## Build locally

The site is managed by [uv](https://docs.astral.sh/uv/) and built by [Zensical](https://zensical.org/):

```bash
cd docs
uv sync --locked --all-groups
uv run --locked zensical build --clean --strict
```

The static output is written to `docs/site/`. To preview changes while editing:

```bash
uv run --locked zensical serve
```

Commit `pyproject.toml` and `uv.lock` together whenever dependencies change. Do not replace the locked workflow with an unpinned `pip install` command.

## GitHub Pages

[`.github/workflows/docs.yml`](../.github/workflows/docs.yml) runs the strict build for documentation pull requests. A push to `main` also uploads the generated site and deploys it with GitHub Pages.

A repository administrator must make this one-time selection before the first deployment:

1. Open **Settings > Pages** in `kas-lab/triplestar_kb`.
2. Under **Build and deployment**, set **Source** to **GitHub Actions**.

No separate hosting provider or deployment secret is required. The configured site URL is <https://kas-lab.github.io/triplestar_kb/>.

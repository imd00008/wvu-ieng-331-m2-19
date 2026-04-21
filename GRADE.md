# Milestone 2 Grade — **Team 19**

**Members:** Ian Donnen, Audrey Doyle, Rylee Lindermuth
**Repo:** imd00008/wvu-ieng-331-m2-19
**Total: 21 / 24**

---

## Scores

| Category              | Score | Max |
|-----------------------|-------|-----|
| Pipeline Functionality | 5     | 6   |
| Parameterization      | 4     | 6   |
| Code Quality          | 6     | 6   |
| Project Structure     | 3     | 3   |
| DESIGN.md             | 3     | 3   |
| **Total**             | **21**| **24** |

---

## Pipeline Functionality — 5/6

Default run succeeded, producing all three outputs (`summary.csv`, `detail.parquet`, `chart.html`). The holdout run with the extended database also completed successfully with all three outputs. The `--seller-state` parameter works end-to-end, filtering results via the SQL `$1` placeholder. However, `--start-date` is parsed by argparse and logged, but it is never passed to `get_seller_performance_scorecard()` or any other query in `main()` — the argument has no effect on output. The `queries.py` module defines `start_date`-aware functions (`get_delivery_time_by_geography`, `get_cohort_retention`, etc.) but none of them are called by the pipeline orchestrator.

---

## Parameterization — 4/6

`--seller-state` is fully parameterized: argparse → `get_seller_performance_scorecard(db_path, state=args.seller_state)` → `_execute_query` → `$1` SQL placeholder. DuckDB handles the safe substitution correctly. Input validation is present (2-character length check raises `ValueError`).

`--start-date` is declared and advertised in the README and help text, but `args.start_date` is never forwarded to any query call — the pipeline only calls `get_seller_performance_scorecard(db_path, state=args.seller_state)`, with no date argument. Running with and without `--start-date 2017-01-01` produces identical output.

---

## Code Quality — 6/6

Excellent across all dimensions:

- **Type hints:** Present on all functions in `pipeline.py`, `queries.py`, and `validation.py`.
- **Docstrings:** Well-written with `Args` and `Returns` sections on every public function.
- **Loguru:** Used consistently throughout — `INFO`, `SUCCESS`, `WARNING`, and `ERROR` levels all appear appropriately.
- **Pathlib:** `Path` used in `queries.py` for SQL file resolution and in `pipeline.py` for output directory management.
- **Specific exceptions:** `FileNotFoundError`, `duckdb.Error`, `ValueError`, and `OSError` are all caught with meaningful messages. The use of a final `except Exception` catch-all is acceptable given the specific handlers above it.

---

## Project Structure — 3/3

Clean `src/` layout with proper package structure. Three well-separated modules (`pipeline.py`, `queries.py`, `validation.py`) with clear separation of concerns. Five SQL files live in `sql/`. `pyproject.toml` correctly defines `[project.scripts]`. `uv.lock` is present.

---

## DESIGN.md — 3/3

Five well-written sections: Parameter Flow, SQL Parameterization, Validation Logic, Error Handling, and Scaling & Adaptation. Each section explains the "why" behind design decisions with specific, accurate references to the actual code (e.g., `$1` placeholder, `SHOW TABLES`, `.fetchall()` memory concern, `cursor.pl()` as a scaling solution). The SQL injection rationale is particularly thoughtful.

---

## Notes

- The `--start-date` parameter is the main gap — it is architecturally present in `queries.py` but the pipeline never invokes the date-aware functions or passes the argument through. Wiring `args.start_date` into `get_seller_performance_scorecard` (or swapping to one of the other date-aware query functions) would have completed the second parameter.
- Validation correctly uses a soft-fail approach, which is well-documented in DESIGN.md.
- The `product_category_name_translation` table missing from the standard DB triggers a validation warning, which is handled gracefully without crashing.

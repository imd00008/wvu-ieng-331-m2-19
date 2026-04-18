# Design Rationale

## Parameter Flow

When the pipeline executes a parameterized report, the flow of variables follows a strict path from the orchestrator down to the database engine.

Taking the Seller Scorecard as an example, if we want to filter by the state of São Paulo (`SP`), the flow is as follows:
1. **The Orchestrator (`pipeline.py`):** Inside the `run_pipeline()` function, the target state is defined when the query function is called: `get_seller_performance_scorecard(db_path, state="SP")`.
2. **The Data Access Layer (`queries.py`):** The `get_seller_performance_scorecard` function receives the `state` argument. It wraps this parameter into a Python list and passes it to the internal execution engine: `_execute_query(db_path, "seller_performance_scorecard.sql", [state])`.
3. **The Execution Engine (`_execute_query`):** This helper function reads the raw SQL text from the file. It then executes the query by passing the SQL text and the parameter list directly to DuckDB: `conn.execute(query, params)`. DuckDB maps the first item in the `params` list (`"SP"`) to the `$1` placeholder in the SQL script.

## SQL Parameterization

Using `seller_performance_scorecard.sql` as our example:

* **Raw SQL Look:** The query contains a `WHERE` clause that looks like: `WHERE seller_state = $1`. The `$1` acts as a placeholder for the first parameter passed during execution.
* **Reading & Passing:** In `queries.py`, the `_execute_query` function uses the `pathlib` library to locate and read the `.sql` file as a raw text string (`sql_file_path.read_text()`). It then hands this string and a list of Python variables to `duckdb.execute()`. DuckDB handles safely injecting the variables into the `$1` slots at the C++ level.
* **Why parameterization over f-strings:** F-strings (`f"WHERE seller_state = '{state}'"`) pose a severe security risk known as SQL Injection. If a user inputs `'SP' OR 1=1`, an f-string would alter the query logic entirely. Parameterized queries treat inputs strictly as literal values, never as executable code. They also automatically handle annoying syntax issues like escaping single quotes in text.
* **Why `.sql` files:** Storing SQL inside Python strings makes the code cluttered and difficult to maintain. Keeping SQL in dedicated `.sql` files allows for syntax highlighting, better version control tracking, and allows data analysts to test the queries in external SQL IDEs without needing to read Python.

## Validation Logic

Our `validation.py` module runs a "Pre-Flight" check on the DuckDB database. Here is why each check exists:

* **Table Existence Check:** * *What it checks:* Uses `SHOW TABLES` to ensure all 9 Olist tables are present.
  * *Why:* Prevents the pipeline from crashing mid-execution due to a missing table reference.
* **Key Columns NOT NULL:**
  * *What it checks:* Ensures primary keys (like `order_id` and `product_id`) are not entirely empty.
  * *Why:* Relational joins and aggregations rely on these keys. If they are missing, our grouping logic breaks.
* **Date Range Validation:**
  * *What it checks:* Verifies `order_purchase_timestamp` is not empty and does not contain future dates.
  * *Why:* E-commerce reporting relies heavily on time-series analysis. Future dates indicate data corruption or parsing errors.
* **Volume Threshold (>1,000 rows):**
  * *What it checks:* Ensures core tables contain a statistically significant amount of data.
  * *Why 1,000:* Since Olist is a large dataset, a table with fewer than 1,000 rows highly implies the dataset was truncated or improperly downloaded.

**Failure Handling:** If any of these checks fail, the system does *not* halt. It uses `loguru` to print a yellow `WARNING` to the terminal. We chose non-halting behavior so that the user can still generate partial reports for debugging purposes, rather than being completely locked out of the pipeline.

## Error Handling

We implemented specific exception handling to ensure the pipeline fails gracefully.

* **Block 1: `FileNotFoundError` in `_execute_query`**
  * *What it catches:* Specifically looks for missing `.sql` files or a missing DuckDB database.
  * *What it does:* It raises a highly descriptive error (`raise FileNotFoundError(f"Database not found at: {db_path}")`) before DuckDB even attempts to connect. This tells the developer exactly which file path to fix.
* **Block 2: `duckdb.Error` in `_execute_query`**
  * *What it catches:* Catches database-level exceptions (e.g., syntax errors in the SQL, type mismatches).
  * *What it does:* It captures the raw DuckDB error, wraps it in our own string to note which specific `.sql` file caused the crash, and then raises it to the user.
* **The Danger of Bare `except:`** If we used a bare `except:` block, it would catch *everything*, including `KeyboardInterrupt`. This means if the program got stuck in an infinite loop, the user could not use `Ctrl+C` to stop it. It also swallows the actual error trace, turning a highly solvable bug into a silent failure where the program simply stops working without explanation.

## Scaling & Adaptation

1. **Handling 10 Million Orders:**
   If the dataset grew to 10 million orders, the first part of our pipeline to break or severely slow down would be the `_execute_query` function. Currently, we use `.fetchall()` to pull the DuckDB cursor into a plain Python list before handing it to Polars (to bypass the `pyarrow` dependency issue). With 10 million rows, generating a massive native Python list in RAM would cause an Out-Of-Memory (OOM) crash. To fix this, we would properly configure `pyarrow` and change the code to `cursor.pl()`, which streams the data natively and efficiently straight from DuckDB's columnar format into Polars.
2. **Adding a JSON API Response:**
   If we needed to output JSON alongside CSVs, we would only need to modify `pipeline.py`. Inside the `run_pipeline()` execution loop, right below the line `df.write_csv(save_path)`, we would add a new line: `df.write_json(output_dir / filename.replace('.csv', '.json'))`. Because the data is already cleanly extracted and validated as a Polars DataFrame, exporting to a new format requires zero changes to the SQL or Data Access Layer.

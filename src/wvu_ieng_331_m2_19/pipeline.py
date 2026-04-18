from pathlib import Path

from loguru import logger

# 2. Import your query functions
from .queries import (
    get_abc_classification,
    get_cohort_retention,  # Add or remove based on the exact queries you wrote
    get_data_audit,
    get_delivery_time_by_geography,
    get_seller_performance_scorecard,
)

# 1. Import your validation tools
from .validation import run_pre_flight_checks, validate_dataframe


def run_pipeline():
    logger.info("🚀 Starting IENG 331 Milestone 2 Pipeline...")

    # Setup paths
    db_path = "data/olist.duckdb"
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)  # Automatically creates the folder if it's missing

    # ==========================================
    # STEP 1: Pre-Flight Raw Material Check
    # ==========================================
    logger.info("🔍 Step 1: Running Database Pre-Flight Validation...")
    validation_passed = run_pre_flight_checks(db_path)

    if not validation_passed:
        logger.warning(
            "⚠️ Pipeline continuing despite validation warnings (Disclaimer: Data reports may be incomplete)."
        )

    # ==========================================
    # STEP 2: The Assembly Line (Tasks)
    # ==========================================
    logger.info("⚙️ Step 2: Executing Queries and Generating Reports...")

    # Format: (Function execution, filename, display label)
    tasks = [
        (get_abc_classification(db_path), "abc_classification.csv", "ABC Inventory"),
        (get_data_audit(db_path), "data_audit.csv", "Data Quality Audit"),
        (
            get_seller_performance_scorecard(db_path, state="SP"),
            "top_sellers_sp.csv",
            "Seller Scorecard (SP)",
        ),
        (
            get_delivery_time_by_geography(db_path),
            "delivery_performance.csv",
            "Regional Delivery Gaps",
        ),
        (get_cohort_retention(db_path), "cohort_retention.csv", "Cohort Retention"),
    ]

    # ==========================================
    # STEP 3: Quality Control & Output
    # ==========================================
    for df, filename, label in tasks:
        # Run the End-of-Line inspection (from validation.py)
        if validate_dataframe(df, label):
            # If it passes, save it to the output folder
            save_path = output_dir / filename
            df.write_csv(save_path)
            logger.success(f"💾 Saved {label} successfully to {save_path}")
        else:
            logger.error(f"❌ Skipped saving {label} due to validation failure.")

    logger.info(
        "🏁 Pipeline Execution Complete. Check the /output folder for your CSVs!"
    )


if __name__ == "__main__":
    run_pipeline()

"""
Run the full v2 pipeline locally for one country, end to end.

Klaviyo sync is DRY-RUN ONLY — it builds payloads and logs 3 samples but sends
nothing. Nothing is written to Klaviyo by this script.

Usage:
    python scripts/run_local.py            # US, today
    python scripts/run_local.py US 2026-06-29
"""
import sys
from datetime import date

from propensity import pipeline
from propensity.util import logger


def main():
    country  = sys.argv[1] if len(sys.argv) > 1 else "US"
    run_date = sys.argv[2] if len(sys.argv) > 2 else str(date.today())
    logger.info(f"=== v2 local run | country={country} run_date={run_date} ===")

    pipeline.freshness_gate(country)
    pipeline.build_features(country, run_date)
    pipeline.train_score(country, run_date)
    pipeline.analysis(country, run_date)
    pipeline.build_profiles(country, run_date)
    pipeline.sync_klaviyo(country, run_date, dry_run=True)   # DRY-RUN — sends nothing

    # verify is an optional audit (needs S3); never let it block the delivery path
    try:
        pipeline.verify(country, run_date)
    except Exception as e:
        logger.warning(f"verify skipped (non-fatal): {e}")

    logger.info("=== done. Artifacts in $PROPENSITY_ARTIFACTS_DIR, plots in $PROPENSITY_OUTPUT_DIR ===")


if __name__ == "__main__":
    main()

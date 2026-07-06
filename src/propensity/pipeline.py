"""
Task callables for the Airflow DAG — one function per granular step.

Each reads/writes artifacts via io.artifacts so steps run as independent Airflow
tasks (separate processes). The DAG (dags/propensity_dag.py) just wires these up;
all logic lives here and in the sub-packages.

`run_date` is Airflow's logical date ('{{ ds }}', YYYY-MM-DD).
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from propensity.config import params
from propensity.config.settings import klaviyo_api_key
from propensity.delivery.klaviyo import sync_profiles_to_klaviyo
from propensity.delivery.profiles import build_top3_profiles
from propensity.features.build import build_training_frame
from propensity.io import artifacts, freshness, read
from propensity.model.analysis import save_analysis
from propensity.model.train import model_stats, train_model
from propensity.model.verify import build_verification
from propensity.util import logger

OUTPUT_DIR = Path(os.getenv("PROPENSITY_OUTPUT_DIR", "/tmp/propensity_output"))

_FEATURES = "features.parquet"
_SCORED   = "scored.parquet"
_PROFILES = "profiles.parquet"


def _resolve_dates(run_date: str):
    """(start_date, end_date, train_end_date) from params, defaulting off run_date."""
    end = params.END_DATE or run_date
    if params.TRAIN_END_DATE:
        train_end = params.TRAIN_END_DATE
    else:
        train_end = (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    return params.START_DATE, end, train_end


# --- tasks --------------------------------------------------------------------

def freshness_gate(country: str):
    freshness.assert_fresh(country)


def build_features(country: str, run_date: str):
    start, end, _ = _resolve_dates(run_date)
    raw = read.get_data(country, start, end)
    df = build_training_frame(
        raw, start_date=start, end_date=end,
        lookback_days=params.LOOKBACK_WINDOW,
        label_window=params.LABEL_WINDOW,
        snapshot_freq=params.SNAPSHOT_FREQUENCY,
    )
    artifacts.save_df(df, country, run_date, _FEATURES)


def train_score(country: str, run_date: str):
    _, _, train_end = _resolve_dates(run_date)
    df = artifacts.load_df(country, run_date, _FEATURES)
    model, _, X_val, _, y_val, scored = train_model(df, train_end, params.LABEL_WINDOW)
    model_stats(model, X_val, y_val, params.STATS_THRESHOLD)
    artifacts.save_model(model, country, run_date)
    artifacts.save_df(scored, country, run_date, _SCORED)


def analysis(country: str, run_date: str):
    _, _, train_end = _resolve_dates(run_date)
    model = artifacts.load_model(country, run_date)
    scored = artifacts.load_df(country, run_date, _SCORED)
    save_analysis(model, scored, train_end, params.LABEL_WINDOW, OUTPUT_DIR / country / run_date, country)


def verify(country: str, run_date: str):
    scored = artifacts.load_df(country, run_date, _SCORED)
    df = build_verification(scored, country)
    artifacts.save_df(df, country, run_date, "verification.parquet")


def build_profiles(country: str, run_date: str):
    scored = artifacts.load_df(country, run_date, _SCORED)
    lookup = read.get_category_lookup(country)
    profiles = build_top3_profiles(scored, lookup)
    artifacts.save_df(profiles, country, run_date, _PROFILES)


def sync_klaviyo(country: str, run_date: str, dry_run=True):
    # dry_run may arrive as a string (Airflow Variable/param). Default to DRY unless explicitly false.
    dry = str(dry_run).strip().lower() not in ("false", "0", "no", "off")
    profiles = artifacts.load_df(country, run_date, _PROFILES)
    api_key = klaviyo_api_key(country)          # per-geo account (prod) / shared sandbox (dev)
    if not dry and not api_key:
        raise ValueError(f"No Klaviyo key for {country}: set KLAVIYO_API_KEY_{country.upper()} (ENV=prod)")
    result = sync_profiles_to_klaviyo(profiles, api_key, dry_run=dry)
    logger.info(f"Klaviyo sync (dry_run={dry}) result: {result}")

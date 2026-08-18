"""
Task callables for the Airflow DAG — one function per granular step.

Each reads/writes artifacts via io.artifacts so steps run as independent Airflow
tasks (separate processes). The DAG (dags/propensity_dag.py) just wires these up;
all logic lives here and in the sub-packages.

`run_date` is the run's as-of date (YYYY-MM-DD) — the latest completed day in the geo's
timezone, resolved once by the DAG's `resolve_dates` task and read from XCom.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from propensity.config import params
from propensity.config.dimensions import DIMENSION_KEYS, get_dimension
from propensity.config.settings import klaviyo_api_key
from propensity.delivery.banding import rank_and_band
from propensity.delivery.klaviyo import sync_profiles_to_klaviyo
from propensity.delivery.profiles import build_top3_profiles, build_v4_profiles
from propensity.delivery.sync_log import log_sync_to_redshift
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

# --- per-dimension artifact names (v4) ---
def _feat_name(dim_key):   return f"features_{dim_key}.parquet"
def _ranked_name(dim_key): return f"ranked_{dim_key}.parquet"
def _model_name(dim_key):  return f"model_{dim_key}.pkl"


def _resolve_dates(run_date: str):
    """(start, end, train_end, mature_cutoff) from params, defaulting off run_date.

    mature_cutoff = end - LABEL_WINDOW  -> the last snapshot date whose label is fully
    observed. Validation uses (train_end, mature_cutoff]; snapshots after mature_cutoff are
    still SCORED (for delivery) but excluded from train/val (immature labels).
    """
    end = params.END_DATE or run_date
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    mature_cutoff = (end_dt - timedelta(days=params.LABEL_WINDOW)).strftime("%Y-%m-%d")
    if params.TRAIN_END_DATE:
        train_end = params.TRAIN_END_DATE
    else:
        train_end = (end_dt - timedelta(days=params.LABEL_WINDOW + params.VALIDATION_WINDOW)).strftime("%Y-%m-%d")
    return params.START_DATE, end, train_end, mature_cutoff


# --- tasks --------------------------------------------------------------------

def freshness_gate(country: str):
    freshness.assert_fresh(country)


def build_features(country: str, run_date: str):
    start, end, _, _ = _resolve_dates(run_date)
    raw = read.get_data(country, start, end)
    df = build_training_frame(
        raw, start_date=start, end_date=end,
        lookback_days=params.LOOKBACK_WINDOW,
        label_window=params.LABEL_WINDOW,
        snapshot_freq=params.SNAPSHOT_FREQUENCY,
    )
    artifacts.save_df(df, country, run_date, _FEATURES)


def train_score(country: str, run_date: str):
    _, _, train_end, mature_cutoff = _resolve_dates(run_date)
    df = artifacts.load_df(country, run_date, _FEATURES)
    final_model, eval_model, X_val, y_val, scored = train_model(
        df, train_end, mature_cutoff, params.LABEL_WINDOW, params.REFIT_ON_ALL_MATURE)
    # metrics from eval_model (train-only) on the held-out mature val -> honest even when we refit
    model_stats(eval_model, X_val, y_val, params.STATS_THRESHOLD)
    artifacts.save_model(final_model, country, run_date)   # deploy the refit (or train-only) model
    artifacts.save_df(scored, country, run_date, _SCORED)


# ===========================================================================
# v4 generalized per-dimension tasks.
#
# One (build_features_dim -> train_score_dim) pair per dimension, all reusing the
# SAME build_training_frame / train_model as the legacy category path. Artifacts
# are namespaced by dimension key so the 7 flows never collide and a single failed
# dimension can be cleared + resumed without re-running the others.
#
# Klaviyo/subscriber frames are dimension-independent; build_shared_frames caches
# them once per run so we don't re-scan the 35M-row Klaviyo fact 7x.
# ===========================================================================
_SHARED_KLAVIYO    = "shared_klaviyo.parquet"
_SHARED_SUBSCRIBER = "shared_subscriber.parquet"


def build_shared_frames(country: str, run_date: str):
    """Read the dimension-independent Klaviyo + subscriber frames once for the whole run."""
    klaviyo, subscriber = read.get_shared_frames(country)
    artifacts.save_df(klaviyo, country, run_date, _SHARED_KLAVIYO)
    artifacts.save_df(subscriber, country, run_date, _SHARED_SUBSCRIBER)


def build_features_dim(country: str, run_date: str, dimension: str):
    """Read one dimension's behavior + build the snapshot feature frame (reuses shared frames)."""
    start, end, _, _ = _resolve_dates(run_date)
    dim = get_dimension(dimension)
    klaviyo = artifacts.load_df(country, run_date, _SHARED_KLAVIYO)
    subscriber = artifacts.load_df(country, run_date, _SHARED_SUBSCRIBER)
    raw = read.get_dimension_data(country, start, end, dim, klaviyo=klaviyo, subscriber=subscriber)
    df = build_training_frame(
        raw, start_date=start, end_date=end,
        lookback_days=params.LOOKBACK_WINDOW,
        label_window=params.LABEL_WINDOW,
        snapshot_freq=params.SNAPSHOT_FREQUENCY,
    )
    artifacts.save_df(df, country, run_date, _feat_name(dimension))


def train_score_dim(country: str, run_date: str, dimension: str):
    """Train CatBoost for one dimension, score every (email, value), keep top-N + bands."""
    _, _, train_end, mature_cutoff = _resolve_dates(run_date)
    dim = get_dimension(dimension)
    df = artifacts.load_df(country, run_date, _feat_name(dimension))
    final_model, eval_model, X_val, y_val, scored = train_model(
        df, train_end, mature_cutoff, params.LABEL_WINDOW, params.REFIT_ON_ALL_MATURE)
    stats = model_stats(eval_model, X_val, y_val, params.STATS_THRESHOLD)
    logger.info(f"[{dimension}] held-out AUC={stats['auc']:.4f} base_rate={stats['conversion_rate']*100:.2f}%")
    artifacts.save_model(final_model, country, run_date, _model_name(dimension))
    ranked = rank_and_band(scored, dim.top_n, dim_key=dimension)
    artifacts.save_df(ranked, country, run_date, _ranked_name(dimension))


def build_profiles_all_dims(country: str, run_date: str):
    """Fan-in: merge every dimension's ranked frame into one wide v4 payload.

    Runs only after ALL train_score_dim tasks succeed (strict all-or-nothing in the DAG), so
    every dimension's ranked_{dim}.parquet is present. The defensive load-guard keeps this
    robust to a manual partial re-run.
    """
    ranked_by_dim = {}
    for key in DIMENSION_KEYS:
        try:
            ranked_by_dim[key] = artifacts.load_df(country, run_date, _ranked_name(key))
        except FileNotFoundError:
            logger.warning(f"ranked artifact missing for '{key}' — dimension will be absent from payload")
    category_lookup = read.get_category_lookup(country)          # WS3: category -> stone/jewelry parts
    profiles = build_v4_profiles(ranked_by_dim, run_date, category_lookup=category_lookup)
    artifacts.save_df(profiles, country, run_date, _PROFILES)


def analysis(country: str, run_date: str):
    _, _, train_end, mature_cutoff = _resolve_dates(run_date)
    model = artifacts.load_model(country, run_date)
    scored = artifacts.load_df(country, run_date, _SCORED)
    save_analysis(model, scored, train_end, mature_cutoff, params.LABEL_WINDOW,
                  OUTPUT_DIR / country / run_date, country)


def verify(country: str, run_date: str):
    scored = artifacts.load_df(country, run_date, _SCORED)
    df = build_verification(scored, country, params.LABEL_WINDOW)
    artifacts.save_df(df, country, run_date, "verification.parquet")


def build_profiles(country: str, run_date: str):
    scored = artifacts.load_df(country, run_date, _SCORED)
    lookup = read.get_category_lookup(country)
    profiles = build_top3_profiles(scored, lookup, run_date)
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


def log_klaviyo_sync(country: str, run_date: str, dry_run=True):
    """Append this run's profiles + send metadata to propensity.klaviyo_sync_log (audit/debug)."""
    dry = str(dry_run).strip().lower() not in ("false", "0", "no", "off")
    profiles = artifacts.load_df(country, run_date, _PROFILES)
    result = log_sync_to_redshift(profiles, country, run_date, dry)
    logger.info(f"Klaviyo sync log (dry_run={dry}) result: {result}")

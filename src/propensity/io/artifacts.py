"""
Intermediate-artifact store for passing data between Airflow tasks.

Tasks are separate processes, so data is persisted here between them. Keyed by
(country, run_date) so concurrent/backfilled runs don't collide.

Local disk for now (LocalExecutor on one EC2 — all tasks share the box). Swap the
read/write bodies for S3 if you move to distributed workers; callers stay the same.
"""
import os
import pickle
from pathlib import Path

import pandas as pd

from propensity.util import logger

ARTIFACTS_DIR = Path(os.getenv("PROPENSITY_ARTIFACTS_DIR", "/tmp/propensity_artifacts"))


def _run_dir(country: str, run_date: str) -> Path:
    d = ARTIFACTS_DIR / country / run_date
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_df(df: pd.DataFrame, country: str, run_date: str, name: str) -> None:
    path = _run_dir(country, run_date) / name
    df.to_parquet(path, index=False)
    logger.info(f"Saved artifact {path} ({len(df):,} rows)")


def load_df(country: str, run_date: str, name: str) -> pd.DataFrame:
    path = ARTIFACTS_DIR / country / run_date / name
    df = pd.read_parquet(path)
    logger.info(f"Loaded artifact {path} ({len(df):,} rows)")
    return df


def save_model(model, country: str, run_date: str, name: str = "model.pkl") -> None:
    path = _run_dir(country, run_date) / name
    with open(path, "wb") as f:
        pickle.dump(model, f)
    logger.info(f"Saved model {path}")


def load_model(country: str, run_date: str, name: str = "model.pkl"):
    path = ARTIFACTS_DIR / country / run_date / name
    with open(path, "rb") as f:
        return pickle.load(f)


# --- "current" (deployed) models: NOT run-date-keyed, so the daily score path always loads the
#     latest weekly retrain. The weekly DAG writes here; the daily-score DAG reads here. ---
def _current_dir(country: str) -> Path:
    d = ARTIFACTS_DIR / "current" / country
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_current_model(model, country: str, name: str) -> None:
    path = _current_dir(country) / name
    with open(path, "wb") as f:
        pickle.dump(model, f)
    logger.info(f"Saved current (deployed) model {path}")


def load_current_model(country: str, name: str):
    path = ARTIFACTS_DIR / "current" / country / name
    with open(path, "rb") as f:
        return pickle.load(f)

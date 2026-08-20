"""
Feature engineering: turn the raw star-table frames into the labeled training set.

Ported from propensity_model2/modules/data_transformation.py
(build_snapshot_features / create_label / clean_features), with v2 changes:
  * Email engagement uses the SAME 30-day lookback as behavior (was lifetime-cumulative).
  * Date columns are converted to datetime ONCE up front (the old code re-converted the
    full frames inside every snapshot iteration).
  * Always reads `sessions` from the fact (the old session_id-vs-sessions fork is gone).
"""
from datetime import timedelta

import numpy as np
import pandas as pd

from propensity.io.read import RawData
from propensity.util import logger

# Single source of truth for the model layer to import.
FEATURE_COLS = [
    "propensity_category", "devices", "channels", "subscriber_channel",
    "days_since_last_active", "days_since_last_purchase", "days_since_subscribed", "is_subscribed",
    "email_opens", "email_clicks", "visits", "product_views", "cart_adds",
    "checkouts", "orders",
]
CATEGORICAL_COLS = ["propensity_category", "devices", "channels", "subscriber_channel"]


def label_col(label_window: int) -> str:
    return f"label_order_next_{label_window}d"


def _build_snapshot(snapshot_date, behavior, subscriber, klaviyo, lookback_days):
    """Aggregate the lookback window into one feature row per (email, category, device, channel)."""
    lookback_start = snapshot_date - timedelta(days=lookback_days)

    b = behavior[(behavior["date"] >= lookback_start) & (behavior["date"] < snapshot_date)]
    s = subscriber[(subscriber["subscription_date"] >= lookback_start) &
                   (subscriber["subscription_date"] < snapshot_date)]
    k = klaviyo[(klaviyo["date"] >= lookback_start) & (klaviyo["date"] < snapshot_date)]  # 30d window (v2)

    feat = (
        b.groupby(["email", "propensity_category", "devices", "channels"], as_index=False)
         .agg(visits=("sessions", "sum"),
              product_views=("product_views", "sum"),
              cart_adds=("cart_adds", "sum"),
              checkouts=("checkouts", "sum"),
              orders=("orders", "sum"))
    )

    # recency of ANY activity (within the lookback window)
    last = (b.groupby(["email", "propensity_category"], as_index=False)
              .agg(last_date=("date", "max")))
    last["days_since_last_active"] = (snapshot_date - last["last_date"]).dt.days
    feat = feat.merge(last[["email", "propensity_category", "days_since_last_active"]],
                      on=["email", "propensity_category"], how="left")

    # recency of PURCHASE (UNBOUNDED — last order in the category over ALL history before the
    # snapshot, not just the lookback). Separating "recently browsed" from "recently bought" lets
    # the model down-weight recent purchasers (who rarely re-buy the same category within 30 days).
    pw = behavior[(behavior["date"] < snapshot_date) & (behavior["orders"] > 0)]
    if len(pw):
        lastp = (pw.groupby(["email", "propensity_category"], as_index=False)
                   .agg(last_purchase_date=("date", "max")))
        lastp["days_since_last_purchase"] = (snapshot_date - lastp["last_purchase_date"]).dt.days
        feat = feat.merge(lastp[["email", "propensity_category", "days_since_last_purchase"]],
                          on=["email", "propensity_category"], how="left")
    else:
        feat["days_since_last_purchase"] = np.nan

    # subscription
    sub = (s.groupby("email", as_index=False)
             .agg(subscriber_channel=("subscriber_channel", "last"),
                  subscription_date=("subscription_date", "min"),
                  is_subscribed=("is_subscribed", "sum")))
    feat = feat.merge(sub, on="email", how="left")
    feat["days_since_subscribed"] = (snapshot_date - feat["subscription_date"]).dt.days

    # email engagement (30-day window, v2)
    eng = (k.groupby("email", as_index=False)
             .agg(email_opens=("email_opens", "sum"), email_clicks=("email_clicks", "sum")))
    feat = feat.merge(eng, on="email", how="left")

    feat["snapshot_date"] = snapshot_date
    return feat


def _add_label(behavior, snapshot_df, label_window):
    """label = 1 if (email, category) ordered within the next `label_window` days."""
    col = label_col(label_window)
    if snapshot_df.empty:
        return snapshot_df

    start = pd.Timestamp(snapshot_df["snapshot_date"].iloc[0])
    end = start + timedelta(days=label_window)
    win = behavior[(behavior["date"] >= start) & (behavior["date"] < end)]

    if win.empty:
        snapshot_df[col] = 0
        return snapshot_df

    lab = (win.groupby(["email", "propensity_category"])
              .agg(_ordered=("orders", lambda x: 1 if x.sum() > 0 else 0))
              .reset_index())
    snapshot_df = snapshot_df.merge(lab, on=["email", "propensity_category"], how="left")
    snapshot_df[col] = snapshot_df["_ordered"].fillna(0).astype(int)
    snapshot_df.drop(columns="_ordered", inplace=True)
    return snapshot_df


def _clean(df, label_window):
    col = label_col(label_window)
    out = df[[
        "snapshot_date", "email", "propensity_category", "devices", "channels",
        "subscriber_channel", "days_since_last_active", "days_since_last_purchase",
        "days_since_subscribed", "visits", "is_subscribed", "email_opens", "email_clicks",
        "product_views", "cart_adds", "checkouts", "orders", col,
    ]].copy()

    out["is_subscribed"]            = np.where(out["subscriber_channel"].notna(), 1, 0)
    out["subscriber_channel"]       = out["subscriber_channel"].fillna("non_subscriber")
    out["days_since_subscribed"]    = out["days_since_subscribed"].fillna(999)
    out["days_since_last_active"]   = out["days_since_last_active"].fillna(999)
    out["days_since_last_purchase"] = out["days_since_last_purchase"].fillna(9999)  # 9999 = never purchased
    out["email_opens"]            = out["email_opens"].fillna(0)
    out["email_clicks"]           = out["email_clicks"].fillna(0)
    out["devices"]                = out["devices"].fillna("unknown")
    out["channels"]               = out["channels"].fillna("unknown")
    out["snapshot_date"]          = pd.to_datetime(out["snapshot_date"])
    return out


def build_training_frame(raw: RawData, *, start_date, end_date,
                         lookback_days, label_window, snapshot_freq) -> pd.DataFrame:
    """Full feature pipeline: snapshots -> labels -> clean -> one training frame."""
    behavior   = raw.behavior.copy()
    subscriber = raw.subscriber.copy()
    klaviyo    = raw.klaviyo.copy()

    # Convert once (not per snapshot).
    behavior["date"]              = pd.to_datetime(behavior["date"])
    subscriber["subscription_date"] = pd.to_datetime(subscriber["subscription_date"])
    klaviyo["date"]               = pd.to_datetime(klaviyo["date"])

    # Weekly grid ANCHORED TO end_date (run_date) so the freshest snapshot is EXACTLY run_date.
    # (A start-anchored grid lands the last snapshot on a fixed weekly point up to ~1 period
    # stale, missing the most recent — highest-intent — activity.)
    _step_days = int("".join(ch for ch in snapshot_freq if ch.isdigit()) or "7")
    _n_snaps = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days // _step_days + 1
    snapshot_dates = pd.date_range(end=pd.Timestamp(end_date), periods=_n_snaps, freq=snapshot_freq)
    logger.info(f"Building {len(snapshot_dates)} snapshots "
                f"(lookback={lookback_days}d, label={label_window}d, freq={snapshot_freq})")

    frames = []
    for snap in snapshot_dates:
        sf = _build_snapshot(pd.Timestamp(snap), behavior, subscriber, klaviyo, lookback_days)
        sf = _add_label(behavior, sf, label_window)
        frames.append(sf)

    df = pd.concat(frames, ignore_index=True)
    df = _clean(df, label_window)

    dist = df[label_col(label_window)].value_counts().to_dict()
    logger.info(f"Features built: {df.shape}; label distribution: {dist}")
    return df


def build_scoring_frame(raw: RawData, *, as_of_date, lookback_days, label_window) -> pd.DataFrame:
    """Features for a SINGLE as-of snapshot, NO future labels — for daily scoring with a saved model.

    Reuses the exact `_build_snapshot` + `_clean` used in training (so there is no training/serving
    skew), but builds only the `as_of_date` snapshot instead of the full grid, and skips labeling
    (labels need future data and aren't needed to score). ~1/N the work of build_training_frame.
    """
    behavior   = raw.behavior.copy()
    subscriber = raw.subscriber.copy()
    klaviyo    = raw.klaviyo.copy()
    behavior["date"]                = pd.to_datetime(behavior["date"])
    subscriber["subscription_date"] = pd.to_datetime(subscriber["subscription_date"])
    klaviyo["date"]                 = pd.to_datetime(klaviyo["date"])

    sf = _build_snapshot(pd.Timestamp(as_of_date), behavior, subscriber, klaviyo, lookback_days)
    sf[label_col(label_window)] = 0            # dummy: unused for scoring, keeps _clean uniform
    df = _clean(sf, label_window)
    logger.info(f"Scoring frame built: {df.shape} (as-of {as_of_date})")
    return df

"""
Pipeline tunables (non-secret). Dates, windows, model version, geo list.

Kept separate from settings.py so secrets and run-parameters don't mix.
"""

# Geos to score. Add countries as warehouse + Klaviyo coverage expands.
COUNTRY_LIST = ["US"]

# Per-geo UTC offset (hours) used to derive that geo's calendar date from the run's
# logical timestamp — must match how reporting.dim_date.full_date buckets days per geo.
# NOTE: fixed offsets (no DST), matching the reporting layer. Add a geo when you add it.
GEO_UTC_OFFSET_HOURS = {"US": -8, "CA": -4}

# Data / training window
START_DATE         = "2024-01-01"   # snapshots + fact backfill start here
END_DATE           = None           # None -> today (resolved at runtime)
TRAIN_END_DATE     = None           # None -> today - 30d (resolved at runtime)

# Feature / label windows (days)
LOOKBACK_WINDOW    = 30             # aggregate behaviour over the prior N days
LABEL_WINDOW       = 30             # ordered within the next N days = positive label
SNAPSHOT_FREQUENCY = "7D"           # snapshot cadence
# Evaluation split: validation uses snapshots in (train_end, run_date - LABEL_WINDOW] so their
# labels are FULLY MATURE. train_end defaults to run_date - LABEL_WINDOW - VALIDATION_WINDOW.
VALIDATION_WINDOW  = 30             # width (days) of the mature validation window
# After early-stopping on the held-out val, refit the DEPLOYED model on ALL mature data
# (train + val) at the chosen iteration count. Metrics stay honest (reported from the
# train-only model on the held-out val). ~2x the fit time. Set False to skip the second fit.
REFIT_ON_ALL_MATURE = False   # deploy the train-only (eval) model — its honest held-out AUC is
                              # what we report, and it avoids a long uninterrupted second fit that
                              # Airflow's zombie-detection kills mid-run. Re-enable only with a
                              # higher scheduler heartbeat timeout.

# Training engine: "catboost" (default) or "lightgbm". Both are supported by
# model/train.py; switch here without touching any other code. CatBoost handles the
# categorical features (channel/subscriber_channel/device/category) far better and
# relies less on raw recency -> higher AUC and fewer recent purchasers in the top bands.
MODEL_TYPE = "catboost"

# Model version stamped onto Klaviyo profiles.
# v3 = CatBoost engine + `days_since_last_purchase` feature -> NOT comparable to v2 scores.
# v2 = LightGBM, star-schema source. v1 = raw-clickstream baseline.
MODEL_VERSION = "v4"   # per-dimension scoring (WS1/WS2); logs to klaviyo_sync_log_v4

# Evaluation: classification threshold for the confusion-matrix / precision / recall stats.
# Reporting knob only — does not affect scores sent to Klaviyo.
STATS_THRESHOLD = 0.9

# Klaviyo score bands: score >= high -> "high", >= medium -> "medium", else "low".
# Placeholder = v1 values; RETUNE after viewing the v2 score-distribution plot.
SCORE_BANDS = {"high": 0.61, "medium": 0.21}

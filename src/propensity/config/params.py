"""
Pipeline tunables (non-secret). Dates, windows, model version, geo list.

Kept separate from settings.py so secrets and run-parameters don't mix.
"""

# Geos to score. Add countries as warehouse + Klaviyo coverage expands.
COUNTRY_LIST = ["US"]

# Data / training window
START_DATE         = "2024-01-01"   # snapshots + fact backfill start here
END_DATE           = None           # None -> today (resolved at runtime)
TRAIN_END_DATE     = None           # None -> today - 30d (resolved at runtime)

# Feature / label windows (days)
LOOKBACK_WINDOW    = 30             # aggregate behaviour over the prior N days
LABEL_WINDOW       = 30             # ordered within the next N days = positive label
SNAPSHOT_FREQUENCY = "7D"           # snapshot cadence

# Model version stamped onto Klaviyo profiles.
# v2 = star-schema source; funnel features are flag-based (not raw counts) and
# channel/device come from the normalized dims -> NOT comparable to v1 scores.
MODEL_VERSION = "v2"

# Evaluation: classification threshold for the confusion-matrix / precision / recall stats.
# Reporting knob only — does not affect scores sent to Klaviyo.
STATS_THRESHOLD = 0.9

# Klaviyo score bands: score >= high -> "high", >= medium -> "medium", else "low".
# Placeholder = v1 values; RETUNE after viewing the v2 score-distribution plot.
SCORE_BANDS = {"high": 0.61, "medium": 0.21}

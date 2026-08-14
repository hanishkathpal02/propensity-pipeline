"""
Per-dimension ranking + banding.

Two jobs, done together because they share the "latest snapshot per (email, value)" reduction:
  1. rank  — keep each email's top-N values for the dimension (N from the registry).
  2. band  — label each delivered value high / medium / low.

Banding uses the SAME absolute score cutoffs as v3 (params.SCORE_BANDS: high >= 0.61,
medium >= 0.21, else low) for EVERY dimension. Decision (2026-08-14): keep score-based
cutoffs so a band means the same probability everywhere and category's medium/low mix stays
identical to what we send today. `_band` is imported from delivery.profiles so there is one
canonical definition — change the thresholds in one place (params.SCORE_BANDS).

A percentile mode is kept available (rank_and_band_percentile) but is NOT the default; it would
only be used if a specific dimension's scores rarely reach 0.61 and needs re-derived cutoffs.
"""
from propensity.delivery.profiles import _band
from propensity.util import logger

_KEYS = ["email", "propensity_category"]


def _latest_per_pair(scored, score_col):
    """Collapse the snapshot grid to one row per (email, value): the freshest snapshot's score."""
    return (scored.sort_values("snapshot_date", ascending=False)
                  .groupby(_KEYS, as_index=False).first()[_KEYS + [score_col]])


def _rank_top_n(latest, top_n, score_col):
    latest["rank"] = (latest.groupby("email")[score_col]
                            .rank(method="first", ascending=False).astype(int))
    return latest[latest["rank"] <= top_n].reset_index(drop=True)


def rank_and_band(scored, top_n: int, dim_key: str = "", score_col: str = "propensity_score"):
    """Each email's top-N (value, score, rank, band) for one dimension — absolute v3 cutoffs."""
    latest = _latest_per_pair(scored, score_col)
    if latest.empty:
        latest["rank"] = []
        latest["band"] = []
        return latest
    top = _rank_top_n(latest, top_n, score_col)
    top["band"] = top[score_col].apply(_band)
    dist = top["band"].value_counts().to_dict()
    logger.info(f"  band[{dim_key}] (absolute 0.61/0.21) delivered top-{top_n}: {dict(dist)}")
    return top


def rank_and_band_percentile(scored, top_n: int, dim_key: str = "",
                             score_col: str = "propensity_score",
                             high_pctl: float = 0.80, medium_pctl: float = 0.50):
    """Optional: band by percentile within the dimension's population (NOT the default)."""
    latest = _latest_per_pair(scored, score_col)
    if latest.empty:
        latest["rank"] = []
        latest["band"] = []
        return latest
    hi = float(latest[score_col].quantile(high_pctl))
    mid = float(latest[score_col].quantile(medium_pctl))
    top = _rank_top_n(latest, top_n, score_col)
    top["band"] = top[score_col].apply(lambda s: "high" if s >= hi else ("medium" if s >= mid else "low"))
    logger.info(f"  band[{dim_key}] (percentile) cutoffs high>={hi:.4f} medium>={mid:.4f}")
    return top

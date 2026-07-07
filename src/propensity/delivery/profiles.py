"""
Build the per-email top-3 propensity profile (wide format) for Klaviyo.

Ported from propensity_model2/modules/klaviyo_profile_sync.build_top3_profiles.
Bands come from config.params.SCORE_BANDS; model version from config.params.MODEL_VERSION.
"""
import hashlib
import re

import pandas as pd

from propensity.config.params import MODEL_VERSION, SCORE_BANDS
from propensity.util import logger

_HIGH = SCORE_BANDS["high"]
_MED  = SCORE_BANDS["medium"]
_DIMS = ["score", "band", "stone", "Jewelry", "Category"]

# strict-enough email check (TLD must be >=2 letters) applied AFTER safe normalization
_EMAIL_RE = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$")


def _band(score: float) -> str:
    if score >= _HIGH:
        return "high"
    if score >= _MED:
        return "medium"
    return "low"


def _normalize_email(e: str) -> str:
    """SAFE corrections only (no guessing): trim/lowercase, collapse '..', drop a dot before
    '@', and strip trailing junk after the (alphabetic) TLD, e.g. 'x@hotmail.com0' -> 'x@hotmail.com'."""
    e = str(e or "").strip().lower()
    e = re.sub(r"\.{2,}", ".", e)     # jerrie..mccain@ -> jerrie.mccain@
    e = re.sub(r"\.+@", "@", e)        # janebradbury24.@ -> janebradbury24@
    e = re.sub(r"[^a-z]+$", "", e)     # comcast.net3604 -> comcast.net  (TLDs end in letters)
    return e


def _is_valid_email(e: str) -> bool:
    """Valid after normalization? Unrecoverable cases (e.g. truncated TLD 'yahoo.c') -> False."""
    return bool(_EMAIL_RE.match(e or "")) and ".." not in e and ".@" not in e


def build_top3_profiles(df_features: pd.DataFrame, category_lookup: pd.DataFrame, run_date: str) -> pd.DataFrame:
    """One row per email: top-3 categories by most-recent score, wide format + metadata.

    prop_scored_at = run_date (the data as-of / latest-completed day), NOT the calendar day
    the job executed.
    """
    df = df_features.copy()
    df["email"] = df["email"].map(_normalize_email)   # safe corrections (trim/lower/dots/trailing-junk)
    total = df["email"].nunique()

    # most-recent snapshot score per (email, category)
    agg = (df.sort_values("snapshot_date", ascending=False)
             .groupby(["email", "propensity_category"], as_index=False)
             .first()[["email", "propensity_category", "propensity_score"]])

    agg["rank"] = (agg.groupby("email")["propensity_score"]
                      .rank(method="first", ascending=False).astype(int))
    top3 = agg[agg["rank"] <= 3].copy()

    top3 = top3.merge(
        category_lookup[["product_category", "stone_type", "jewelry_type"]],
        left_on="propensity_category", right_on="product_category", how="left",
    ).drop(columns="product_category")

    top3["band"] = top3["propensity_score"].apply(_band)
    top3["propensity_score"] = top3["propensity_score"].round(3)

    rows = []
    for email, grp in top3.groupby("email"):
        row = {"email": email}
        for _, r in grp.sort_values("rank").iterrows():
            rk = int(r["rank"])
            row[f"prop_{rk}_score"]    = r["propensity_score"]
            row[f"prop_{rk}_band"]     = r["band"]
            row[f"prop_{rk}_stone"]    = r["stone_type"]
            row[f"prop_{rk}_Jewelry"]  = r["jewelry_type"]
            row[f"prop_{rk}_Category"] = r["propensity_category"]
        rows.append(row)
    wide = pd.DataFrame(rows)

    # ensure all rank columns exist even when a customer has < 3 categories
    for rk in (1, 2, 3):
        for dim in _DIMS:
            col = f"prop_{rk}_{dim}"
            if col not in wide.columns:
                wide[col] = None

    wide["prop_model_version"] = MODEL_VERSION
    wide["prop_scored_at"]     = run_date          # data as-of date (latest completed day)

    col_order = (["email"]
                 + [f"prop_{rk}_{dim}" for rk in (1, 2, 3) for dim in _DIMS]
                 + ["prop_model_version", "prop_scored_at"])
    wide = wide[col_order]

    # deterministic 1..100 A/B bucket from the email hash
    wide["test_bucket"] = wide["email"].apply(
        lambda e: (int(hashlib.md5(e.encode()).hexdigest(), 16) % 100) + 1)

    # validity flag (email already safe-normalized above); only valid_email=True is synced
    wide["valid_email"] = wide["email"].map(_is_valid_email)
    n_valid = int(wide["valid_email"].sum())
    logger.info(f"Top-3 profiles built: {len(wide):,} of {total:,} scored emails "
                f"({n_valid:,} valid, {len(wide) - n_valid:,} invalid)")
    return wide

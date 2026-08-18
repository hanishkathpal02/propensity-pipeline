"""
Build the per-email top-3 propensity profile (wide format) for Klaviyo.

Ported from propensity_model2/modules/klaviyo_profile_sync.build_top3_profiles.
Bands come from config.params.SCORE_BANDS; model version from config.params.MODEL_VERSION.
"""
import hashlib
import re

import pandas as pd

from propensity.config.dimensions import DIMENSIONS
from propensity.config.params import MODEL_VERSION, SCORE_BANDS
from propensity.delivery import grammar
from propensity.util import logger

_HIGH = SCORE_BANDS["high"]
_MED  = SCORE_BANDS["medium"]
_DIMS = ["score", "band", "stone", "Jewelry", "Category"]

# v4 payload layout, derived from the registry so it can never drift from what's scored.
MULTI_DIMS  = [d for d in DIMENSIONS if d.top_n > 1]    # category, stone, jewelry, metal (top-3)
SINGLE_DIMS = [d for d in DIMENSIONS if d.top_n == 1]   # stone_origin, product_gender, price_range
_MAX_RANK   = max((d.top_n for d in DIMENSIONS), default=3)

# WS3 grammar: singular/plural display forms for these display attributes, at these ranks.
# (Field naming + rank scope to confirm with Email team; rank-1 is the default that drives
# subject lines.) Suppression core = a profile must have rank-1 for all of these + price.
GRAMMAR_DIMS  = ("category", "stone", "jewelry")
GRAMMAR_RANKS = (1, 2, 3)                      # spec: all six grammar fields, ranks 1-3
CORE_FIELDS   = [f"prop_1_{d.field}" for d in MULTI_DIMS] + ["prop_price_range"]


def payload_columns():
    """Ordered list of every v4 profile column (registry-derived; the one source of truth).

    Used by build_v4_profiles (to shape the frame), sync_log (LOG_COLS), and the sync-log DDL,
    so all three stay in lockstep — add a dimension to the registry and all three update.
    """
    cols = ["email"]
    for d in MULTI_DIMS:                                      # each value + its OWN score + band
        for rk in range(1, d.top_n + 1):
            cols += [f"prop_{rk}_{d.field}", f"prop_{rk}_{d.field}_score", f"prop_{rk}_{d.field}_band"]
    for d in SINGLE_DIMS:                                     # single-value dims: no rank suffix
        cols += [f"prop_{d.field}", f"prop_{d.field}_score", f"prop_{d.field}_band"]
    for rk in GRAMMAR_RANKS:                                  # WS3 singular/plural display forms
        for name in GRAMMAR_DIMS:
            cols += [f"prop_{rk}_{name}_singular", f"prop_{rk}_{name}_plural"]
    cols += ["prop_model_version", "prop_scored_at", "test_bucket", "valid_email"]
    return cols

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


# ===========================================================================
# v4 — merge every dimension's ranked+banded frame into ONE wide payload.
#
# Each dimension already carries its OWN score + band (rank_and_band in banding.py); this
# just pivots and outer-joins them on email. No cross-dimension inheritance: prop_1_metal is
# the metal model's #1 metal, independent of prop_1_Category. Category additionally populates
# the legacy prop_N_score/prop_N_band so existing email flows keep working unchanged.
# ===========================================================================
_SUFFIX = {"propensity_category": "", "propensity_score": "_score", "band": "_band"}


def _pivot_multi(ranked: pd.DataFrame, field: str) -> pd.DataFrame:
    """top-N ranked frame -> wide prop_{rank}_{field}[_score][_band], indexed by email."""
    r = ranked[["email", "rank", "propensity_category", "propensity_score", "band"]].copy()
    r["propensity_score"] = r["propensity_score"].round(3)
    # collapse any emails that merged under normalization to one row per (email, rank)
    r = r.sort_values("propensity_score", ascending=False).drop_duplicates(["email", "rank"])
    w = r.pivot(index="email", columns="rank",
                values=["propensity_category", "propensity_score", "band"])
    w.columns = [f"prop_{int(rk)}_{field}{_SUFFIX[val]}" for val, rk in w.columns]
    return w


def _pivot_single(ranked: pd.DataFrame, field: str) -> pd.DataFrame:
    """top-1 ranked frame -> wide prop_{field}[_score][_band], indexed by email."""
    r = (ranked[ranked["rank"] == 1]
         .sort_values("propensity_score", ascending=False)
         .drop_duplicates("email").set_index("email"))
    out = pd.DataFrame(index=r.index)
    out[f"prop_{field}"]       = r["propensity_category"]
    out[f"prop_{field}_score"] = r["propensity_score"].round(3)
    out[f"prop_{field}_band"]  = r["band"]
    return out


def _suppress_incomplete(wide: pd.DataFrame) -> pd.DataFrame:
    """Drop profiles missing any CORE attribute (a dim_product data gap) and log WHY, so the
    data team gets an exact list of catalog gaps to fix. Missing gender/stone_origin is allowed."""
    present = [c for c in CORE_FIELDS if c in wide.columns]
    miss = wide[present].isna().any(axis=1) if present else pd.Series(False, index=wide.index)
    n = int(miss.sum())
    if n:
        for c in present:
            k = int(wide.loc[miss, c].isna().sum())
            if k:
                logger.info(f"  suppress-reason: {k:,} profiles missing {c}")
    logger.info(f"suppressed {n:,} profiles missing a core attribute; {len(wide) - n:,} remain")
    return wide[~miss].reset_index(drop=True)


def _add_grammar_fields(wide: pd.DataFrame, category_lookup) -> None:
    """WS3: add singular/plural display forms for category/stone/jewelry (in-place)."""
    cmap = {}
    if category_lookup is not None and not category_lookup.empty:
        cmap = {r["product_category"]: (r["stone_type"], r["jewelry_type"])
                for _, r in category_lookup.iterrows()}

    def cat_forms(v):
        if pd.isna(v):
            return (None, None)
        st, jw = cmap.get(v, (None, None))
        return grammar.category_forms(st, jw) if st is not None else (v, v)  # fallback: raw
    fns = {"category": cat_forms,
           "stone":    lambda v: grammar.stone_forms(v) if pd.notna(v) else (None, None),
           "jewelry":  lambda v: grammar.jewelry_forms(v) if pd.notna(v) else (None, None)}
    for rk in GRAMMAR_RANKS:
        for name in GRAMMAR_DIMS:
            col = f"prop_{rk}_{name}"
            forms = wide[col].map(fns[name]) if col in wide.columns else pd.Series([(None, None)] * len(wide))
            wide[f"prop_{rk}_{name}_singular"] = forms.map(lambda t: t[0])
            wide[f"prop_{rk}_{name}_plural"]   = forms.map(lambda t: t[1])

    # WS3 spec: log unmapped jewelry types so the curated lookup can be extended
    jw_cols = [f"prop_{rk}_jewelry" for rk in GRAMMAR_RANKS if f"prop_{rk}_jewelry" in wide.columns]
    if jw_cols:
        seen = pd.unique(wide[jw_cols].values.ravel())
        unmapped = sorted({str(v) for v in seen if pd.notna(v) and grammar.unmapped_jewelry(v)})
        if unmapped:
            logger.warning(f"WS3 unmapped jewelry types ({len(unmapped)}) — extend JEWELRY_FORMS: {unmapped}")


def build_v4_profiles(ranked_by_dim: dict, run_date: str, category_lookup=None) -> pd.DataFrame:
    """One row per email across all dimensions. ranked_by_dim: {dim_key: ranked DataFrame}."""
    frames = []
    for d in MULTI_DIMS + SINGLE_DIMS:
        rk = ranked_by_dim.get(d.key)
        if rk is None or rk.empty:
            logger.warning(f"  no ranked rows for '{d.key}' — its columns will be absent")
            continue
        rk = rk.copy()
        rk["email"] = rk["email"].map(_normalize_email)      # same safe corrections as v3
        pivot = _pivot_multi(rk, d.field) if d.top_n > 1 else _pivot_single(rk, d.field)
        frames.append(pivot)

    wide = pd.concat(frames, axis=1).reset_index().rename(columns={"index": "email"})
    wide = _suppress_incomplete(wide)          # drop dim_product-incomplete profiles (core-5)
    _add_grammar_fields(wide, category_lookup)  # WS3 singular/plural display forms

    wide["prop_model_version"] = MODEL_VERSION
    wide["prop_scored_at"]     = run_date
    wide["test_bucket"] = wide["email"].apply(
        lambda e: (int(hashlib.md5(e.encode()).hexdigest(), 16) % 100) + 1)
    wide["valid_email"] = wide["email"].map(_is_valid_email)

    for c in payload_columns():                              # guarantee full, stable column set
        if c not in wide.columns:
            wide[c] = None
    wide = wide[payload_columns()]

    n_valid = int(wide["valid_email"].sum())
    logger.info(f"v4 profiles built: {len(wide):,} emails across {len(frames)} dimensions "
                f"({n_valid:,} valid)")
    return wide

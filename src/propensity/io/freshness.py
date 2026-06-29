"""Freshness gate — refuse to score on stale tables.

Run before the model reads anything. Asserts each warehouse table has data
recent enough; raises StaleDataError otherwise (fail loud, don't score).
"""
from datetime import date, timedelta

from propensity.io.redshift import fetch_df
from propensity.util import logger


class StaleDataError(RuntimeError):
    pass


# (table, date column to check, whether it's country-scoped)
_CHECKS = [
    ("profiles.dim_visitor_email_map",      "etl_loaded_at::date",  False),  # full refresh daily
    ("propensity.fact_propensity_category", "date",                 True),
    ("propensity.fact_subscription",        "subscription_date",    True),
    ("propensity.fact_klaviyo_engagements", "date",                 True),
]


def assert_fresh(country: str = "US", max_lag_days: int = 2) -> None:
    """Raise StaleDataError if any table's latest date is older than today - max_lag_days."""
    cutoff = date.today() - timedelta(days=max_lag_days)
    logger.info(f"Freshness check (country={country}, cutoff={cutoff})")

    stale = []
    for table, datecol, scoped in _CHECKS:
        where = f"WHERE country = '{country}'" if scoped else ""
        max_dt = fetch_df(f"SELECT MAX({datecol}) AS max_dt FROM {table} {where}")["max_dt"].iloc[0]

        if max_dt is None:
            stale.append(f"{table}: empty")
        else:
            if hasattr(max_dt, "date"):       # datetime -> date
                max_dt = max_dt.date()
            status = "OK" if max_dt >= cutoff else "STALE"
            logger.info(f"  {table}: max={max_dt} [{status}]")
            if max_dt < cutoff:
                stale.append(f"{table}: max={max_dt} < cutoff {cutoff}")

    if stale:
        raise StaleDataError("Stale/empty tables — refusing to score:\n  " + "\n  ".join(stale))
    logger.info("Freshness check passed.")

"""
Read the star tables into the DataFrames the feature builder expects.

Email is resolved at READ time by joining the visitor-keyed facts to
profiles.dim_visitor_email_map (so the daily full-refresh of the map
retro-attributes newly-identified visitors). Only resolvable visitors/emails
come back — that's the filter that keeps pandas memory sane (e.g. the 35M-row
Klaviyo fact is reduced to just the resolvable-email population).

Column names are aliased here to match what features/ expects:
    product_category -> propensity_category, device -> devices,
    channel -> channels, cart_additions -> cart_adds.
"""
from collections import namedtuple

from propensity.io.redshift import fetch_df
from propensity.util import logger

RawData = namedtuple("RawData", ["behavior", "klaviyo", "subscriber"])

# ---------------------------------------------------------------------------
# Behavioral fact -> email grain (sum across a person's multiple visitor_ids)
# ---------------------------------------------------------------------------
BEHAVIOR_QUERY = """
SELECT
    f.date,
    m.email,
    f.product_category      AS propensity_category,
    f.device                AS devices,
    f.channel               AS channels,
    SUM(f.sessions)         AS sessions,
    SUM(f.product_views)    AS product_views,
    SUM(f.cart_additions)   AS cart_adds,
    SUM(f.checkouts)        AS checkouts,
    SUM(f.orders)           AS orders
FROM propensity.fact_propensity_category f
JOIN profiles.dim_visitor_email_map m
  ON f.visitor_id = m.visitor_id
 AND f.country    = m.country
WHERE f.country = '{country}'
  AND f.date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY f.date, m.email, f.product_category, f.device, f.channel
"""

# ---------------------------------------------------------------------------
# Subscription fact -> email grain. No date filter (full history): the feature
# builder windows subscriptions to each snapshot's lookback itself.
# ---------------------------------------------------------------------------
SUBSCRIBER_QUERY = """
SELECT
    s.subscription_date,
    m.email,
    s.subscriber_channel,
    1 AS is_subscribed
FROM propensity.fact_subscription s
JOIN profiles.dim_visitor_email_map m
  ON s.visitor_id = m.visitor_id
 AND s.country    = m.country
WHERE s.country = '{country}'
GROUP BY s.subscription_date, m.email, s.subscriber_channel
"""

# ---------------------------------------------------------------------------
# Klaviyo fact -> filtered to the resolvable email population (shrinks 35M rows).
# Case-insensitive join because dim emails are raw-case and Klaviyo emails vary.
# ---------------------------------------------------------------------------
KLAVIYO_QUERY = """
SELECT
    k.date,
    k.email,
    k.email_opens,
    k.email_clicks
FROM propensity.fact_klaviyo_engagements k
JOIN (
    SELECT DISTINCT LOWER(email) AS email
    FROM profiles.dim_visitor_email_map
    WHERE country = '{country}'
) m ON LOWER(k.email) = m.email
WHERE k.country = '{country}'
"""

# ---------------------------------------------------------------------------
# Category -> stone/jewelry lookup (authoritative; used by delivery/top-3).
# ---------------------------------------------------------------------------
CATEGORY_LOOKUP_QUERY = """
SELECT DISTINCT product_category, stone_type, jewelry_type
FROM reporting.dim_product
WHERE country = '{country}'
  AND is_valid_product > 0
  AND product_category IS NOT NULL
  AND stone_type       IS NOT NULL
  AND jewelry_type     IS NOT NULL
"""


def get_data(country: str, start_date: str, end_date: str) -> RawData:
    """Load the three source frames for one country over [start_date, end_date]."""
    logger.info(f"Reading star tables for {country}: {start_date} -> {end_date}")

    behavior = fetch_df(BEHAVIOR_QUERY.format(country=country, start_date=start_date, end_date=end_date))
    logger.info(f"  behavior   : {behavior.shape}")

    klaviyo = fetch_df(KLAVIYO_QUERY.format(country=country))
    logger.info(f"  klaviyo    : {klaviyo.shape}")

    subscriber = fetch_df(SUBSCRIBER_QUERY.format(country=country))
    logger.info(f"  subscriber : {subscriber.shape}")

    return RawData(behavior=behavior, klaviyo=klaviyo, subscriber=subscriber)


def get_category_lookup(country: str):
    """category -> (stone_type, jewelry_type) from dim_product."""
    return fetch_df(CATEGORY_LOOKUP_QUERY.format(country=country))

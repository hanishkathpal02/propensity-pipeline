"""
v4 per-dimension registry — the single source of truth for what gets scored.

Every dimension goes through the SAME generalized CatBoost pipeline (read -> build ->
train -> score -> rank -> band); the only thing that varies is this config row. To add
a dimension: add a row here (and make sure its column exists on
propensity.fact_propensity_attributes). Nothing else in read/build/train changes.

Fields
------
key     : internal id (artifact namespacing, DAG task id, logs)
column  : the grain column on fact_propensity_attributes to score on
top_n   : how many ranked values to deliver per email (3 = multi-value, 1 = single-value)
field   : the name used in the Klaviyo profile property (prop_{rank}_{field} / prop_{field})
exclude : placeholder values filtered out at read time (kept verbatim in the fact)
"""
from collections import namedtuple

Dimension = namedtuple("Dimension", ["key", "column", "top_n", "field", "exclude"])

# non-informative placeholders present in dim_product; filtered per-dimension at read
PLACEHOLDERS = ("Not Available", "CWA")

DIMENSIONS = [
    #          key              column                 top_n  field             exclude
    Dimension("category",       "product_category",     3,    "category",       PLACEHOLDERS),
    Dimension("stone",          "stone_type",           3,    "stone",          PLACEHOLDERS),
    Dimension("jewelry",        "jewelry_type",         3,    "jewelry",        PLACEHOLDERS),
    Dimension("metal",          "metal_type",           3,    "metal",          PLACEHOLDERS),
    Dimension("stone_origin",   "lab_category",         1,    "stone_origin",   PLACEHOLDERS),
    Dimension("product_gender", "gender",               1,    "product_gender", PLACEHOLDERS),
    Dimension("price_range",    "product_price_range",  1,    "price_range",    PLACEHOLDERS),
]

DIMENSIONS_BY_KEY = {d.key: d for d in DIMENSIONS}
DIMENSION_KEYS = [d.key for d in DIMENSIONS]


def get_dimension(key: str) -> Dimension:
    """Look up one dimension by key (raises KeyError with the valid set on a typo)."""
    try:
        return DIMENSIONS_BY_KEY[key]
    except KeyError:
        raise KeyError(f"unknown dimension '{key}'; valid: {DIMENSION_KEYS}")

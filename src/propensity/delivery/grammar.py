"""
WS3 — deterministic singular/plural grammar for stone / Jewelry / Category (v4).

"Raw" rules: values are used verbatim (live casing) — NO lab-grown merge, NO Blue-Sapphire
rename (the email template still owns those). We only produce grammatical singular/plural.

- Jewelry: a CURATED lookup over the closed dim_product jewelry_type set (owns the irregulars:
  Earrings/Strands are stored PLURAL; Brooch -> Brooches; multi-word pluralizes the last word).
- Stone:   rule-based (pluralize the LAST word) + a tiny exceptions set. Stones are stored singular.
- Category: stone_singular + Jewelry_{singular|plural}.
- Fallback for an unmapped value: singular = plural = raw (never "Earringss"); caller logs the miss.
"""
import re

# --- Jewelry: curated closed-set lookup  {stored_value: (singular, plural)} ---
# Verified against reporting.dim_product (US). Stored value is the KEY (as it appears live).
JEWELRY_FORMS = {
    "Ring":            ("Ring", "Rings"),
    "Pendant":         ("Pendant", "Pendants"),
    "Earrings":        ("Earring", "Earrings"),          # stored PLURAL
    "Engagement Ring": ("Engagement Ring", "Engagement Rings"),
    "Bracelet":        ("Bracelet", "Bracelets"),
    "Necklace":        ("Necklace", "Necklaces"),
    "Band":            ("Band", "Bands"),
    "Loose Gemstone":  ("Loose Gemstone", "Loose Gemstones"),
    "Strands":         ("Strand", "Strands"),            # stored PLURAL
    "Cufflink":        ("Cufflink", "Cufflinks"),
    "Charm":           ("Charm", "Charms"),
    "Name Necklace":   ("Name Necklace", "Name Necklaces"),
    "Button cover":    ("Button cover", "Button covers"),
    "Brooch":          ("Brooch", "Brooches"),           # -ch -> -ches
    "Tiara":           ("Tiara", "Tiaras"),
    "Anklet":          ("Anklet", "Anklets"),
    "Lapel-Pin":       ("Lapel-Pin", "Lapel-Pins"),
    "Free Gift":       ("Free Gift", "Free Gift"),       # not jewelry -> leave raw
}

# Stones stored singular; a few that shouldn't be pluralized (placeholders / uncountable).
STONE_NO_PLURAL = {"Plain"}


def _pluralize_word(w: str) -> str:
    """English plural of a single word, preserving case of the stem."""
    if re.search(r"[^aeiouAEIOU]y$", w):          # consonant + y  -> -ies  (Ruby -> Rubies)
        return w[:-1] + "ies"
    if re.search(r"(s|x|z|ch|sh)$", w, re.I):      # sibilant       -> -es
        return w + "es"
    return w + "s"


def _pluralize_last_word(phrase: str) -> str:
    """Pluralize only the LAST word of a multi-word phrase (Blue Sapphire -> Blue Sapphires)."""
    parts = phrase.split(" ")
    parts[-1] = _pluralize_word(parts[-1])
    return " ".join(parts)


def stone_forms(stone):
    """(singular, plural) for a stored stone value — verbatim singular; rule-based plural."""
    if not stone:
        return stone, stone
    s = str(stone).strip()
    if s in STONE_NO_PLURAL:
        return s, s
    return s, _pluralize_last_word(s)


def jewelry_forms(jewelry):
    """(singular, plural) for a stored jewelry_type — curated lookup; fallback = last-word rule."""
    if not jewelry:
        return jewelry, jewelry
    j = str(jewelry).strip()
    if j in JEWELRY_FORMS:
        return JEWELRY_FORMS[j]
    # unmapped (new type): best-effort last-word plural; caller should log to extend the lookup
    return j, _pluralize_last_word(j)


def category_forms(stone, jewelry):
    """(singular, plural) for Category = stone(sing) + Jewelry(sing|plural) — the display form."""
    st_s, _ = stone_forms(stone)
    jw_s, jw_p = jewelry_forms(jewelry)
    if not st_s and not jw_s:
        return None, None
    sing = f"{st_s} {jw_s}".strip()
    plur = f"{st_s} {jw_p}".strip()
    return sing, plur


def unmapped_jewelry(jewelry) -> bool:
    """True if a jewelry value isn't in the curated lookup (so the caller can log/collect it)."""
    return bool(jewelry) and str(jewelry).strip() not in JEWELRY_FORMS

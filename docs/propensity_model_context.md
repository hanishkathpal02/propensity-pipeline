# Context: Propensity Scoring Model — A/B Test Findings & Improvement Directions

## Purpose of this document
Handoff context for improving the customer propensity scoring model used in Angara's US email program (Klaviyo US account). Summarizes what a July 2026 A/B test revealed about what the model does and does not contribute, and what the next model iteration should target.

---

## 1. Test setup (what was run)

- **Test window analyzed:** July 7–20, 2026, Klaviyo US account, ~190K delivered emails, 10 matched daily campaigns per arm.
- **Arms:**
  - **A_NormalCalendar** — regular/generic calendar emails.
  - **B_PropensityCalendar** — category-personalized emails, where the category shown is selected by the propensity model.
- **Audience structure:** Both arms split into identical propensity tiers via Klaviyo lists:
  `(Propensity_High | Propensity_Med | Propensity_Low)` × `(Subscriber | Customer | Cart)` audiences.
  Tier sizes per arm ≈ High ~4.7K, Med ~39K, Low ~50K delivered over the window.
- **Offer tracks:** 15% (Customer/Cart audiences) and 12% (Subscriber audiences).
- **Both arms receive the same campaigns on the same days to all three tiers** — the propensity score does NOT change cadence, frequency, offer depth, or suppression in either arm. In the matched sends, the ONLY treatment difference is generic creative (A) vs. category-personalized creative (B), where the category comes from the model.
- **Metrics note:** the live purchase metric in this account is **"Placed order" (metric ID TG8u2t)**. Two other metrics — "Placed Order" (YxaQcp) and "Order Placed" (UVZVgx) — return zero for these campaigns and should not be used. Added to Cart = RMzrKN (server-side, carries values).

## 2. Headline results (July 7–20)

| Metric (per delivered) | A_Normal | B_Propensity | Lift | Significance |
|---|---|---|---|---|
| Open rate | 29.21% | 29.45% | ~0 | flat |
| Unique click rate | 0.444% (417) | 0.574% (542) | **+29%** | z≈3.9, p<0.001 ✅ |
| Unique Add-to-Cart rate | 0.0373% (35) | 0.0392% (37) | +5% | not significant ❌ |
| ATC events | 89 | 121 | +35% | whale/outlier-skewed |
| Order rate ("Placed order") | 0.0074% (7 buyers, 8 orders, $19.8K) | 0.0074% (7 buyers, 9 orders, $16.5K) | 0% | dead heat |
| Click→cart conversion (unique adders / unique clickers) | 8.4% | **6.8%** | negative | directional |

**Funnel decay: +29% clicks → +5% carts → 0% orders.**

## 3. Tier-level results (both arms combined view)

Click rate by tier, pooled across the window:

| Tier | A click rate | B click rate | B lift |
|---|---|---|---|
| High | 1.43% | 1.90% | +33% |
| Med | 0.60% | 0.75% | +25% |
| Low | 0.23% | 0.31% | +37% |

- **Ranking power is excellent:** High clicks at ~5–6x the rate of Low in both arms. The model discriminates engagement propensity very well.
- **Open-rate quirk:** High tier opens LOWER (~22–24%) than Med/Low (~30%) while clicking far more — likely Apple MPP / image-blocking distortion. Opens are unreliable for High-intent users; clicks are the right engagement signal.
- Orders per tier are 2–4 per cell — pure noise at current volume.

## 4. Key analytical conclusions (agreed in review)

1. **The lift is a content-personalization effect, not a propensity-scoring effect.** The +29% click lift is uniform across all three tiers (a main effect, no tier×arm interaction). Since the score does not alter treatment (same cadence/offers for every tier in both arms), the model's only active role in B is *category selection for the creative*. That category could plausibly be derived by simpler means (last browsed/purchased category, rules).
2. **Current test design cannot measure the propensity model's incremental value.** The score is used as a stratification/reporting variable, not a decision variable. Nothing is treated differently because of the score, so its ROI is structurally unmeasurable in this design.
3. **The clicks B earns are lower-intent.** B's click→cart conversion (6.8%) is below A's (8.4%): personalized category creative earns "curiosity clicks" that don't proportionally deepen into cart/purchase intent. By the cart stage the arms are at parity; at the order stage, a dead heat.
4. **No commercial win proven yet.** Engagement win is real and significant; cart and order lifts are zero/insignificant. Sample sizes at cart (35–37 uniques/arm) and order (7 buyers/arm) levels are far too small; purchase cycles in fine jewelry are long, so downstream effects may lag.
5. **Caveats:** High tier is small (~4.7K/arm) so a real tier interaction could hide inside noise; only the matched/overlap sends were analyzed — if the two calendars differ on other days, cadence effects could contaminate; cart values are whale-skewed (single carts of $15K–$42K flip revenue comparisons).

## 5. What the model improvement should target

The model currently predicts *engagement propensity* well but that prediction (a) isn't acted on and (b) doesn't translate to purchase. Priorities:

1. **Shift the target variable from click/engagement propensity toward purchase propensity** (or a two-stage model: P(click) × P(purchase | click), or direct expected-revenue). Current scores rank clickers, not buyers — evidenced by orders being flat across tiers and one large purchase coming from a "Low" scored profile.
2. **Improve/validate category-affinity prediction quality**, since category selection is the model's only component with demonstrated value. Measure per-category hit rate: did the person click/cart/buy the predicted category? B's low click→cart conversion suggests category relevance drives clicks but may miss purchase-stage preferences (price band, metal, occasion).
3. **Add intent-depth features** to separate browsers from buyers: recency/frequency of PDP views, cart events, price-band affinity, session depth, discount sensitivity, time-since-last-purchase, AOV history. Goal: make High tier mean "likely to BUY," not "likely to CLICK."
4. **Make the score actionable, and design the next test to measure it.** Recommended next experiment: hold category-personalized content constant for everyone (it won on engagement), then A/B *score-driven treatment differences* — e.g., cadence/frequency by tier, offer depth by tier, or send-time — vs. uniform treatment. Ideally a 2×2 (content × score-driven-cadence) to isolate the model's incremental value. Until the score changes what someone receives, model improvements cannot show ROI.
5. **Recalibrate tier cutoffs / distribution.** High is only ~5% of the base (~4.7K of ~94K) — too small to power tier-level tests. Consider decile scores or continuous scores instead of 3 buckets, and define tiers to balance statistical power with business use.
6. **Define success metrics up front for the next iteration:** e.g., unique ATC rate +20% or revenue-per-recipient +15% over an 8-week window, evaluated on "Placed order" (TG8u2t) with pre-registered analysis. Avoid metric drift toward whatever looks good.

## 6. Data/infra notes for whoever builds this

- Klaviyo US: conversion metric IDs — Placed order: `TG8u2t` (live), Added to Cart: `RMzrKN` (has $ values). Do NOT use `YxaQcp` ("Placed Order") or `UVZVgx` ("Order Placed") — zero attribution for these campaigns.
- Test lists follow the naming pattern: `US - Non_Social_{Subscriber|Customer|Cart}_Audience_{CON|NCS} - Active (Propensity_{High|Med|Low}) {A_NormalCalendar|B_PropensityCalendar}`.
- Campaign naming: `July N, 2026 | {Theme} | T# | Email_1 | {12%|15%} (Propensity {A_NormalCalendar|B_PropensityCalendar})`.
- Reporting: use Klaviyo `get_campaign_report` with `group_by_audience=true` to break results by tier list; opens are MPP-distorted for High tier — prefer clicks/carts/orders.
- Attribution windows and single-whale carts materially distort revenue comparisons at this volume — report unique buyers/adders alongside values.

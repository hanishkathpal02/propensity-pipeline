# Propensity v4 — Phase 1 Design (Per-Dimension Scoring + New Attributes)

**Status:** design locked, pre-build · **Model version:** `v4` · **Market:** US (pilot) · **Owner:** CRM Data Science
**Requested by:** Email Marketing ("Klaviyo Profile Attributes — Part 2")

> Note on versioning: the *engine* upgrade to CatBoost + `days_since_last_purchase` shipped as **v3** (live in prod US Klaviyo, 2026‑07‑27). This attribute expansion is therefore **v4**, not the "v3" named in the original ticket.

---

## 1. Goal

Expand the live Klaviyo propensity feed along three axes:
- **WS1 — per-dimension scoring:** every dimension carries its **own** `_score` + `_band` (today there is one score/band per *rank*, and it is really the Category prediction that stone/Jewelry inherit).
- **WS2 — new attributes:** predict new product dimensions per profile.
- **WS3 — grammar fields:** raw singular/plural for stone/Jewelry/Category so subject-line templates stop breaking ("Earringss").

Phase 1 delivers a **feasible, data-backed subset**; the rest is explicitly deferred (§8).

---

## 2. Scope — Phase 1 dimensions

All dimensions go through **one generalized CatBoost per-dimension pipeline** (no affinity/aggregate path — uniform scoring so every `_score`/`_band` means the same thing: 30‑day purchase propensity for that value).

| Dimension | Source (`reporting.dim_product`) | Ranks | Notes |
|---|---|---|---|
| category *(retained)* | `product_category` | top-3 | feeds **legacy** `prop_N_score/_band` **and** `prop_N_Category_*` |
| stone | `stone_type` | top-3 | |
| Jewelry | `jewelry_type` | top-3 | |
| metal | `metal_type` | top-3 | **kept with karat** ("14K Yellow Gold") per decision |
| stone_origin | `lab_category` | **top-1** | binary Natural / Lab-Grown |
| product_gender | `gender` | **top-1** | ~97% "Women" — low variance; treat as a preference flag |
| price_range | `product_price_range` | **top-1** | 8 pre-bucketed tiers ($300‑500 … $7500+) |

**Independent ranks:** because each dimension is scored on its own grain, `prop_1_stone` is the customer's #1 stone *overall* — it may **not** be the stone of `prop_1_Category`. (`prop_N_Category` is still composed as stone+Jewelry for display.) Email gates each dimension by its own band.

---

## 3. Data-availability audit (verified against live `dim_product`, US)

| Attribute | Column | Verdict |
|---|---|---|
| metal | `metal_type` | ✅ clean (14K/18K White/Yellow/Rose Gold, Platinum, Silver) |
| stone_origin | `lab_category` | ✅ clean binary (Natural 1.07M / Lab-Grown 186K) |
| price_range | `product_price_range` | ✅ pre-bucketed |
| product_gender | `gender` | ✅ present, heavily Women-skewed |
| style | `jewelry_style` | ⚠️ **deferred** — comma-list of tags ("Solitaire,Classic"), no primary style |
| collection | `collection_label1` | ⚠️ **deferred** — campaign-batch names, ~88% "Not Available" |
| occasion | `engagement_label1` | ⚠️ **deferred** — only ENGAGEMENT vs not; no occasion taxonomy |
| certification | `gia_label` | ⚠️ **deferred** — 98% Non-GIA, no signal |

Filter data-quality noise across all: **`Not Available`, `CWA`**.

---

## 4. Architecture

### 4.1 Data layer — attributes behavior fact (`fact_propensity_attributes`)
`fact_web_analytics` has `product_id` → join `dim_product` and carry each dimension as **its own VARCHAR column** (not JSON/SUPER — Redshift is columnar; flat typed columns scan/group fastest).

- New/extended fact grain: `(visitor, country, date, product_category, stone, jewelry_type, metal_type, lab_category, gender, product_price_range, device, channel)` + metrics (sessions, product_views, cart_adds, checkouts, orders).
- Read per dimension = `GROUP BY email, {dimension_column}` — same identity-resolution join to `profiles.dim_visitor_email_map` as today.
- No row explosion beyond the natural (attribute-combo) grain increase; direct evolution of the current category fact.

### 4.2 Generalized per-dimension pipeline
Parameterize the existing machinery by `dimension` (the grouping column):
- **`features/build.py`** — group by `(email, {dim_value}, snapshot)`; identical 30‑day lookback features + "bought that value in next 30d" label.
- **`model/train.py`** — unchanged CatBoost path; run per dimension; score every `(email, dim_value)`; take **top-N** (3 or 1).
- **Banding** — **per-dimension calibration** (each dimension's score distribution differs, so a flat 0.61 selects a different population fraction). Default to live cutoffs (high ≥0.61 / medium 0.21–0.60 / low ≤0.20); **recommend percentile-based** bands per dimension so "high/medium/low" keeps a consistent meaning without hand-tuning.

### 4.3 Delivery — assemble one wide profile
`delivery/profiles.py` merges every dimension's top-N into one payload:
- `prop_{rank}_{dim}` + `prop_{rank}_{dim}_score` + `prop_{rank}_{dim}_band` (top-3 dims)
- `prop_{dim}` + `_score` + `_band` (single-value dims: stone_origin, product_gender, price_range)
- **legacy** `prop_N_score` / `prop_N_band` retained unchanged (= Category prediction)
- **WS3 grammar fields** (§4.4)
- metadata: `prop_model_version="v4"`, `prop_scored_at`, `propensity_source=true`, `test_bucket`

### 4.4 WS3 — grammar (deterministic, no model)
- **Jewelry** → curated **singular↔plural lookup** (closed set enumerated from `dim_product`; owns irregulars: Earrings↔Earring, Necklace↔Necklaces, Engagement Ring↔Engagement Rings).
- **Stone** → **+s on the last word** rule (Diamond→Diamonds, Blue Sapphire→Blue Sapphires, Lab Grown Diamond→Lab Grown Diamonds) + tiny exceptions list.
- **Category** → compose `stone_singular + Jewelry_{singular|plural}`.
- **"Raw" only** — do NOT merge lab-grown or rename Blue Sapphire→Sapphire (template still owns those).
- **Fallback** if unmapped → `singular = plural = raw` (never "Earringss") and **log the miss** to extend the lookup.
- Fields (ranks 1–3): `prop_N_{stone|Jewelry|Category}_{singular|plural}`.

---

## 5. Orchestration (Airflow DAG)

```
                          ┌ build_feat_stone   → train_stone   ┐
  load_attr_fact (shared) ├ build_feat_metal   → train_metal   ┤
  → analyze → freshness   ├ … 7 dimension flows …              ┼─► build_profiles ─► sync_klaviyo ─► log_sync
    [+ WS3 grammar prep]  └ build_feat_price   → train_price   ┘   (fan-in, all_success)
                            (heavy tasks gated by a Pool: 1–2 concurrent)
```

- **Sequential, pool-capped:** this server is **~30 GB RAM / ~8 cores, shared with Airbyte**; each CatBoost train peaks ~10 GB / ~6 cores, so **7 in parallel is impossible**. An Airflow **Pool caps heavy trains to 1–2 concurrent**. Runtime ≈ 7 × ~25 min ≈ **~3 hrs** — fine for a **weekly** cadence.
- **Fan-in, strict all-or-nothing:** `build_profiles` uses `trigger_rule=all_success` — it runs (and therefore `sync_klaviyo` sends) **only if every dimension model succeeded**. A single failure → **nothing is sent**.
- **Synchronized send:** all attributes assembled first, then **one upsert per profile**, one `prop_scored_at` — never a profile with fresh `stone` but stale `metal`.
- **Resume-from-failed (no wasted re-training):** each dimension is a **separate task** and its output persists in the artifact store keyed by `(country, run_date, dimension)`. If the 5th of 7 fails: fix it, **clear only the failed task + downstream** (`build_profiles`/`sync`/`log`) in the **same run** — Airflow re-runs only the 5th (the 4 successful stay `success`, artifacts reused). *Resume = clear the failed task in the same run, not a fresh trigger (a new `run_date` re-runs everything).*
- **Graceful vs strict:** per decision, **strict** — do not down-grade to "send with dimension absent." A failed dimension blocks the send until resolved.

---

## 6. Klaviyo payload (rank-1 shown; 2/3 follow)

```jsonc
"properties": {
  // legacy (unchanged) = Category prediction
  "prop_1_score": 0.62, "prop_1_band": "high",

  // per-dimension value + independent score + band
  "prop_1_stone": "Diamond",     "prop_1_stone_score": 0.62,     "prop_1_stone_band": "high",
  "prop_1_Jewelry": "Ring",      "prop_1_Jewelry_score": 0.44,   "prop_1_Jewelry_band": "medium",
  "prop_1_Category": "Diamond Ring","prop_1_Category_score":0.62, "prop_1_Category_band":"high",
  "prop_1_metal": "14K Yellow Gold","prop_1_metal_score":0.38,   "prop_1_metal_band":"medium",

  // grammar (raw singular/plural)
  "prop_1_stone_singular":"Diamond","prop_1_stone_plural":"Diamonds",
  "prop_1_Jewelry_singular":"Ring","prop_1_Jewelry_plural":"Rings",
  "prop_1_Category_singular":"Diamond Ring","prop_1_Category_plural":"Diamond Rings",

  // single-value dimensions
  "prop_stone_origin":"Lab-Grown","prop_stone_origin_score":0.55,"prop_stone_origin_band":"medium",
  "prop_product_gender":"Women","prop_product_gender_score":0.90,"prop_product_gender_band":"high",
  "prop_price_range":"$500-$1,000","prop_price_range_score":0.66,"prop_price_range_band":"high",

  // metadata
  "prop_model_version":"v4","prop_scored_at":"2026-07-26","propensity_source":true,"test_bucket":42
}
```
Band thresholds documented on-profile intent: `high ≥ 0.61 · medium 0.21–0.60 · low ≤ 0.20` (default; per-dimension calibration may shift raw cutoffs while keeping the meaning).

---

## 7. Naming & policy decisions (locked)

- **`product_gender`** (not the ticket's `prop_gender_target`) — it is *product* gender, not inferred recipient. Confirm rename with Email.
- **Metal kept with karat** ("14K Yellow Gold") — top-3 may fragment across karats of one color; accepted for granularity.
- **Wide VARCHAR fact** — no JSON/SUPER.
- **Uniform CatBoost** — every dimension; no affinity path.
- **Strict all-or-nothing** send; **resume-from-failed** via per-dim tasks + artifact cache.
- **Null / low-confidence:** a dimension is **absent** (property not written) when it has no data or clears no band — per "quality over reach" (a smaller, precise scored set beats broad low-confidence coverage).
- **Cadence:** weekly; keep `prop_scored_at` current.

---

## 8. Deferred (with reason)

| Item | Why deferred |
|---|---|
| style | `jewelry_style` is a multi-tag list with no primary; needs a defined primary/secondary style taxonomy |
| collection | `collection_label1` = campaign-batch names, ~88% null |
| occasion | only an ENGAGEMENT flag in data; no occasion taxonomy |
| certification | 98% Non-GIA — no signal |
| purchase_intent (self/gift) | **no data** — needs an order-level gift signal (ship≠bill / gift-wrap) |
| pairing | needs product-relationship feed |
| birthstone / birth_month | needs customer DOB (likely Klaviyo-side, not the warehouse) |
| next_purchase_window | different method — **survival / time-to-event** model |
| promo_pref / discount_affinity | needs coupon/promo-response feed; also only in the JSON, not the ticket tables |

---

## 9. Acceptance criteria (from the ticket)

- WS1: every Phase-1 dimension carries its own `prop_{rank}_{dim}_score` + `_band`; legacy `prop_N_score/_band` retained unchanged; band thresholds documented.
- WS2: each Phase-1 attribute predicted per profile (rank-1, + 2/3 where multi-value), with a documented value taxonomy.
- WS3: all six grammar fields populated ranks 1–3 (where data exists), live casing, raw rules; canonical singular↔plural lookup shared (incl. irregulars).
- Metadata on every profile; `prop_model_version = v4`.
- Null/low-confidence behavior defined (absent, not empty string).
- Sandbox sample (~20–30 US profiles, all ranks + fields, incl. a Lab-Grown, Blue Sapphire, Earrings, Engagement Ring) for "Preview as profile" before go-live.

---

## 10. Open items to confirm

1. **Band calibration** — percentile-based per dimension (recommended) vs re-derived numeric thresholds.
2. **`product_gender` rename** — Email OK moving off `prop_gender_target`?
3. **price_range display string** — confirm exact tier labels (`$500-$1,000` vs `500-1000`).
4. **Wide-fact build** — new proc `sp_load_fact_propensity_attributes` (mirrors current category proc) + grants + backfill.

---

## 11. Rollout sequence

1. **WS3 grammar** — deterministic, no model dependency; ships Email value first.
2. **Wide fact** (proc + backfill) — the foundational data lift.
3. **Generalize `build`/`train`**; run the 7 dimensions; per-dimension banding.
4. **Delivery assembly** + WS3 fields.
5. **Sandbox sample** → **dry run** → **go-live** (flip `propensity_sync_dry_run=false`, as with v3).

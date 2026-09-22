# Propensity per-attribute scores — status for Email Marketing

**Re:** "Klaviyo Profile Attributes — Part 2"
**From:** CRM Data Science · **Market:** US (pilot)

We've built per-attribute propensity scoring. **Every attribute below now gets its own
independent score + band** (previously stone/metal/etc. just inherited the Category score),
refreshed per profile.

## ✅ Delivered — scores prepared for the attributes where the data supports it
- **Category, Stone, Jewelry, Metal** (metal kept with karat, e.g. "14K Yellow Gold") — **top-3**
  each, each value with its own score + band.
- **Stone Origin** (Natural / Lab-Grown), **Product Gender**, **Price Range** — **top-1** each,
  with score + band.
- Plus the **singular/plural grammar fields** so subject-line templates stop breaking ("Earringss").

Because each attribute is scored on its own, a customer's #1 metal or stone is their top metal/stone
**overall** — it may not be the metal/stone of their #1 category. Gate each attribute by its own band.

## 🚫 Out of scope this phase — source data isn't usable
- **Style** — stored as a comma-list of tags ("Solitaire,Classic") with no primary style to pick.
- **Collection** — ~88% "Not Available" (campaign-batch names, not stable collections).
- **Occasion** — only an engagement flag exists; there's no occasion taxonomy in the data.
- **Certification (GIA)** — 98% non-GIA, so there's no signal to learn from.

These need new or cleaned source data before they can be modeled, so they're **out of scope for now**.

## Status
Everything in "Delivered" is built and validated offline (e.g. the metal model holds a strong
held-out AUC of ~0.87). We're finalizing the daily delivery mechanics; go-live timing and the
daily refresh cadence will be confirmed separately.

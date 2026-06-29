"""
Validation analysis: ROC, score distribution, lift, feature importance, and summaries.

Ported from propensity_model2/modules/analysis.py. Score bands come from
config.params.SCORE_BANDS (no longer hardcoded), so retuning is a one-spot change.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import auc, roc_auc_score, roc_curve

from propensity.config.params import SCORE_BANDS
from propensity.features.build import label_col
from propensity.util import logger

_HIGH = SCORE_BANDS["high"]
_MED  = SCORE_BANDS["medium"]


def _band(score: float) -> str:
    if score >= _HIGH:
        return "high"
    if score >= _MED:
        return "medium"
    return "low"


def save_analysis(model, scored_df, train_end_date, label_window, output_dir: Path, country: str) -> None:
    """Write ROC / score-dist / lift / importance plots + metric CSVs to output_dir/analysis/.

    Derives the validation split from the scored frame (snapshot_date > train_end_date)
    and uses the stored propensity_score — so it needs no separate X_val/y_val artifacts.
    """
    out = Path(output_dir) / "analysis"
    out.mkdir(parents=True, exist_ok=True)

    lc = label_col(label_window)
    val = scored_df[scored_df["snapshot_date"] > pd.Timestamp(train_end_date)]
    y_val   = val[lc].astype(int).reset_index(drop=True)
    y_score = val["propensity_score"].reset_index(drop=True).to_numpy()
    base_rate = float(y_val.mean())

    _roc(y_val, y_score, out, country)
    _score_distribution(y_val, y_score, out, country)
    _lift(y_val, y_score, base_rate, out, country)
    _feature_importance(model, out, country)
    _metrics_csv(y_val, y_score, base_rate, out, country)
    _profile_summary(scored_df, out, country)
    logger.info(f"Analysis saved to {out}")


def _roc(y_val, y_score, out, country):
    fpr, tpr, _ = roc_curve(y_val, y_score)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(fpr, tpr, color="steelblue", lw=2, label=f"AUC = {auc(fpr, tpr):.3f}")
    ax.plot([0, 1], [0, 1], color="grey", lw=1, ls="--", label="Random")
    ax.set(xlabel="False Positive Rate", ylabel="True Positive Rate", title=f"ROC — {country}")
    ax.legend(loc="lower right")
    fig.tight_layout(); fig.savefig(out / f"roc_curve_{country}.png", dpi=150); plt.close(fig)


def _score_distribution(y_val, y_score, out, country):
    buyers, non_buyers = y_score[y_val == 1], y_score[y_val == 0]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(non_buyers, bins=50, alpha=0.5, color="steelblue", density=True,
            label=f"Non-buyers (n={len(non_buyers):,})")
    ax.hist(buyers, bins=50, alpha=0.7, color="coral", density=True,
            label=f"Buyers (n={len(buyers):,})")
    ax.axvline(_HIGH, color="red", ls="--", lw=1.2, label=f"High ({_HIGH})")
    ax.axvline(_MED, color="orange", ls="--", lw=1.2, label=f"Medium ({_MED})")
    ax.set(xlabel="Propensity Score", ylabel="Density",
           title=f"Score Distribution: Buyers vs Non-Buyers — {country}")
    ax.legend()
    fig.tight_layout(); fig.savefig(out / f"score_distribution_{country}.png", dpi=150); plt.close(fig)


def _lift(y_val, y_score, base_rate, out, country):
    df = pd.DataFrame({"score": y_score, "actual": list(y_val)})
    df["decile"] = pd.qcut(df["score"], q=10, labels=False, duplicates="drop")
    lift = (df.groupby("decile")["actual"].agg(["mean", "count"]).reset_index()
              .sort_values("decile", ascending=False).reset_index(drop=True))
    lift["decile_label"] = [f"Top {(i + 1) * 10}%" for i in range(len(lift))]
    lift["lift"] = lift["mean"] / base_rate

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(lift["decile_label"], lift["mean"] * 100, color="steelblue", alpha=0.7, label="Purchase rate %")
    ax1.axhline(base_rate * 100, color="grey", ls="--", lw=1.2, label=f"Baseline ({base_rate*100:.2f}%)")
    ax1.set(ylabel="Actual Purchase Rate (%)", xlabel="Score Decile (high → low)",
            title=f"Lift Chart — {country}")
    ax2 = ax1.twinx()
    ax2.plot(lift["decile_label"], lift["lift"], color="coral", marker="o", lw=2, label="Lift vs baseline")
    ax2.set_ylabel("Lift (vs baseline)")
    l1, lab1 = ax1.get_legend_handles_labels()
    l2, lab2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, lab1 + lab2, loc="upper right")
    plt.xticks(rotation=30)
    fig.tight_layout(); fig.savefig(out / f"lift_chart_{country}.png", dpi=150); plt.close(fig)


def _feature_importance(model, out, country):
    imp = (pd.Series(model.feature_importance(importance_type="gain"), index=model.feature_name())
             .sort_values().tail(15))
    fig, ax = plt.subplots(figsize=(8, 6))
    imp.plot(kind="barh", ax=ax, color="steelblue")
    ax.set(xlabel="Feature Importance (Gain)", title=f"Top 15 Features — {country}")
    fig.tight_layout(); fig.savefig(out / f"feature_importance_{country}.png", dpi=150); plt.close(fig)


def _metrics_csv(y_val, y_score, base_rate, out, country):
    df = pd.DataFrame({"score": y_score, "actual": list(y_val)})
    df["decile"] = pd.qcut(df["score"], q=10, labels=False, duplicates="drop")
    lift = (df.groupby("decile")["actual"].agg(purchase_rate="mean", n="count").reset_index()
              .sort_values("decile", ascending=False))
    lift["lift"] = lift["purchase_rate"] / base_rate
    lift.to_csv(out / f"lift_table_{country}.csv", index=False)

    pd.DataFrame([{
        "country": country,
        "auc": round(roc_auc_score(y_val, y_score), 4),
        "base_rate_pct": round(base_rate * 100, 4),
        "val_samples": len(y_val),
        "val_buyers": int(y_val.sum()),
    }]).to_csv(out / f"validation_summary_{country}.csv", index=False)


def _profile_summary(df_features, out, country):
    # most-recent snapshot score per (email, category)
    df = (df_features.sort_values("snapshot_date", ascending=False)
                     .groupby(["email", "propensity_category"], as_index=False)
                     .first()[["email", "propensity_category", "propensity_score"]])
    top = df.groupby("email")["propensity_score"].max().reset_index()
    top["band"] = top["propensity_score"].apply(_band)
    dist = top["band"].value_counts().reindex(["high", "medium", "low"], fill_value=0)
    logger.info(f"Profile bands (top score/email) — {country}: {dist.to_dict()}")

    rows = [{"metric": "total_unique_profiles", "value": int(df["email"].nunique())}]
    rows += [{"metric": f"band_{b}", "value": int(dist[b])} for b in ["high", "medium", "low"]]
    pd.DataFrame(rows).to_csv(out / f"profile_summary_{country}.csv", index=False)

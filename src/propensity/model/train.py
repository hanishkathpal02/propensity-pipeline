"""
LightGBM training + evaluation.

Ported from propensity_model2/modules/data_transformation.py (create_model, model_stats).
Feature lists are imported from features.build so train and feature-build never drift.
"""
import lightgbm as lgb
import pandas as pd
from sklearn.metrics import (accuracy_score, confusion_matrix, precision_score,
                             recall_score, roc_auc_score)

from propensity.features.build import CATEGORICAL_COLS, FEATURE_COLS, label_col
from propensity.util import logger

LGB_PARAMS = {
    "objective": "binary",
    "metric": "auc",
    "boosting_type": "gbdt",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "max_depth": -1,
    "feature_fraction": 1.0,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1,
    "seed": 42,
    "bagging_seed": 42,
    "feature_fraction_seed": 42,
    "deterministic": True,
}


def _lgb_params(y):
    vc = y.value_counts()
    return dict(LGB_PARAMS, scale_pos_weight=vc.get(0, 1) / max(vc.get(1, 1), 1))


def train_model(df_features: pd.DataFrame, train_end_date, mature_cutoff_date,
                label_window: int, refit_all_mature: bool = False):
    """Time-split train/validation, fit LightGBM, score the full frame.

    Validation is (train_end, mature_cutoff] so its labels are FULLY MATURE (no leakage and
    no immature-label bias). Snapshots after mature_cutoff are excluded from train/val but
    still scored (predict runs on the full frame) for delivery.

    If `refit_all_mature`, a second model is fit on ALL mature data (train + val) for the
    early-stopped iteration count and used for scoring/delivery — while the FIRST model
    (train-only) is returned as `eval_model` for HONEST held-out metrics.

    Returns (final_model, eval_model, X_val, y_val, df_features_with_score).
    """
    lc = label_col(label_window)
    X = df_features[FEATURE_COLS].copy()
    y = df_features[lc].copy()
    for col in CATEGORICAL_COLS:
        X[col] = X[col].astype("category")

    # Time-based split. train ≤ train_end; validate on (train_end, mature_cutoff] only
    # (mature labels). Snapshots > mature_cutoff are neither trained nor validated on.
    snap = df_features["snapshot_date"]
    train_mask = snap <= pd.Timestamp(train_end_date)
    val_mask   = (snap > pd.Timestamp(train_end_date)) & (snap <= pd.Timestamp(mature_cutoff_date))
    X_train, y_train = X[train_mask], y[train_mask]
    X_val,   y_val   = X[val_mask],   y[val_mask]

    logger.info(f"Train: {X_train.shape}  Val: {X_val.shape}")
    logger.info(f"Train labels: {y_train.value_counts().to_dict()}")
    logger.info(f"Val   labels: {y_val.value_counts().to_dict()}")

    # eval_model: fit on train only, early-stop on the mature val -> honest held-out metrics
    eval_model = lgb.train(
        params=_lgb_params(y_train),
        train_set=lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_COLS),
        valid_sets=[
            lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_COLS),
            lgb.Dataset(X_val,   label=y_val,   categorical_feature=CATEGORICAL_COLS),
        ],
        valid_names=["train", "valid"],
        num_boost_round=1000,
        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(50)],
    )
    best_it = eval_model.best_iteration or 1000
    logger.info(f"Best iteration: {best_it}")

    # final_model: optionally refit on ALL mature data (train+val) for best_it rounds.
    if refit_all_mature and bool(val_mask.any()):
        X_mat, y_mat = X[train_mask | val_mask], y[train_mask | val_mask]
        logger.info(f"Refit on all mature data: {X_mat.shape} for {best_it} rounds")
        final_model = lgb.train(
            params=_lgb_params(y_mat),
            train_set=lgb.Dataset(X_mat, label=y_mat, categorical_feature=CATEGORICAL_COLS),
            num_boost_round=best_it,
            callbacks=[lgb.log_evaluation(50)],
        )
    else:
        final_model = eval_model

    df_features = df_features.copy()
    df_features["propensity_score"] = final_model.predict(X)   # score full frame (incl. immature) for delivery
    return final_model, eval_model, X_val, y_val, df_features


def model_stats(model, X_val, y_val, threshold: float) -> dict:
    """AUC + accuracy/precision/recall/F1 + confusion matrix at `threshold`."""
    y_score = model.predict(X_val)
    y_bin = (y_score >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(y_val, y_bin).ravel()
    precision = precision_score(y_val, y_bin, zero_division=0)
    recall = recall_score(y_val, y_bin, zero_division=0)
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    stats = {
        "auc": roc_auc_score(y_val, y_score),
        "accuracy": accuracy_score(y_val, y_bin),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "threshold": threshold,
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        "conversion_rate": float(y_val.mean()),
        "total": int(len(y_val)),
        "positives": int(y_val.sum()),
    }
    logger.info(f"AUC={stats['auc']:.4f}  P={precision:.4f}  R={recall:.4f}  "
                f"F1={f1:.4f}  (threshold={threshold})  base_rate={stats['conversion_rate']:.4%}")
    logger.info(f"Confusion @ {threshold}: TN={tn:,} FP={fp:,} FN={fn:,} TP={tp:,}")
    return stats

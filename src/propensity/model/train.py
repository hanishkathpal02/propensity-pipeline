"""
Model training + evaluation — engine-agnostic (LightGBM or CatBoost).

The engine is chosen by `params.MODEL_TYPE` ("catboost" default, "lightgbm" fallback).
Both paths return a `PropensityModel` wrapper that exposes a small Booster-like API
(`predict`, `feature_name`, `feature_importance`), so everything downstream
(`pipeline`, `analysis`, `artifacts`) is identical regardless of engine.

Feature lists are imported from features.build so train and feature-build never drift.
"""
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import (accuracy_score, confusion_matrix, precision_score,
                             recall_score, roc_auc_score)

from propensity.config import params
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

CAT_PARAMS = {
    "learning_rate": 0.05,
    "depth": 6,
    "eval_metric": "AUC",
    "random_seed": 42,
    "thread_count": 6,          # cap threads so a retrain doesn't starve the co-located services
    "allow_writing_files": False,
    "verbose": 200,
}
_CAT_MAX_ITERS = 1500           # upper bound; early stopping picks the real count


# --------------------------------------------------------------------------- #
# Engine-agnostic wrapper                                                      #
# --------------------------------------------------------------------------- #
class PropensityModel:
    """Wraps a fitted LightGBM Booster or CatBoost model behind one small API.

    Exposes `predict` (1-D positive-class probability), `feature_name`, and
    `feature_importance` — the subset of the LightGBM Booster interface that the rest
    of the pipeline uses — so callers never branch on the engine. Picklable (used by
    io.artifacts), because it only holds the underlying model + column lists.
    """

    def __init__(self, kind, model, feature_cols, categorical_cols):
        self.kind = kind                              # "lightgbm" | "catboost"
        self._model = model
        self._features = list(feature_cols)
        self._categoricals = list(categorical_cols)

    def _prep(self, X):
        Xf = X[self._features].copy()
        if self.kind == "catboost":
            for c in self._categoricals:
                Xf[c] = Xf[c].astype(str).fillna("unknown")
        else:  # lightgbm — category dtype; the Booster remaps by value via stored categories
            for c in self._categoricals:
                Xf[c] = Xf[c].astype("category")
        return Xf

    def predict(self, X):
        Xf = self._prep(X)
        if self.kind == "catboost":
            return self._model.predict_proba(Xf)[:, 1]
        return self._model.predict(Xf)

    def feature_name(self):
        return self._features

    def feature_importance(self, importance_type="gain"):
        if self.kind == "catboost":
            return np.asarray(self._model.get_feature_importance())   # PredictionValuesChange
        return self._model.feature_importance(importance_type=importance_type)


def _lgb_params(y):
    vc = y.value_counts()
    return dict(LGB_PARAMS, scale_pos_weight=vc.get(0, 1) / max(vc.get(1, 1), 1))


def _spw(y):
    vc = y.value_counts()
    return vc.get(0, 1) / max(vc.get(1, 1), 1)


# --------------------------------------------------------------------------- #
# Engine-specific fit helpers — each returns (final_model, eval_model)         #
#   eval_model  = fit on TRAIN only, early-stopped on the mature val (honest metrics)
#   final_model = refit on ALL mature (train+val) at the early-stopped count, else eval_model
# --------------------------------------------------------------------------- #
def _fit_lightgbm(X, y, train_mask, val_mask, refit_all_mature):
    X = X.copy()
    for c in CATEGORICAL_COLS:
        X[c] = X[c].astype("category")
    X_train, y_train = X[train_mask], y[train_mask]
    X_val,   y_val   = X[val_mask],   y[val_mask]

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
    logger.info(f"[lightgbm] best iteration: {best_it}")

    if refit_all_mature and bool(val_mask.any()):
        X_mat, y_mat = X[train_mask | val_mask], y[train_mask | val_mask]
        logger.info(f"[lightgbm] refit on all mature data: {X_mat.shape} for {best_it} rounds")
        final = lgb.train(
            params=_lgb_params(y_mat),
            train_set=lgb.Dataset(X_mat, label=y_mat, categorical_feature=CATEGORICAL_COLS),
            num_boost_round=best_it,
            callbacks=[lgb.log_evaluation(50)],
        )
    else:
        final = eval_model
    return final, eval_model


def _fit_catboost(X, y, train_mask, val_mask, refit_all_mature):
    from catboost import CatBoostClassifier, Pool   # lazy import (only needed for this engine)

    X = X.copy()
    for c in CATEGORICAL_COLS:
        X[c] = X[c].astype(str).fillna("unknown")
    cat_idx = [FEATURE_COLS.index(c) for c in CATEGORICAL_COLS]
    X_train, y_train = X[train_mask], y[train_mask]
    X_val,   y_val   = X[val_mask],   y[val_mask]

    eval_model = CatBoostClassifier(iterations=_CAT_MAX_ITERS, scale_pos_weight=_spw(y_train),
                                    early_stopping_rounds=50, **CAT_PARAMS)
    eval_model.fit(Pool(X_train, y_train, cat_features=cat_idx),
                   eval_set=Pool(X_val, y_val, cat_features=cat_idx))
    best_it = eval_model.get_best_iteration()
    logger.info(f"[catboost] best iteration: {best_it}")

    if refit_all_mature and bool(val_mask.any()):
        X_mat, y_mat = X[train_mask | val_mask], y[train_mask | val_mask]
        logger.info(f"[catboost] refit on all mature data: {X_mat.shape} for {best_it + 1} iters")
        final = CatBoostClassifier(iterations=best_it + 1, scale_pos_weight=_spw(y_mat), **CAT_PARAMS)
        final.fit(Pool(X_mat, y_mat, cat_features=cat_idx))
    else:
        final = eval_model
    return final, eval_model


_FIT = {"lightgbm": _fit_lightgbm, "catboost": _fit_catboost}


def train_model(df_features: pd.DataFrame, train_end_date, mature_cutoff_date,
                label_window: int, refit_all_mature: bool = False):
    """Time-split train/validation, fit the configured engine, score the full frame.

    Validation is (train_end, mature_cutoff] so its labels are FULLY MATURE (no leakage and
    no immature-label bias). Snapshots after mature_cutoff are excluded from train/val but
    still SCORED (predict runs on the full frame) for delivery.

    If `refit_all_mature`, the deployed model is refit on ALL mature data (train + val) at the
    early-stopped iteration count, while the train-only model is returned as `eval_model` for
    HONEST held-out metrics.

    Returns (final_model, eval_model, X_val, y_val, df_features_with_score) — the two models
    are `PropensityModel` wrappers, so downstream code is engine-agnostic.
    """
    kind = params.MODEL_TYPE
    if kind not in _FIT:
        raise ValueError(f"Unknown MODEL_TYPE={kind!r}; expected one of {list(_FIT)}")

    lc = label_col(label_window)
    X = df_features[FEATURE_COLS].copy()      # raw (un-cast) features; each engine preps its own way
    y = df_features[lc].astype(int).copy()

    snap = df_features["snapshot_date"]
    train_mask = snap <= pd.Timestamp(train_end_date)
    val_mask   = (snap > pd.Timestamp(train_end_date)) & (snap <= pd.Timestamp(mature_cutoff_date))
    logger.info(f"[{kind}] Train: {int(train_mask.sum()):,}  Val: {int(val_mask.sum()):,}")
    logger.info(f"[{kind}] Train labels: {y[train_mask].value_counts().to_dict()}")
    logger.info(f"[{kind}] Val   labels: {y[val_mask].value_counts().to_dict()}")

    final_raw, eval_raw = _FIT[kind](X, y, train_mask, val_mask, refit_all_mature)
    final_model = PropensityModel(kind, final_raw, FEATURE_COLS, CATEGORICAL_COLS)
    eval_model  = PropensityModel(kind, eval_raw,  FEATURE_COLS, CATEGORICAL_COLS)

    X_val, y_val = X[val_mask], y[val_mask]
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

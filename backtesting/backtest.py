#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.metrics import (
    precision_recall_curve,
    roc_curve,
    auc,
    average_precision_score,
    roc_auc_score,
    confusion_matrix,
    brier_score_loss,
)
import matplotlib.pyplot as plt
import seaborn as sns

try:
    import mlflow
except Exception:  # pragma: no cover
    mlflow = None


def ensure_outdir(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"y_true", "y_pred"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    # clip predictions to [0,1]
    df["y_pred"] = df["y_pred"].clip(0, 1)
    return df


def compute_metrics(df: pd.DataFrame, threshold: float) -> dict:
    y_true = df["y_true"].astype(int).to_numpy()
    y_pred = df["y_pred"].to_numpy()
    y_hat = (y_pred >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(y_true, y_hat).ravel()
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    pr_auc = average_precision_score(y_true, y_pred)
    roc_auc = roc_auc_score(y_true, y_pred)
    brier = brier_score_loss(y_true, y_pred)

    return {
        "threshold": threshold,
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "pr_auc": float(pr_auc),
        "roc_auc": float(roc_auc),
        "brier": float(brier),
    }


def plot_pr_roc(df: pd.DataFrame, outdir: Path) -> dict:
    y_true = df["y_true"].astype(int).to_numpy()
    y_pred = df["y_pred"].to_numpy()

    # PR curve
    precision, recall, _ = precision_recall_curve(y_true, y_pred)
    ap = average_precision_score(y_true, y_pred)
    plt.figure(figsize=(6, 5))
    plt.step(recall, precision, where="post", label=f"AP={ap:.3f}")
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title("Precision-Recall Curve")
    plt.legend()
    pr_path = outdir / "precision_recall.png"
    plt.tight_layout()
    plt.savefig(pr_path)
    plt.close()

    # ROC curve
    fpr, tpr, _ = roc_curve(y_true, y_pred)
    roc = auc(fpr, tpr)
    plt.figure(figsize=(6, 5))
    plt.plot(fpr, tpr, label=f"AUC={roc:.3f}")
    plt.plot([0, 1], [0, 1], linestyle="--", color="gray")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.legend()
    roc_path = outdir / "roc.png"
    plt.tight_layout()
    plt.savefig(roc_path)
    plt.close()

    return {"precision_recall": str(pr_path), "roc": str(roc_path)}


def plot_calibration(df: pd.DataFrame, outdir: Path, bins: int = 10) -> str:
    df = df.copy()
    df["bin"] = pd.qcut(df["y_pred"], q=bins, duplicates="drop")
    calib = (
        df.groupby("bin").agg(pred_mean=("y_pred", "mean"), obs_rate=("y_true", "mean")).reset_index()
    )

    plt.figure(figsize=(6, 5))
    sns.scatterplot(x="pred_mean", y="obs_rate", data=calib)
    plt.plot([0, 1], [0, 1], linestyle="--", color="gray")
    plt.xlabel("Predicted Probability")
    plt.ylabel("Observed Positive Rate")
    plt.title("Calibration Plot")
    path = outdir / "calibration.png"
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    calib.to_csv(outdir / "calibration_bins.csv", index=False)
    return str(path)


def per_group_metrics(df: pd.DataFrame, threshold: float, groups=("region", "industry")) -> pd.DataFrame:
    cols = [g for g in groups if g in df.columns]
    if not cols:
        return pd.DataFrame()
    rows = []
    for keys, gdf in df.groupby(cols):
        m = compute_metrics(gdf, threshold)
        if isinstance(keys, tuple):
            for i, c in enumerate(cols):
                m[c] = keys[i]
        else:
            m[cols[0]] = keys
        rows.append(m)
    return pd.DataFrame(rows)


def save_report_json(metrics: dict, artifacts: dict, outdir: Path) -> None:
    report = {"metrics": metrics, "artifacts": artifacts}
    with open(outdir / "report.json", "w") as f:
        json.dump(report, f, indent=2)


def maybe_log_mlflow(run_name: str, params: dict, metrics: dict, artifacts: dict) -> None:
    if mlflow is None:
        return
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        return
    mlflow.set_tracking_uri(tracking_uri)
    exp_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "backtesting")
    mlflow.set_experiment(exp_name)
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        for name, path in artifacts.items():
            if isinstance(path, str) and os.path.isfile(path):
                mlflow.log_artifact(path, artifact_path=name)


def main():
    p = argparse.ArgumentParser(description="Backtesting metrics and report")
    p.add_argument("--input", required=True, help="CSV with y_true,y_pred and optional groups")
    p.add_argument("--outdir", default="backtesting/report", help="Output directory for report")
    p.add_argument("--threshold", type=float, default=0.5, help="Decision threshold")
    p.add_argument("--bins", type=int, default=10, help="Calibration bins")
    p.add_argument("--run-name", default="backtest", help="MLflow run name")
    args = p.parse_args()

    outdir = Path(args.outdir)
    ensure_outdir(outdir)

    df = load_data(Path(args.input))
    metrics = compute_metrics(df, args.threshold)
    artifacts = {}
    artifacts.update(plot_pr_roc(df, outdir))
    artifacts["calibration"] = plot_calibration(df, outdir, bins=args.bins)

    # Per-group metrics if available
    pg = per_group_metrics(df, args.threshold)
    if not pg.empty:
        pg_path = outdir / "per_group_metrics.csv"
        pg.to_csv(pg_path, index=False)
        artifacts["per_group_metrics"] = str(pg_path)

    # Save metrics and report
    with open(outdir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    save_report_json(metrics, artifacts, outdir)

    # Simple markdown summary
    md = outdir / "summary.md"
    with open(md, "w") as f:
        f.write("# Backtesting Summary\n\n")
        for k, v in metrics.items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## Artifacts\n")
        for k, v in artifacts.items():
            f.write(f"- {k}: {v}\n")

    # MLflow logging if configured
    maybe_log_mlflow(args.run_name, vars(args), metrics, artifacts)
    print("Backtesting complete. Report saved to:", outdir)


if __name__ == "__main__":
    main()

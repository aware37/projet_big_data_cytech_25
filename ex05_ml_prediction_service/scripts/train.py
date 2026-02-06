"""Train a fare-prediction model on NYC Yellow Taxi parquet data.

Usage
-----
.. code-block:: bash

    python scripts/train.py \\
        --input s3://nyc-processed/cleaned/2024-01/ \\
        --max-rows 5000000
"""

import argparse
import gc
import json
import os

import pandas as pd
from joblib import dump
from sklearn.model_selection import train_test_split

from taxi_ml.config import Paths
from taxi_ml.features import add_time_features, split_xy
from taxi_ml.io import minio_storage_options, read_parquet_any
from taxi_ml.model import build_model, rmse
from taxi_ml.validate import validate_train_df

NEEDED_COLS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "payment_type_id",
    "pu_location_id",
    "do_location_id",
    "total_amount",
]


def parse_args():
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    p = argparse.ArgumentParser(
        description="Train fare-prediction model"
    )
    p.add_argument(
        "--input", required=True, nargs="+",
        help="Parquet paths (local or s3://)",
    )
    p.add_argument(
        "--minio-endpoint",
        default=os.getenv(
            "MINIO_ENDPOINT", "http://localhost:9000"
        ),
    )
    p.add_argument(
        "--minio-access",
        default=os.getenv("MINIO_ACCESS_KEY", "minio"),
    )
    p.add_argument(
        "--minio-secret",
        default=os.getenv("MINIO_SECRET_KEY", "minio123"),
    )
    p.add_argument("--test-size", type=float, default=0.2)
    p.add_argument(
        "--max-rows", type=int, default=None,
        help="Cap rows after concat (random sample).",
    )
    return p.parse_args()


def filter_aberrant(df: pd.DataFrame) -> pd.DataFrame:
    """Remove aberrant rows before training.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame *after* :func:`add_time_features` has been
        called.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame (index reset).
    """
    n_before = len(df)
    mask = (
        (df["total_amount"] > 0)
        & (df["total_amount"] <= 200)
        & (df["trip_distance"] > 0)
        & (df["trip_distance"] <= 100)
        & (df["trip_duration_min"] > 0)
        & (df["trip_duration_min"] <= 180)
        & (df["passenger_count"] >= 0)
        & (df["passenger_count"] <= 9)
    )
    df = df.loc[mask].reset_index(drop=True)
    removed = n_before - len(df)
    pct = 100 * removed / max(n_before, 1)
    print(
        f"[TRAIN] Outlier filter: removed {removed:,} "
        f"rows ({pct:.1f}%)"
    )
    return df


def main():
    """Entry point: read data, train, evaluate, save artifacts."""
    args = parse_args()
    paths = Paths()
    os.makedirs(paths.artifacts_dir, exist_ok=True)

    storage_options = None
    if any(p.startswith("s3://") for p in args.input):
        storage_options = minio_storage_options(
            args.minio_endpoint,
            args.minio_access,
            args.minio_secret,
        )

    # Read all input files
    frames = []
    for src in args.input:
        print(f"[TRAIN] Reading {src} ...")
        so = storage_options if src.startswith("s3://") else None
        chunk = read_parquet_any(src, storage_options=so)
        keep = [c for c in NEEDED_COLS if c in chunk.columns]
        chunk = chunk[keep]
        frames.append(chunk)
        print(f"         → {len(chunk):,} rows")

    df = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()
    mem_mb = df.memory_usage(deep=True).sum() / 1e6
    print(
        f"[TRAIN] Total: {len(df):,} rows  "
        f"({mem_mb:.0f} MB)"
    )

    # Optional cap
    if args.max_rows and len(df) > args.max_rows:
        print(f"[TRAIN] Capping to {args.max_rows:,} rows")
        df = df.sample(
            n=args.max_rows, random_state=42,
        ).reset_index(drop=True)

    validate_train_df(df)
    df = add_time_features(df)

    # Filter aberrant rows for better RMSE
    df = filter_aberrant(df)

    x, y, feature_cols = split_xy(df)
    del df
    gc.collect()

    cat_cols = [
        "rate_code_id", "payment_type_id",
        "pu_location_id", "do_location_id",
    ]
    num_cols = [c for c in feature_cols if c not in cat_cols]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=args.test_size, random_state=42,
    )
    del x, y
    gc.collect()

    model = build_model(cat_cols, num_cols)
    print("[TRAIN] Fitting model …")
    model.fit(x_train, y_train)

    pred = model.predict(x_test)
    score = rmse(y_test, pred)

    metrics = {
        "rmse": score,
        "n_rows": int(len(x_train) + len(x_test)),
        "features": feature_cols,
    }

    dump(model, paths.model_path)
    with open(paths.metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(f"[TRAIN] RMSE={score:.4f}")
    print(f"[TRAIN] model saved -> {paths.model_path}")
    print(f"[TRAIN] metrics saved -> {paths.metrics_path}")


if __name__ == "__main__":
    main()

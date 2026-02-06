"""Validation functions for taxi trip data.

Provides schema and constraint checks for both training
and inference DataFrames.
"""

import pandas as pd


TRAIN_REQUIRED_COLS = [
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

INFER_REQUIRED_COLS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "payment_type_id",
    "pu_location_id",
    "do_location_id",
]


def validate_train_df(df: pd.DataFrame) -> None:
    """Validate a training DataFrame.

    Checks that required columns are present and that basic
    constraints are satisfied (no NaN target, no negative
    distances, non-empty DataFrame).

    Parameters
    ----------
    df : pd.DataFrame
        Raw training DataFrame.

    Raises
    ------
    ValueError
        If columns are missing, target contains NaN,
        distances are negative, or DataFrame is empty.
    """
    if df.empty:
        raise ValueError("Training DataFrame is empty")

    missing = [c for c in TRAIN_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for training: {missing}")

    if df["total_amount"].isna().any():
        raise ValueError("total_amount contains NaN")

    if (df["trip_distance"] < 0).any():
        raise ValueError("trip_distance contains negative values")

    if (df["passenger_count"] < 0).any():
        raise ValueError("passenger_count contains negative values")


def validate_infer_df(df: pd.DataFrame) -> None:
    """Validate an inference DataFrame.

    Checks that required columns are present and that basic
    constraints are satisfied (no negative distances,
    non-empty DataFrame).

    Parameters
    ----------
    df : pd.DataFrame
        Raw inference DataFrame.

    Raises
    ------
    ValueError
        If columns are missing, distances are negative,
        or DataFrame is empty.
    """
    if df.empty:
        raise ValueError("Inference DataFrame is empty")

    missing = [c for c in INFER_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for inference: {missing}")

    if (df["trip_distance"] < 0).any():
        raise ValueError("trip_distance contains negative values")

    if (df["passenger_count"] < 0).any():
        raise ValueError("passenger_count contains negative values")

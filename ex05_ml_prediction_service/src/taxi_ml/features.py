"""Feature engineering for NYC Yellow Taxi trip data.

Extracts time-based features from raw datetime columns
and provides train/infer feature splitting.
"""

import pandas as pd


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create time features from pickup/dropoff datetimes.

    Computes trip duration in minutes, hour of day, day of week,
    and day of month from the pickup timestamp.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with ``tpep_pickup_datetime`` and
        ``tpep_dropoff_datetime`` columns.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with added columns:
        ``trip_duration_min``, ``pickup_hour``,
        ``pickup_dayofweek``, ``pickup_day``.
    """
    out = df.copy()
    out["tpep_pickup_datetime"] = pd.to_datetime(
        out["tpep_pickup_datetime"]
    )
    out["tpep_dropoff_datetime"] = pd.to_datetime(
        out["tpep_dropoff_datetime"]
    )

    duration = (
        out["tpep_dropoff_datetime"] - out["tpep_pickup_datetime"]
    ).dt.total_seconds()
    out["trip_duration_min"] = (duration / 60.0).clip(lower=0)

    out["pickup_hour"] = out["tpep_pickup_datetime"].dt.hour
    out["pickup_dayofweek"] = out["tpep_pickup_datetime"].dt.dayofweek
    out["pickup_day"] = out["tpep_pickup_datetime"].dt.day

    return out


def split_xy(df: pd.DataFrame):
    """Split DataFrame into feature matrix and target vector.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame produced by :func:`add_time_features`.

    Returns
    -------
    X : pd.DataFrame
        Feature matrix.
    y : pd.Series or None
        Target vector (``total_amount``), or ``None`` if the
        column is absent (inference mode).
    feature_cols : list of str
        Names of the feature columns used.
    """
    feature_cols = [
        "passenger_count",
        "trip_distance",
        "trip_duration_min",
        "pickup_hour",
        "pickup_dayofweek",
        "pickup_day",
        "rate_code_id",
        "payment_type_id",
        "pu_location_id",
        "do_location_id",
    ]
    x = df[feature_cols].copy()
    y = (
        df["total_amount"].copy()
        if "total_amount" in df.columns
        else None
    )
    return x, y, feature_cols

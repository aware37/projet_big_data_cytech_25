"""I/O utilities for reading parquet data.

Handles both local and S3-compatible (MinIO) storage,
with automatic column renaming from NYC TLC conventions.
"""

import os
import pandas as pd


#: Mapping from original NYC TLC column names to snake_case.
_COL_RENAME = {
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type_id",
    "Airport_fee": "airport_fee",
}


def read_parquet_any(path, storage_options=None):
    """Read a parquet file from a local or S3-compatible path.

    Automatically renames original TLC column names to
    snake_case so downstream code uses a single convention.

    Parameters
    ----------
    path : str
        Local file path or ``s3://bucket/key`` URI.
    storage_options : dict or None
        Credentials dict for s3fs (see
        :func:`minio_storage_options`).

    Returns
    -------
    pd.DataFrame
        DataFrame with renamed columns.
    """
    df = pd.read_parquet(path, storage_options=storage_options)
    rename = {
        k: v for k, v in _COL_RENAME.items() if k in df.columns
    }
    if rename:
        df = df.rename(columns=rename)
    return df


def build_s3_path(bucket, object_key):
    """Build an ``s3://`` URI.

    Parameters
    ----------
    bucket : str
        Bucket name.
    object_key : str
        Object key inside the bucket.

    Returns
    -------
    str
        Full ``s3://bucket/key`` path.
    """
    return f"s3://{bucket}/{object_key}"


def minio_storage_options(endpoint_url, access_key, secret_key):
    """Return storage options dict for s3fs / MinIO.

    Parameters
    ----------
    endpoint_url : str
        MinIO endpoint, e.g. ``http://localhost:9000``.
    access_key : str
        Access key.
    secret_key : str
        Secret key.

    Returns
    -------
    dict
        Ready-to-use ``storage_options`` for pandas.
    """
    return {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint_url},
    }


def env_or_default(name, default):
    """Get an environment variable or fall back to *default*.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : str
        Fallback value.

    Returns
    -------
    str
        Value of the variable.
    """
    return os.getenv(name, default)

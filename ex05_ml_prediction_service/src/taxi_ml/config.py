"""Configuration dataclasses for the taxi ML service."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MinioConfig:
    """MinIO S3-compatible storage access configuration.

    Attributes
    ----------
    endpoint_url : str
        MinIO endpoint, e.g. ``http://localhost:9000``.
    access_key : str
        Access key.
    secret_key : str
        Secret key.
    bucket : str
        Bucket name, e.g. ``nyc-processed``.
    object_key : str
        Object key inside the bucket.
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    object_key: str


@dataclass(frozen=True)
class Paths:
    """Local filesystem paths for model artifacts.

    Attributes
    ----------
    artifacts_dir : str
        Directory for all artifacts.
    model_path : str
        Path to the serialized model.
    metrics_path : str
        Path to the JSON metrics file.
    """

    artifacts_dir: str = "artifacts"
    model_path: str = "artifacts/model.joblib"
    metrics_path: str = "artifacts/metrics.json"

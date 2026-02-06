"""Model building and evaluation utilities.

Provides a scikit-learn pipeline using
:class:`~sklearn.ensemble.HistGradientBoostingRegressor`
with ordinal-encoded categorical features.
"""

import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import HistGradientBoostingRegressor


def build_model(categorical_cols, numeric_cols) -> Pipeline:
    """Build a scikit-learn regression pipeline.

    Uses ``OrdinalEncoder`` for categorical features and
    ``HistGradientBoostingRegressor`` as the estimator, which
    handles high-cardinality categories (e.g. 265 location IDs)
    efficiently.

    Parameters
    ----------
    categorical_cols : list of str
        Column names to treat as categorical.
    numeric_cols : list of str
        Column names to treat as numeric.

    Returns
    -------
    sklearn.pipeline.Pipeline
        Fitted-ready pipeline with preprocessing and model.
    """
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("ordinal", OrdinalEncoder(
                handle_unknown="use_encoded_value",
                unknown_value=-1,
            )),
        ]
    )

    pre = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, numeric_cols),
            ("cat", categorical_pipe, categorical_cols),
        ],
    )

    reg = HistGradientBoostingRegressor(
        max_depth=8,
        learning_rate=0.05,
        max_iter=400,
        random_state=42,
    )

    return Pipeline(steps=[("preprocess", pre), ("model", reg)])


def rmse(y_true, y_pred) -> float:
    """Compute Root Mean Squared Error.

    Parameters
    ----------
    y_true : array-like
        Ground truth target values.
    y_pred : array-like
        Predicted target values.

    Returns
    -------
    float
        RMSE value.
    """
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))

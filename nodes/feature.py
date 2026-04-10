"""NODE 3: Feature — computes the revenue column.

Formula: revenue = quantity × unit_price × (1 − discount_percent / 100)
"""

import numpy as np
from utils.logger import get_logger

log = get_logger("feature")


def feature_node(state: dict) -> dict:
    """Add 'revenue' column to the working df."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))

    try:
        df = state["df"].copy()
        df["revenue"] = (
            df["quantity"]
            * df["unit_price"]
            * (1 - df["discount_percent"] / 100.0)
        )
        # Null out impossible values
        df.loc[df["revenue"] < 0, "revenue"] = np.nan

        non_null_rev = df["revenue"].notna().sum()
        log.info(f"Revenue computed — {non_null_rev} non-null values")
        state["df"] = df

    except Exception as e:
        errors.append(f"feature: revenue computation failed — {e}")
        fallback_used.append("feature: revenue set to NaN due to error")
        log.error(f"Revenue computation failed: {e}")
        df = state["df"].copy()
        df["revenue"] = np.nan
        state["df"] = df

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state

"""NODE 2: Cleaner — produces a clean df copy from raw_df.

Never modifies raw_df.  Each cleaning step is independently try/excepted.
"""

import re
import numpy as np
import pandas as pd
from utils.logger import get_logger

log = get_logger("cleaner")


def _parse_price(val):
    """Parse a price string like '₹1234.56' into a float."""
    if pd.isna(val):
        return np.nan
    cleaned = re.sub(r"[₹$€£,\s]", "", str(val).strip())
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return np.nan


def cleaner_node(state: dict) -> dict:
    """Clean raw_df into a working df copy."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))

    try:
        # Step 1: Copy raw_df
        clean = state["raw_df"].copy()

        # Step 2: Strip whitespace from object columns
        try:
            for col in clean.select_dtypes("object").columns:
                clean[col] = clean[col].str.strip()
                clean[col] = clean[col].replace({"nan": np.nan, "": np.nan})
            log.info("Stripped whitespace from object columns")
        except Exception as e:
            errors.append(f"cleaner: whitespace strip failed — {e}")
            log.error(f"Whitespace strip failed: {e}")

        # Step 3: Clean unit_price → float
        try:
            clean["unit_price"] = clean["unit_price"].apply(_parse_price)
            log.info("Cleaned unit_price to float")
        except Exception as e:
            errors.append(f"cleaner: unit_price cleaning failed — {e}")
            log.error(f"unit_price cleaning failed: {e}")

        # Step 4: Clean quantity → float, null out outliers
        try:
            clean["quantity"] = pd.to_numeric(clean["quantity"], errors="coerce")
            clean.loc[clean["quantity"] > 1000, "quantity"] = np.nan
            log.info("Cleaned quantity, nulled outliers >1000")
        except Exception as e:
            errors.append(f"cleaner: quantity cleaning failed — {e}")
            log.error(f"quantity cleaning failed: {e}")

        # Step 5: Clean discount_percent → float, null out invalid, fill NaN with 0
        try:
            clean["discount_percent"] = pd.to_numeric(
                clean["discount_percent"], errors="coerce"
            )
            mask = (clean["discount_percent"] < 0) | (clean["discount_percent"] > 100)
            clean.loc[mask, "discount_percent"] = np.nan
            clean["discount_percent"] = clean["discount_percent"].fillna(0.0)
            log.info("Cleaned discount_percent, filled NaN with 0.0")
        except Exception as e:
            errors.append(f"cleaner: discount_percent cleaning failed — {e}")
            log.error(f"discount_percent cleaning failed: {e}")

        # Step 6: Remove duplicate order_ids (keep first)
        try:
            before = len(clean)
            clean = clean.drop_duplicates(subset=["order_id"], keep="first")
            after = len(clean)
            log.info(f"Removed {before - after} duplicate order_id rows")
        except Exception as e:
            errors.append(f"cleaner: dedup failed — {e}")
            log.error(f"Dedup failed: {e}")

        # Step 7: Clean delivery_days → numeric
        try:
            clean["delivery_days"] = pd.to_numeric(
                clean["delivery_days"], errors="coerce"
            )
            log.info("Cleaned delivery_days to numeric")
        except Exception as e:
            errors.append(f"cleaner: delivery_days cleaning failed — {e}")
            log.error(f"delivery_days cleaning failed: {e}")

        # Step 8: Normalize return_status → lowercase strip
        try:
            clean["return_status"] = clean["return_status"].str.lower().str.strip()
            log.info("Normalized return_status to lowercase")
        except Exception as e:
            errors.append(f"cleaner: return_status normalization failed — {e}")
            log.error(f"return_status normalization failed: {e}")

        state["df"] = clean

    except Exception as e:
        errors.append(f"cleaner: entire node crashed — {e}")
        fallback_used.append("cleaner: entire node crashed, using raw_df copy")
        state["df"] = state["raw_df"].copy()
        log.error(f"Entire cleaner node crashed: {e}")

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    log.info(f"Clean df shape: {state['df'].shape}")
    return state

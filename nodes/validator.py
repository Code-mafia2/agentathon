"""NODE 1: Validator — computes Q3 audit metrics on raw_df ONLY.

Q3 is LOCKED after this node. Nothing downstream touches issues.
"""

import pandas as pd
from utils.logger import get_logger

log = get_logger("validator")


def validator_node(state: dict) -> dict:
    """Compute 5 data-quality metrics on raw_df. Never modifies raw_df."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))

    issues = {
        "duplicate_order_ids": 0,
        "quantity_outliers": 0,
        "price_format_errors": 0,
        "invalid_discounts": 0,
        "total_null_cells": 0,
    }

    try:
        df = state["raw_df"]

        # 1. Duplicate order IDs
        try:
            issues["duplicate_order_ids"] = int(df["order_id"].duplicated().sum())
            log.info(f"Duplicate order IDs: {issues['duplicate_order_ids']}")
        except Exception as e:
            errors.append(f"validator: duplicate_order_ids failed — {e}")
            log.error(f"duplicate_order_ids failed: {e}")

        # 2. Quantity outliers (>1000)
        try:
            qty = pd.to_numeric(df["quantity"], errors="coerce")
            issues["quantity_outliers"] = int((qty > 1000).sum())
            log.info(f"Quantity outliers (>1000): {issues['quantity_outliers']}")
        except Exception as e:
            errors.append(f"validator: quantity_outliers failed — {e}")
            log.error(f"quantity_outliers failed: {e}")

        # 3. Price format errors (non-null values that can't parse to float)
        try:
            count = 0
            for val in df["unit_price"]:
                if pd.isna(val):
                    continue
                cleaned = str(val).strip()
                for ch in ["₹", "$", "€", "£", ",", " "]:
                    cleaned = cleaned.replace(ch, "")
                try:
                    float(cleaned)
                except ValueError:
                    count += 1
            issues["price_format_errors"] = count
            log.info(f"Price format errors: {issues['price_format_errors']}")
        except Exception as e:
            errors.append(f"validator: price_format_errors failed — {e}")
            log.error(f"price_format_errors failed: {e}")

        # 4. Invalid discounts (< 0 or > 100, excluding nulls)
        try:
            disc = pd.to_numeric(df["discount_percent"], errors="coerce")
            valid_disc = disc.dropna()
            issues["invalid_discounts"] = int(
                ((valid_disc < 0) | (valid_disc > 100)).sum()
            )
            log.info(f"Invalid discounts: {issues['invalid_discounts']}")
        except Exception as e:
            errors.append(f"validator: invalid_discounts failed — {e}")
            log.error(f"invalid_discounts failed: {e}")

        # 5. Total null cells across ALL columns
        try:
            issues["total_null_cells"] = int(df.isnull().sum().sum())
            log.info(f"Total null cells: {issues['total_null_cells']}")
        except Exception as e:
            errors.append(f"validator: total_null_cells failed — {e}")
            log.error(f"total_null_cells failed: {e}")

    except Exception as e:
        errors.append(f"validator: entire node crashed — {e}")
        fallback_used.append("validator: entire node crashed, all metrics set to 0")
        log.error(f"Entire validator node crashed: {e}")

    log.info(f"Q3 issues (LOCKED): {issues}")

    state["issues"] = issues
    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state

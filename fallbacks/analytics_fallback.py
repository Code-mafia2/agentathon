"""Analytics repair node — fallback branch for failed Q1/Q2/Q4.

Only runs when auditor routes to "fail".
Re-runs each failed Q with more aggressive cleaning.
"""

import numpy as np
import pandas as pd
from utils.logger import get_logger

log = get_logger("analytics_repair")


def analytics_repair_node(state: dict) -> dict:
    """Re-run failed Q computations with aggressive fallback approaches."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))
    df = state["df"].copy()

    # Fill NaN in numeric columns for aggressive repair
    for col in ["quantity", "unit_price", "discount_percent", "delivery_days"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Recompute revenue if needed
    if "revenue" not in df.columns or df["revenue"].isna().all():
        try:
            df["revenue"] = (
                df["quantity"]
                * df["unit_price"]
                * (1 - df["discount_percent"] / 100.0)
            )
            df.loc[df["revenue"] < 0, "revenue"] = 0
            fallback_used.append("analytics_repair: recomputed revenue with fillna(0)")
        except Exception as e:
            errors.append(f"analytics_repair: revenue recompute failed — {e}")

    # ── Repair Q1 ────────────────────────────────────────────────
    q1 = state.get("q1")
    if _needs_repair(q1):
        try:
            sub = df[["product_category", "revenue"]].dropna()
            if not sub.empty:
                rev = sub.groupby("product_category")["revenue"].sum()
                rev = rev.sort_values(ascending=False)
                lines = []
                for rank, (cat, val) in enumerate(rev.items(), 1):
                    lines.append(f"{rank}. {cat}: ₹{val:,.2f}")
                state["q1"] = "\n".join(lines)
                fallback_used.append("analytics_repair: Q1 repaired")
                log.info("Q1 repaired successfully")
            else:
                state["q1"] = "No valid data for this metric"
                fallback_used.append("analytics_repair: Q1 no data after repair")
        except Exception as e:
            state["q1"] = "Data unavailable"
            errors.append(f"analytics_repair: Q1 repair failed — {e}")
            fallback_used.append(f"analytics_repair: Q1 repair failed — {e}")

    # ── Repair Q2 ────────────────────────────────────────────────
    q2 = state.get("q2")
    if _needs_repair(q2):
        try:
            sub = df[["customer_region", "delivery_days"]].dropna()
            if not sub.empty:
                avg = sub.groupby("customer_region")["delivery_days"].mean()
                avg = avg.sort_values(ascending=False)
                lines = []
                for rank, (region, val) in enumerate(avg.items(), 1):
                    lines.append(f"{rank}. {region}: {val:.2f} days")
                state["q2"] = "\n".join(lines)
                fallback_used.append("analytics_repair: Q2 repaired")
                log.info("Q2 repaired successfully")
            else:
                state["q2"] = "No valid data for this metric"
                fallback_used.append("analytics_repair: Q2 no data after repair")
        except Exception as e:
            state["q2"] = "Data unavailable"
            errors.append(f"analytics_repair: Q2 repair failed — {e}")
            fallback_used.append(f"analytics_repair: Q2 repair failed — {e}")

    # ── Repair Q4 ────────────────────────────────────────────────
    q4 = state.get("q4")
    if _needs_repair(q4):
        try:
            sub = df[["payment_method", "return_status"]].dropna()
            if not sub.empty:
                total = sub.groupby("payment_method").size()
                returned = (
                    sub[sub["return_status"] == "returned"]
                    .groupby("payment_method")
                    .size()
                )
                rate = (returned / total * 100).fillna(0.0)
                rate = rate.sort_values(ascending=False)
                lines = []
                for rank, (method, val) in enumerate(rate.items(), 1):
                    lines.append(f"{rank}. {method}: {val:.2f}%")
                state["q4"] = "\n".join(lines)
                fallback_used.append("analytics_repair: Q4 repaired")
                log.info("Q4 repaired successfully")
            else:
                state["q4"] = "No valid data for this metric"
                fallback_used.append("analytics_repair: Q4 no data after repair")
        except Exception as e:
            state["q4"] = "Data unavailable"
            errors.append(f"analytics_repair: Q4 repair failed — {e}")
            fallback_used.append(f"analytics_repair: Q4 repair failed — {e}")

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state


def _needs_repair(val) -> bool:
    """Check if a Q value needs repair."""
    if val is None:
        return True
    if isinstance(val, str):
        if val.strip() == "":
            return True
        if val.startswith("Computation error"):
            return True
    return False



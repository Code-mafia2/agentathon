"""NODE 4: Analytics — computes Q1, Q2, Q4 independently.

Each question is independently try/excepted. Q2 failing never kills Q1.
All computation is pure pandas — no LLM calls.
"""

from utils.logger import get_logger

log = get_logger("analytics")


def analytics_node(state: dict) -> dict:
    """Compute Q1 (revenue by category), Q2 (avg delivery by region),
    Q4 (return rate by payment method)."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))
    df = state["df"]

    # ── Q1: Revenue by product_category ──────────────────────────
    try:
        sub = df[["product_category", "revenue"]].dropna()
        if sub.empty:
            state["q1"] = "No valid data for this metric"
            log.warning("Q1: no valid data after dropna")
        else:
            rev = sub.groupby("product_category")["revenue"].sum()
            rev = rev.sort_values(ascending=False)
            lines = []
            for rank, (cat, val) in enumerate(rev.items(), 1):
                lines.append(f"{rank}. {cat}: ₹{val:,.2f}")
            state["q1"] = "\n".join(lines)
            log.info(f"Q1 computed — {len(rev)} categories")
    except KeyError as e:
        state["q1"] = f"Column not found in data"
        errors.append(f"analytics: Q1 column missing — {e}")
        log.error(f"Q1 column missing: {e}")
    except Exception as e:
        state["q1"] = f"Computation error: {str(e)}"
        errors.append(f"analytics: Q1 failed — {e}")
        log.error(f"Q1 failed: {e}")

    # ── Q2: Avg delivery days by customer_region ─────────────────
    try:
        sub = df[["customer_region", "delivery_days"]].dropna()
        if sub.empty:
            state["q2"] = "No valid data for this metric"
            log.warning("Q2: no valid data after dropna")
        else:
            avg = sub.groupby("customer_region")["delivery_days"].mean()
            avg = avg.sort_values(ascending=False)
            lines = []
            for rank, (region, val) in enumerate(avg.items(), 1):
                lines.append(f"{rank}. {region}: {val:.2f} days")
            state["q2"] = "\n".join(lines)
            log.info(f"Q2 computed — {len(avg)} regions")
    except KeyError as e:
        state["q2"] = f"Column not found in data"
        errors.append(f"analytics: Q2 column missing — {e}")
        log.error(f"Q2 column missing: {e}")
    except Exception as e:
        state["q2"] = f"Computation error: {str(e)}"
        errors.append(f"analytics: Q2 failed — {e}")
        log.error(f"Q2 failed: {e}")

    # ── Q4: Return rate by payment_method ────────────────────────
    try:
        sub = df[["payment_method", "return_status"]].dropna()
        if sub.empty:
            state["q4"] = "No valid data for this metric"
            log.warning("Q4: no valid data after dropna")
        else:
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
            log.info(f"Q4 computed — {len(rate)} payment methods")
    except KeyError as e:
        state["q4"] = f"Column not found in data"
        errors.append(f"analytics: Q4 column missing — {e}")
        log.error(f"Q4 column missing: {e}")
    except Exception as e:
        state["q4"] = f"Computation error: {str(e)}"
        errors.append(f"analytics: Q4 failed — {e}")
        log.error(f"Q4 failed: {e}")

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state

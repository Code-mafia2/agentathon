"""NODE 5: Auditor — validates Q1, Q2, Q4 results and routes the pipeline.

Sets state["audit_status"] to "pass" or "fail".
The conditional edge in the graph reads this to decide next node.
"""

import re
from utils.logger import get_logger

log = get_logger("auditor")


def auditor_node(state: dict) -> dict:
    """Validate analytics results and set audit_status."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))
    failures = []

    # Check 1: q1, q2, q4 are non-None, non-empty strings
    for key in ("q1", "q2", "q4"):
        val = state.get(key)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            failures.append(f"{key} is None or empty")

    # Check 2: q1, q2, q4 do not start with "Computation error"
    for key in ("q1", "q2", "q4"):
        val = state.get(key)
        if isinstance(val, str) and val.startswith("Computation error"):
            failures.append(f"{key} contains computation error: {val[:80]}")

    # Check 3: df["revenue"] has at least 1 non-NaN value
    try:
        df = state["df"]
        if "revenue" not in df.columns or df["revenue"].notna().sum() == 0:
            failures.append("revenue column has no valid values")
    except Exception as e:
        failures.append(f"revenue check failed: {e}")

    # Check 4: df has at least 1 row
    try:
        if len(state["df"]) == 0:
            failures.append("df has 0 rows")
    except Exception as e:
        failures.append(f"row count check failed: {e}")

    # Check 5: Any return rate value is between 0 and 100
    try:
        q4_val = state.get("q4", "")
        if q4_val and not q4_val.startswith("Computation error"):
            # Extract percentage values from Q4 string
            pct_values = re.findall(r"([\d.]+)%", q4_val)
            if pct_values:
                for pct_str in pct_values:
                    pct = float(pct_str)
                    if pct < 0 or pct > 100:
                        failures.append(f"Q4 return rate {pct}% out of range [0,100]")
                        break
    except Exception as e:
        failures.append(f"Q4 range check failed: {e}")

    if failures:
        state["audit_status"] = "fail"
        for f in failures:
            errors.append(f"auditor: FAIL — {f}")
            log.warning(f"Audit FAIL: {f}")
        log.info(f"Audit result: FAIL ({len(failures)} issue(s))")
    else:
        state["audit_status"] = "pass"
        log.info("Audit result: PASS — all checks OK")

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state

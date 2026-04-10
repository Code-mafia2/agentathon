"""NODE 8: Formatter — strict output assembly. No logic, only formatting."""

from utils.logger import get_logger

log = get_logger("formatter")


def formatter_node(state: dict) -> dict:
    """Assemble the final output string from all Q answers."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))

    try:
        issues = state.get("issues", {})

        # Safe access with fallback for each Q
        q1 = state.get("q1") or "Data unavailable"
        q2 = state.get("q2") or "Data unavailable"
        q4 = state.get("q4") or "Data unavailable"
        q5 = state.get("q5") or "Data unavailable"

        # Build Q3 string from issues dict
        q3_str = (
            f"Duplicate order IDs: {issues.get('duplicate_order_ids', 0)}, "
            f"Quantity outliers (>1000): {issues.get('quantity_outliers', 0)}, "
            f"Price format errors: {issues.get('price_format_errors', 0)}, "
            f"Invalid discounts: {issues.get('invalid_discounts', 0)}, "
            f"Total null cells: {issues.get('total_null_cells', 0)}"
        )

        output = (
            f"Q1: {q1}\n\n"
            f"Q2: {q2}\n\n"
            f"Q3: {q3_str}\n\n"
            f"Q4: {q4}\n\n"
            f"Q5: {q5}\n"
        )

        state["output"] = output
        log.info("Output formatted successfully")

    except Exception as e:
        errors.append(f"formatter: assembly failed — {e}")
        fallback_used.append("formatter: assembly failed, using minimal output")
        log.error(f"Formatter failed: {e}")

        # Emergency fallback — always produce something
        state["output"] = (
            f"Q1: {state.get('q1', 'Data unavailable')}\n\n"
            f"Q2: {state.get('q2', 'Data unavailable')}\n\n"
            f"Q3: Data unavailable\n\n"
            f"Q4: {state.get('q4', 'Data unavailable')}\n\n"
            f"Q5: {state.get('q5', 'Data unavailable')}\n"
        )

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state

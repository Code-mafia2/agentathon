"""Deterministic Q5 fallback — no LLM required.

FB-4: Build Q5 from parsed Q1/Q2/Q4 strings.
FB-5: Hardcoded safe string if template parsing fails.
"""

import re
from utils.logger import get_logger

log = get_logger("groq_fallback")

# FB-5: Hardcoded safe string — always works, no dependencies
HARDCODED_SAFE_STRING = (
    "Revenue analysis reveals strong category-level differentiation with "
    "top segments significantly outperforming others in total contribution. "
    "Regional delivery performance varies considerably, with certain regions "
    "showing above-average fulfilment times that require logistics review. "
    "Data quality issues including nulls and duplicates indicate the need "
    "for upstream validation before business-critical decisions are made."
)


def _parse_first_line(text: str, pattern: str) -> dict:
    """Parse the first ranked line from a Q result string.

    Example input for Q1: "1. Electronics: ₹1,234,567.89"
    Returns: {"label": "Electronics", "value": "₹1,234,567.89"}
    """
    if not text:
        return {"label": "Unknown", "value": "N/A"}

    first_line = text.strip().split("\n")[0]
    match = re.match(pattern, first_line)
    if match:
        return match.groupdict()
    return {"label": "Unknown", "value": "N/A"}


def build_deterministic_q5(
    q1: str, q2: str, q4: str, issues: dict
) -> str:
    """Build a deterministic 3-sentence Q5 from pre-computed Q results.

    This is FB-4 — called when Groq API is unavailable.
    """
    # Parse top category from Q1: "1. CategoryName: ₹123,456.78"
    q1_parsed = _parse_first_line(
        q1, r"\d+\.\s+(?P<label>[^:]+):\s+(?P<value>₹[\d,]+\.?\d*)"
    )
    top_category = q1_parsed["label"]
    top_revenue = q1_parsed["value"]

    # Parse slowest region from Q2: "1. RegionName: 12.34 days"
    q2_parsed = _parse_first_line(
        q2, r"\d+\.\s+(?P<label>[^:]+):\s+(?P<value>[\d.]+\s*days)"
    )
    slowest_region = q2_parsed["label"]
    slowest_days = q2_parsed["value"]

    # Parse highest return method from Q4: "1. MethodName: 12.34%"
    q4_parsed = _parse_first_line(
        q4, r"\d+\.\s+(?P<label>[^:]+):\s+(?P<value>[\d.]+%)"
    )
    highest_return_method = q4_parsed["label"]
    highest_return_pct = q4_parsed["value"]

    total_nulls = issues.get("total_null_cells", 0)
    dup_ids = issues.get("duplicate_order_ids", 0)

    template = (
        f"{top_category} leads all product categories in revenue at "
        f"{top_revenue}, signaling strong consumer demand in this segment. "
        f"The {slowest_region} region records the highest average delivery "
        f"time at {slowest_days}, while {highest_return_method} carries "
        f"the highest return rate at {highest_return_pct}, both requiring "
        f"immediate operational attention. "
        f"With {total_nulls} null cells and "
        f"{dup_ids} duplicate order IDs identified, "
        f"upstream data quality improvements are critical for reliable "
        f"business intelligence."
    )

    log.info("Deterministic Q5 template built successfully")
    return template

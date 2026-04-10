"""Shared state schema for the RetailIQ LangGraph pipeline."""

from typing import TypedDict, Optional, Any


class RetailIQState(TypedDict):
    raw_df:        Any              # pd.DataFrame — NEVER mutated after set
    df:            Any              # pd.DataFrame — working clean copy
    issues:        dict             # Q3 counts — written ONCE in validator
    q1:            Optional[str]
    q2:            Optional[str]
    q4:            Optional[str]
    q5:            Optional[str]
    output:        Optional[str]
    errors:        list             # append-only log of all errors
    fallback_used: list             # append-only audit trail of fallbacks
    audit_status:  Optional[str]    # "pass" or "fail" — set by auditor

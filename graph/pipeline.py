"""LangGraph pipeline — StateGraph with exactly 1 conditional edge."""

from langgraph.graph import StateGraph, END
from utils.state import RetailIQState
from nodes.validator import validator_node
from nodes.cleaner import cleaner_node
from nodes.feature import feature_node
from nodes.analytics import analytics_node
from nodes.auditor import auditor_node
from nodes.insight import insight_node
from nodes.formatter import formatter_node
from fallbacks.analytics_fallback import analytics_repair_node
from utils.logger import get_logger

log = get_logger("pipeline")


def route_after_audit(state: RetailIQState) -> str:
    """Route based on auditor's decision — the ONLY conditional edge."""
    result = state.get("audit_status", "pass")
    log.info(f"Audit routing decision: {result}")
    return result


def build_graph():
    """Build and compile the RetailIQ LangGraph pipeline."""
    graph = StateGraph(RetailIQState)

    # Register all nodes
    graph.add_node("validator",        validator_node)
    graph.add_node("cleaner",          cleaner_node)
    graph.add_node("feature",          feature_node)
    graph.add_node("analytics",        analytics_node)
    graph.add_node("auditor",          auditor_node)
    graph.add_node("analytics_repair", analytics_repair_node)
    graph.add_node("insight",          insight_node)
    graph.add_node("formatter",        formatter_node)

    # Linear edges
    graph.set_entry_point("validator")
    graph.add_edge("validator",  "cleaner")
    graph.add_edge("cleaner",    "feature")
    graph.add_edge("feature",    "analytics")
    graph.add_edge("analytics",  "auditor")

    # Only conditional edge in the entire graph
    graph.add_conditional_edges(
        "auditor",
        route_after_audit,
        {
            "pass": "insight",
            "fail": "analytics_repair",
        }
    )

    graph.add_edge("analytics_repair", "insight")
    graph.add_edge("insight",          "formatter")
    graph.add_edge("formatter",        END)

    log.info("Pipeline graph compiled")
    return graph.compile()

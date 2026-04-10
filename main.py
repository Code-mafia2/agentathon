"""RetailIQ Agent — main entrypoint.

Usage:
    python main.py <csv_path> [team_name]

Example:
    python main.py train_data.csv team-name
    python main.py test_data.csv team-name
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
from utils.loader import load_csv
from utils.state import RetailIQState
from utils.logger import get_logger
from graph.pipeline import build_graph

load_dotenv()
log = get_logger("main")


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <csv_path> [team_name]")
        sys.exit(1)

    csv_path  = sys.argv[1]
    team_name = sys.argv[2] if len(sys.argv) > 2 else "heapify"

    log.info(f"Input: {csv_path}")
    log.info(f"Team:  {team_name}")

    # Load CSV before graph starts
    raw_df = load_csv(csv_path)

    # Initialize state — raw_df is set HERE and NEVER touched again
    initial_state: RetailIQState = {
        "raw_df":        raw_df,
        "df":            raw_df.copy(),
        "issues":        {},
        "q1":            None,
        "q2":            None,
        "q4":            None,
        "q5":            None,
        "output":        None,
        "errors":        [],
        "fallback_used": [],
        "audit_status":  None,
    }

    # Build and run the LangGraph pipeline
    graph = build_graph()
    final_state = graph.invoke(initial_state)

    # Write output file
    output_path = Path(f"output/{team_name}.txt")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(final_state["output"], encoding="utf-8")

    # Print audit summary
    print(f"\n✅ Output written → {output_path}")
    if final_state["fallback_used"]:
        print(f"⚠️  Fallbacks triggered: {len(final_state['fallback_used'])}")
        for fb in final_state["fallback_used"]:
            print(f"   - {fb}")
    if final_state["errors"]:
        print(f"⚠️  Errors logged: {len(final_state['errors'])}")

    # Print output for verification
    print("\n" + "=" * 50)
    print(final_state["output"])


if __name__ == "__main__":
    main()

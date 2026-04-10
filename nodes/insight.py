"""NODE 7: Insight — generates Q5 via Groq LLM with 5-layer fallback chain.

FB-1: API timeout  → retry after 3s
FB-2: Rate limit   → wait 10s, retry
FB-3: Not 3 sentences → re-prompt stricter
FB-4: Groq unavailable → deterministic template
FB-5: Template fails → hardcoded safe string
"""

import os
import re
import time
from dotenv import load_dotenv
from utils.logger import get_logger
from fallbacks.groq_fallback import build_deterministic_q5, HARDCODED_SAFE_STRING

log = get_logger("insight")

load_dotenv()


def _count_sentences(text: str) -> int:
    """Count sentences ending with . ! or ?"""
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s for s in sentences if s.strip()]
    return len(sentences)


def _extract_q5_text(response) -> str:
    """Extract text from Groq response object."""
    return response.choices[0].message.content.strip()


def insight_node(state: dict) -> dict:
    """Generate Q5 business insight using Groq LLM with full fallback chain."""
    errors = list(state.get("errors", []))
    fallback_used = list(state.get("fallback_used", []))

    q1 = state.get("q1", "")
    q2 = state.get("q2", "")
    q4 = state.get("q4", "")
    issues = state.get("issues", {})

    # Build the main prompt
    prompt = f"""You are a business analyst writing for an executive audience.

Based ONLY on these computed metrics — do not invent numbers:

Revenue by category:
{q1}

Average delivery days by region:
{q2}

Return rate by payment method:
{q4}

Write EXACTLY 3 sentences summarizing the business health.
Rules:
- Exactly 3 sentences. Count them. 3.
- No bullet points, no headers, no lists.
- Reference specific numbers from the data above.
- Do not invent any numbers not shown above.
- Plain paragraph text only.
"""

    # Try Groq API
    try:
        GROQ_API_KEY = os.getenv("GROQ_API_KEY")
        if not GROQ_API_KEY:
            raise EnvironmentError("GROQ_API_KEY not found in .env")

        from groq import Groq
        client = Groq(api_key=GROQ_API_KEY)

        # Primary attempt
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.3,
                timeout=10,
            )
            q5_text = _extract_q5_text(response)

        except Exception as timeout_err:
            err_str = str(timeout_err).lower()

            # FB-1: Timeout
            if "timeout" in err_str or "timed out" in err_str:
                log.warning("FB-1: API timeout, retrying after 3s")
                fallback_used.append("insight_node: timeout retry")
                time.sleep(3)
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=300,
                    temperature=0.3,
                    timeout=15,
                )
                q5_text = _extract_q5_text(response)

            # FB-2: Rate limit
            elif "429" in err_str or "rate" in err_str:
                log.warning("FB-2: Rate limit, waiting 10s")
                fallback_used.append("insight_node: rate_limit retry")
                time.sleep(10)
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=300,
                    temperature=0.3,
                    timeout=15,
                )
                q5_text = _extract_q5_text(response)
            else:
                raise  # Let outer except catch it

        # FB-3: Validate sentence count
        sentence_count = _count_sentences(q5_text)
        if sentence_count != 3:
            log.warning(
                f"FB-3: Got {sentence_count} sentences, re-prompting"
            )
            fallback_used.append("insight_node: sentence_count retry")

            # Extract first lines for context
            q1_line1 = (q1.split("\n")[0] if q1 else "N/A")
            q2_line1 = (q2.split("\n")[0] if q2 else "N/A")
            q4_line1 = (q4.split("\n")[0] if q4 else "N/A")

            strict_prompt = (
                "Respond with exactly 3 sentences separated by periods. "
                "No more. No less. Use data from: "
                f"{q1_line1}, {q2_line1}, {q4_line1}"
            )
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": strict_prompt}],
                max_tokens=300,
                temperature=0.2,
                timeout=10,
            )
            q5_text = _extract_q5_text(response)

            # If still not 3, accept whatever we got
            final_count = _count_sentences(q5_text)
            if final_count != 3:
                log.warning(
                    f"FB-3 retry still got {final_count} sentences, accepting"
                )

        state["q5"] = q5_text
        log.info("Q5 generated via Groq LLM")

    except Exception as groq_err:
        # FB-4: Groq unavailable → deterministic template
        log.warning(f"FB-4: Groq unavailable ({groq_err}), using template")
        fallback_used.append(
            f"insight_node: groq_unavailable → deterministic_template"
        )
        errors.append(f"insight: Groq API failed — {groq_err}")

        try:
            q5_text = build_deterministic_q5(q1, q2, q4, issues)
            state["q5"] = q5_text
            log.info("Q5 generated via deterministic template (FB-4)")

        except Exception as template_err:
            # FB-5: Template itself failed → hardcoded safe string
            log.warning(f"FB-5: Template failed ({template_err}), using hardcoded")
            fallback_used.append(
                "insight_node: template_failed → hardcoded_fallback"
            )
            errors.append(f"insight: template failed — {template_err}")
            state["q5"] = HARDCODED_SAFE_STRING
            log.info("Q5 set to hardcoded safe string (FB-5)")

    state["errors"] = errors
    state["fallback_used"] = fallback_used
    return state

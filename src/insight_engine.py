"""
insight_engine.py

Turns raw metrics into something a manager can actually act on.
Structures everything as: Problem → Cause → Action → Impact.
"""


def generate_smart_insight(
    bottleneck_task: str,
    avg_duration: float,
    wait_pct: float,
    recommendation: dict
) -> dict:
    """Link the bottleneck data to a clear decision chain.

    Combines the detected problem, its likely cause, a recommended action,
    and the expected impact into one structured block.
    """

    problem = f"**{bottleneck_task}** is causing significant process delays (avg {avg_duration:,.1f}m)."

    # high waiting % usually means queue backup, not task complexity
    if wait_pct > 70:
        cause = f"High manual dependency with {wait_pct:.1f}% idle waiting time."
    else:
        cause = "High task variability and execution complexity slowing down completion."

    action = recommendation.get("recommendation_text", "Optimize the process flow to reduce manual overhead.")
    impact = f"Reduce delay by {recommendation.get('estimated_savings', '20-30%')} and improve overall completion time."

    return {
        "problem": problem,
        "cause": cause,
        "action": action,
        "impact": impact
    }

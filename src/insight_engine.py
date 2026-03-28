"""
src/insight_engine.py

Synthesizes data from various detectors into a structured, business-ready 'Smart Insight'.
Links problem, cause, action, and impact in a logical chain.
"""

def generate_smart_insight(
    bottleneck_task: str, 
    avg_duration: float, 
    wait_pct: float,
    recommendation: dict
) -> dict:
    """Combines metrics and recommendations into a structured decision chain."""
    
    problem = f"**{bottleneck_task}** is causing significant process delays (avg {avg_duration:,.1f}m)."
    
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

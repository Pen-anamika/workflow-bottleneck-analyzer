"""
src/health_analyzer.py

Computes an overall 'Workflow Health Score' by aggregating SLA compliance, 
bottleneck intensity, exceptions, and idle waiting ratios.
"""

import pandas as pd

def calculate_workflow_health(
    df: pd.DataFrame, 
    bottleneck_pct: float, 
    total_exception_cases: int
) -> dict:
    """Calculate a 0-100 health score for the entire workflow.
    
    Weights:
    - SLA Violation Rate: 30%
    - Bottleneck Impact:   30%
    - Exception Rate:      20%
    - Waiting Time Ratio:  20%
    """
    if df.empty:
        return {"score": 0, "status": "No Data", "interpretation": "No data available."}

    # 1. SLA Violation Rate (0-100)
    sla_violation_rate = (df["sla_violation"].sum() / len(df)) * 100
    
    # 2. Bottleneck Impact (0-100) -- using bottleneck_pct directly
    bottleneck_score = bottleneck_pct
    
    # 3. Exception Rate (0-100)
    total_cases = df["case_id"].nunique()
    exception_rate = (total_exception_cases / total_cases) * 100 if total_cases > 0 else 0
    
    # 4. Waiting Time Ratio (0-100)
    total_duration = df["duration_minutes"].sum()
    total_waiting = df["waiting_time_minutes"].sum()
    waiting_ratio = (total_waiting / total_duration) * 100 if total_duration > 0 else 0
    
    # Calculate weighted penalties individually
    penalties = [
        ("SLA violation rate", sla_violation_rate * 0.3),
        ("Bottleneck impact", bottleneck_score * 0.3),
        ("Workflow exceptions", exception_rate * 0.2),
        ("Waiting-to-processing ratio", waiting_ratio * 0.2)
    ]
    
    total_penalty = sum(p[1] for p in penalties)
    health_score = max(0, min(100, 100 - total_penalty))
    
    # Identify top 2 contributors to the penalty
    # Only include if they have a non-zero impact
    sorted_penalties = sorted(penalties, key=lambda x: x[1], reverse=True)
    top_contributors = [p[0] for p in sorted_penalties if p[1] > 0][:2]
    
    # Status and Interpretation
    if health_score >= 80:
        status = "Healthy"
        interpretation = "Workflow is performing optimally with minimal delays and high compliance."
    elif health_score >= 50:
        status = "Moderate"
        interpretation = "Workflow is moderately efficient but affected by specific operational constraints."
    else:
        status = "Critical"
        interpretation = "Workflow efficiency is low. Significant bottlenecks and non-compliance detected."
        
    return {
        "score": round(health_score, 1),
        "status": status,
        "interpretation": interpretation,
        "top_contributors": top_contributors,
        "metrics": {
            "sla_viol_rate": round(sla_violation_rate, 1),
            "btl_impact": round(bottleneck_score, 1),
            "exc_rate": round(exception_rate, 1),
            "wait_ratio": round(waiting_ratio, 1)
        }
    }

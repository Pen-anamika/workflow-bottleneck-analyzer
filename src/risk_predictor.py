"""
risk_predictor.py

Predicts workflow risk using a simple heuristic combining task duration and variability.
"""

import pandas as pd


def predict_bottleneck_risk(df: pd.DataFrame, stats_df: pd.DataFrame) -> list[dict]:
    """Calculate a 0-100 risk score and identify specific contributing factors.
    
    Higher average duration + higher variability + frequency = higher risk.
    """
    if "task" not in df.columns or "duration_minutes" not in df.columns:
        return []

    # Calculate standard deviation and execution counts
    task_metrics = df.groupby("task")["duration_minutes"].agg(["std", "count"]).fillna(0)
    
    # Global averages for factor identification
    global_avg_dur = stats_df["avg_duration_minutes"].mean()
    global_avg_std = task_metrics["std"].mean()
    global_avg_count = task_metrics["count"].mean()
    
    risks = []
    
    # Get max values to normalize against (scaling 0 to 1)
    max_avg = stats_df["avg_duration_minutes"].max() if not stats_df.empty else 1
    max_std = task_metrics["std"].max() if not task_metrics.empty else 1
    
    if max_avg <= 0: max_avg = 1
    if max_std <= 0: max_std = 1
    
    for task in stats_df.index:
        avg_dur = stats_df.loc[task, "avg_duration_minutes"]
        std_dur = task_metrics.loc[task, "std"]
        count_val = task_metrics.loc[task, "count"]
        
        # Risk score (weighted: 50% slow, 30% unpredictable, 20% volume)
        dur_score = (avg_dur / max_avg) * 50
        var_score = (std_dur / max_std) * 30
        vol_score = (count_val / task_metrics["count"].max()) * 20
        
        risk_score = round(dur_score + var_score + vol_score, 1)
        
        # Identify Contributing Factors
        factors = []
        if avg_dur > global_avg_dur * 1.2:
            factors.append("High average duration")
        if std_dur > global_avg_std * 1.2:
            factors.append("High variability in execution time")
        if count_val > global_avg_count * 1.2:
            factors.append("High frequency of occurrence")
            
        # Select best explanation based on score
        if risk_score >= 70:
            explanation = "Critical bottleneck risk: Combination of high impact and instability."
        elif risk_score >= 40:
            explanation = "Moderate operational risk: Monitor for process efficiency."
        else:
            explanation = "Low procedural risk: Tasks are performing within normal parameters."
            
        risks.append({
            "task": task,
            "risk_score": risk_score,
            "explanation": explanation,
            "factors": factors
        })
        
    # Sort descending by risk score
    risks.sort(key=lambda x: x["risk_score"], reverse=True)
    return risks

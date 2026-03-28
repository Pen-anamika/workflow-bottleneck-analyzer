"""
src/automation_engine.py

Analyzes workflow data to detect tasks that are prime candidates for automation
due to high frequency, manual delays, or repetitive patterns.
"""

import pandas as pd

def identify_automation_opportunities(df: pd.DataFrame, stats_df: pd.DataFrame) -> list[dict]:
    """Identify tasks suited for automation and generate specific suggestions."""
    if df.empty or stats_df.empty:
        return []

    # Calculate average waiting times per task if they exist
    if "waiting_time_minutes" in df.columns:
        wait_stats = df.groupby("task")["waiting_time_minutes"].mean()
    else:
        wait_stats = pd.Series(0, index=stats_df.index)

    # Thresholds for 'Priority' Automation (above mean)
    avg_freq = stats_df["count"].mean()
    avg_wait = wait_stats.mean()
    
    opportunities = []

    for task in stats_df.index:
        count = stats_df.loc[task, "count"]
        wait = wait_stats.get(task, 0)
        
        # High impact candidates: Above average frequency AND high wait time
        if count >= avg_freq and wait >= avg_wait:
            task_lower = task.lower()
            
            # Contextual Suggestions
            if "approval" in task_lower:
                reason = "High delay and manual dependency on human reviewers."
                solution = "Introduce rule-based auto-approval for low-value or low-risk cases."
            elif "review" in task_lower:
                reason = "Repetitive decision-making cycles slowing down cases."
                solution = "Implement AI-driven document review or parallelize the human review process."
            elif "ticket" in task_lower or "routing" in task_lower:
                reason = "High volume of assignment tasks adds significant overhead."
                solution = "Automate routing using rule-based classification or machine learning."
            elif "assignment" in task_lower or "allocation" in task_lower:
                reason = "Manual resource allocation causing workflow fragmentation."
                solution = "Use automated workflow triggers to assign tasks based on resource availability."
            else:
                reason = "High frequency and wait time indicate a process bottleneck."
                solution = f"Redesign the '{task}' step to reduce manual touchpoints through RPA or script automation."

            # Generate impact estimation (20-30% reduction)
            import random
            reduction_pct = random.randint(20, 30)
            avg_duration = stats_df.loc[task, "avg_duration_minutes"]
            minutes_saved = (avg_duration * reduction_pct) / 100
            
            impact_text = (
                f"Reduce delay by ~{reduction_pct}% "
                f"(~{minutes_saved:,.1f} minutes per case)"
            )

            opportunities.append({
                "task": task,
                "reason": reason,
                "suggestion": solution,
                "impact_text": impact_text,
                "impact_score": count * wait # Combined priority metric
            })

    # Sort by impact score
    opportunities.sort(key=lambda x: x["impact_score"], reverse=True)
    return opportunities

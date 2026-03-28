"""
src/context_analyzer.py

Identifies the industry and operational context (human vs system) for a given workflow log.
"""

import pandas as pd

def detect_workflow_context(df: pd.DataFrame) -> dict:
    """Identify workflow industry, human-involved steps, and system-driven steps."""
    if df.empty or "task" not in df.columns or "user" not in df.columns:
        return {
            "workflow_type": "Unknown",
            "human_tasks": [],
            "system_tasks": []
        }

    unique_tasks = df["task"].unique()
    task_str = " ".join(unique_tasks).lower()
    
    # 1. Detect Workflow Type
    if any(k in task_str for k in ["lead", "proposal", "deal"]):
        w_type = "Sales Workflow"
    elif any(k in task_str for k in ["ticket", "support", "incident"]):
        w_type = "Customer Support Workflow"
    elif any(k in task_str for k in ["onboarding", "hr", "hire"]):
        w_type = "Employee Onboarding Workflow"
    else:
        w_type = "Generic Business Workflow"
        
    # 2. Divide tasks by human vs system
    human_tasks = []
    system_tasks = []
    
    for task in unique_tasks:
        # Check if this task is primarily performed by a non-system user
        users = df.loc[df["task"] == task, "user"].unique()
        if len(users) == 1 and users[0].lower() == "system":
            system_tasks.append(task)
        else:
            human_tasks.append(task)
            
    return {
        "workflow_type": w_type,
        "human_tasks": sorted(human_tasks),
        "system_tasks": sorted(system_tasks)
    }

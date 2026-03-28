"""
bottleneck_detector.py

Analyses preprocessed workflow event logs to surface bottleneck tasks —
those steps that consistently take the longest and therefore slow down
the overall workflow.
"""

import pandas as pd


def detect_bottlenecks(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, str, float, float]:
    """Detect the workflow bottleneck and quantify its impact.

    How bottleneck detection works
    ------------------------------
    1. Group all event rows by their ``task`` name.
    2. For each task compute three summary statistics:
       • **avg_duration_minutes** — arithmetic mean of durations.
         A high average indicates that the task is *consistently* slow.
       • **median_duration_minutes** — the 50th-percentile duration.
         The median is more robust to outliers than the mean and helps
         confirm whether the high average is representative.
       • **count** — number of times the task was executed.
         Ensures the statistic is backed by sufficient data.
    3. The task with the **highest average duration** is flagged as
       the bottleneck, because it represents the step where cases
       spend the most wall-clock time on average before moving forward.

    Bottleneck impact calculation
    -----------------------------
    Once the bottleneck is identified, two impact metrics are computed:

    • **total_bottleneck_time** — the *sum* of ``duration_minutes``
      across every execution of the bottleneck task.  This measures the
      absolute amount of time that has been consumed by that step in
      aggregate (across all cases in the log).

    • **bottleneck_pct** — the bottleneck task's total time expressed
      as a percentage of *all* task time in the entire workflow:

          bottleneck_pct = (total_bottleneck_time / total_workflow_time) × 100

      A value close to 100 % means the workflow is almost entirely
      bottlenecked at that single step; a lower value means time is
      more evenly spread.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed workflow DataFrame that must contain at least
        the columns ``task`` and ``duration_minutes``
        (as produced by :func:`preprocessing.preprocess_workflow`).

    Returns
    -------
    tuple[pd.DataFrame, str, float, float]
        * ``task_stats``         — DataFrame of per-task statistics sorted
                                   by average duration (descending).
        * ``bottleneck_task``    — Name of the detected bottleneck task.
        * ``total_bn_time``      — Total minutes consumed by the bottleneck
                                   task across all executions.
        * ``bottleneck_pct``     — Percentage of total workflow time
                                   consumed by the bottleneck task (0–100).

    Raises
    ------
    ValueError
        If ``task`` or ``duration_minutes`` columns are missing.
    """

    # ------------------------------------------------------------------
    # 0. Validate that the expected columns are present
    # ------------------------------------------------------------------
    required = {"task", "duration_minutes"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns for bottleneck detection: "
            f"{', '.join(sorted(missing))}"
        )

    # ------------------------------------------------------------------
    # 1. Group by task and compute summary statistics
    #
    #    We use .agg() with a dictionary so each metric gets a clear
    #    column name in the resulting DataFrame.
    # ------------------------------------------------------------------
    task_stats = (
        df.groupby("task")["duration_minutes"]
        .agg(
            avg_duration_minutes="mean",       # average time per execution
            median_duration_minutes="median",  # middle value (robust to outliers)
            count="count",                     # how many times the task ran
        )
    )

    # ------------------------------------------------------------------
    # 2. Round for readability and sort so the slowest task is on top
    # ------------------------------------------------------------------
    task_stats = task_stats.round(2)
    task_stats = task_stats.sort_values("avg_duration_minutes", ascending=False)

    # ------------------------------------------------------------------
    # 3. Identify the bottleneck
    #
    #    The bottleneck is defined as the task whose average duration is
    #    the highest.  This is a simple but effective heuristic: the
    #    step where work items spend the most wall-clock time is the
    #    primary constraint on overall throughput.
    # ------------------------------------------------------------------
    bottleneck_task: str = task_stats["avg_duration_minutes"].idxmax()

    # ------------------------------------------------------------------
    # 4. Calculate bottleneck impact
    #
    #    a) Total time in the bottleneck step — sum of durations for
    #       every execution of that task across all cases.
    #    b) Total workflow time — sum of durations for ALL tasks in the
    #       entire log (every step, every case).
    #    c) Percentage share — what fraction of all workflow time is
    #       consumed by the bottleneck step alone.
    # ------------------------------------------------------------------
    # a) Sum of duration_minutes only for rows matching the bottleneck task
    total_bn_time: float = round(
        df.loc[df["task"] == bottleneck_task, "duration_minutes"].sum(), 2
    )

    # b) Grand total across all tasks
    total_workflow_time: float = df["duration_minutes"].sum()

    # c) Percentage — guard against zero-division on empty data
    if total_workflow_time > 0:
        bottleneck_pct: float = round(
            (total_bn_time / total_workflow_time) * 100, 2
        )
    else:
        bottleneck_pct = 0.0

    # ------------------------------------------------------------------
    # 5. Print a human-readable summary
    # ------------------------------------------------------------------
    print("📊  Task Duration Statistics (sorted by avg duration):\n")
    print(task_stats.to_string())
    print(
        f"\n🔴  Detected bottleneck → '{bottleneck_task}' "
        f"(avg {task_stats.loc[bottleneck_task, 'avg_duration_minutes']:.2f} min)"
    )
    print(
        f"📌  Impact: {total_bn_time:,.0f} min total "
        f"({bottleneck_pct:.1f}% of all workflow time)"
    )
    return task_stats, bottleneck_task, total_bn_time, bottleneck_pct


def generate_bottleneck_insight(
    bottleneck_task: str, avg_duration: float, num_executions: int
) -> tuple[str, str]:
    """Generate a simple automated explanation of the detected bottleneck.

    Parameters
    ----------
    bottleneck_task : str
        The name of the task identified as the bottleneck.
    avg_duration : float
        The average duration of the bottleneck task in minutes.
    num_executions : int
        The number of times the bottleneck task was executed.

    Returns
    -------
    tuple[str, str]
        * explanation : A short explanation describing why the step might be a bottleneck.
        * improvement : A suggested improvement (e.g., automation, reviewers).
    """
    explanation = (
        f"The '{bottleneck_task}' step is slowing down the overall process. "
        f"It takes an average of {avg_duration:,.1f} minutes to complete, "
        f"and it has been executed {num_executions:,} times. This high duration "
        f"causes a backlog in the workflow timeline."
    )

    task_lower = bottleneck_task.lower()
    if "approval" in task_lower or "review" in task_lower:
        improvement = "Consider introducing automated approval rules for low-risk cases or adding additional reviewers."
    elif "create" in task_lower or "generat" in task_lower:
        improvement = "Consider pre-fetching required data or using templates to accelerate generation."
    else:
        improvement = "Consider automating parts of this step or reallocating resources to reduce the workload."

    return explanation, improvement


def generate_recommendation(
    task_name: str, avg_duration: float, execution_count: int
) -> dict:
    """Generate a dynamic, context-aware recommendation to optimise the bottleneck.

    Combines task-name pattern matching (Approval / Review / Routing / Assignment)
    with data-driven signals (very high avg_duration, high execution_count) to
    produce a concise 2–3 line operational suggestion.

    Parameters
    ----------
    task_name : str
        Name of the bottleneck task.
    avg_duration : float
        Average duration in minutes.
    execution_count : int
        Number of times the task was executed.

    Returns
    -------
    dict
        ``recommendation_text`` — tailored suggestion string.
        ``estimated_savings``   — projected improvement range (dynamic).
    """
    task_lower = task_name.lower()
    parts: list[str] = []

    # ── 1. Task-type specific core suggestion ──────────────────────────────
    if "approval" in task_lower:
        parts.append(
            "Automate approval for low-risk cases or introduce parallel reviewers."
        )
    elif "review" in task_lower:
        parts.append(
            "Reduce review cycles or introduce clear guidelines to speed up decision making."
        )
    elif "routing" in task_lower or "assignment" in task_lower:
        parts.append(
            "Implement automated routing using business rules or AI-based classification."
        )
    else:
        parts.append(
            f"Consider redesigning or automating the '{task_name}' step to reduce manual effort."
        )

    # ── 2. Data-driven context signals ────────────────────────────────────
    signals_triggered = 0

    if avg_duration > 300:          # 5× a "normal" 60-min task
        parts.append(
            "This step is significantly slower than others and requires immediate intervention."
        )
        signals_triggered += 1
    elif avg_duration > 60:
        parts.append(
            "Duration is above average — reducing manual intervention here will yield measurable gains."
        )
        signals_triggered += 1

    if execution_count > 100:
        parts.append(
            "This step occurs frequently, so even a small speed-up will have a high compound impact."
        )
        signals_triggered += 1

    # ── 3. Build concise recommendation (max 3 sentences) ─────────────────
    recommendation_text = " ".join(parts)

    # ── 4. Dynamic savings estimate based on how bad the signals are ───────
    if signals_triggered >= 2:
        estimated_savings = "25–35%"
    elif signals_triggered == 1:
        estimated_savings = "20–30%"
    else:
        estimated_savings = "15–25%"

    return {
        "recommendation_text": recommendation_text,
        "estimated_savings": estimated_savings,
    }
def generate_priority_actions(task_stats: pd.DataFrame, total_bn_time: float) -> list[str]:
    """Rank and return the top 3 priority actions based on duration, impact, and frequency.
    
    Priority Score = (Avg Duration) * (Count) [Impact]
    """
    if task_stats.empty:
        return []

    # Calculate an 'Action Priority Score' to rank the need for intervention
    # Impact is already represented by (avg * count), which is total time spent.
    # We sort by this to find the highest aggregate 'waste' of time.
    priority_df = task_stats.copy()
    priority_df["priority_score"] = priority_df["avg_duration_minutes"] * priority_df["count"]
    priority_df = priority_df.sort_values("priority_score", ascending=False)
    
    actions = []
    
    for task_name in priority_df.head(3).index:
        task_lower = task_name.lower()
        
        if "approval" in task_lower:
            actions.append("Automate approval for low-risk cases")
        elif "review" in task_lower:
            actions.append("Introduce parallel reviewers to reduce wait time")
        elif "create" in task_lower or "generat" in task_lower:
            actions.append("Use templates to reduce manual generation steps")
        elif "assignment" in task_lower or "routing" in task_lower:
            actions.append("Implement automated rule-based task routing")
        else:
            actions.append(f"Standardize and automate the '{task_name}' process")
            
    return actions[:3]

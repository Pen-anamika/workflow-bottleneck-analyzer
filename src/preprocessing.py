"""
preprocessing.py

Prepares raw workflow event-log data for bottleneck analysis by
cleaning timestamps and computing task durations.
"""

import pandas as pd


def preprocess_workflow(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich a workflow DataFrame for bottleneck analysis.

    Processing steps:
        1. Drop rows where either timestamp is missing (NaT / NaN).
        2. Ensure timestamp columns are proper datetime objects.
        3. Compute ``duration_minutes`` — the wall-clock time each task
           took, expressed in minutes.
        4. Guarantee duration is always positive (use absolute value).
        5. Return the enriched DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw workflow log with at least ``start_time`` and ``end_time``
        columns (datetime or parseable strings).

    Returns
    -------
    pd.DataFrame
        A copy of the input with missing-timestamp rows removed and a
        new ``duration_minutes`` column added.
    """

    # Work on a copy so the caller's original DataFrame is not mutated.
    df = df.copy()

    # ------------------------------------------------------------------
    # 1. Ensure start_time and end_time are proper datetime objects
    #    If they were loaded as strings (e.g. the caller skipped
    #    data_loader), convert them here so subtraction works.
    # ------------------------------------------------------------------
    for col in ("start_time", "end_time"):
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # ------------------------------------------------------------------
    # 2. Remove rows with missing timestamps
    #    After coercion, any unparseable values become NaT.  Rows that
    #    lack either a start or end time are not useful for duration
    #    analysis, so we drop them.
    # ------------------------------------------------------------------
    before = len(df)
    df = df.dropna(subset=["start_time", "end_time"])
    dropped = before - len(df)
    if dropped:
        print(f"⚠️  Dropped {dropped} row(s) with missing timestamps.")

    # ------------------------------------------------------------------
    # 3. Compute duration_minutes
    #
    #    How it works:
    #      • Subtracting two datetime columns produces a Timedelta.
    #      • .dt.total_seconds() converts it to a float (seconds).
    #      • Dividing by 60 gives minutes.
    #
    #    Example:
    #      start_time = 2026-01-17 08:47:00
    #      end_time   = 2026-01-17 09:41:45
    #      duration   = 54.75 minutes
    # ------------------------------------------------------------------
    df["duration_minutes"] = (
        (df["end_time"] - df["start_time"])
        .dt.total_seconds()  # timedelta → seconds (float)
        / 60                 # seconds  → minutes
    )

    # ------------------------------------------------------------------
    # 4. Ensure duration is always positive
    #    In a well-formed log end_time >= start_time, but we guard
    #    against reversed timestamps by taking the absolute value.
    # ------------------------------------------------------------------
    df["duration_minutes"] = df["duration_minutes"].abs()

    # ------------------------------------------------------------------
    # 5. Simulate processing vs waiting time
    #    Processing time = 10-30% of total duration (active work).
    #    Waiting time    = 70-90% of total duration (idle/queue time).
    # ------------------------------------------------------------------
    import random
    # Use a fixed seed for consistency during the same run if needed, 
    # but here we generate per row.
    ratios = [random.uniform(0.1, 0.3) for _ in range(len(df))]
    df["processing_time_minutes"] = (df["duration_minutes"] * ratios).round(2)
    df["waiting_time_minutes"] = (df["duration_minutes"] - df["processing_time_minutes"]).round(2)

    # ------------------------------------------------------------------
    # 7. SLA Violation Detection
    #    Define Service Level Agreements (SLA) per task in minutes.
    # ------------------------------------------------------------------
    SLA_THRESHOLDS = {
        "Lead Created": 5,
        "Lead Reviewed": 30,
        "Manager Approval": 120,
        "Proposal Sent": 60,
        "Deal Closed": 60
    }
    
    # Map thresholds to the rows
    df["sla_threshold"] = df["task"].map(SLA_THRESHOLDS).fillna(99999)
    # Identify violations
    df["sla_violation"] = df["duration_minutes"] > df["sla_threshold"]

    # ------------------------------------------------------------------
    # 8. Reset the index (rows may have been dropped) and return
    # ------------------------------------------------------------------
    df = df.reset_index(drop=True)

    return df


def compute_case_durations(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the total end-to-end workflow duration for each case_id.

    This measures the true wall-clock completion time by finding the
    difference between the earliest start_time and the latest end_time
    for each workflow instance.

    Parameters
    ----------
    df : pd.DataFrame
        Preprocessed workflow DataFrame with `case_id`, `start_time`,
        and `end_time`.

    Returns
    -------
    pd.DataFrame
        DataFrame with two columns: `case_id` and `total_duration_minutes`.
    """
    # Find the very first start_time and the very last end_time per case
    case_aggs = df.groupby("case_id").agg(
        first_start=("start_time", "min"),
        last_end=("end_time", "max"),
    )

    # Compute end-to-end duration in minutes
    total_mins = (case_aggs["last_end"] - case_aggs["first_start"]).dt.total_seconds() / 60

    # Build the resulting DataFrame
    case_durations = pd.DataFrame({
        "case_id": case_aggs.index,
        "total_duration_minutes": total_mins.abs(),
    }).reset_index(drop=True)

    return case_durations


def detect_exception_flows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Detect exception flows such as loops (repeated tasks) and deviations
    (out-of-order task sequences or missing steps) within each workflow case.

    Normal Workflow:
    The expected sequence follows a strict chronological order:
    1. Lead Created
    2. Lead Reviewed
    3. Manager Approval
    4. Proposal Sent
    5. Deal Closed

    What an Exception Means:
    1. Loop/Rework: A task is performed more than once for the same case.
       This indicates inefficiency or repetitive effort.
    2. Deviation/Unexpected Flow: Tasks are performed out of the expected
       order (e.g., skip a step or jump backward). This indicates process
       inconsistency or bypassing controls.

    Parameters
    ----------
    df : pd.DataFrame
        Workflow DataFrame containing 'case_id', 'task', and 'start_time'.

    Returns
    -------
    tuple[pd.DataFrame, int]
        - A DataFrame with columns: ['case_id', 'issue_type', 'affected_task']
        - The total number of unique case_ids that have at least one exception.
    """
    exceptions = []
    
    # Define the expected workflow sequence
    expected_order = [
        "Lead Created",
        "Lead Reviewed",
        "Manager Approval",
        "Proposal Sent",
        "Deal Closed"
    ]
    expected_order_map = {task: i for i, task in enumerate(expected_order)}

    # Ensure data is sorted by case and time to correctly identify sequences
    df_sorted = df.sort_values(by=["case_id", "start_time"])

    for case_id, group in df_sorted.groupby("case_id"):
        tasks = group["task"].tolist()
        if not tasks:
            continue

        # 1. Detect Loops (Rework)
        # Identify any task that appears more than once in the same case
        seen_tasks = set()
        for t in tasks:
            if t in seen_tasks:
                exceptions.append({
                    "case_id": case_id,
                    "issue_type": "loop",
                    "affected_task": t
                })
            seen_tasks.add(t)

        # 2. Detect Deviations (Unexpected Flow / Missing Steps)
        # A deviation occurs if the sequence doesn't start at the beginning
        # or if a transition skips steps or goes backwards.
        
        # Check if the process started correctly
        if expected_order_map.get(tasks[0], -1) != 0:
            exceptions.append({
                "case_id": case_id,
                "issue_type": "deviation",
                "affected_task": tasks[0]
            })

        # Check transitions between consecutive tasks
        for i in range(1, len(tasks)):
            prev_t = tasks[i - 1]
            curr_t = tasks[i]
            
            # Skip consecutive identical tasks for deviation check (handled by loop check)
            if curr_t == prev_t:
                continue
                
            prev_idx = expected_order_map.get(prev_t, -1)
            curr_idx = expected_order_map.get(curr_t, -1)
            
            # A valid transition is exactly one step forward in the expected_order
            if curr_idx != prev_idx + 1:
                exceptions.append({
                    "case_id": case_id,
                    "issue_type": "deviation",
                    "affected_task": curr_t
                })

    exceptions_df = pd.DataFrame(exceptions)
    
    if exceptions_df.empty:
        # Return empty df with expected columns
        exceptions_df = pd.DataFrame(columns=["case_id", "issue_type", "affected_task"])
        total_exception_count = 0
    else:
        total_exception_count = exceptions_df["case_id"].nunique()

    return exceptions_df, total_exception_count

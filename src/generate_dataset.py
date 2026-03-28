"""
generate_dataset.py

Generates a realistic simulated workflow event-log dataset for testing
a workflow bottleneck detection system.

Workflow steps (in order):
    Lead Created → Lead Reviewed → Manager Approval → Proposal Sent → Deal Closed

The "Manager Approval" step occasionally takes significantly longer to
simulate a realistic bottleneck.

Output: data/workflow_logs.csv
"""

import csv
import os
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NUM_CASES = 200  # number of workflow instances to generate

WORKFLOW_STEPS = [
    "Lead Created",
    "Lead Reviewed",
    "Manager Approval",
    "Proposal Sent",
    "Deal Closed",
]

# Pool of simulated users who may handle each step
USERS = {
    "Lead Created":     ["alice", "bob", "charlie", "diana"],
    "Lead Reviewed":    ["eve", "frank", "grace"],
    "Manager Approval": ["harry", "isabella"],
    "Proposal Sent":    ["jack", "kate", "leo"],
    "Deal Closed":      ["mike", "nancy"],
}

# Delay configuration per step  (min_hours, max_hours)
# These represent the *duration* of the step itself, not the gap before it.
STEP_DURATION = {
    "Lead Created":     (0.5, 2.0),
    "Lead Reviewed":    (1.0, 4.0),
    "Manager Approval": (1.0, 6.0),   # normal duration
    "Proposal Sent":    (0.5, 3.0),
    "Deal Closed":      (0.5, 2.0),
}

# Gap between the end of one step and the start of the next (hours)
INTER_STEP_GAP = {
    "Lead Reviewed":    (0.5, 4.0),
    "Manager Approval": (0.5, 3.0),
    "Proposal Sent":    (0.5, 2.0),
    "Deal Closed":      (0.5, 3.0),
}

# Bottleneck parameters for "Manager Approval"
BOTTLENECK_PROBABILITY = 0.35          # 35 % of cases hit the bottleneck
BOTTLENECK_EXTRA_HOURS = (12.0, 48.0)  # extra delay added when bottleneck fires


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _random_hours(low: float, high: float) -> timedelta:
    """Return a timedelta representing a random number of hours in [low, high]."""
    return timedelta(hours=random.uniform(low, high))


def _random_start() -> datetime:
    """Return a random start datetime within the last 90 days."""
    now = datetime(2026, 3, 16, 12, 0, 0)  # anchored reference time
    offset = timedelta(days=random.uniform(0, 90))
    # Random hour of the business day (08:00 – 18:00)
    hour = random.randint(8, 17)
    minute = random.randint(0, 59)
    base = now - offset
    return base.replace(hour=hour, minute=minute, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Core generation logic
# ---------------------------------------------------------------------------

def generate_case(case_id: int) -> list[dict]:
    """Generate all event rows for a single workflow case."""
    events: list[dict] = []
    current_time = _random_start()

    for i, step in enumerate(WORKFLOW_STEPS):
        # Determine step duration
        dur_low, dur_high = STEP_DURATION[step]
        duration = _random_hours(dur_low, dur_high)

        # If this is not the first step, add an inter-step gap
        if i > 0:
            gap_low, gap_high = INTER_STEP_GAP[step]
            current_time += _random_hours(gap_low, gap_high)

        start_time = current_time

        # Inject bottleneck for Manager Approval
        if step == "Manager Approval" and random.random() < BOTTLENECK_PROBABILITY:
            extra_low, extra_high = BOTTLENECK_EXTRA_HOURS
            duration += _random_hours(extra_low, extra_high)

        end_time = start_time + duration
        user = random.choice(USERS[step])

        events.append({
            "case_id":    f"CASE-{case_id:04d}",
            "task":       step,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time":   end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "user":       user,
        })

        # Next step starts after the current step ends
        current_time = end_time

    return events


def generate_dataset(num_cases: int = NUM_CASES) -> list[dict]:
    """Generate the full event-log dataset for *num_cases* workflow instances."""
    all_events: list[dict] = []
    for case_id in range(1, num_cases + 1):
        all_events.extend(generate_case(case_id))
    return all_events


def save_to_csv(events: list[dict], filepath: str) -> None:
    """Write the event list to a CSV file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fieldnames = ["case_id", "task", "start_time", "end_time", "user"]
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)
    print(f"✅  Dataset saved → {filepath}  ({len(events)} rows)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Generate the workflow event-log dataset and write it to CSV."""
    random.seed(42)  # reproducible results

    # Resolve output path relative to the project root (one level up from src/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
    output_path = os.path.join(project_root, "data", "workflow_logs.csv")

    events = generate_dataset()
    save_to_csv(events, output_path)

    # Quick summary
    bottleneck_cases = sum(
        1
        for e in events
        if e["task"] == "Manager Approval"
        and (
            datetime.strptime(e["end_time"], "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(e["start_time"], "%Y-%m-%d %H:%M:%S")
        ).total_seconds()
        / 3600
        > 8  # longer than 8 hours ≈ bottleneck
    )
    total_cases = len(set(e["case_id"] for e in events))
    print(f"📊  Total cases: {total_cases}")
    print(f"⚠️   Manager Approval bottlenecks: {bottleneck_cases}")


if __name__ == "__main__":
    main()

"""
data_loader.py

Reusable utility for loading workflow event-log CSV files into a clean
pandas DataFrame, with validation and datetime parsing.
"""

import os

import pandas as pd


# Columns that every valid workflow log must contain
REQUIRED_COLUMNS = {"case_id", "task", "start_time", "end_time", "user"}


def load_workflow_data(file_path: str) -> pd.DataFrame:
    """Load a workflow event-log CSV and return a cleaned DataFrame.

    Steps performed:
        1. Verify the file exists on disk.
        2. Read the CSV into a DataFrame.
        3. Validate that all required columns are present.
        4. Convert timestamp columns to proper datetime objects.
        5. Strip leading/trailing whitespace from string columns.
        6. Return the cleaned DataFrame.

    Parameters
    ----------
    file_path : str
        Path to the CSV file containing workflow logs.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with datetime-typed timestamp columns.

    Raises
    ------
    FileNotFoundError
        If *file_path* does not point to an existing file.
    ValueError
        If the CSV is missing one or more required columns, or if
        timestamp values cannot be parsed.
    """

    # ------------------------------------------------------------------
    # 1. Check that the file actually exists
    # ------------------------------------------------------------------
    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"Workflow log file not found: {file_path}"
        )

    # ------------------------------------------------------------------
    # 2. Read the CSV into a DataFrame
    # ------------------------------------------------------------------
    df = pd.read_csv(file_path)

    # ------------------------------------------------------------------
    # 3. Validate required columns
    #    Compare the set of expected columns against what was loaded.
    #    If any are missing, raise a clear error listing them.
    # ------------------------------------------------------------------
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"The following required columns are missing from "
            f"'{file_path}': {', '.join(sorted(missing))}"
        )

    # ------------------------------------------------------------------
    # 4. Convert start_time and end_time to datetime objects
    #    Using errors='coerce' would silently turn bad values into NaT;
    #    instead we raise so the caller knows right away.
    # ------------------------------------------------------------------
    for col in ("start_time", "end_time"):
        try:
            df[col] = pd.to_datetime(df[col])
        except Exception as exc:
            raise ValueError(
                f"Could not parse column '{col}' as datetime: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # 5. Clean string columns — strip extra whitespace
    # ------------------------------------------------------------------
    for col in ("case_id", "task", "user"):
        df[col] = df[col].astype(str).str.strip()

    # ------------------------------------------------------------------
    # 6. Return the cleaned DataFrame
    # ------------------------------------------------------------------
    return df

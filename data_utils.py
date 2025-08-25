# data_utils.py
import pandas as pd

def safe_get_value(df, condition, column, default=None):
    """
    Safely get a value from a DataFrame with error handling
    
    Args:
        df: DataFrame to search
        condition: Boolean series for filtering
        column: Column name to extract value from
        default: Default value if not found
    
    Returns:
        Value from the DataFrame or default
    """
    try:
        filtered = df[condition]
        if filtered.empty:
            return default
        return filtered[column].iloc[0]
    except (KeyError, IndexError):
        return default

def safe_get_first_row(df, condition, default=None):
    """
    Safely get the first row from a DataFrame matching condition
    
    Args:
        df: DataFrame to search
        condition: Boolean series for filtering
        default: Default value if not found
    
    Returns:
        First matching row or default
    """
    try:
        filtered = df[condition]
        if filtered.empty:
            return default
        return filtered.iloc[0]
    except (KeyError, IndexError):
        return default
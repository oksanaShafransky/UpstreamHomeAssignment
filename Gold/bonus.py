from typing import List
import pandas as pd
import re

# Runs a batch SQL detection on the specified columns of a pandas DataFrame using the specified
# regex patterns and produces a report of violating messages.
#
# Args:
#     df (pandas.DataFrame): The DataFrame to process.
#     column_names (list[str]): A list of column names to run the detection on.
#     regex_patterns (list[str]): A list of regex patterns to use for SQL injection detection.
#
# Returns:
#     pandas.DataFrame: A DataFrame containing the violating messages and the corresponding
#     column names.
#
# FOOD for THOUGHT:
# Regular expression matching can be expensive, especially when processing a large batch of data.
# Here are a few ways to potentially make SQL injection detection less expensive CPU wise:
# 1. Parallelize the detection process.
# For example, you can split the input data into multiple partitions and run the detection algorithm on each partition in parallel.
# This can greatly reduce the processing time, especially for large datasets (we can use spark for example).
#
# 2. Filter records before running detection - filter out records that are unlikely to contain SQL injection attacks before running the detection.
# For example, you can filter out records that don't contain any SQL keywords or special characters.
# This can greatly reduce the number of records that need to be processed by the detection algorithm, which can save a lot of CPU time.
#
# 3. Use a whitelist instead of a blacklist - we can use a whitelist of allowed characters or patterns.
# This can make the detection algorithm simpler and faster, since it only needs to check if the input contains allowed characters or patterns, rather than searching for specific SQL injection patterns.

def sqlInjectionReport(bronze_df: pd.DataFrame, column_names: List[str], regex_patterns: List[str])->pd.DataFrame:
    violating_messages = []
    compiled_patterns = [re.compile(p) for p in regex_patterns]

    # Iterate over specified columns and detect SQL injection
    for col in column_names:
        for idx, value in bronze_df[col].items():
            is_violating = False

            # Check if value matches any of the regex patterns
            for pattern in compiled_patterns:
                if pattern.search(value):
                    is_violating = True
                    break

            # If SQL injection is detected, add message to list
            if is_violating:
                violating_messages.append((value, col))

    # Return results as DataFrame
    if violating_messages:
        return pd.DataFrame(violating_messages, columns=['violating_message', 'column_name'])
    else:
        return pd.DataFrame(columns=['violating_message', 'column_name'])

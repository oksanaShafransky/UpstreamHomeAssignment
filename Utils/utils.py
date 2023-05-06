from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def save_data_to_parquet(df: pd.DataFrame, path: str, partitions: List[str]):
    vehicle_messages_table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        vehicle_messages_table,
        root_path=path,
        existing_data_behavior="overwrite_or_ignore",
        partition_cols=partitions,
    )
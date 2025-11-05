# Core function to read data
import json
import pandas as pd
from datetime import datetime, timedelta

def read_data_from_dataframe(
    df: pd.DataFrame, 
    partition_day: datetime = None,
    timestamp_key_name: str = 'event_timestamp',
):
    df[timestamp_key_name] = pd.to_datetime(df[timestamp_key_name])
    time_last_sync = partition_day
    filter_df = df.loc[df[timestamp_key_name].dt.date == time_last_sync.date()]
    filter_df[timestamp_key_name] = filter_df[timestamp_key_name].astype(str)
    
    return json.loads(filter_df.to_json(orient='records'))
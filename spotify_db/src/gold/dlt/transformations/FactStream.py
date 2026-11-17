import dlt
from pyspark.sql.functions import col

@dlt.table
def factstream_stg():
    df=spark.readStream.table("spotify_cata.silver.factstream")
    return df;

dlt.create_streaming_table("factstream")
dlt.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = col("stream_timestamp"),
  stored_as_scd_type = "1", # optional
  track_history_except_column_list = None, # optional
  name = None, # optional
  once = False # optional
)



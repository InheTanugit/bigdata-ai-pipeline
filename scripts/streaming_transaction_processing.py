import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum, count, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Structured Logging Setup
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name
        }
        if hasattr(record, "props"):
            log_record.update(record.props)
        return json.dumps(log_record)

def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = get_logger("StreamingTransactionProcessing")


# Create Spark session 
spark = SparkSession.builder \
    .appName("KafkaToParquetStreaming") \
    .getOrCreate()

print("🔥 STREAMING FILE EXECUTED 🔥")

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("device", StringType()) \
    .add("location", StringType()) \
    .add("event_time", StringType())

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert event_time to TIMESTAMP
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

parsed_df.select("event_time").printSchema()

# Aggregation: total spend per user
agg_df = (
    parsed_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(
        sum("amount").alias("total_spend"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    )
)

# Flatten window column
feature_df = agg_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("user_id"),
    col("total_spend"),
    col("transaction_count"),
    col("avg_amount")
)



# Batch Processing with Logging
def process_batch(batch_df, batch_id):
    try:
        # Aggregated stats
        stats = batch_df.select(
            count("*").alias("count"),
            avg("total_spend").alias("avg_total_spend")
        ).collect()[0]
        
        count_val = stats["count"]
        avg_spend = stats["avg_total_spend"] or 0.0
        
        # 1. Log Ingestion
        logger.info("Streaming pipeline metrics", extra={"props": {
            "batch_id": batch_id,
            "records_ingested": count_val,
            "stage": "ingestion"
        }})
        
        # 2. Log Feature Engineering
        logger.info("Feature engineering completed", extra={"props": {
            "batch_id": batch_id,
            "avg_total_spend": float(avg_spend),
            "stage": "features"
        }})
        
        if count_val > 0:
            # Write to Parquet
            batch_df.write \
                .format("parquet") \
                .mode("append") \
                .save("../data/feature_store")
                
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        raise e

# Write aggregated output (through foreachBatch with Structured Logging)
query = feature_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "../checkpoints/feature_store") \
    .outputMode("append") \
    .start()



print("🚀 Streaming query started")
query.awaitTermination()

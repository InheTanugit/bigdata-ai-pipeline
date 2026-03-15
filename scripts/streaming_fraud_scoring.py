import logging
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum, count
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
from pyspark.sql.functions import to_json, struct

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

logger = get_logger("StreamingFraudScoring")


# Spark Session
spark = SparkSession.builder \
    .appName("StreamingFraudScoring") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

print("🔥 Streaming Fraud Scoring Started 🔥")

# Load Trained Model
script_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(script_dir, "..", "models", "fraud_model")
model = PipelineModel.load(model_path)


# Define Kafka Schema
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
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "transaction_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

# Window Feature Engineering
feature_df = (
    parsed_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("user_id")
    )
    .agg(
        sum("amount").alias("total_spend"),
        count("*").alias("transaction_count")
    )
)

feature_df = feature_df.withColumn(
    "avg_amount",
    col("total_spend") / col("transaction_count")
)

# Apply Model (assembler already inside Pipeline model)
predictions = model.transform(feature_df)

# Select Output Columns 
output_df = predictions.select(
    "user_id",
    "total_spend",
    "transaction_count",
    "avg_amount",
    "prediction",
    "probability"
)

# Batch Processing with Logging
def process_batch(batch_df, batch_id):
    try:
        # 1. Log Ingestion (Total records in batch)
        stats = batch_df.select(
            count("*").alias("count"),
            sum((col("prediction") == 1.0).cast("int")).alias("fraud_count")
        ).collect()[0]
        
        count_val = stats["count"]
        fraud_val = stats["fraud_count"] or 0
        
        logger.info("Streaming pipeline metrics", extra={"props": {
            "batch_id": batch_id,
            "records_ingested": count_val,
            "stage": "ingestion"
        }})
        
        # 2. Log Feature Engineering
        logger.info("Feature engineering completed", extra={"props": {
            "batch_id": batch_id,
            "stage": "features"
        }})
        
        # 3. Log Fraud Predictions
        logger.info("Fraud prediction scoring", extra={"props": {
            "batch_id": batch_id,
            "frauds_detected": fraud_val,
            "stage": "prediction"
        }})
        
        if count_val > 0:
            # Convert to JSON for Kafka
            kafka_payload = batch_df.select(to_json(struct("*")).alias("value"))
            
            # Write to Kafka
            kafka_payload.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", "fraud_predictions") \
                .save()
                
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        raise e

# Output Predictions (through foreachBatch with Structured Logging)
query = output_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "checkpoints/fraud_scoring") \
    .outputMode("append") \
    .start()

query.awaitTermination()

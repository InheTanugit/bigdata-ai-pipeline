![Python](https://img.shields.io/badge/Python-3.10-blue)
![Spark](https://img.shields.io/badge/Spark-StructuredStreaming-orange)
![Kafka](https://img.shields.io/badge/Kafka-EventStreaming-black)
![License](https://img.shields.io/badge/License-MIT-green)

## Motivation -->

- Financial systems require real-time fraud detection to prevent
suspicious transactions before they are completed.

- This project demonstrates how modern data platforms combine
event streaming, distributed processing, and machine learning
to perform real-time fraud detection.

## Real-Time Fraud Detection Pipeline -->

- A real-time fraud detection system that processes streaming transaction data using Apache Kafka and Apache Spark Structured Streaming, applies machine learning inference, and outputs fraud predictions for downstream systems.

- This project demonstrates how modern data platforms process event streams and perform real-time ML scoring.

## Architecture -->
                          ┌─────────────────────────────┐
                          │   Transaction Generator     │
                          │  (Python Producer Script)   │
                          └──────────────┬──────────────┘
                                         │
                                         │ JSON Events
                                         ▼
                          ┌─────────────────────────────┐
                          │         Apache Kafka        │
                          │     Topic: transactions     │
                          └──────────────┬──────────────┘
                                         │
                                         │ Stream Ingestion
                                         ▼
                     ┌────────────────────────────────────────┐
                     │        Spark Structured Streaming       │
                     │----------------------------------------│
                     │                                        │
                     │ 1. Read Kafka Stream                   │
                     │ 2. Parse JSON Transaction Events       │
                     │ 3. Real-time Feature Engineering       │
                     │    • total_spend                       │
                     │    • transaction_count                 │
                     │    • avg_amount                        │
                     │                                        │
                     └──────────────┬─────────────────────────┘
                                    │
                                    │ Feature Data
                                    ▼
                        ┌───────────────────────────────┐
                        │        Feature Store          │
                        │         (Parquet Files)       │
                        └──────────────┬────────────────┘
                                       │
                                       │ Training Data
                                       ▼
                       ┌────────────────────────────────┐
                       │        Model Training          │
                       │--------------------------------│
                       │  PySpark ML Pipeline           │
                       │  Algorithm: LogisticRegression │
                       │                                │
                       │  Steps:                        │
                       │  • VectorAssembler             │
                       │  • Model Training              │
                       │  • Model Persistence           │
                       └──────────────┬─────────────────┘
                                      │
                                      │ Saved Model
                                      ▼
                           ┌───────────────────────────┐
                           │        ML Model           │
                           │     fraud_model/          │
                           └─────────────┬─────────────┘
                                         │
                                         │ Streaming Inference
                                         ▼
                ┌─────────────────────────────────────────────┐
                │        Spark Streaming Inference            │
                │---------------------------------------------│
                │                                             │
                │ 1. Load Trained ML Model                    │
                │ 2. Apply Model on Streaming Features        │
                │ 3. Generate Fraud Prediction                │
                │                                             │
                └──────────────┬──────────────────────────────┘
                               │
                               │ Prediction Output
                               ▼
                      ┌─────────────────────────────┐
                      │         Apache Kafka        │
                      │   Topic: fraud_predictions  │
                      └──────────────┬──────────────┘
                                     │
                                     │ Real-time Results
                                     ▼
                     ┌────────────────────────────────┐
                     │      Downstream Consumers      │
                     │--------------------------------│
                     │  • Fraud Monitoring Dashboard  │
                     │  • Alerting System             │
                     │  • Analytics Pipeline          │
                     └────────────────────────────────┘

## Pipeline Flow -->

1. Transaction events are generated by a Python producer.
2. Events are published to Kafka topic `transactions`.
3. Spark Structured Streaming consumes the events.
4. Features are computed in real-time.
5. A trained ML model performs fraud prediction.
6. Predictions are published to Kafka topic `fraud_predictions`.
7. Downstream systems consume predictions for alerts or analytics.
                     
## Features -->

- Real-time transaction ingestion using Kafka
- Stream processing using Spark Structured Streaming
- Feature engineering for transaction aggregation
- Machine learning inference using Spark ML
- Fraud prediction output published to Kafka
- Modular pipeline for easy extension

## Tech Stack -->

- Apache Kafka – event streaming platform
- Apache Spark – stream processing engine
- PySpark – distributed data processing
- Spark MLlib – machine learning
- Python
- Parquet – feature store format

## Project Structure

```
bigdata-ai-pipeline/
│
├── data/
│   └── feature_store/
│
├── models/
│   └── fraud_model/
│
├── scripts/
│   ├── train_model.py
│   ├── streaming_fraud_scoring.py
│   └── streaming_transaction_processing.py
│
└── README.md
```

## Installation -->
1. Install dependencies
pip install pyspark kafka-python

2. Start Kafka 
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

3. Create Kafka Topics
bin/kafka-topics.sh --create \
--topic transaction_events \
--bootstrap-server localhost:9092 \
--partitions 1 --replication-factor 1

bin/kafka-topics.sh --create \
--topic fraud_predictions \
--bootstrap-server localhost:9092 \
--partitions 1 --replication-factor 1

4. Train the Model
python train_model.py

⁕ This will:

- Load feature store data
- Train a logistic regression model
- Save the model to the models/ directory
⁕ Start the Spark streaming job:

```bash
spark-submit scripts/streaming_fraud_scoring.py
```

### 🔬 Alternative: Test using Sample Dataset

Instead of running a continuous data generator, you can use the provided sample dataset to feed accurate micro-batches to the pipeline.

1. **Push data to Kafka**:
   ```bash
   cat data/sample_transactions.jsonl | bin/kafka-console-producer.sh \
   --bootstrap-server localhost:9092 \
   --topic transaction_events
   ```

2. **Run the streaming app** (it will process the data immediately).

## The pipeline will -->

- Consume transactions from Kafka
- Generate features
- Apply fraud detection model
- Publish predictions to Kafka

## View Predictions -->

Run a Kafka consumer to see fraud predictions.

bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic fraud_predictions \
--from-beginning

## Example output -->

{
"user_id":"u15",
"total_spend":457.63,
"transaction_count":1,
"avg_amount":457.63,
"prediction":0.0,
"probability":[1.0,0.0]
}

## Example Use Cases -->

- Fraud detection in payment systems
- Real-time transaction monitoring
- Event-driven ML inference pipelines
- Streaming analytics platforms

## Future Improvements -->

- Add Docker deployment
- Integrate Airflow for orchestration
- Add dashboard for fraud alerts
- Implement advanced ML models

## Contributing -->

- Contributions are welcome. Please open an issue or submit a pull request for improvements.

## License -->

- This project is licensed under the MIT License.


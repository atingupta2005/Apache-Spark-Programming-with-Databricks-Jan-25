**Module 5: Streaming with Spark Structured Streaming**

### Real-Time Data Processing Concepts

Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. It enables real-time processing of data streams using the same abstractions as batch processing, making it easy to build scalable data pipelines.

**Key Features:**
- **Incremental Query Execution**: Continuously processes streaming data as new records arrive.
- **Fault Tolerance**: Uses checkpoints and write-ahead logs for resilience.
- **Exactly-Once Processing**: Ensures data consistency across streaming and batch workloads.
- **Unification with Spark SQL and DataFrames**: Uses the same DataFrame/Dataset API for batch and streaming workloads.

#### **Example 1: Setting Up a Streaming DataFrame**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark Session
spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()

# Read a stream from a directory
df = spark.readStream.format("json").option("path", "dbfs:/mnt/data/data/streaming-json/").load()

# Inspect the schema of the streaming DataFrame
df.printSchema()

# Select specific columns
df_selected = df.select("timestamp", "value")

# Apply filtering to exclude records with low values
df_filtered = df_selected.filter(col("value") > 10)

# Add a new column with transformed data
df_transformed = df_filtered.withColumn("normalized_value", col("value") / 100)

# Write transformed stream to the console
df_transformed.writeStream.format("console").outputMode("append").start()
```

#### **Example 2: Processing Streaming Data**
```python
# Select specific columns and filter data
df_selected = df.select("timestamp", "value").filter(col("value") > 10)

# Group by and aggregate the stream
df_grouped = df_selected.groupBy("timestamp").agg({"value": "sum"})

# Write aggregated data to memory query
query = df_grouped.writeStream.format("memory").queryName("streaming_aggregates").outputMode("complete").start()

# Query the results in memory
spark.sql("SELECT * FROM streaming_aggregates ORDER BY timestamp DESC").show()
```

#### **Example 3: Writing Stream to Memory with Additional Transformations**
```python
from pyspark.sql.functions import year, month, dayofmonth

# Extract date components from the timestamp
df_with_date = df.withColumn("year", year("timestamp"))\
                  .withColumn("month", month("timestamp"))\
                  .withColumn("day", dayofmonth("timestamp"))

# Filter for records in a specific year
df_filtered = df_with_date.filter(col("year") == 2025)

# Write filtered results to memory
query = df_filtered.writeStream\
    .format("memory")\
    .queryName("filtered_stream")\
    .outputMode("append")\
    .start()

# Query the memory table
spark.sql("SELECT * FROM filtered_stream").show()
```

---

### Sources: Kafka, Delta Lake, and Files

#### **1. Streaming from Kafka**
Kafka is a popular distributed messaging system used for high-throughput, low-latency data streams.

#### **Example 4: Reading Data from Kafka and Applying Transformations**
```python
kafka_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "<BROKER_URL>")\
    .option("subscribe", "topic_name")\
    .load()

# Deserialize Kafka messages
deserialized_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Add processing timestamp and filter messages
deserialized_df = deserialized_df.withColumn("processed_at", current_timestamp())\
                                 .filter(col("value").isNotNull())

# Write transformed messages to the console
deserialized_df.writeStream.format("console").start()
```

#### **Example 5: Writing Transformed Kafka Data Back to Kafka**
```python
output_df = deserialized_df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
output_df.writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", "<BROKER_URL>")\
    .option("topic", "output_topic")\
    .start()
```

#### **2. Streaming from Delta Lake with Transformations**

#### **Example 6: Streaming Aggregated Data from Delta Lake**
```python
from pyspark.sql.functions import avg, col

delta_df = spark.readStream.format("delta").option("path", "dbfs:/mnt/data/data/delta-table/").load()

# Perform aggregation on streaming data
delta_aggregated = delta_df.groupBy("category").agg(avg(col("value")).alias("avg_value"))

# Write aggregated results to a new Delta table
delta_aggregated.writeStream\
    .format("delta")\
    .outputMode("complete")\
    .option("checkpointLocation", "dbfs:/mnt/data/checkpoints/delta/")\
    .option("path", "dbfs:/mnt/data/output/delta-aggregated/")\
    .start()
```

---

### Window Functions and Watermarking

#### **Example 7: Complex Tumbling Window Aggregation**
```python
from pyspark.sql.functions import window

# Perform window-based aggregation on streaming data
aggregated_df = df.groupBy(window(col("timestamp"), "10 minutes"), "category").count()

# Write results to memory
tag_query = aggregated_df.writeStream\
    .outputMode("update")\
    .format("memory")\
    .queryName("windowed_aggregates")\
    .start()

# Query the results in memory
spark.sql("SELECT * FROM windowed_aggregates").show()
```

#### **Example 8: Using Watermarking for Late Data Management**
```python
watermarked_df = df.withWatermark("timestamp", "5 minutes")\
    .groupBy(window(col("timestamp"), "10 minutes"), "category").count()

# Write watermarked results to the console
watermarked_df.writeStream.format("console").outputMode("update").start()
```

---

### Fault Tolerance and Checkpointing

#### **Example 9: Checkpointing with Multiple Sinks**
```python
# Write stream data to Parquet and Delta sinks with checkpointing
query_parquet = df.writeStream\
    .format("parquet")\
    .option("checkpointLocation", "dbfs:/mnt/data/checkpoints/parquet/")\
    .option("path", "dbfs:/mnt/data/output/parquet/")\
    .start()

query_delta = df.writeStream\
    .format("delta")\
    .option("checkpointLocation", "dbfs:/mnt/data/checkpoints/delta/")\
    .option("path", "dbfs:/mnt/data/output/delta/")\
    .start()

query_parquet.awaitTermination()
query_delta.awaitTermination()
```

---

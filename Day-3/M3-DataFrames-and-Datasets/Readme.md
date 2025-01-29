**Module 3: DataFrames and Datasets**

### Understanding DataFrames and Datasets

**1. What is a Schema?**
A schema defines the structure of a dataset, specifying the column names, data types, and constraints. It ensures that the data adheres to a predefined format, enabling consistency and compatibility across processing workflows.

- **Benefits of Schema:**
  - Enforces data integrity and validation.
  - Enables query optimization.
  - Facilitates compatibility with downstream applications.

*Example*: Viewing the schema of a DataFrame:
```python
df.printSchema()
```

**2. DataFrames**
A DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database. It is designed to support both structured and semi-structured data processing.

- **Key Features of DataFrames:**
  - Schema-based processing.
  - Lazy evaluation for efficient execution.
  - Catalyst optimizer for query planning.
  - Multi-language support: Python, Scala, SQL, R.

*Example*: Loading a JSON dataset:
```python
# Load JSON data
flight_data_json = "dbfs:/mnt/data/data/flight-data/json/2015-summary.json"
df = spark.read.format("json").load(flight_data_json)

df.show(5)
```

**3. Datasets**
Datasets provide type-safe operations with compile-time type checking, combining the benefits of RDDs with the optimizations of DataFrames. While primarily used in Scala and Java, the equivalent in Python is the DataFrame API.

- **Differences Between DataFrames and Datasets:**
  - **Type Safety**: Datasets offer type safety, while DataFrames do not.
  - **Languages**: Datasets are native to Scala and Java; DataFrames are universal.
  - **Serialization**: Datasets use encoders for object serialization.

---

### File Formats in Spark

**1. JSON (JavaScript Object Notation)**
A lightweight, semi-structured format ideal for data exchange.

- **Advantages:**
  - Human-readable.
  - Flexible structure for nested data.

*Example*: Reading and writing JSON:
```python
# Reading JSON
json_df = spark.read.format("json").load("dbfs:/mnt/data/data/flight-data/json/2015-summary.json")

# Writing JSON
json_df.write.format("json").save("dbfs:/mnt/data/output/flight-data-json")
```

**2. Parquet**
A columnar storage format optimized for analytics, offering efficient compression and query performance.

- **Advantages:**
  - Supports schema evolution.
  - Optimized for columnar operations.

*Example*: Working with Parquet:
```python
# Reading Parquet
parquet_df = spark.read.format("parquet").load("dbfs:/mnt/data/data/flight-data/parquet/2010-summary.parquet")

# Writing Parquet
parquet_df.write.format("parquet").save("dbfs:/mnt/data/output/flight-data-parquet")
```

**3. Avro**
A row-based binary format commonly used for data serialization.

- **Advantages:**
  - Compact and efficient.
  - Supports schema definition and evolution.

*Example*: Working with Avro:
```python
# Reading Avro
avro_df = spark.read.format("avro").load("dbfs:/mnt/data/data/flight-data/avro/2010-summary.avro")

# Writing Avro
avro_df.write.format("avro").save("dbfs:/mnt/data/output/flight-data-avro")
```

**4. CSV (Comma-Separated Values)**
A widely used format for simple tabular data.

- **Advantages:**
  - Easy to understand and use.
  - Compatible with many tools.

*Example*: Working with CSV:
```python
# Reading CSV
csv_df = spark.read.format("csv").option("header", True).load("dbfs:/mnt/data/data/flight-data/csv/2010-summary.csv")

# Writing CSV
csv_df.write.format("csv").option("header", True).save("dbfs:/mnt/data/output/flight-data-csv")
```

---

### Schema Management and Optimization

**1. Inferring Schema**
Spark automatically detects column names and data types based on file contents.

*Example*: Inferring schema from JSON:
```python
schema_inferred_df = spark.read.json(flight_data_json)
schema_inferred_df.printSchema()
```

**2. Explicit Schema Definition**
Define schemas for strict data validation and compatibility.

*Example*: Defining a schema:
```python
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), True)
])

schema_defined_df = spark.read.format("json").schema(schema).load(flight_data_json)
schema_defined_df.printSchema()
```

---

### Practical Use Cases

**1. Joins**
Combine datasets to analyze relationships.

*Example*: Inner join:
```python
flights_2010 = spark.read.json("dbfs:/mnt/data/data/flight-data/json/2010-summary.json")
joined_df = df.join(flights_2010, df.DEST_COUNTRY_NAME == flights_2010.DEST_COUNTRY_NAME, "inner")
joined_df.show(5)
```

**2. Aggregations**
Summarize data using group operations.

*Example*: Calculating totals:
```python
from pyspark.sql.functions import sum
agg_df = df.groupBy("DEST_COUNTRY_NAME").agg(sum("count").alias("total_count"))
agg_df.show(5)
```

**3. Filters**
Select specific subsets of data.

*Example*: Filtering data:
```python
filtered_df = df.filter(df["count"] > 1000)
filtered_df.show(5)
```

---

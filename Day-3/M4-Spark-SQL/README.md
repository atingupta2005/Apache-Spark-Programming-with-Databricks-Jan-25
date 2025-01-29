**Module 4: Spark SQL**

### SQL in Spark: Overview and Usage

Spark SQL is a module in Apache Spark that provides a structured data processing interface using SQL-like queries. It bridges the gap between the relational and procedural paradigms, allowing data analysts and developers to interact with data using SQL while leveraging Spark's scalability and performance.

**Key Features of Spark SQL:**
- **Unified Data Access**: Access data from various sources like JSON, Parquet, Avro, Hive, and JDBC.
- **Schema-Based Processing**: Enables schema definition and enforcement for structured data.
- **Integration with DataFrames**: Allows seamless transitions between SQL queries and DataFrame APIs.
- **Optimized Query Execution**: Uses the Catalyst Optimizer for efficient query planning.

*Example 1: Running SQL queries on a DataFrame*
```python
# Load data from JSON
flight_data_json = "dbfs:/mnt/data/data/flight-data/json/2015-summary.json"
df = spark.read.format("json").load(flight_data_json)

# Register DataFrame as a SQL table
df.createOrReplaceTempView("flight_data")

# Query using Spark SQL
query = """
SELECT DEST_COUNTRY_NAME, SUM(count) AS total_flights 
FROM flight_data 
GROUP BY DEST_COUNTRY_NAME 
ORDER BY total_flights DESC 
LIMIT 10
"""
sql_result = spark.sql(query)
sql_result.show()

# Additional data transformations
df_filtered = df.filter(df.count > 500)
df_grouped = df_filtered.groupBy("DEST_COUNTRY_NAME").agg({'count': 'sum'})
df_grouped.show()
```

---

### Creating and Managing Tables with Spark SQL

Spark SQL allows users to create and manage tables, supporting both temporary and permanent tables.

**1. Temporary Views:**
Temporary views exist only within the Spark session and are not persistent.

*Example 2: Creating and querying a temporary view*
```python
# Creating a temporary view
df.createOrReplaceTempView("temp_flights")

# Querying the temporary view
query = """
SELECT ORIGIN_COUNTRY_NAME, COUNT(*) AS num_flights
FROM temp_flights
GROUP BY ORIGIN_COUNTRY_NAME
HAVING num_flights > 500
ORDER BY num_flights DESC
LIMIT 5
"""
spark.sql(query).show()

# More complex query with multiple aggregations
query_advanced = """
SELECT ORIGIN_COUNTRY_NAME, SUM(count) AS total_passengers, AVG(count) AS avg_flights
FROM temp_flights
GROUP BY ORIGIN_COUNTRY_NAME
ORDER BY total_passengers DESC
LIMIT 10
"""
spark.sql(query_advanced).show()
```

**2. Global Temporary Views:**
Global temporary views are accessible across multiple sessions using the `global_temp` database.

*Example 3: Creating and querying a global temporary view*
```python
# Create a global temporary view
df.createOrReplaceGlobalTempView("global_flights")

# Query the global view
query = "SELECT * FROM global_temp.global_flights WHERE count > 500"
spark.sql(query).show()
```

**3. Managed Tables:**
Managed tables are stored in Spark's warehouse directory and managed by Spark.

*Example 4: Creating a managed table and inserting data*
```python
spark.sql("CREATE TABLE IF NOT EXISTS managed_table (DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)")
spark.sql("INSERT INTO managed_table SELECT * FROM temp_flights")
spark.sql("SELECT * FROM managed_table").show()
```

**4. External Tables:**
External tables reference data stored outside Spark's warehouse, such as in a data lake.

*Example 5: Creating an external table*
```python
spark.sql("CREATE TABLE external_table USING PARQUET LOCATION 'dbfs:/mnt/data/data/flight-data/parquet'")
spark.sql("SELECT * FROM external_table").show()
```

---

### Performance Optimization with Catalyst Optimizer

The Catalyst Optimizer is a key component of Spark SQL, designed to enhance query performance through logical and physical plan optimization.

**Key Techniques:**
- **Predicate Pushdown:** Filters are applied early to reduce data scanning.
- **Column Pruning:** Reads only required columns from the data source.
- **Join Optimization:** Reorders joins for optimal execution.
- **Query Caching:** Uses in-memory storage for frequently accessed data.

*Example 6: Using query caching for performance optimization*
```python
# Cache query result
cached_result = spark.sql("""
SELECT DEST_COUNTRY_NAME, SUM(count) AS total_flights
FROM flight_data
GROUP BY DEST_COUNTRY_NAME
""")
cached_result.cache()
cached_result.count()
cached_result.show()

# Explain the query plan
cached_result.explain()
```

---

### Using Databricks SQL for Queries and Visualization

Databricks SQL provides a user-friendly interface for running SQL queries and creating visualizations.

**Steps to Use Databricks SQL:**
1. Navigate to the SQL editor in Databricks.
2. Select or create a SQL warehouse.
3. Write and execute SQL queries.
4. Create dashboards and visualizations.

*Example 7: Running a SQL query in Databricks SQL*
```python
query = """
SELECT DEST_COUNTRY_NAME, SUM(count) AS total_flights
FROM flight_data
GROUP BY DEST_COUNTRY_NAME
ORDER BY total_flights DESC
LIMIT 10
"""
spark.sql(query).show()
```

---

### Exhaustive Code Examples

*Example 8: Aggregation with multiple conditions*
```python
query = """
SELECT ORIGIN_COUNTRY_NAME, COUNT(*) AS num_flights, SUM(count) AS total_passengers
FROM flight_data
GROUP BY ORIGIN_COUNTRY_NAME
HAVING num_flights > 500
ORDER BY total_passengers DESC
"""
spark.sql(query).show()
```

*Example 9: Complex joins and filtering*
```python
flights_2010 = spark.read.json("dbfs:/mnt/data/data/flight-data/json/2010-summary.json")
flights_2010.createOrReplaceTempView("flights_2010")

query = """
SELECT a.DEST_COUNTRY_NAME, a.count AS count_2015, b.count AS count_2010
FROM flight_data a
INNER JOIN flights_2010 b
ON a.DEST_COUNTRY_NAME = b.DEST_COUNTRY_NAME
WHERE a.count > 500 AND b.count > 500
ORDER BY a.count DESC
"""
spark.sql(query).show()
```

---

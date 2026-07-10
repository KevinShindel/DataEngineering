# The Multiplex Pattern

The Multiplex pattern addresses a common production challenge: efficiently processing multiple event types that arrive through a single data transport mechanism.

In production environments, multiple business systems often share a single data transport such as:

- One Kafka topic
- One cloud storage path
- One message queue

**Without Multiplex**
- N event types → N separate pipelines
- N separate checkpoints and source scans
- Source change must be applied N times

**With Multiplex**
- N event types → 1 ingestion pipeline
- 1 checkpoint, 1 source scan shared across all domains
- Source change applied in one place


## Ingest Once, Fan Out by Type

The Multiplex pattern follows a simple but powerful approach:

- **Single Ingestion**: Read all event types into one bronze table
- **Type-Based Filtering**: Use the event type field to separate domains
- **Fan-Out Processing**: Create domain-specific tables downstream

**Key Benefits**:

- **Single checkpoint management**
- **Shared source scanning**
- **Centralized error handling**
- **Simplified monitoring**



##  Sinks

Sinks provide a mechanism to write streaming data from a Spark Declarative Pipeline to external Delta tables that exist outside the pipeline's managed scope.
**Only the Python API is supported** — SQL is not supported for sinks. Only append_flow can write to a sink.

### Supported Sink Types
Databricks supports four types of sinks — each suited for a different destination and use case.

1. **Delta Table Sink**
   - Unity Catalog managed tables
   - External Delta tables
   - Write by path or table name
2. **Apache Kafka Sink**
   - Write back to Kafka topics
   - Low-latency operational use cases
   - Reverse ETL out of Databricks
3. **Azure Event Hubs Sink**
   - Uses Kafka interface format
   - Real-time event streaming
   - Fraud detection · recommendations
4. **Python Custom Sink**
   - Write to any data store
   - Uses PySpark custom data sources
   - Maximum flexibility

### Managed Tables vs. Sinks
Every standard dataset in a Spark Declarative Pipeline — streaming table or materialized view — is owned and managed by the pipeline. A sink breaks this intentionally: it lets the pipeline write streaming data to a plain Delta table that exists outside the pipeline's managed scope.

1. **Managed Table (Default)**
   - Data stays within Unity Catalog
   - Full pipeline lineage tracking
   - Supports expectations and CDC
   - Streaming Tables and Materialized Views
2. **Sink**
   - Write to external systems outside Databricks
   - Enables reverse ETL and operational use cases
   - Supports Kafka, Event Hubs, custom targets
   - No expectations — append only

###  Delta Sink In Action
A Delta sink writes pipeline output to a Delta table outside the pipeline's managed lifecycle — unlocking configurations not possible on pipeline-managed streaming tables, such as Iceberg compatibility.

**Streaming Tables Cannot**:
- Enable Iceberg compatibility
- Share with non-Databricks platforms
**Delta Sink Can**:
- Have full Delta table property control
- Enable Iceberg UniForm for cross-platform reads


#### Implementation

```python

## Step 1 — Register the Sink
from pyspark import pipelines as dp
dp.create_sink(
    name    = "my_sink",
    format  = "delta",
    options = {
        "tableName": "catalog.schema.table"
    }
)

## Step 2- Write to the Sink

@dp.append_flow(
name   = "my_sink_flow",
target = "my_sink"
)
def my_sink_flow():
    return spark.readStream.table(
        "schema.source_table"
    )
```

1. Checkpointing is handled automatically by append_flow
2. Only new records written per run — no overwrites
3. Python only — no SQL equivalent
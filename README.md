# DataEngineering

This repository contains resources and examples for learning and practicing Data Engineering concepts, focusing on Apache Spark, PySpark, and Databricks.

## 📋 Prerequisites

1. Install JDK: `sudo apt-get install default-jdk`
2. Download Spark: [https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz)
3. Install requirements: `pip install -r requirements.txt`

## 📁 Folder Structure

### 🏗️ Databricks Fundamental
Explores the fundamentals of the Databricks Lakehouse platform with notebooks and configurations for:
- Transforming data for Gold and Silver layers
- Apache Spark programming basics
- Data ingestion using Delta Lake
- Data privacy considerations
- Performance optimization in Databricks
- Workflows and automation
- Delta Lake streaming capabilities
- Unity Catalog for data governance

### ⚡ DatabricksAdvanced
Advanced Databricks topics covering:
- Change Data Capture (CDC) and Slowly Changing Dimensions (SCD)
- Data ingestion patterns and best practices
- Complex data transformations
- Spark optimizations
- Advanced SQL operations

### 📥 DataIngestion
Comprehensive guide for data ingestion strategies:
- Comparison of `read_files` vs `cloud_files` with performance benchmarks
- Auto Loader capabilities for large-scale data ingestion
- Ingestion patterns (CTAS, COPY INTO, Streaming Tables, MERGE INTO)
- File discovery and change tracking
- Schema evolution handling

### 🔌 Connection
Database connectivity and data federation:
- Setting up connections to external databases (PostgreSQL, etc.)
- Creating foreign catalogs with Unity Catalog
- Data Federation for querying external systems

### ⚙️ Optimization
Performance tuning and optimization techniques:
- Table optimization with Z-order clustering
- Vacuum and log cleaning operations
- Auto-optimize configurations
- Partitioning strategies and best practices
- Task duration monitoring

### 🔐 PII
Data privacy and security:
- PII (Personally Identifiable Information) masking and anonymization
- Dynamic masking with role-based access control
- Column-level security policies

### 🚀 CI/CD
Deployment and automation:
- Manual, programmatic, and infrastructure-as-code deployment approaches
- Databricks Asset Bundles (DAB) with YAML configuration
- Terraform integration for infrastructure deployment
- Project structure and templates
- Testing frameworks for data pipelines

### 🔥 PySparkBasics
Collection of Python scripts demonstrating basic to advanced PySpark operations:
- RDD (Resilient Distributed Datasets) operations: collect, filter, reduce, map, flatMap, distinct
- DataFrame manipulations: reading/writing JSON, adding columns, concatenating DataFrames
- Machine Learning examples: K-Means, Decision Trees, Naive Bayes, Random Forest, Logistic Regression, Topic Modeling
- SQL query execution in Spark
- Working with multiple files and parallelization

### 📚 SparkByExamples
Practical examples for Apache Spark concepts:
- Introduction to Spark
- Working with dates and time
- Joins and data merging
- RDD operations
- Pivot and stack transformations
- Exercise files with real-world datasets (police stations, reported crimes)

## 📄 License
See LICENSE file for details.

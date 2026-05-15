# DataEngineering

This repository contains resources and examples for learning and practicing Data Engineering concepts, focusing on Apache Spark, PySpark, and Databricks.

## 📋 Prerequisites

1. Install JDK: `sudo apt-get install default-jdk`
2. Download Spark: [https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz)
3. Install requirements: `pip install -r requirements.txt`

## 📁 Folder Structure

### 🏗️ Databricks Fundamental
This folder explores the fundamentals of the Databricks Lakehouse platform. It includes notebooks and configurations for:
- Transforming data for Gold and Silver layers
- Apache Spark programming basics
- Data ingestion using Delta Lake
- Data privacy considerations
- Performance optimization in Databricks
- Workflows and automation
- Delta Lake streaming capabilities
- Unity Catalog for data governance

### 🔥 PySparkBasics
A collection of Python scripts demonstrating basic to advanced PySpark operations. Covers:
- RDD (Resilient Distributed Datasets) operations like collect, filter, reduce, map, flatMap, distinct
- DataFrame manipulations: reading/writing JSON, adding columns, concatenating DataFrames
- Machine Learning examples: K-Means, Decision Trees, Naive Bayes, Random Forest, Logistic Regression, Topic Modeling
- SQL query execution in Spark
- Working with multiple files and parallelization

### 📚 SparkByExamples
Practical examples for Apache Spark concepts, including:
- Introduction to Spark
- Working with dates and time
- Joins and data merging
- RDD operations
- Pivot and stack transformations
- Exercise files with real-world datasets (police stations, reported crimes)

## 📄 License
See LICENSE file for details.

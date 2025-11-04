# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI Docs Vector Search
# MAGIC This notebook demonstrates how to manually create a vector search table

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

import mlflow
config = mlflow.models.ModelConfig(development_config='config.yaml')
CATALOG = config.get('catalog')
dbutils.widgets.text('catalog', CATALOG)
SCHEMA = config.get('schema')
dbutils.widgets.text('schema', SCHEMA)
RAW_DATA_VOL = config.get('raw_data_vol')

# COMMAND ----------

from pyspark.sql import functions as F

doc_df = (
    spark.read
    .format("text")
    .option("wholetext", "true")
    .load(f"/Volumes/{CATALOG}/{SCHEMA}/{RAW_DATA_VOL}/scrape/*.md")
    .withColumn("unique_id", F.monotonically_increasing_id())
    .select(
        "unique_id",
        F.col("_metadata.file_path").alias("file_name"),
        "value"
    )
)

display(doc_df)

# COMMAND ----------

doc_df.write.mode('overwrite').saveAsTable(f'{CATALOG}.{SCHEMA}.documents')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `${catalog}`.`${schema}`.documents SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()
client.create_endpoint(
    name="bixi_vs_endpoint",
    endpoint_type="STANDARD"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Index

# COMMAND ----------

index = client.create_delta_sync_index(
    endpoint_name="bixi_vs_endpoint",
    source_table_name=f"{CATALOG}.{SCHEMA}.documents",
    index_name=f"{CATALOG}.{SCHEMA}.documents_index",
    pipeline_type="TRIGGERED",
    primary_key="unique_id",                # Must be present in your table
    embedding_source_column="value",  # Text column for embedding
    embedding_model_endpoint_name="databricks-gte-large-en" # or any available model
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *, 
# MAGIC   floor(unique_id / 5) AS unique_id_bin_10 
# MAGIC FROM vector_search(
# MAGIC   index=>'`${catalog}`.`${schema}`.documents_index',
# MAGIC   query_text=>"Trip Fares",
# MAGIC   num_results=>50,
# MAGIC   query_type=>'hybrid'
# MAGIC )
# MAGIC ORDER BY unique_id_bin_10 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `${catalog}`.`${schema}`.doc_search(
# MAGIC   description STRING COMMENT 'A search of bixi documents'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   unique_id INTEGER,
# MAGIC   file_name STRING,
# MAGIC   value STRING,
# MAGIC   search_score STRING
# MAGIC )
# MAGIC COMMENT 'Returns the top three documents matching semantic search.
# MAGIC '
# MAGIC RETURN
# MAGIC SELECT *
# MAGIC FROM vector_search(
# MAGIC   index=>'`${catalog}`.`${schema}`.documents_index',
# MAGIC   query_text=>description,
# MAGIC   num_results=>3,
# MAGIC   query_type=>'hybrid'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `${catalog}`.`${schema}`.doc_search("Trip Fares")

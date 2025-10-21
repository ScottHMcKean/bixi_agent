# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI Docs Vector Search
# MAGIC This notebook demonstrates how to manually create a vector search table

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import functions as F

doc_df = (
    spark.read
    .format("text")
    .option("wholetext", "true")
    .load("/Volumes/hack/bixi/raw_data/scrape/*.md")
    .withColumn("unique_id", F.monotonically_increasing_id())
    .select(
        "unique_id",
        F.col("_metadata.file_path").alias("file_name"),
        "value"
    )
)

display(doc_df)

# COMMAND ----------

doc_df.write.mode('overwrite').saveAsTable('hack.bixi.documents')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hack.bixi.documents SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

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
    source_table_name="hack.bixi.documents",
    index_name="hack.bixi.documents_index",
    pipeline_type="TRIGGERED",
    primary_key="unique_id",                # Must be present in your table
    embedding_source_column="value",  # Text column for embedding
    embedding_model_endpoint_name="databricks-gte-large-en" # or any available model
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vector_search(
# MAGIC   index=>'hack.bixi.documents_index',
# MAGIC   query_text=>"Trip Fares",
# MAGIC   num_results=>3,
# MAGIC   query_type=>'hybrid'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hack.bixi.doc_search(
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
# MAGIC   index=>'hack.bixi.documents_index',
# MAGIC   query_text=>description,
# MAGIC   num_results=>3,
# MAGIC   query_type=>'hybrid'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hack.bixi.doc_search("Trip Fares")

# Databricks notebook source
# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS mvp CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG mvp

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG mvp

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS staging CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA staging

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS bronze CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS prata CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA prata

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS ouro CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ouro

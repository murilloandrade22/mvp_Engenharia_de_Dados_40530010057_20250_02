# Databricks notebook source
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA ouro")

# COMMAND ----------

df_verificacao = spark.table("mvp.prata.bpss")
display(df_verificacao)

# COMMAND ----------

df_ouro1 = spark.table("mvp.prata.bpss")
colunas_especificas_p1 = ["min", "fg_pct", "3p_pct", "ft_pct", "pts_per_min"]

df_pergunta1 = df_ouro1.select(*colunas_especificas_p1)
df_pergunta1.write.format("delta").mode("overwrite").saveAsTable("mvp.ouro.P1")

# COMMAND ----------

df_ouro2 = spark.table("mvp.prata.bpss")
colunas_especificas_p2 = ["season","fg_pct","3p_pct","ft_pct","pts_per_min"]

df_pergunta2 = df_ouro2.select(*colunas_especificas_p2)
df_pergunta2.write.format("delta").mode("overwrite").saveAsTable("mvp.ouro.P2")

# COMMAND ----------

df_ouro3 = spark.table("mvp.prata.bpss")
colunas_especificas_p3 = ["player","min","fgm","3pm","ftm","pts"]

df_pergunta3 = df_ouro3.select(*colunas_especificas_p3)
#somando atributos e agrupando por jogador
df_pergunta3_sum = (
    df_pergunta3
    .groupBy("player")
    .sum("min", "fgm", "3pm", "ftm", "pts")
    .withColumnRenamed("sum(min)", "min")
    .withColumnRenamed("sum(fgm)", "fgm")
    .withColumnRenamed("sum(3pm)", "3pm")
    .withColumnRenamed("sum(ftm)", "ftm")
    .withColumnRenamed("sum(pts)", "pts")
)
df_pergunta3_sum.write.format("delta").mode("overwrite").saveAsTable("mvp.ouro.P3")

# COMMAND ----------

df_ouro4 = spark.table("mvp.prata.bpss")
colunas_especificas_p4 = ["season","player","fg_pct","3p_pct","ft_pct","pts_per_min","min"]

df_pergunta4 = df_ouro4.select(*colunas_especificas_p4)
df_pergunta4.write.format("delta").mode("overwrite").saveAsTable("mvp.ouro.P4")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, when

df_ouro5 = spark.table("mvp.prata.bpss")
colunas_especificas_p5 = ["season", "player", "age_in_season", "fg_pct", "3p_pct", "ft_pct", "pts_per_min"]

df_pergunta5 = df_ouro5.select(*colunas_especificas_p5)

# Define janela para comparar com o ano anterior do mesmo jogador
window_spec = Window.partitionBy("player").orderBy("season")

# Adiciona colunas de desempenho do ano anterior
df_comp = (
    df_pergunta5
    .withColumn("prev_fg_pct", lag("fg_pct").over(window_spec))
    .withColumn("prev_3p_pct", lag("3p_pct").over(window_spec))
    .withColumn("prev_ft_pct", lag("ft_pct").over(window_spec))
    .withColumn("prev_pts_per_min", lag("pts_per_min").over(window_spec))
    .withColumn("fg_pct_trend", when(col("fg_pct") > col("prev_fg_pct"), "melhorou")
                                .when(col("fg_pct") < col("prev_fg_pct"), "piorou")
                                .otherwise("igual"))
    .withColumn("3p_pct_trend", when(col("3p_pct") > col("prev_3p_pct"), "melhorou")
                                .when(col("3p_pct") < col("prev_3p_pct"), "piorou")
                                .otherwise("igual"))
    .withColumn("ft_pct_trend", when(col("ft_pct") > col("prev_ft_pct"), "melhorou")
                                .when(col("ft_pct") < col("prev_ft_pct"), "piorou")
                                .otherwise("igual"))
    .withColumn("pts_per_min_trend", when(col("pts_per_min") > col("prev_pts_per_min"), "melhorou")
                                     .when(col("pts_per_min") < col("prev_pts_per_min"), "piorou")
                                     .otherwise("igual"))
    .select("season", "player", "age_in_season", "fg_pct", "fg_pct_trend",
            "3p_pct", "3p_pct_trend", "ft_pct", "ft_pct_trend",
            "pts_per_min", "pts_per_min_trend")
)

display(df_comp)

# COMMAND ----------

# Agrupando por idade e contando "melhorou" e "piorou" para cada medida
from pyspark.sql.functions import count, expr

df_trend_counts = (
    df_comp
    .groupBy("age_in_season")
    .agg(
        count(when(col("fg_pct_trend") == "melhorou", True)).alias("fg_pct_melhorou"),
        count(when(col("fg_pct_trend") == "piorou", True)).alias("fg_pct_piorou"),
        count(when(col("3p_pct_trend") == "melhorou", True)).alias("3p_pct_melhorou"),
        count(when(col("3p_pct_trend") == "piorou", True)).alias("3p_pct_piorou"),
        count(when(col("ft_pct_trend") == "melhorou", True)).alias("ft_pct_melhorou"),
        count(when(col("ft_pct_trend") == "piorou", True)).alias("ft_pct_piorou"),
        count(when(col("pts_per_min_trend") == "melhorou", True)).alias("pts_per_min_melhorou"),
        count(when(col("pts_per_min_trend") == "piorou", True)).alias("pts_per_min_piorou")
    )
)

df_trend_counts.write.format("delta").mode("overwrite").saveAsTable("mvp.ouro.P5")
# Databricks notebook source
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA prata")

# COMMAND ----------

from pyspark.sql.functions import col, split, trim, expr

# escolhendo apenas jogadores da NBA e da fase playoffs
df = spark.table("mvp.bronze.bpss").filter((col("League") == "NBA") & (col("Stage") == "Playoffs"))

# escolhendo colunas específicas
colunas_especificas = ["season", "player", "min", "fgm", "fga", "3pm", "3pa", "ftm", "fta", "pts", "birth_year"]

# filtra jogadores com mais de 100 minutos
df_filtro = df.filter(col("min") > 100).select(*colunas_especificas)

# criando colunas adicionais (no caso de porcentagem de acerto, vou deixar como 0
#se não houver tentativa)
df_prata = (
    df_filtro
    .withColumn("fg_pct", expr("CASE WHEN fga > 0 THEN fgm / fga ELSE 0 END"))
    .withColumn("3p_pct", expr("CASE WHEN 3pa > 0 THEN 3pm / 3pa ELSE 0 END"))
    .withColumn("ft_pct", expr("CASE WHEN fta > 0 THEN ftm / fta ELSE 0 END"))
    .withColumn("pts_per_min", expr("pts / min"))
    .withColumn("season_start_year", trim(split(col("season"), "-")[0]))
    .withColumn("age_in_season", expr("CAST(season_start_year AS INT) - birth_year"))
    .drop("season_start_year")
    .drop("birth_year")
)

display(df_prata.limit(10))

# COMMAND ----------

df_prata.write.format("delta").mode("overwrite").saveAsTable("mvp.prata.BPSS")

# COMMAND ----------

from pyspark.sql.functions import expr

# Comentários para colunas de porcentagem (mínimo 0, máximo 1)
spark.sql("COMMENT ON COLUMN BPSS.fg_pct IS 'Porcentagem de acerto de arremessos de quadra. min: 0, max: 1'")
spark.sql("COMMENT ON COLUMN BPSS.3p_pct IS 'Porcentagem de acerto de arremessos de 3 pontos. min: 0, max: 1'")
spark.sql("COMMENT ON COLUMN BPSS.ft_pct IS 'Porcentagem de acerto de lances livres. min: 0, max: 1'")

# Calcula mínimos e máximos para pts_per_min e age_in_season
stats = spark.table("mvp.prata.bpss").agg(
    expr("min(pts_per_min)").alias("min_pts_per_min"),
    expr("max(pts_per_min)").alias("max_pts_per_min"),
    expr("min(age_in_season)").alias("min_age_in_season"),
    expr("max(age_in_season)").alias("max_age_in_season")
).collect()[0]

min_pts_per_min = stats["min_pts_per_min"]
max_pts_per_min = stats["max_pts_per_min"]
min_age_in_season = stats["min_age_in_season"]
max_age_in_season = stats["max_age_in_season"]

spark.sql(f"COMMENT ON COLUMN BPSS.pts_per_min IS 'Pontos por minuto jogado. min: {min_pts_per_min}, max: {max_pts_per_min}'")
spark.sql(f"COMMENT ON COLUMN BPSS.age_in_season IS 'Idade do jogador na temporada. min: {min_age_in_season}, max: {max_age_in_season}'")

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando a tabela, é possível ver que o limite mínimo de 100 minutos jogados na temporada tirou possíveis valores nulos do dataset original, e na criação de atributos coloquei que se a tentativa de algum arremesso fosse 0, o valor final da porcentagem seria 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bpss
# MAGIC limit 10

# COMMAND ----------

df_analise = spark.table("mvp.prata.bpss")

# Análise de qualidade por atributo
from pyspark.sql.functions import col, count, countDistinct, isnan, when

colunas = df_analise.columns

# Verifica se a coluna é numérica antes de aplicar isnan
def is_numeric_type(dtype):
    return dtype in ["double", "float", "int", "bigint", "decimal", "long", "short"]

analise = (
    df_analise
    .agg(
        *[
            count(
                when(
                    col(c).isNull() | (is_numeric_type(df_analise.schema[c].dataType.typeName()) and isnan(col(c))),
                    c
                )
            ).alias(f"{c}_nulls")
            for c in colunas
        ]       
    )
)

display(analise)

# COMMAND ----------

# MAGIC %md
# MAGIC Não há valores nulos.
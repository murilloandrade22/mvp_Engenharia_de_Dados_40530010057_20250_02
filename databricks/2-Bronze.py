# Databricks notebook source
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA staging")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME basquete

# COMMAND ----------

spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

##fiz o upload diretamente na pasta
df = spark.read.csv(
    "/Volumes/mvp/staging/basquete/players_stats_by_season_full_details.csv",
    header=True,
    inferSchema=True
)
display(df.limit(10))

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("BPSS")

# COMMAND ----------

spark.sql("COMMENT ON COLUMN BPSS.League IS 'Liga em que o jogador atuou'")
spark.sql("COMMENT ON COLUMN BPSS.Season IS 'Temporada do campeonato'")
spark.sql("COMMENT ON COLUMN BPSS.Stage IS 'Fase da competição'")
spark.sql("COMMENT ON COLUMN BPSS.Player IS 'Nome do jogador'")
spark.sql("COMMENT ON COLUMN BPSS.Team IS 'Nome do time'")
spark.sql("COMMENT ON COLUMN BPSS.GP IS 'Jogos disputados'")
spark.sql("COMMENT ON COLUMN BPSS.MIN IS 'Minutos jogados'")
spark.sql("COMMENT ON COLUMN BPSS.FGM IS 'Arremessos convertidos'")
spark.sql("COMMENT ON COLUMN BPSS.FGA IS 'Arremessos tentados'")
spark.sql("COMMENT ON COLUMN BPSS.3PM IS 'Arremessos de 3 pontos convertidos'")
spark.sql("COMMENT ON COLUMN BPSS.3PA IS 'Arremessos de 3 pontos tentados'")
spark.sql("COMMENT ON COLUMN BPSS.FTM IS 'Lances livres convertidos'")
spark.sql("COMMENT ON COLUMN BPSS.FTA IS 'Lances livres tentados'")
spark.sql("COMMENT ON COLUMN BPSS.TOV IS 'Erros cometidos (turnovers)'")
spark.sql("COMMENT ON COLUMN BPSS.PF IS 'Faltas cometidas'")
spark.sql("COMMENT ON COLUMN BPSS.ORB IS 'Rebotes ofensivos'")
spark.sql("COMMENT ON COLUMN BPSS.DRB IS 'Rebotes defensivos'")
spark.sql("COMMENT ON COLUMN BPSS.REB IS 'Total de rebotes'")
spark.sql("COMMENT ON COLUMN BPSS.AST IS 'Assistências'")
spark.sql("COMMENT ON COLUMN BPSS.STL IS 'Roubos de bola'")
spark.sql("COMMENT ON COLUMN BPSS.BLK IS 'Tocos (bloqueios)'")
spark.sql("COMMENT ON COLUMN BPSS.PTS IS 'Pontos marcados'")
spark.sql("COMMENT ON COLUMN BPSS.birth_year IS 'Ano de nascimento'")
spark.sql("COMMENT ON COLUMN BPSS.birth_month IS 'Mês de nascimento'")
spark.sql("COMMENT ON COLUMN BPSS.birth_date IS 'Dia de nascimento'")
spark.sql("COMMENT ON COLUMN BPSS.height IS 'Altura (formato original)'")
spark.sql("COMMENT ON COLUMN BPSS.height_cm IS 'Altura em centímetros'")
spark.sql("COMMENT ON COLUMN BPSS.weight IS 'Peso (formato original)'")
spark.sql("COMMENT ON COLUMN BPSS.weight_kg IS 'Peso em quilogramas'")
spark.sql("COMMENT ON COLUMN BPSS.nationality IS 'Nacionalidade do jogador'")
spark.sql("COMMENT ON COLUMN BPSS.high_school IS 'Escola de ensino médio'")
spark.sql("COMMENT ON COLUMN BPSS.draft_round IS 'Rodada do draft'")
spark.sql("COMMENT ON COLUMN BPSS.draft_pick IS 'Escolha do draft'")
spark.sql("COMMENT ON COLUMN BPSS.draft_team IS 'Time que selecionou no draft'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bpss
# MAGIC limit 10
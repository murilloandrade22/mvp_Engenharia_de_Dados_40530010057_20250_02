# Databricks notebook source
spark.sql("USE CATALOG mvp")
spark.sql("USE SCHEMA ouro")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from p1
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ============================================================== 1) Existe correlação entre tempo em quadra e eficiência dos jogadores?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CORR(fg_pct, min) AS correlacao_fg_pct_min,
# MAGIC   CORR(3p_pct, min) AS correlacao_3p_pct_min,
# MAGIC   CORR(ft_pct, min) AS correlacao_ft_pct_min,
# MAGIC   CORR(pts_per_min, min) AS correlacao_pts_per_min_min
# MAGIC FROM p1
# MAGIC where fg_pct>0 and 3p_pct>0 and ft_pct>0

# COMMAND ----------

# MAGIC %md
# MAGIC Baixa correlação com porcentagem de arremessos em quadra e com pontos por minuto. Sem correlação com porcentagem de arremessos de 3 pontos e de lances livres.

# COMMAND ----------

# MAGIC %md
# MAGIC ==============================================================
# MAGIC 2) Como é a evolução das características ofensivas ao longo do tempo na NBA?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   season,
# MAGIC   AVG(fg_pct) AS avg_fg_pct --média de acertos de arremessos em quadra
# MAGIC FROM p2
# MAGIC where fg_pct>0
# MAGIC GROUP BY season

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   season,
# MAGIC   AVG(3p_pct) AS avg_3p_pct --média de acertos de arremessos de 3
# MAGIC FROM p2
# MAGIC where 3p_pct>0
# MAGIC GROUP BY season

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   season,
# MAGIC   AVG(ft_pct) AS avg_ft_pct --média de acertos de lances livres
# MAGIC FROM p2
# MAGIC where ft_pct>0
# MAGIC GROUP BY season

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   season,
# MAGIC   AVG(pts_per_min) AS avg_pts_per_min --média de pontos por minuto
# MAGIC FROM p2
# MAGIC where pts_per_min>0
# MAGIC GROUP BY season

# COMMAND ----------

# MAGIC %md
# MAGIC ============================================================== 3) Quais jogadores mais se destacam no mata mata?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT player,metrica,tipo_metrica FROM (
# MAGIC (
# MAGIC   SELECT player, ROUND(min,1) AS metrica, 'Minutos Jogados' AS tipo_metrica,1 as ordem
# MAGIC   FROM p3
# MAGIC   ORDER BY min DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC   SELECT player, fgm AS metrica, 'Cestas Marcadas' AS tipo_metrica,2 as ordem
# MAGIC   FROM p3
# MAGIC   ORDER BY fgm DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC   SELECT player, 3pm AS metrica, 'Cestas de 3 Marcadas' AS tipo_metrica,3 as ordem
# MAGIC   FROM p3
# MAGIC   ORDER BY 3pm DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC   SELECT player, ftm AS metrica, 'Lances Livres Marcados' AS tipo_metrica,4 as ordem
# MAGIC   FROM p3
# MAGIC   ORDER BY ftm DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC   SELECT player, pts AS metrica, 'Pontos Marcados' AS tipo_metrica,5 as ordem
# MAGIC   FROM p3
# MAGIC   ORDER BY pts DESC
# MAGIC   LIMIT 3
# MAGIC )) melhores
# MAGIC ORDER BY ordem, metrica DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ============================================================== 4) Quais são as maiores atuações no mata mata?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT percentile_cont(0.6) WITHIN GROUP (ORDER BY min) AS mediana_min FROM p4
# MAGIC --calculando mediana

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT player,season,metrica,tipo_metrica FROM (
# MAGIC (
# MAGIC   SELECT season,player, ROUND(fg_pct,1) AS metrica, 'Porcentagem de Cestas Convertidas' AS tipo_metrica,1 as ordem
# MAGIC   FROM p4
# MAGIC   WHERE min >= 232
# MAGIC   ORDER BY fg_pct DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC   SELECT season,player, ROUND(3p_pct,1) AS metrica, 'Porcentagem de Cestas de 3 Convertidas' AS tipo_metrica,2 as ordem
# MAGIC   FROM p4
# MAGIC   WHERE min >= 450
# MAGIC   ORDER BY 3p_pct DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC   SELECT season,player, ROUND(ft_pct,1) AS metrica, 'Porcentagem de Lances Livres Convertidos' AS tipo_metrica,3 as ordem
# MAGIC   FROM p4
# MAGIC   WHERE min >= 450
# MAGIC   ORDER BY ft_pct DESC
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC  SELECT season,player, ROUND(pts_per_min,1) AS metrica, 'Pontos por Minuto' AS tipo_metrica,4 as ordem
# MAGIC   FROM p4
# MAGIC   WHERE min >= 232
# MAGIC   ORDER BY pts_per_min DESC
# MAGIC   LIMIT 3
# MAGIC )) melhores
# MAGIC ORDER BY ordem, metrica DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ============================================================== 5) Existe correlação entre idade dos jogadores e desempenho nos Playoffs?

# COMMAND ----------

# MAGIC %sql
# MAGIC select age_in_season as idade,(fg_pct_melhorou - fg_pct_piorou) + 20 as melhora_cestas from p5

# COMMAND ----------

# MAGIC %sql
# MAGIC select age_in_season as idade,(3p_pct_melhorou - 3p_pct_piorou) + 15 as melhora_cestas_3pts from p5

# COMMAND ----------

# MAGIC %sql
# MAGIC select age_in_season as idade,(ft_pct_melhorou - ft_pct_piorou) + 29 as melhora_cestas_lances from p5

# COMMAND ----------

# MAGIC %sql
# MAGIC select age_in_season as idade,(pts_per_min_melhorou - pts_per_min_piorou) + 39 as media_pontos_por_minuto from p5

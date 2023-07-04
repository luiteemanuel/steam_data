# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName(" Importação do GCS para Databricks") \
    .config("spark.hadoop.fs.gs.project.id", "estudos-databricks") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/Workspace/Repos/luite8555@gmail.com/steam_data/estudos-databricks-a40d04e07f41.json") \
    .getOrCreate()


# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("gs://estudy-games/games.csv")
total_linhas = df.count()
# Exibir o número total de linhas
print("Total de linhas:", total_linhas)

# COMMAND ----------

df = df.withColumn("Peak CCU", F.col("Peak CCU").cast(IntegerType()))

# COMMAND ----------

#Removendo colunas  
game_df = df.drop('Estimated owners', 'Full audio languages', 'Metacritic url', 'User score', 'Score rank', 'Notes', 'Average playtime forever','Screenshots', 'Movies', 'Tags')

# COMMAND ----------

total_linhas = game_df.count()
# Exibir o número total de linhas
print("Total de linhas:", total_linhas)
game_df.display()


# COMMAND ----------

# Verificando valores de uma coluna
(
    game_df
    .select('Required age', 'DLC count')
    .distinct()
    .show()
)


# COMMAND ----------

games_caro = (
    game_df
    .filter((F.col('Peak CCU') > 1000) & 
            (F.col('Price') >= 50.00))
)
linha = games_caro.count()
print(linha)
games_caro.display()


# COMMAND ----------

# Top 10 Jogos mais jogado simultaneamente. 
top_10_jogos = (
    game_df
    .orderBy(F.desc('Peak CCU'))
    .limit(10)
)
top_10_jogos.display()

# COMMAND ----------

# Jogos com Restrição de Idade +18
Jogos_idade = (
    game_df
    .filter((F.col('Required age') >= 18))
)
Jogos_idade.display()
Jogos_idade.count()

# COMMAND ----------

# Jogos com Restrição de Idade +18
Jogos_idade_menor = (
    game_df
    .filter((F.col('Required age') < 18))
)
Jogos_idade_menor.display()
Jogos_idade_menor.count()

# COMMAND ----------

#filtrando jogos com DLC 
jogos_dlc = (
    game_df
    .filter((F.col('DLC count') >=1))
    .orderBy((F.desc('DLC count')))
)
jogos_dlc.count()
jogos_dlc.display()

# COMMAND ----------

# filtrnado valores por idioma
idioma_game = ( 
    game_df
    .filter(F.col('Supported languages').contains('Portuguese - Brazil'))
)
idioma_game.display()

# COMMAND ----------

#selecionando a tabela e contanto as linhas
total_reviews = game_df.select(F.col('Reviews')).count()
print("Total de reviews:", total_reviews)


# COMMAND ----------

#dropando jogos com Reviews em branco
game_reviews = (
    game_df
    .dropna(subset=['Reviews'])
)
#game_reviews.count()
game_reviews.display()

# COMMAND ----------

# Jogos multiplataforma 
jogos_multi = (
    game_df
    .withColumn('Multiplatform', when((game_df['Linux'] == 'True') & 
    (game_df['Windows'] == 'True') & 
    (game_df['Mac'] == 'True'), 'Sim')
    .otherwise('Não'))

)
jogos_multi.display()

# COMMAND ----------

# jogos com maiores Recomendação 

recomenda = (
    game_df
    .withColumn("Recommendations", F.col("Recommendations").cast(IntegerType()))
    .orderBy(F.desc('Recommendations'))
)
display(recomenda)

# COMMAND ----------



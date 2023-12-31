# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import to_date, to_timestamp, date_format


spark = SparkSession.builder \
    .appName(" Importação do GCS para Databricks") \
    .config("spark.hadoop.fs.gs.project.id", "estudos-databricks") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/Workspace/Repos/luite8555@gmail.com/steam_data/estudos-databricks-a40d04e07f41.json") \
    .getOrCreate()


# COMMAND ----------

# CONECTANDO AO GOOGLE STORAGE E ARMAZENANDO DADOS EM UM DATAFRAME SPARK! 
df = spark.read.format("csv").option("header", "true").load("gs://estudy-games/games.csv")
total_linhas = df.count()
# Exibir o número total de linhas
print("Total de linhas:", total_linhas)

# COMMAND ----------

# MAGIC %md
# MAGIC TRATAMENTO DOS DADOS!!
# MAGIC

# COMMAND ----------

game_df = (
    # ADCIONANDO A COLUNA Multiplataforma
    df.withColumn('Multiplataforma', when((df['Linux'] == 'True') & 
    (df['Windows'] == 'True') & (df['Mac'] == 'True'), 'Sim')
    .otherwise('Não'))
    #Removendo colunas  
        .drop('Estimated owners', 'Full audio languages', 'Metacritic url', 'User score', 'Score rank', 'Notes', 'Average playtime forever', 'Screenshots', 'Movies', 'Tags', 'Support url', 'Header image')
        .dropDuplicates(['Name'])
        .dropna(subset=['Name'])
        .dropna(subset=['Reviews'])
    #Mudando tipo dos Dados 
        .withColumn("Peak CCU", F.col("Peak CCU").cast(IntegerType()))
        .withColumn("Windows", F.col("Windows").cast(BooleanType()))
        .withColumn("Mac", F.col("Mac").cast(BooleanType()))
        .withColumn("Linux", F.col("Linux").cast(BooleanType()))
)
# Convertendo a coluna 'Release date' para o formato correto
if 'Release date' in game_df.columns:
    game_df = game_df.withColumn('Release date', to_date(game_df['Release date'], 'MMM d, yyyy'))
    game_df = game_df.withColumn('Release date', date_format(game_df['Release date'], 'dd/MM/yyyy'))
else:
    print("A coluna 'Release date' não existe no DataFrame.")

# COMMAND ----------

game_df.display(3)

# COMMAND ----------

game_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC FILTRANDO VALORES E PARA EXTRAIR INFORMAÇOES RELEVANTES!
# MAGIC

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

# FILTRANDO JOGOS POR MAIORES PICOS DE JOGADORES DE JOGOS QUE CUSTAM MAIS DE 50$ 
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

# Jogos com Restrição de Idade menor que 18 anos
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

# jogos com maiores recomendações
recomenda = (
    game_df
    .withColumn("Recommendations", F.col("Recommendations").cast(IntegerType()))
    .orderBy(desc('Recommendations'))
    .select( 'Name', 'Recommendations')
    .limit(10)

)
display(recomenda)

# COMMAND ----------

# Filtrando jogos do gênero "RPG" e ordenar por pico de jogadores simultâneos (Peak CCU) em ordem decrescente
jogos_genero = (
    game_df
    .select('Name', 'Peak CCU', 'Price', 'Required age')
    .filter(col('Genres').contains('RPG'))
    .orderBy(desc('Peak CCU'))
)
# Exibir resultados
jogos_genero.display()


# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC ##Conferindo se os dados foram montados e se temos acessoa pasta inbound  

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

# MAGIC %md
# MAGIC # Lendo os dados da camada inbound

# COMMAND ----------


path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Removendo colunas

# COMMAND ----------

dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

# COMMAND ----------

# MAGIC %md
# MAGIC # Criando uma coluna de identificação

# COMMAND ----------


#import org.apache.spark.sql.functions.col
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------


df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Salvando na camada bronze

# COMMAND ----------

path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode("Overwrite").save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ex:outras transformações

# COMMAND ----------

df_endereco = df_bronze.select("anuncio.endereco")

path = "dbfs:/mnt/dados/bronze/dataset_endereco"
df_endereco.write.format("parquet").mode("overwrite").save(path)

# COMMAND ----------



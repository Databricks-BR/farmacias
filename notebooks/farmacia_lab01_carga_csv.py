# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/lab_agosto_2025/main/images/head_lab.png">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descrição
# MAGIC
# MAGIC | projeto | aplicação | módulo | tabela | objetivo |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | ACADEMY | Laboratório 1 | ETL Bronze | Diversas CSV | Ingestão de arquivos publicos CSV - bases de teste para os Laboratorios |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referências
# MAGIC * [Leitura de Arquivos CSV](https://learn.microsoft.com/pt-br/azure/databricks/external-data/csv)
# MAGIC * [Notebook Exemplo - CSV](https://docs.databricks.com/_extras/notebooks/source/read-csv-files.html)
# MAGIC * [Salvando uma Tabela DELTA](https://docs.databricks.com/delta/tutorial.html#create-a-table)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parâmetros Iniciais

# COMMAND ----------


import pandas as pd
from pyspark.sql import SparkSession

url = f"https://raw.githubusercontent.com//Databricks-BR/farmacias/main/dados/"


catalog_name = f"pague_menos"


# COMMAND ----------

# DBTITLE 1,ALTERE ESSE PARAMETRO
#schema_name  = f"<<<<<-----COLOQUE SEU USER NAME AQUI --------->>>>"

schema_name  = f"financeiro"


# COMMAND ----------

create_catalog = f"CREATE CATALOG IF NOT EXISTS {catalog_name}"
spark.sql (create_catalog)


create_schema = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"
spark.sql (create_schema)

create_schema = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.sand_box"
spark.sql (create_schema)


# COMMAND ----------

# DBTITLE 1,Gravando a tabela DELTA - FATURAMENTO

entity_name  = f"acoes_b3_drogarias"

table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
file_name = f"{url}{entity_name}.csv"

df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta

# COMMAND ----------

# DBTITLE 1,Gravando a tabela DELTA - SENSO IBGE 2010

entity_name  = f"ibge_senso"

table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
file_name = f"{url}{entity_name}.csv"

df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta

# COMMAND ----------

# DBTITLE 1,Gravando a tabela DELTA - municipios

entity_name  = f"drogarias_geo"

table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
file_name = f"{url}{entity_name}.csv"

df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta

# COMMAND ----------

# DBTITLE 1,Gravando a tabela DELTA - naturezas

entity_name  = f"municipios"

table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
file_name = f"{url}{entity_name}.csv"

df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta

# COMMAND ----------


entity_name  = f"naturezas"

table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
file_name = f"{url}{entity_name}.csv"

df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta

# COMMAND ----------


entity_name  = f"ranking_empresas"

table_name   = f"{catalog_name}.{schema_name}.{entity_name}"
file_name = f"{url}{entity_name}.csv"

df = pd.read_csv(file_name)                          # leitura arquivo CSV utilizando Dataframe Pandas
s_df = spark.createDataFrame(df)                     # converte Dataframe Pandas em Spark Dataframe
s_df.write.mode("overwrite").saveAsTable(table_name) # grava o DataFrame na Tabela Delta

# COMMAND ----------

schema_name  = f"medicamentos"


create_schema = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"
spark.sql (create_schema)

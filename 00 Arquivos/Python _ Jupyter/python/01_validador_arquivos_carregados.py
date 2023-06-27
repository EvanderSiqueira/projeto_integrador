# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Validador dos Arquivos Carregados

# COMMAND ----------

# MAGIC %md
# MAGIC #### Definições

# COMMAND ----------

# importando bibliotecas necessárias
from pyspark.sql.functions import regexp_replace, to_date, split, lit
from pyspark.sql.types import StructType, StructField, StringType
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import os

# COMMAND ----------

# general - csse_covid_19_daily_reports
url_github = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports'

# us - csse_covid_19_daily_reports_us
url_github_us = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports_us'

# lake
caminho_lake_general = 'dbfs:/mnt/projetointegrador/raw/general'
caminho_lake_us = 'dbfs:/mnt/projetointegrador/raw/us'


# partição 
particao = 'DATE_KEY'

# COMMAND ----------

def check_diretorio(caminho):
    try:
        dbutils.fs.ls(caminho)
        print('Pasta já existe: ' + caminho)
    except: 
        dbutils.fs.mkdirs(caminho)
        print('Pasta criada: ' + caminho)

# COMMAND ----------

# função que conta quantos arquivos tem carregado no Github
def contagem_arquivos_git(url_github, tipo):

    result = requests.get(url_github)
    soup = BeautifulSoup(result.text, 'html.parser')
    csvfiles1 = soup.find_all(title=re.compile("\.csv$"))

    df1 = pd.DataFrame(csvfiles1, columns=['Cam_Git'])
    df1['DATE_KEY'] = df1['Cam_Git'].str.replace('.csv', '', case=False)
    df1['Tipo'] = tipo

    df = spark.createDataFrame(df1) 

    return df


# COMMAND ----------

# função que conta quantos arquivos foram carregados no lake
def contagem_arquivos_lake(caminho_lake, particao):
    #caminho_lake = 'dbfs://' + caminho_lake
    # obtendo lista de partições da fato
    lista_part_fato = dbutils.fs.ls(caminho_lake)
    #print(lista_part_fato)

    schema_vazio = StructType([StructField("coluna_vazia", StringType(), nullable=True)])

    # checando se a lista está preenchida
    if not lista_part_fato:        
        return spark.createDataFrame([], schema_vazio)
    else:        
        # caso esteja preenchida, obter os nomes das partições
        df_part_fato = spark.createDataFrame(data = lista_part_fato).select('name')

        
        # separando o mês o mês do texto
        df_part_fato = (
            df_part_fato
                #.withColumn('calendar_key_filter', split(df_part_fato['name'], '=').getItem(0))
                .withColumn("DATE_KEY", regexp_replace(df_part_fato['name'], '.csv', ''))
                .withColumnRenamed('name', 'Cam_Lake')
                #.withColumnRenamed('name', 'DATE_KEY')
        )        
        # filtrando apenas partições
        #df_part_fato = df_part_fato.filter(df_part_fato.calendar_key_filter == 'DATE_KEY')
        

        # checando de df tem valor
        if df_part_fato.count() == 0:
            return spark.createDataFrame([], schema_vazio)
        else:
            return df_part_fato

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validador

# COMMAND ----------

# MAGIC %md Checando se pasta no lake existe

# COMMAND ----------

# rodando função para checar a existencia da estrutura, caso não exista, crie.
check_diretorio(caminho_lake_general)

# rodando função para checar a existencia da estrutura, caso não exista, crie.
check_diretorio(caminho_lake_us)

# COMMAND ----------

# MAGIC %md Carregando dados

# COMMAND ----------

# arquivos lake

# general
df_arquivos_lake_general = contagem_arquivos_lake(caminho_lake_general, particao)
df_arquivos_git_general = contagem_arquivos_git(url_github, 'General') # arquivos github

# us
df_arquivos_lake_us = contagem_arquivos_lake(caminho_lake_us, particao)
df_arquivos_git_us = contagem_arquivos_git(url_github_us, 'US') # arquivos github

# COMMAND ----------

# MAGIC %md Obtendo datas faltantes, caso haja

# COMMAND ----------

# General
check_general = df_arquivos_lake_general.count() == 0 
check_us = df_arquivos_lake_us.count() == 0 

if check_general:
    faltantes_general = (
        df_arquivos_git_general
            .select(*['DATE_KEY'])
    )
else:
    faltantes_general = df_arquivos_git_general.join(df_arquivos_lake_general, 'DATE_KEY', 'left')
    faltantes_general = (
        faltantes_general
            .where('Cam_Lake is NULL')
            .select(*['DATE_KEY'])
    )

# US

if check_us:
    faltantes_us = (
        df_arquivos_git_us
            .select(*['DATE_KEY'])
    )
else:
    faltantes_us = df_arquivos_git_us.join(df_arquivos_lake_us, 'DATE_KEY', 'left')
    faltantes_us = (
        faltantes_us
            .where('Cam_Lake is NULL')
            .select(*['DATE_KEY'])
    )

# COMMAND ----------

# MAGIC %md Gerando datas que faltam carregar no Lake

# COMMAND ----------

# General
(
    faltantes_general
       .coalesce(1)
        .write
        .format("csv")
        .mode("overwrite")
        .options(delimiter='|')
        .option("header", True)
        .save("/mnt/projetointegrador/raw/datas_faltantes/datas_general")
)

# COMMAND ----------

# US
(
    faltantes_us
       .coalesce(1)
        .write
        .format("csv")
        .mode("overwrite")
        .options(delimiter='|')
        .option("header", True)
        .save("/mnt/projetointegrador/raw/datas_faltantes/datas_us")
)
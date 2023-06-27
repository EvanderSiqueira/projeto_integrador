# Databricks notebook source
# MAGIC %md
# MAGIC ### Extração dos dados Github

# COMMAND ----------

# importando bibliotecas necessárias
from pyspark.sql.functions import regexp_replace, to_date
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
from time import sleep

from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importando datas do validador

# COMMAND ----------

try:
    datas_faltantes_general = spark.read.option('header', 'true').option('inferSchema', 'true').csv("/mnt/projetointegrador/raw/datas_faltantes/datas_general/")
except: 0

try:
    datas_faltantes_us = spark.read.option('header', 'true').option('inferSchema', 'true').csv("/mnt/projetointegrador/raw/datas_faltantes/datas_us/")
except: 0

# COMMAND ----------

check_faltantes = datas_faltantes_us.union(datas_faltantes_general)

if check_faltantes.count() == 0:
    dbutils.notebook.exit("Não tem faltantes")

# COMMAND ----------

datas_faltantes_general = datas_faltantes_general.withColumn("DATE_KEY", col("DATE_KEY").cast(StringType()))
datas_faltantes_us = datas_faltantes_us.withColumn("DATE_KEY", col("DATE_KEY").cast(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Definições

# COMMAND ----------

def gerando_df_github(url_github, url_base_arquivo, tipo, faltantes):
    
    if faltantes.count() == 0:
        return None

    csvfiles = faltantes.select("DATE_KEY").rdd.flatMap(lambda x: x).collect()

    #print(csvfiles)
    # criando dataframe
    for i in csvfiles:
            sleep(15)
            # obtendo nome do arquivo
            #print(i)
            nome_arq = i + '.csv'
            # var_data = i

            #print(i)
            # criando o caminho completo do arquivo
            caminho_completo_arq = url_base_arquivo + nome_arq
            
            # leitura dos dados do git e conversão para pyspark
            df = pd.read_csv(caminho_completo_arq, sep=',')
            # df['DATE_KEY'] = var_data

            # salvamento dos arquivos na pasta
            arq = '/dbfs/mnt/projetointegrador/raw/' + tipo + '/' + nome_arq
            df.to_csv(arq, sep='|', encoding='utf-8', header=True, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Carregando dados
# MAGIC
# MAGIC Iremos analisar dados da `csse_covid_19_daily_reports` e também da pasta `csse_covid_19_daily_reports_us` que representam os dados do Estados Unidos, disponíveis em: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/

# COMMAND ----------

# MAGIC %md
# MAGIC ##### csse_covid_19_daily_reports

# COMMAND ----------


# ================================================================================================================================================
# general - csse_covid_19_daily_reports

# caminho dos arquivos csv 
url_github = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports/'

# caminho raw dos arquivos csv
url_base_arquivo = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'


# ================================================================================================================================================
# us - csse_covid_19_daily_reports_us

# caminho dos arquivos csv 
url_github_us = 'https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports_us/'

# caminho raw dos arquivos csv
url_base_arquivo_us = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Gerando dados

# COMMAND ----------

# general - csse_covid_19_daily_reports
gerando_df_github(url_github, url_base_arquivo, 'general', datas_faltantes_general)

# COMMAND ----------

# us - csse_covid_19_daily_reports

gerando_df_github(url_github_us, url_base_arquivo_us, 'us', datas_faltantes_us)
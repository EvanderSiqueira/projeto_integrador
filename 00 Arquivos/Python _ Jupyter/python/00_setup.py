# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Configuração inicial do ambiente

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

# raw
caminho_lake_raw = 'dbfs:/mnt/projetointegrador/raw/'
caminho_lake_general = 'dbfs:/mnt/projetointegrador/raw/general'
caminho_lake_us = 'dbfs:/mnt/projetointegrador/raw/us'

# trusted
caminho_lake_trusted = 'dbfs:/mnt/projetointegrador/trusted/'

# refined
caminho_lake_refined = 'dbfs:/mnt/projetointegrador/refined/'
caminho_lake_refined_fato = 'dbfs:/mnt/projetointegrador/refined/fato_covid/'
caminho_lake_refined_calendar = 'dbfs:/mnt/projetointegrador/refined/dim_calendar/'

# COMMAND ----------

def check_diretorio(caminho):
    try:
        dbutils.fs.ls(caminho)
        print('Pasta já existe: ' + caminho)
    except: 
        dbutils.fs.mkdirs(caminho)
        print('Pasta criada: ' + caminho)

# COMMAND ----------

cam = [caminho_lake_raw, caminho_lake_general, caminho_lake_us, caminho_lake_trusted, caminho_lake_refined, caminho_lake_refined_fato, caminho_lake_refined_calendar]

for c in cam:
    check_diretorio(c)
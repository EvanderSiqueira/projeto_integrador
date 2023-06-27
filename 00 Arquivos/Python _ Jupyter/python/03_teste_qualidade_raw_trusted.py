# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Definições

# COMMAND ----------

# spark
from pyspark.sql.functions import lit, col, when, isnull, concat, substring, expr, to_date, split, concat_ws, input_file_name, regexp_replace, lag, coalesce
from pyspark.sql.types import DateType, IntegerType, DecimalType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Carregando dados General e US

# COMMAND ----------

caminho_general = '/mnt/projetointegrador/raw/general/'
caminho_us = '/mnt/projetointegrador/raw/us/'
caminho_trusted = '/mnt/projetointegrador/trusted/'

# COMMAND ----------

# us
dados_us = spark.read.option('header', 'true').option('delimiter', ',').option('inferSchema', 'false').csv(caminho_us)
dados_us = (
    dados_us
        .withColumn("FONTE", lit("US"))
)

# COMMAND ----------


# general
dados_general = spark.read.option('header', 'true').option('delimiter', ',').option('inferSchema', 'false').csv(caminho_general)
dados_general = (
    dados_general
        .withColumn("FONTE", lit("GENERAL"))
)

# removendo caso que não usaremos
dados_general = dados_general.filter(dados_general.Admin2.isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Ajustes colunas

# COMMAND ----------

# MAGIC %md ##### General

# COMMAND ----------

# Reorder columns
dados_general_aj = (
    dados_general
        .withColumnRenamed("Province_State", "STATE")
        .withColumnRenamed("Last_Update", "DATE_ID")
        .withColumnRenamed("Country_Region", "COUNTRY")
        .withColumnRenamed("Long_", "LONG")
        .withColumnRenamed("Lat", "LAT")
        .withColumnRenamed("Confirmed", "CONFIRMED")
        .withColumnRenamed("Deaths", "DEATHS")
        .withColumnRenamed("Recovered", "RECOVERED")
        .withColumnRenamed("Active", "ACTIVE")
        .withColumnRenamed("Incident_Rate", "INCIDENT_RATE")
        .withColumnRenamed("Case_Fatality_Ratio", "CASE_FATALITY_RATIO")
)

# COMMAND ----------

# MAGIC %md ##### US

# COMMAND ----------


# Select columns
dados_us_aj = (
    dados_us
        .withColumnRenamed("Country_Region", "COUNTRY")
        .withColumnRenamed("Province_State", "STATE")
        .withColumnRenamed("Last_Update", "DATE_ID")
        .withColumnRenamed("Lat", "LAT")
        .withColumnRenamed("Long_", "LONG")
        .withColumnRenamed("Confirmed", "CONFIRMED")
        .withColumnRenamed("Deaths", "DEATHS")
        .withColumnRenamed("Recovered", "RECOVERED")
        .withColumnRenamed("Incident_Rate", "INCIDENT_RATE")
        .withColumnRenamed("Active", "ACTIVE")
        .withColumnRenamed("DATE_KEY", "DATE_KEY")
        .withColumnRenamed("Mortality_Rate", "new_CASE_FATALITY_RATIO")
)

# COMMAND ----------

# general
dados_general_aj = dados_general_aj.select("FONTE", "STATE", "COUNTRY", "DATE_ID", "LAT", "LONG", "CONFIRMED", "DEATHS", "RECOVERED", "ACTIVE", "INCIDENT_RATE", "CASE_FATALITY_RATIO")

# us
dados_us_aj = dados_us_aj.select("FONTE", "STATE", "COUNTRY", "DATE_ID", "LAT", "LONG", "CONFIRMED", "DEATHS", "RECOVERED", "ACTIVE", "INCIDENT_RATE", "new_CASE_FATALITY_RATIO")
dados_us_aj = dados_us_aj.withColumnRenamed("new_CASE_FATALITY_RATIO", "CASE_FATALITY_RATIO")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Qualidade e Completude

# COMMAND ----------

# Combine general and us DataFrames
base_qualidade = dados_general_aj.union(dados_us_aj)
base_qualidade = base_qualidade.alias('base_qualidade')

# COMMAND ----------

# ajustando colunas
base_qualidade = (
    base_qualidade
        .withColumn("FONTE", col("FONTE").cast("string"))
        .withColumn("STATE", col("STATE").cast("string"))
        .withColumn("COUNTRY", col("COUNTRY").cast("string"))
        .withColumn("DATE_ID", col("DATE_ID").cast(DateType()))
        .withColumn("LAT", col("LAT").cast("string"))
        .withColumn("LONG", col("LONG").cast("string"))
        .withColumn("CONFIRMED", col("CONFIRMED").cast(IntegerType()))
        .withColumn("DEATHS", col("DEATHS").cast(IntegerType()))
        .withColumn("RECOVERED", col("RECOVERED").cast(IntegerType()))
        .withColumn("ACTIVE", col("ACTIVE").cast(IntegerType()))
        .withColumn("INCIDENT_RATE", col("INCIDENT_RATE").cast(DecimalType()))
        .withColumn("CASE_FATALITY_RATIO", col("CASE_FATALITY_RATIO").cast(DecimalType()))
    
)

# COMMAND ----------

# Combine general and us DataFrames
base_qualidade = (
    base_qualidade
        .withColumn("teste_completude_lat", when(isnull(col("LAT")), "sem_latitude").otherwise("correto"))
        .withColumn("teste_completude_long", when(isnull(col("LONG")), "sem_longitude").otherwise("correto"))
        .withColumn("teste_range_lat", when(isnull(col("LAT")), "sem_latitude").otherwise(when((col("LAT") >= -90) & (col("LAT") <= 90), "correto").otherwise("latitude_fora_range")))
        .withColumn("teste_range_long", when(isnull(col("LONG")), "sem_longitude").otherwise(when((col("LONG") >= -180) & (col("LONG") <= 180), "correto").otherwise("long_fora_range")))
        .withColumn("teste_confirmed", when(isnull(col("CONFIRMED")), "confirmed_nulos").otherwise(when(col("CONFIRMED") < 0, "confirmed_negativos").otherwise("correto")))
        .withColumn("teste_deaths", when(isnull(col("DEATHS")), "deaths_nulos").otherwise(when(col("DEATHS") < 0, "deaths_negativos").otherwise("correto")))
        .withColumn("teste_recovered", when(isnull(col("RECOVERED")), "recovered_nulos").otherwise(when(col("RECOVERED") < 0, "recovered_negativos").otherwise("correto")))
        .withColumn("teste_active", when(isnull(col("ACTIVE")), "active_nulos").otherwise(when(col("ACTIVE") < 0, "active_negativos").otherwise("correto")))
        .withColumn("teste_incident_rate", when(isnull(col("INCIDENT_RATE")), "incident_rate_nulos").otherwise(when(col("INCIDENT_RATE") < 0, "incident_rate_negativos").otherwise("correto")))
        .withColumn("teste_case_fatality_ratio", when(isnull(col("CASE_FATALITY_RATIO")), "case_fatality_ratio_nulos").otherwise(when(col("CASE_FATALITY_RATIO") < 0, "case_fatality_ratio_negativos").otherwise("correto")))
        .withColumn("check_calc_case_fatality_ratio", (col("DEATHS") / col("CONFIRMED") * 100) == col("CASE_FATALITY_RATIO"))
        .withColumn("NEW_CASE_FATALITY_RATIO", col("DEATHS") / col("CONFIRMED") * 100)
)

# COMMAND ----------

# Limpando colunas de teste

'''
base_qualidade = (
    base_qualidade.filter(
        (base_qualidade.teste_completude_lat == 'correto') &
        (base_qualidade.teste_completude_long == 'correto') &
        (base_qualidade.teste_range_lat == 'correto') &
        (base_qualidade.teste_range_long == 'correto') &
        (base_qualidade.teste_confirmed == 'correto') &
        (base_qualidade.teste_deaths == 'correto') &
        (base_qualidade.teste_recovered == 'correto') &
        (base_qualidade.teste_active == 'correto')
    )
)
'''

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Tabela Trusted

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Criando colunas não-acumuladas

# COMMAND ----------

WindowSpec = Window.partitionBy(*['STATE', 'COUNTRY']).orderBy(*['DATE_ID','STATE','COUNTRY'])

base_trusted = (
    base_qualidade
        .withColumn("CONFIRMED_NC", col("CONFIRMED") - lag("CONFIRMED").over(WindowSpec))
        .withColumn("DEATHS_NC", col("DEATHS") - lag("DEATHS").over(WindowSpec))
        .withColumn("RECOVERED_NC", col("RECOVERED") - lag("RECOVERED").over(WindowSpec))
        .withColumn("ACTIVE_NC", col("ACTIVE") - lag("ACTIVE").over(WindowSpec))
)

base_trusted = (
    base_trusted
        .withColumn("CONFIRMED_NC", coalesce('CONFIRMED_NC', 'CONFIRMED'))
        .withColumn("DEATHS_NC", coalesce('DEATHS_NC', 'DEATHS'))
        .withColumn("RECOVERED_NC", coalesce('RECOVERED_NC', 'RECOVERED'))
        .withColumn("ACTIVE_NC", coalesce('ACTIVE_NC', 'ACTIVE'))
)

# COMMAND ----------

# MAGIC %md ##### Ajustes Finais

# COMMAND ----------

base_trusted = (
    base_trusted
        .select("FONTE", "STATE", "COUNTRY", "DATE_ID", "LAT", "LONG", "CONFIRMED", "DEATHS", "RECOVERED", "ACTIVE", "INCIDENT_RATE", "NEW_CASE_FATALITY_RATIO", "CONFIRMED_NC", "DEATHS_NC", "RECOVERED_NC", "ACTIVE_NC")
        .withColumnRenamed("NEW_CASE_FATALITY_RATIO", "CASE_FATALITY_RATIO")

        # colunas acumuladas
        .withColumnRenamed("CONFIRMED", "CONFIRMED_AC")
        .withColumnRenamed("DEATHS", "DEATHS_AC")
        .withColumnRenamed("RECOVERED", "RECOVERED_AC")
        .withColumnRenamed("ACTIVE", "ACTIVE_AC")

        # colunas não-acumuladas

        .withColumnRenamed("CONFIRMED_NC", "CONFIRMED")
        .withColumnRenamed("DEATHS_NC", "DEATHS")
        .withColumnRenamed("RECOVERED_NC", "RECOVERED")
        .withColumnRenamed("ACTIVE_NC", "ACTIVE")

    )

# COMMAND ----------

# MAGIC %md ##### Exportando

# COMMAND ----------

cam_final = "/mnt/projetointegrador/trusted/base_trusted"

base_trusted.write.format('csv').mode('overwrite').partitionBy('DATE_ID').options(header='True', delimiter='|').save(cam_final)
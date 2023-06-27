# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Definições

# COMMAND ----------

# spark
from pyspark.sql.functions import year, month, quarter, weekofyear, dayofweek, date_format, col, concat, coalesce, lit

# COMMAND ----------

# MAGIC %md ##### Carregando dados Trusted

# COMMAND ----------

base_trusted = spark.read.option("header", "true").option("sep", "|").csv("/mnt/projetointegrador/trusted/base_trusted/")
base_trusted = base_trusted.alias('base_trusted')

# COMMAND ----------

base_trusted.createOrReplaceTempView('base_trusted')

# COMMAND ----------

# MAGIC %sql select * from base_trusted where DATE_ID is null

# COMMAND ----------

base_trusted = (
    base_trusted
        .withColumn('LOCATION_KEY', concat(coalesce(base_trusted.STATE, lit("")), base_trusted.COUNTRY))
)

# COMMAND ----------

# MAGIC %md ##### dim_location

# COMMAND ----------

dim_location = (
    base_trusted
        .select("LOCATION_KEY", "COUNTRY", "STATE", "LAT", "LONG")
        .dropDuplicates(["LOCATION_KEY"])
)

dim_location = dim_location.select("LOCATION_KEY", "COUNTRY", "STATE", "LAT", "LONG")
dim_location = dim_location.alias('dim_location')

# COMMAND ----------

# MAGIC %md ##### dim_calendar

# COMMAND ----------

dim_calendar =(
    base_trusted
        .select("DATE_ID")
        .distinct()
        .withColumn("Ano", year("DATE_ID"))
        .withColumn("Nome do Mês", date_format("DATE_ID", "MMMM"))
        .withColumn("Mês", month("DATE_ID"))
        .withColumn("Trimestre", quarter("DATE_ID"))
        .withColumn("Semana do Ano", weekofyear("DATE_ID"))
        .withColumn("Nome do Dia", date_format("DATE_ID", "EEEE"))
        .withColumn("Dia da Semana", dayofweek("DATE_ID"))
)

# COMMAND ----------

# MAGIC %md ##### fato_covid

# COMMAND ----------

fato_covid = base_trusted.select("LOCATION_KEY", "DATE_ID", "FONTE", "CONFIRMED", "DEATHS", "RECOVERED", "ACTIVE","CONFIRMED_AC", "DEATHS_AC", "RECOVERED_AC", "ACTIVE_AC", "INCIDENT_RATE", "CASE_FATALITY_RATIO")

# COMMAND ----------

fato_covid = fato_covid.withColumn("DATE_KEY_PART", col("DATE_ID"))

# COMMAND ----------

# MAGIC %md ##### Exportando Refined

# COMMAND ----------

cam_final = "/mnt/projetointegrador/refined/fato_covid"

# fato
fato_covid.write.format('csv').mode('overwrite').partitionBy('DATE_KEY_PART').options(header='True', delimiter='|').save(cam_final)

# COMMAND ----------

cam_final = "/mnt/projetointegrador/refined/dim_location"

# dim_location
dim_location.write.format('csv').mode('overwrite').options(header='True', delimiter='|').save(cam_final)

# COMMAND ----------

cam_final = "/mnt/projetointegrador/refined/dim_calendar"

#dim_calendar
dim_calendar.write.format('csv').mode('overwrite').options(header='True', delimiter='|').save(cam_final)

# COMMAND ----------

cam_final = "/mnt/projetointegrador/refined/base_trusted"

#base_trusted
base_trusted.write.format('csv').mode('overwrite').options(header='True', delimiter='|').save(cam_final)
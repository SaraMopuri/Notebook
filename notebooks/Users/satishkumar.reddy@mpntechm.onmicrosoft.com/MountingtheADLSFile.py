# Databricks notebook source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "c395666a-8b91-4587-86c8-ca8badb7a124",
           "dfs.adls.oauth2.credential": "1?Oa0_r-eM51qrJruAFcPAlQV.es1N8w",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://datalake800.azuredatalakestore.net/Azure",
  mount_point = "/mnt/Azure/",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/Azure/

# COMMAND ----------

df4 = spark.read.csv("/mnt/Azure/Data.csv",inferSchema="true",header="true")

# COMMAND ----------

df4.show()

# COMMAND ----------

from pyspark.sql.functions import col, to_date,date_format
df1=df.select("Closed Date")

df1.show()

# COMMAND ----------

  df1=df.select("Created Date").show()

# COMMAND ----------

display(df1)

# COMMAND ----------

df1=df.select('Created date','Closed date')

# COMMAND ----------

df1.show()

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import col, to_date,date_format,when
df5 = df4.withColumn("Week",date_format(to_date(col('closed date'),'mm/dd/yyyy'),'W'))
df6 = df5.withColumn("weekwithoutnull", when(col('Week').isNull(), date_format(to_date(col('closed date'),'mm/dd/yyyy'),'Y')).otherwise(date_format(to_date(col('closed date'),'mm/dd/yyyy'),'W')))
display(df6)

# COMMAND ----------

df1.show()

# COMMAND ----------

df2=df1.select("week","Closed Date")

# COMMAND ----------

df2.show()

# COMMAND ----------

df1 = df.withColumn("Week",date_format(to_date(col('closed date'),'mm/dd/yyyy'),'w'))
if(df1["week"]==null)
df1 = df.withColumn("Week",date_format(to_date(col('closed date'),'mm/dd/yyyy'),'y'))
else
df1 = df.withColumn("Week",date_format(to_date(col('closed date'),'mm/dd/yyyy'),'w'))

df1.show()


df = df.withColumn(m, F.when(col > cap, cap).otherwise(col))

# COMMAND ----------

df.show()

# COMMAND ----------

df1.show()

# COMMAND ----------

df= None

# COMMAND ----------

df.show()

# COMMAND ----------

df1.show()

# COMMAND ----------

df1=None

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.show()

# COMMAND ----------

df=spark.read.csv("\mnt\CCRM\Country.csv",inferSchema="true",header="true")
display(df)

# COMMAND ----------

df = spark.read.csv("\mnt\CCRM\Country.csv",inferSchema="true",header="true")
display(df)

# COMMAND ----------

df = spark.read.csv("/mnt/CCRM/Country.csv",inferSchema="true",header="true")

# COMMAND ----------

display(df)

# COMMAND ----------

countryDF = df.filter(df.CountrySKU.isNotNull())

# COMMAND ----------

display(countryDF)

# COMMAND ----------

df1 = spark.read.csv("\mnt\CCRM\People.csv",inferSchema="true",header="true")

# COMMAND ----------

df1 = spark.read.csv("/mnt/CCRM/People.csv",inferSchema="true",header="true")

# COMMAND ----------

display(df1)

# COMMAND ----------

pplDf= df1.filter(df1.PersonName.isNotNull())

# COMMAND ----------

display(pplDf)

# COMMAND ----------

DFOuter = pplDf.join(countryDF,pplDf.CountryCode == countryDF.CountrySKU,'Outer')
display(DFOuter)

# COMMAND ----------

DFROuter = pplDf.join(countryDF,pplDf.CountryCode == countryDF.CountrySKU,'rightOuter')
display(DFROuter)

# COMMAND ----------

DFLOuter = pplDf.join(countryDF,pplDf.CountryCode == countryDF.CountrySKU,'leftOuter')
display(DFLOuter)

# COMMAND ----------

DFLSemi = pplDf.join(countryDF,pplDf.CountryCode == countryDF.CountrySKU,'leftSemi')
display(DFLSemi)

# COMMAND ----------

DFLAnti = pplDf.join(countryDF,pplDf.CountryCode == countryDF.CountrySKU,'leftAnti')
display(DFLAnti)

# COMMAND ----------

DFCross = pplDf.crossJoin(countryDF)
display(DFCross)

# COMMAND ----------


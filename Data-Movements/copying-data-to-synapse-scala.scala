// Databricks notebook source
dbutils.secrets.help()

// COMMAND ----------

dbutils.secrets.listScopes()

// COMMAND ----------

dbutils.secrets.list("formula1-scope")

// COMMAND ----------

// MAGIC %python
// MAGIC empty=""
// MAGIC for i in dbutils.secrets.get(scope="formula1-scope", key="dl-access-key"):
// MAGIC     empty+=i
// MAGIC print(empty)

// COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formuladl28.dfs.core.windows.net",
    dbutils.secrets.get(scope="formula1-scope",key="dl-access-key"))

val df = spark.read.format("csv").option("header","true").load("abfss://data@formuladl28.dfs.core.windows.net/raw/Log.csv")

display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df=spark.read.options(inferSchema=True,header=True).csv("abfss://data@formuladl28.dfs.core.windows.net/raw/Log.csv")

// COMMAND ----------


val df = spark.read.format("csv")
.options(Map("inferSchema"->"true","header"->"true"))
.load("abfss://data@formuladl28.dfs.core.windows.net/raw/Log.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val dfcorrect=df.select(col("Id"),
                        col("Correlationid"),
                        col("Operationname"),
                        col("Status"),
                        col("Eventcategory"),
                        col("Level"),
                        col("Time"),
                        col("Subscription"),
                        col("Eventinitiatedby"),
                        col("Resourcetype"),
                        col("Resourcegroup"))

// COMMAND ----------


val tablename="logdata"
val tmpdir="abfss://tmp@formuladl28.dfs.core.windows.net/log"

// This is the connection to our Azure Synapse dedicated SQL pool
val connection = "jdbc:sqlserver://synapse-ws28.sql.azuresynapse.net:1433;database=newpool;user=sqladminuser;password=Azure123;encrypt=true;trustServerCertificate=false;"

// We can use the write function to write to an external data store
dfcorrect.write
  .mode("append") // Here we are saying to append to the table
  .format("com.databricks.spark.sqldw")
  .option("url", connection)
  .option("tempDir", tmpdir) // For transfering to Azure Synapse, we need temporary storage for the staging data
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .save()

// COMMAND ----------


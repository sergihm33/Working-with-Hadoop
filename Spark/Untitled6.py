#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, max
import time

start_time = time.time()

# Configuracion de la sesion Spark
spark2 = SparkSession.builder     .appName("HDFS Query2")     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")     .getOrCreate()

# Lectura de datos desde HDFS
telecom_data = spark2.read.json("hdfs://namenode:8020/user/root/data/*.json")

exploded_telecom = telecom_data.select(
    col("cell").alias("cellId"),
    explode(col("events")).alias("event")
).select(
    col("cellId"),
    col("event.internet_traffic").alias("internet_traffic")
)

exploded_telecom.createOrReplaceTempView("exploded_telecom")

consulta = """
SELECT 
    cellId AS cell,
    MAX(internet_traffic) AS Maximo_Internet_Traffic
FROM exploded_telecom
GROUP BY cellId
ORDER BY Maximo_Internet_Traffic DESC
LIMIT 10
"""

resultado = spark2.sql(consulta)

resultado.show()

end_time = time.time()
execution_time = end_time - start_time

print(f"Tiempo de ejecución: {execution_time} segundos")

spark2.stop()


# In[ ]:





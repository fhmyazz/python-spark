import pandas as pd    

from pyspark.sql import SparkSession
from pyspark import SparkContext

# Create the pandas DataFrame 
data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54],['Ann',34]] 
pandasDF = pd.DataFrame(data, columns = ['Name', 'Age'])

def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
                .appName(app) \
                    .getOrCreate()
                    
        print(f"[INFO] Success connect SPARK ENGINE .....")
        return spark
    except:
        print(f"[INFO] Success Can't SPARK ENGINE .....")

spark = spark_conn('Testing', {"ip":"spark://DESKTOP-8AHJDS9.localdomain:7077"})

sparkDF=spark.createDataFrame(pandasDF) 
da = sparkDF.toPandas()
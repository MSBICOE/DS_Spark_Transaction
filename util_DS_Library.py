# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark.sql import SparkSession, SQLContext

# initialise sparkContext
# warehouseLocation = "D:/Data/BDF/data_lake"

def spinup_DS_Spark():

    spark = SparkSession.builder \
        .master('local') \
        .appName('AUDS_Spark') \
        .config("spark.executor.memory", "96g") \
        .config("spark.driver.memory", "72g") \
        .config("spark.memory.offHeap.enabled", True) \
        .config("spark.memory.offHeap.size","32g") \
        .config("spark.cores.max", "6") \
        .config("spark.executor.processTreeMetrics.enabled", False) \
        .getOrCreate()
        # .enableHiveSupport() \
        # .config('spark.executor.memory', '96gb') \
        ## it is important to setup the offheap size
        
    sc = spark.sparkContext
    
    sqlContext = SQLContext(sc)

    return spark, sqlContext, sc
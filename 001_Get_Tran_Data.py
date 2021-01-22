# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 00:53:06 2021

@author: steven.wang
"""


import util_DS_Library as ds

## first spin up the spark node
spark, sqlc, sc = ds.spinup_DS_Spark()

df = sqlc.read.parquet("D:/Data/BDF/transaction/2020/12/transactional_20201201.parquet")

df.show(n = 2)

df.createOrReplaceTempView("ds_test")

query = "SELECT * FROM ds_test limit 4;"

# Run the query
results = spark.sql(query)

results.show()

## this will be the last line
sc.stop()

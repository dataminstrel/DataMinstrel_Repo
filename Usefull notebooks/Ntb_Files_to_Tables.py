#!/usr/bin/env python
# coding: utf-8

# ## Ntb_Files_to_Tables
# 
# New notebook

# In[1]:


def move_files_to_tables(lakehouse):
    spark = SparkSession.builder.getOrCreate()

    # Access Hadoop FileSystem through Spark
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{lakehouse}/Files")
    print(path)
    # List files in the top-level folder
    status = fs.listStatus(path)

    for fileStatus in status:
        filepath = (fileStatus.getPath().toString())
        #print(filepath)
        relative_path = filepath.split("/Files/")[1]        
        if relative_path.startswith('part'):
            a = 1 # didn't want to figure out how to skip
        else:       
            print(relative_path)     
            table = f"{lakehouse}/Tables/{relative_path}"
            #print(table)
            content = spark.read.parquet(filepath)
            #display(content)
            

            # Define the wait time in seconds
            wait_time_seconds = 5

            content.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(table)


# In[2]:


Rohan = "abfss://" #fill in the absolute path to the lakehouse you want to use
MistyMountainGoblins = "abfss://" #fill in the absolute path to the lakehouse you want to use
Dwarves = "abfss://" #fill in the absolute path to the lakehouse you want to use
Elves = "abfss://" #fill in the absolute path to the lakehouse you want to use
Isengard = "abfss://" #fill in the absolute path to the lakehouse you want to use
Gondor = "abfss://" #fill in the absolute path to the lakehouse you want to use
Hobbits = "abfss://" #fill in the absolute path to the lakehouse you want to use


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import time


move_files_to_tables(MistyMountainGoblins)
move_files_to_tables(Dwarves)
move_files_to_tables(Elves)
move_files_to_tables(Isengard)
move_files_to_tables(Gondor)
move_files_to_tables(Hobbits)
move_files_to_tables(Rohan)


#!/usr/bin/env python
# coding: utf-8

# ## Workspaces
# 
# New notebook

# In[1]:


import sempy.fabric as fabric
import pandas as pd
from pyspark.sql.functions import lit


# In[2]:


#This function will remove all characters from the columns that would cause an error on trying to save
def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns


# In[3]:


Table_Name = 'Workspaces'
LH_Name = "LH_SemanticLink_Data"


# In[4]:


lakehouse = notebookutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")


# In[5]:


workspaces = fabric.list_workspaces()
sparkdf = spark.createDataFrame(workspaces)


# In[7]:


try:
    sql_truncate = f"TRUNCATE TABLE {LH_Name}.{Table_Name}"
    spark.sql(sql_truncate)
except Exception as e:
    print(f"truncate failed with error: {e}")


# In[9]:


if not workspaces.empty: # check if the list is not empty to avoid errors
    try:
        workspaces = fnc_PrepareColumns(workspaces)
        sparkdf = spark.createDataFrame(workspaces)
        sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
        display(sparkdf)                      
    except Exception as e:
        print(f"Error fetching Workspace objects for: {e}")


# In[10]:


notebookutils.session.stop()


#!/usr/bin/env python
# coding: utf-8

# ## Nbt_Landing_GetUsers
# 
# New notebook

# In[6]:


Table_Name = 'Target table name'
LH_Name = "Target lakehouse name"


# this install is necessary if you don't have an environment that includes semantic link labs

# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install semantic-link-labs


# In[3]:


from pyspark.sql.functions import lit, current_timestamp
import sempy_labs as sempy_labs
import pandas as pd


# In[7]:


lakehouse = mssparkutils.lakehouse.get(LH_Name)

lh_abfs_path = lakehouse.get("properties").get("abfsPath")


# In[8]:


def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns


# In[10]:


try:
    spark.sql("TRUNCATE TABLE LH_SemanticLink_Data.Users")
except Exception as e:
    print(f"truncate failed with error: {e}")


# In[9]:


Workspaces = spark.sql("""select Id, Name
from LH_SemanticLink_Data.Workspaces""")


# In[11]:


for Id, Name in Workspaces.toLocalIterator():
    try:
        temp_items = sempy_labs.list_workspace_users(workspace=Id)
        itemdf = pd.DataFrame(temp_items)
    except Exception as e:
        print(e)
    if not itemdf.empty: # check if the list is not empty to avoid errors
    #prepare items and write them away
        try:
            itemdf = fnc_PrepareColumns(itemdf)
            sparkdf = spark.createDataFrame(itemdf)
            sparkdf = sparkdf.withColumn('WSID', lit(Id))
            sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
            #print(Table_Name_Items, "created at :", f"{lh_abfs_path}/Tables/{Table_Name_Items}")                        
        except Exception as e:
            print(f"Error fetching Workspace objects for {Id}: {e}")
            continue


# In[12]:


mssparkutils.session.stop()


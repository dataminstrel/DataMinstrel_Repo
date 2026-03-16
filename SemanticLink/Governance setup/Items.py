#!/usr/bin/env python
# coding: utf-8

# ## Ntb_Items
# 
# New notebook

# In[2]:


import sempy.fabric as fabric
import pandas as pd
from pyspark.sql.functions import lit


# In[9]:


#This function will remove all characters from the columns that would cause an error on trying to save
def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns


# In[11]:


Table_Name = 'Items'
LH_Name = "LH_SemanticLink_Data"


# In[12]:


lakehouse = notebookutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")


# In[13]:


try:
    sql_truncate = f"TRUNCATE TABLE {LH_Name}.{Table_Name}"
    spark.sql(sql_truncate)
except Exception as e:
    print(f"truncate failed with error: {e}")


# In[14]:


sql_workspaces = f"select Id, Name from {LH_Name}.Workspaces"
Workspaces = spark.sql(sql_workspaces)


# 

# In[16]:


for row in Workspaces.toLocalIterator():
    try:
        Id = row["Id"]
        temp_items = fabric.list_items(workspace=Id)
        itemdf = pd.DataFrame(temp_items)
        print(row)
        if not itemdf.empty: # check if the list is not empty to avoid errors
            try:
                itemdf = fnc_PrepareColumns(itemdf)
                sparkdf = spark.createDataFrame(itemdf)
                sparkdf = sparkdf.withColumn('WSID', lit(Id))
                sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
                #display(sparkdf)                      
            except Exception as e:
                print(f"Error fetching Workspace objects for {Name}: {e}")
                continue            
    except Exception as e:
        print(f"Error fetching Workspace {Id}: {e}")
        continue


# In[17]:


notebookutils.session.stop()


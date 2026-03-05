#!/usr/bin/env python
# coding: utf-8

# ## Nbt_Landing_GetRelationships
# 
# New notebook

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install semantic-link-labs


# In[2]:


Table_Name = 'Relations'
LH_Name = "LH_SemanticLink_Data"


# In[3]:


from pyspark.sql.functions import lit, current_timestamp
import pandas as pd
import sempy.fabric as fabric


# In[6]:


# Mount the Lakehouse for direct file system access
lakehouse = notebookutils.lakehouse.get(LH_Name)

# Retrieve and store local and ABFS paths of the mounted Lakehouse
lh_abfs_path = lakehouse.get("properties").get("abfsPath")


# In[7]:


def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns


# In[8]:


try:
    spark.sql("TRUNCATE TABLE LH_SemanticLink_Data.Relations")
except Exception as e:
    print(f"truncate failed with error: {e}")


# In[9]:


SemanticModels = spark.sql("""select Id, WSID
from LH_SemanticLink_Data.Items
where Type='SemanticModel' and DisplayName<>'Report Usage Metrics Model'""")


# In[10]:


for Id, WSID in SemanticModels.toLocalIterator():
    dataset_ID = Id
    try:
        relationships = fabric.list_relationships(dataset=dataset_ID, workspace=WSID)
        
        relationships = pd.DataFrame(relationships)
        if not relationships.empty: # check if the list is not empty to avoid errors
            relationships = fnc_PrepareColumns(relationships)
            sparkdf = spark.createDataFrame(relationships)
            sparkdf = sparkdf.withColumn('WSID', lit(WSID))
            sparkdf = sparkdf.withColumn('SMID', lit(dataset_ID))
            sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
    except Exception as e:
        print(f"Error fetching semantic model relationships for {Id}: {e}")
    continue


# In[11]:


notebookutils.session.stop()


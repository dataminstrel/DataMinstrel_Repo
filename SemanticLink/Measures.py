#!/usr/bin/env python
# coding: utf-8

# ## Measures
# 
# New notebook

# # Settings

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install semantic-link-labs


# In[2]:


Table_Name = 'Measures'
LH_Name = "LH_SemanticLink_Data"
LH_desc = "Lakehouse for Power BI usage monitoring"


# In[3]:


from pyspark.sql.functions import lit, current_timestamp
import pandas as pd
import sempy.fabric as fabric


# In[4]:


# Mount the Lakehouse for direct file system access
lakehouse = notebookutils.lakehouse.get(LH_Name)

# Retrieve and store local and ABFS paths of the mounted Lakehouse
lh_abfs_path = lakehouse.get("properties").get("abfsPath")


# In[5]:


def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns


# In[6]:


SemanticModels = spark.sql("""select Id, WSID
from LH_SemanticLink_Data.Items
where Type='SemanticModel' and DisplayName<>'Report Usage Metrics Model'""")


# In[7]:


try:
    spark.sql("TRUNCATE TABLE LH_SemanticLink_Data.Measures")
except Exception as e:
    print(f"truncate failed with error: {e}")


# In[8]:


for Id, WSID in SemanticModels.toLocalIterator():
    try:
        measures = fabric.list_measures(dataset=Id, workspace=WSID)
        #print(measures)
        measures.drop('Detail Rows Definition', inplace=True, axis=1)
        measures.drop('Format String Definition', inplace=True, axis=1)
        
        measuresdf = pd.DataFrame(measures)
        
        if not measuresdf.empty: # check if the list is not empty to avoid errors
            measuresdf = fnc_PrepareColumns(measuresdf)
            sparkdf = spark.createDataFrame(measuresdf)
            sparkdf = sparkdf.withColumn('WSID', lit(WSID))
            sparkdf = sparkdf.withColumn('SMID', lit(Id))
            sparkdf.write.format("delta").option("mergeSchema", "true").mode("append").save(f"{lh_abfs_path}/Tables/{Table_Name}")
    except Exception as e:
        print(f"Error fetching semantic model objects for {Id}: {e}")
        continue
   


# In[9]:


notebookutils.session.stop()


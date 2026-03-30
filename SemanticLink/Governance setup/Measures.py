#!/usr/bin/env python
# coding: utf-8

# ## Ntb_Measures
# 
# null

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install semantic-link-labs


# In[2]:

#'Measures' is a reserved table name, you can use it and the lakehouse will accept it, but when you connect to the sql endpoint it will start complaining
Table_Name = 'Measures_table'
LH_Name = "LH_SemanticLink_Data"


# In[3]:


from pyspark.sql.functions import lit, current_timestamp
import sempy_labs as sempy_labs
import pandas as pd
import sempy.fabric as fabric


# In[4]:


# Mount the Lakehouse for direct file system access
lakehouse = mssparkutils.lakehouse.get(LH_Name)

# Retrieve and store local and ABFS paths of the mounted Lakehouse
lh_abfs_path = lakehouse.get("properties").get("abfsPath")


# In[5]:


def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns


# In[6]:


SQL_SemanticModels = F"select Id, WSID\
                        from {LH_Name}.Items\
                        where Type='SemanticModel' and DisplayName<>'Report Usage Metrics Model'"

SemanticModels = spark.sql(SQL_SemanticModels)


# In[7]:


try:
    sql_truncate = f"TRUNCATE TABLE {LH_Name}.{Table_Name}"
    spark.sql(sql_truncate)
except Exception as e:
    print(f"truncate failed with error: {e}")


# In[8]:


for Id, WSID in SemanticModels.toLocalIterator():
    try:
        measures = fabric.list_measures(dataset=Id, workspace=WSID)
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
   


# In[11]:


mssparkutils.session.stop()


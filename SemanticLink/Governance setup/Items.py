import sempy.fabric as fabric
import pandas as pd
from pyspark.sql.functions import lit

#This function will remove all characters from the columns that would cause an error on trying to save
def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns

Table_Name = 'Target_Table'
LH_Name = "LH_Name"

lakehouse = notebookutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")

workspaces = fabric.list_workspaces()
sparkdf = spark.createDataFrame(workspaces)

try:
    sql = f"TRUNCATE TABLE {LH_Name}.Items"
    spark.sql(sql)
except Exception as e:
    print(f"truncate failed with error: {e}")


for _, row in workspaces.iterrows():
    Id = row["Id"]
    temp_items = fabric.list_items(workspace=Id)
    itemdf = pd.DataFrame(temp_items)
    print(row)
    if not itemdf.empty: # check if the list is not empty to avoid errors
        try:
            itemdf = fnc_PrepareColumns(itemdf)
            sparkdf = spark.createDataFrame(itemdf)
            sparkdf = sparkdf.withColumn('WSID', lit(Id))
            #sparkdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"{lh_abfs_path}/Tables/{Table_Name}")
            #display(sparkdf)                      
        except Exception as e:
            print(f"Error fetching Workspace objects for {Name}: {e}")
            continue

import sempy.fabric as fabric

#This function will remove all characters from the columns that would cause an error on trying to save
def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns
 

Table_Name = 'Landing_Fabric_Capacities'
LH_Name = "LH_SemanticLink_Data"
LH_desc = "Lakehouse for Power BI usage monitoring"

lakehouse = mssparkutils.lakehouse.get(LH_Name)
lh_abfs_path = lakehouse.get("properties").get("abfsPath")


workspaces = fabric.list_capacities()
workspaces = fnc_PrepareColumns(workspaces)
sparkdf = spark.createDataFrame(workspaces)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"{lh_abfs_path}/Tables/{Table_Name}")

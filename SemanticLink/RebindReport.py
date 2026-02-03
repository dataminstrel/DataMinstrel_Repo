#!/usr/bin/env python
# coding: utf-8

# ## RebindReport
# 
# null

# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install semantic-link-labs


# In[ ]:


import sempy_labs as sempy_labs
import sempy_labs.report as reportsempy


# In[ ]:


# Define your parameters
# The name of the report you wish to rebind
report_name = "Name" 

# The workspace your report is located in
workspace_name = "workspace name"

# Name of the dataset you wish to rebind to
new_dataset_name = "Dataset_name"

# The Workspace the new dataset is located in
# Can be same or different
new_dataset_workspace = "Workspace_new_Dataset"  


# In[ ]:


reportsempy.report_rebind(
    report=report_name,
    dataset=new_dataset_name,
    report_workspace=workspace_name,
    dataset_workspace=new_dataset_workspace
)


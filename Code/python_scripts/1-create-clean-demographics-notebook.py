#!/usr/bin/env python
# coding: utf-8

# # 1-create-clean-demographics-noteook - Plan
# This notebook is for processing the demographic dataset into a usable format. This is the first step in our process because when we come to cleaning and processing the other data that we have (notebook 2 to 5), we want to remove unrealistic data for example, events that happen before 1910 or in the future. 
# 
# In this first notebook, we take in 2 different reference datasets that allow us to map demographic information (age and sex) to nhs_numbers. 
# 
# 1) Read the two datasets into a DemographicDataset 
# 2) Process this - meaning clean and make it uniform and then save in an arrow format. 

# # Locations/paths naming convention
# 
# 1. We do not use relative paths.
# 2. We do not explicitly use the word FOLDER in the naming, so `MEGADATA_LOCATION`, not `MEGADATA_FOLDER_LOCATION`. 
# 1. Location and path are in `UPPER_CASE`.
# 2. When referred to as `_LOCATION`, the variable contain a string with the path.
# 3. When referred to as `_PATH`, the variable is an `AnyPath` path object.
# 4. The folder order is "what it is" / "Where it's from" so, for example megadata/primary_care not primary_care/megadata; so `MEGADATA_PRIMARY_CARE_LOCATION` or `PROCESSED_DATASETS_PRIMARY_CARE_PATH`
# 

# In[ ]:


VERSION = "version010_2025_05_SR"


# ### Import Library

# In[ ]:


from tretools.datasets.demographic_dataset import DemographicDataset


# In[ ]:


import tretools


# In[ ]:


# import pathlib
from cloudpathlib import AnyPath
from datetime import datetime
import polars as pl


# In[ ]:


# polars namespace additions

# In subsequent version this code may be integrated with the establisted TRE Tools package

@pl.api.register_lazyframe_namespace("TRE")
class TRETools:
    def __init__(self, lzdf: pl.LazyFrame) -> None:
        self._lzdf = lzdf
        
    def unique_with_logging(self, *args, label: str = "Unique", **kwargs) -> pl.LazyFrame:
        before = self._lzdf.select(pl.first()).collect().height
        filtered_lzdf = self._lzdf.unique(*args, **kwargs)
        after = filtered_lzdf.select(pl.first()).collect().height
        
        if before > 0:
            change = ((after - before) / before) * 100
            change_str = f" ({'+' if change > 0 else ''}{change:.1f}%)"
        
        unchanged = " (row count unchanged)" if after == before else ""
        
        print(f"[{label}: on {args}] Before unique: {before} rows, After unique: {after} rows{unchanged}{change_str}")
        return filtered_lzdf    
    
    def filter_with_logging(self, *args, label: str = "Filter", **kwargs) -> pl.LazyFrame:
        before = self._lzdf.select(pl.first()).collect().height
        filtered_lzdf = self._lzdf.filter(*args, **kwargs)
        after = filtered_lzdf.select(pl.first()).collect().height
        
        if before > 0:
            change = ((after - before) / before) * 100
            change_str = f" ({'+' if change > 0 else ''}{change:.1f}%)"
        
        unchanged = " (row count unchanged)" if after == before else ""
        print(f"[{label}] Before filter: {before} rows, After filter: {after} rows{unchanged}{change_str}")
        return filtered_lzdf
    
    def join_with_logging(
        self,
        other: pl.LazyFrame,
        *args,
        how: str = "inner",
        label: str = "Join",
        **kwargs
    ) -> pl.LazyFrame:
        left_before = self._lzdf.select(pl.first()).collect().height
        right_before = other.select(pl.first()).collect().height
        joined_lzdf = self._lzdf.join(other, *args, how=how, **kwargs)
        after = joined_lzdf.select(pl.first()).collect().height
        
        if left_before > 0:
            change = ((after - left_before) / left_before) * 100
            change_str = f" ({'+' if change > 0 else ''}{change:.1f}%)" if abs(change) > 1.00 else f" ({after - left_before} rows {' removed' if change < 0 else ' added'})"
        
        unchanged = " (row count unchanged)" if after == left_before else ""
        print(f"[{label}] Join type: {how.upper()}")
        print(f"[{label}] Left: {left_before} rows, Right: {right_before} rows -> After: {after} rows{unchanged}{change_str}")
        return joined_lzdf


# ### Scripting for automated next notebook initation

# In[ ]:


from IPython.display import Javascript


# In[ ]:


def redirect_to_next_notebook_in_pipeline(other_notebook):
    
    js_code = f"""
    if (typeof Jupyter !== 'undefined' && Jupyter.notebook && Jupyter.notebook.kernel) {{
        // only runs when cell is executed, not from cached output
        console.log("Redirecting to next notebook in pipeline...");
        Jupyter.notebook.save_checkpoint();
        Jupyter.notebook.session.delete();
        
        setTimeout(function() {{
            window.location.href = '{other_notebook}.ipynb';
        }}, 1500)
    }} else {{
        console.log("Found cached output. Not an active notebook context. Skipping redirect.")
    }}
    """
    display(Javascript(js_code))


# ### Make paths
# 
# Make the folders for the results and a variable for each of the files with the path. 

# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# In[ ]:


PROCESSED_DATASETS_LOCATION =  f"{ROOT_LOCATION}/{VERSION}/processed_datasets"


# In[ ]:


DEMOGRAPHICS_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/demographics"
AnyPath(DEMOGRAPHICS_LOCATION).mkdir(parents=True, exist_ok=True)


# In[ ]:


MAPPING_FILE_LOCATION = (
    "/genesandhealth/library-red/genesandhealth/"
    "2025_02_10__MegaLinkage_forTRE.csv" # actually tab-delimited
)

DEMOGRAPHICS_FILE_LOCATION = (
    "/genesandhealth/library-red/genesandhealth/phenotypes_rawdata/"
    "QMUL__Stage1Questionnaire/2025_04_25__S1QSTredacted.csv"
)


# ### Load the data 
# 
# Here we are loading the data into the DemographicDataset object. We then define a config. This config is a simple way of pointing out the key columns in each of the mapping files so that we can process the data in a clean way. 

# In[ ]:


demographics = DemographicDataset(
    path_to_mapping_file=MAPPING_FILE_LOCATION,
    path_to_demographic_file=DEMOGRAPHICS_FILE_LOCATION,
)


# In[ ]:


## This step creates DOBs for all people who have a questionnaire (with an Oragene_ID)
demographics.demographics = (
    demographics.demographics
    .lazy()
    .select(
        pl.col("S1QST_Oragene_ID"),
        pl.col("S1QST_Gender"),
        pl.col("S1QST_MM-YYYY_ofBirth"),
    )
    .TRE
    .filter_with_logging(
        pl.col("S1QST_MM-YYYY_ofBirth").ne("NA"),
        label="EXCLUDING `NA` DATE"
    )
    .TRE
    .unique_with_logging(
        subset=["S1QST_Oragene_ID"],
        label="Check for repeated rows of matching `S1QST_Oragene_ID`"
    )    
    .select(
        pl.col("S1QST_Oragene_ID"),
        pl.col("S1QST_MM-YYYY_ofBirth"),
        pl.col("S1QST_Gender"),
    )
    .collect()
)


# In[ ]:


demographics.mapped_data = (
    demographics.mapped_data
    .lazy()
    .TRE
    .filter_with_logging(
        pl.col("55273exomes_release_2024-OCT-08").is_not_null(),
        pl.col("pseudonhs_2024-07-10").is_not_null(), # there are some rows with NON-NULL exome_id but NULL pseudo_nhs_number
        label="Only include NON-NULL exome_id and NON-NULL pseudo_nhs_number for 55k Regenie"
    )
    .TRE
    .filter_with_logging(
        pl.col("OrageneID").is_not_null(),
        label="Sanity check to ensure no NULL OrageneID. row count should remain unchanged"
    )
    .TRE
    .unique_with_logging(
        ["pseudonhs_2024-07-10"],
        label="Sanity check: row count should remain unchanged when uniquing by pseudo_nhs_number"
    )
    .TRE
    .unique_with_logging(
        ["OrageneID"],
        label="Sanity check: row count should remain unchanged when uniquing by OrageneID"
    )
    .collect()
)


# In[ ]:


config = {
    "mapping": {
        "OrageneID": "study_id",
        "pseudonhs_2024-07-10": "nhs_number"
    },
    "demographics": {
        "S1QST_Oragene_ID": "study_id",
        "S1QST_MM-YYYY_ofBirth": "dob",
        "S1QST_Gender": "gender"
    }
}


# ### Process the data 
# 
# In this we are processing the data. We then print the first 5 rows to double check that the format is correct. 

# In[ ]:


demographics.process_dataset(column_maps=config, round_to_day_in_month=1)


# In[ ]:


demographics.log.append(f"{datetime.now()}: Data cleaned and processed")


# ### Save the data
# 

# In[ ]:


demographics.write_to_feather(f"{DEMOGRAPHICS_LOCATION}/clean_demographics.arrow")


# In[ ]:


demographics.write_to_log(f"{DEMOGRAPHICS_LOCATION}/clean_demographics_log.txt")


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("2-process-datasets-discovery-primary-care")


# In[ ]:





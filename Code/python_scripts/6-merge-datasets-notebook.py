#!/usr/bin/env python
# coding: utf-8

# # 6-merge-datasets-notebook - Plan
# 
# In this notebook, we are going to load all the data from file that was created in notebooks 2 to 5. We are aiming to get all the data of the same type together so we end up with:
# 
# 1) ICD10 dataset
# 2) OPCS dataset
# 3) SNOMED dataset
# 
# In addition to this, we have mapped the SNOMED datasets to ICD10 and we will merge these in as well so we end up with:
# 
# 4) SNOMED_MAPPED_AND_ICD dataset

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


VERSION = 'version010_2025_05_SR'


# In[ ]:


from datetime import datetime
from tretools.datasets.processed_dataset import ProcessedDataset
from tretools.datasets.dataset_enums.dataset_types import DatasetType
from tretools.codelists.codelist_types import CodelistType


# In[ ]:


import polars as pl
from cloudpathlib import AnyPath


# ## Paths

# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"


# In[ ]:


# INPUT_PATH = f"{ROOT_LOCATION}/{VERSION}/processed_datasets"
# OUTPUT_PATH = f"{ROOT_LOCATION}/{VERSION}/merged_datasets"
# AnyPath(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)


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


# ### ICD10 Datasets \[Native\]
# 
# Datasets are Primary, Barts, Bradford, NHSD but there is no native ICD-10 data in Primary, hence only 3 native ICD datasets collected.

# In[ ]:


barts_icd = ProcessedDataset(path=f"{MEGADATA_LOCATION}/barts_health/merged_ICD.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.ICD10.value,
                             log_path=f"{MEGADATA_LOCATION}/barts_health/merged_ICD_log.txt")


# In[ ]:


bradford_icd = ProcessedDataset(path=f"{MEGADATA_LOCATION}/bradford/icd.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.ICD10.value,
                             log_path=f"{MEGADATA_LOCATION}/bradford/icd_log.txt")


# In[ ]:


nhs_d_icd = ProcessedDataset(path=f"{MEGADATA_LOCATION}/nhs_digital/nhs_d_merged_ICD10.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.ICD10.value,
                             log_path=f"{MEGADATA_LOCATION}/nhs_digital/nhs_d_merged_ICD10_log.txt")


# In[ ]:


barts_icd.merge_with_dataset(bradford_icd)


# In[ ]:


barts_icd.merge_with_dataset(nhs_d_icd)


# In[ ]:


dedup = barts_icd.deduplicate()


# In[ ]:


for log in bradford_icd.log:
    dedup.log.append(log)

for log in nhs_d_icd.log:
    dedup.log.append(log)
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_LOCATION}/icd_only.arrow")
dedup.write_to_log(f"{MEGADATA_LOCATION}/icd_only_log.txt")


# ### OPCS Datasets
# 
# Only (hospital) secondary care sources use OPCS, i.e Bart and Bradford

# In[ ]:


barts_opcs = ProcessedDataset(path=f"{MEGADATA_LOCATION}/barts_health/merged_OPCS.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.OPCS.value,
                             log_path=f"{MEGADATA_LOCATION}/barts_health/merged_OPCS_log.txt")


# In[ ]:


bradford_opcs = ProcessedDataset(path=f"{MEGADATA_LOCATION}/bradford/opcs.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.OPCS.value,
                             log_path=f"{MEGADATA_LOCATION}/bradford/opcs_log.txt")


# In[ ]:


barts_opcs.merge_with_dataset(bradford_opcs)


# In[ ]:


dedup = barts_opcs.deduplicate()


# In[ ]:


for log in bradford_opcs.log:
    dedup.log.append(log)

dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_LOCATION}/opcs_only.arrow")
dedup.write_to_log(f"{MEGADATA_LOCATION}/opcs_only_log.txt")


# ### SNOMED Datasets

# In[ ]:


primary_snomed = ProcessedDataset(path=f"{MEGADATA_LOCATION}/primary_care/final_merged_data.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.SNOMED.value,
                             log_path=f"{MEGADATA_LOCATION}/primary_care/final_log.txt")


# In[ ]:


barts_snomed = ProcessedDataset(path=f"{MEGADATA_LOCATION}/barts_health/merged_SNOMED.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.SNOMED.value,
                             log_path=f"{MEGADATA_LOCATION}/barts_health/merged_SNOMED_log.txt")


# In[ ]:


bradford_snomed = ProcessedDataset(path=f"{MEGADATA_LOCATION}/bradford/snomed.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.SNOMED.value,
                             log_path=f"{MEGADATA_LOCATION}/bradford/snomed_log.txt")


# In[ ]:


nhs_d_snomed = ProcessedDataset(path=f"{MEGADATA_LOCATION}/nhs_digital/nhs_d_merged_SNOMED.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.SNOMED.value,
                             log_path=f"{MEGADATA_LOCATION}/nhs_digital/nhs_d_merged_SNOMED_log.txt")


# In[ ]:


primary_snomed.merge_with_dataset(barts_snomed)
primary_snomed.merge_with_dataset(bradford_snomed)


# In[ ]:


nhs_d_snomed.data = (
    # We have to do a little madness here to comply with expected tretools input
    nhs_d_snomed.data
    .with_columns(
        pl.col("code")
        .cast(pl.Int64, strict=False)
    )
    .filter(
        pl.col("code").is_not_null()
    )
)
primary_snomed.merge_with_dataset(nhs_d_snomed)


# In[ ]:


dedup = primary_snomed.deduplicate()


# In[ ]:


for log in barts_snomed.log:
    dedup.log.append(log)

for log in bradford_snomed.log:
    dedup.log.append(log)

for log in nhs_d_snomed.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_LOCATION}/snomed_only.arrow")
dedup.write_to_log(f"{MEGADATA_LOCATION}/snomed_log.txt")


# ### Now we create the merged ICD dataset
# #### This combines "native" ICD10 with SNOMED -> ICD10 mapped codes
# 
# We first load the mapped dataset (that is the dataset that is mapped from SNOMED to ICD dataset). 

# In[ ]:


mapped_data_primary_icd = ProcessedDataset( # Notebook #2
    path=f"{MEGADATA_LOCATION}/primary_care/final_mapped_data.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/primary_care/final_mapped_log.txt"
)


# In[ ]:


mapped_data_barts_icd = ProcessedDataset( # Notebook #3
    path=f"{MEGADATA_LOCATION}/barts_health/final_mapped_snomed_to_icd.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/barts_health/final_mapped_snomed_to_icd_log.txt"
)


# In[ ]:


mapped_data_bradford_icd = ProcessedDataset( # Notebook #4
    path=f"{MEGADATA_LOCATION}/bradford/final_mapped_snomed_to_icd.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/bradford/final_mapped_snomed_to_icd_log.txt"
)


# In[ ]:


mapped_data_nhs_digital_icd = ProcessedDataset( # Notebook #5
    path=f"{MEGADATA_LOCATION}/nhs_digital/final_mapped_snomed_to_icd.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/nhs_digital/final_mapped_snomed_to_icd_log.txt"
)


# Then we load the mega file of ICD 10 datas (i.e. the one we just made above)

# In[ ]:


icd_mega_file = ProcessedDataset(
    path=f"{MEGADATA_LOCATION}/icd_only.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/icd_only_log.txt"
)


# Now we merge everything together and deduplicate. We merge all the log files together and write to file. 

# In[ ]:


icd_mega_file.merge_with_dataset(mapped_data_primary_icd)


# In[ ]:


icd_mega_file.merge_with_dataset(mapped_data_barts_icd)


# In[ ]:


icd_mega_file.merge_with_dataset(mapped_data_bradford_icd)


# In[ ]:


icd_mega_file.merge_with_dataset(mapped_data_nhs_digital_icd)


# In[ ]:


dedup = icd_mega_file.deduplicate()


# In[ ]:


for log in mapped_data_primary_icd.log:
    dedup.log.append(log)

for log in mapped_data_barts_icd.log:
    dedup.log.append(log)

for log in mapped_data_bradford_icd.log:
    dedup.log.append(log)

for log in mapped_data_nhs_digital_icd.log:
    dedup.log.append(log)

dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_LOCATION}/icd_and_mapped_snomed.arrow")
dedup.write_to_log(f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_log.txt")


# In[ ]:


dedup = ProcessedDataset(
    path=f"{MEGADATA_LOCATION}/icd_and_mapped_snomed.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_log.txt"
)


# We are now going to truncate all the ICD10 codes so they they only have 3 digits and save this as its own dataset. 
# 
# This uses tretools' `truncate_icd_to_3_digits()` which literally just takes first 3 characters of the code field, i.e. does not do anything with `"NA"` or invalid codes (e.g. `"-1"`)

# In[ ]:


three_digit_only = dedup.truncate_icd_to_3_digits()


# In[ ]:


three_digit_only.write_to_feather(f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_3_digit_only.arrow")
three_digit_only.write_to_log(f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_3_digit_only_log.txt")


# Now we are going deduplicate this and save again. 

# In[ ]:


merged = ProcessedDataset(
    path=f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_3_digit_only.arrow",
    dataset_type="MEGA",
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_3_digit_only_log.txt"
)


# In[ ]:


dedup_3_digit = merged.deduplicate()


# In[ ]:


dedup_3_digit.write_to_feather(f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_3_digit_deduplication.arrow")


# In[ ]:


dedup_3_digit.write_to_log(f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_3_digit_deduplication_log.txt")


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("7-three-and-four-digit-ICD")


# **THIS vvvvvvvv NOT RUN**

# ## Data without NHS-D
# 
# One of the industry partners requested a clean dataset - merged - without the NHS-D data included as they do not have a current license. Below is the code run - 26th March 2024 - to produce this. 

# In[ ]:


from datetime import datetime
from tretools.datasets.processed_dataset import ProcessedDataset
from tretools.datasets.dataset_enums.dataset_types import DatasetType
from tretools.codelists.codelist_types import CodelistType
import pathlib


# In[ ]:


INPUT_PATH = "processed_datasets"
OUTPUT_PATH = "merged_datasets"
AnyPath(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)


# ### ICD10 Datasets

# In[ ]:


barts_icd = ProcessedDataset(path=f"{INPUT_PATH}/barts_health/megadata/merged_ICD.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.ICD10.value,
                             log_path=f"{INPUT_PATH}/barts_health/megadata/merged_ICD_log.txt")


# In[ ]:


bradford_icd = ProcessedDataset(path=f"{INPUT_PATH}/bradford/megadata/icd.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.ICD10.value,
                             log_path=f"{INPUT_PATH}/bradford/megadata/icd_log.txt")


# In[ ]:


barts_icd.merge_with_dataset(bradford_icd)


# In[ ]:


dedup = barts_icd.deduplicate()


# In[ ]:


for log in bradford_icd.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{OUTPUT_PATH}/icd_only_no_nhs_d.arrow")
dedup.write_to_log(f"{OUTPUT_PATH}/icd_only_no_nhs_d_log.txt")


# In[ ]:


dedup.write_to_csv(f"{OUTPUT_PATH}/icd_only_no_nhs_d.csv")


# ### Merging without NHS-D data

# SNOMED mapped to ICD and merged with ICD10

# In[ ]:


snomed_mapped = ProcessedDataset(path=f"{INPUT_PATH}/primary_care/megadata/final_mapped_data.arrow", 
                             dataset_type="MEGA", 
                             coding_system=CodelistType.ICD10.value,
                             log_path=f"{INPUT_PATH}/primary_care/megadata/final_mapped_log.txt")


# In[ ]:


snomed_mapped.merge_with_dataset(bradford_icd)


# In[ ]:


snomed_mapped.merge_with_dataset(barts_icd)


# In[ ]:


dedup = snomed_mapped.deduplicate()


# In[ ]:


for log in bradford_icd.log:
    dedup.log.append(log)

for log in barts_icd.log:
    dedup.log.append(log)    
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{OUTPUT_PATH}/icd_with_mapped_snomed_no_nhs_d.arrow")
dedup.write_to_log(f"{OUTPUT_PATH}/icd_with_mapped_snomed_no_nhs_d_log.txt")


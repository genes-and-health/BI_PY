#!/usr/bin/env python
# coding: utf-8

# # 3-process-datasets-barts-health -- Plan
# 
# This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as feather files (binary) with their log files where appropriate.
# 
# In this second notebook, we do the following:
# 
# 1.    Process barts health care datasets - SNOMED
# 2.   Process barts health care dataset - ICD10
# 3.   Process barts health care datast - OPCS
# 4.   Map SNOMED datasets to ICD and combine with ICD10 dataset
# 
# ## Processing Barts Health Datasets
# 
# The barts healthcare datasets are in SNOMED, ICD10 and OPCS. There are 5 "cuts" of data representing 5 time periods when the data was made available. Each dataset comprises of multiple CSV files.
# 
# We process the datasets in the following way:
# 
# 1.   Load the data for each "type" of coding system per file
# 2.   Deduplicate and process
# 3.   Save as arrow file and with log file.
# 4.   We then remove all unrealistic dates via the dataset that we created in the first notebook
# 5.   Save this cleaned data file with log
# 
# Once all the datasets have been created:
# 
# 1.  Reload each dataset from file and merge with other datasets of the same type for a time period (i.e. the same cut of the data)
# 2. Deduplicate and merge all the log files together
# 3. Save in processed merged data folder
# 
# Finally merge all the same types of dataset together, deduplicate and save.
# 
# NB. processing of Bradford data now takes place in notebook \#4.

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


# ## Imports
# 
# This is where imports for tretools + other are pulled in

# In[ ]:


from datetime import datetime
from tretools.datasets.raw_dataset import RawDataset
from tretools.datasets.dataset_enums.dataset_types import DatasetType
from tretools.codelists.codelist_types import CodelistType
from tretools.datasets.demographic_dataset import DemographicDataset
from tretools.datasets.processed_dataset import ProcessedDataset

from cloudpathlib import AnyPath
import polars as pl
import subprocess


# ## Paths

# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# In[ ]:


INPUT_LOCATION = "/genesandhealth/library-red/genesandhealth/phenotypes_rawdata/DSA__BartsHealth_NHS_Trust"


# In[ ]:


PROCESSED_DATASETS_LOCATION =  f"{ROOT_LOCATION}/{VERSION}/processed_datasets"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"
MAPPING_FILES_LOCATION =  f"{ROOT_LOCATION}/{VERSION}/mapping_files"


# In[ ]:


PROCESSED_DATASETS_BARTS_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/barts_health"
MEGADATA_BARTS_LOCATION = f"{MEGADATA_LOCATION}/barts_health"


# In[ ]:


PREPROCESSED_FILES_LOCATION = f"{ROOT_LOCATION}/{VERSION}/preprocessed_files"


# ## Create paths as necessary

# In[ ]:


# PROCESSED_DATASETS_BARTS_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/barts_health/processed_datasets"
# MEGADATA_PATH = f"{PROCESSED_DATASETS_LOCATION}/barts_health/megadata"

AnyPath(PROCESSED_DATASETS_BARTS_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(MEGADATA_BARTS_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(PREPROCESSED_FILES_LOCATION).mkdir(parents=True, exist_ok=True)


# We create the paths to the dataset original folders

# In[ ]:


MAR_2022_INPUT_PATH = f"{INPUT_LOCATION}/2022_03_ResearchDatasetv1.3"
MAY_2023_INPUT_PATH = f"{INPUT_LOCATION}/2023_05_ResearchDatasetv1.5"
DEC_2023_INPUT_PATH = f"{INPUT_LOCATION}/2023_12_ResearchDatasetv1.6"
SEP_2024_INPUT_PATH = f"{INPUT_LOCATION}/2024_09_ResearchDataset"

# At present, we also omit
# apr_2021_path = f"{INPUT_LOCATION}/2021_04_PathologyLab"


# ## Scripting for automated next notebook initation

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


# # Load the data and transform it
# 
# We are loading the demographic data that was created in notebook 1 so that we can remove unrealistic dates.
# 

# In[ ]:


deduplication_options = ['nhs_number', 'code', 'date']


# In[ ]:


demographic_file_path = f"{PROCESSED_DATASETS_LOCATION}/demographics/clean_demographics.arrow"


# In[ ]:


date_start=datetime.strptime("1910-01-01", "%Y-%m-%d")
# date_end=datetime.strptime("2024-01-29", "%Y-%m-%d")
# Modified to lastest date found in source dir
## MS to consider extracting reasonable end date automatically from file/dir timestamps (low priority)
date_end=datetime.today()  # datetime.strptime("2024-10-26", "%Y-%m-%d")


# In[ ]:


#  Line creating demographic object not present, now added SR
demographics = DemographicDataset(path=demographic_file_path)


# 
# # 1st cut of data - March 2022 Data
# 
# In this first set of data we have 1 ICD file, 1 OPCS file and 3 SNOMED files that relate to diagosis, procedures and problems. We will combine the snomed codes into one dataset.
# 
# We will save a copy of the files in `processed_data/` folder. We will then remove unrealistic dates with the demographic dataset, and save these files in to `clean_processed_data/`.
# 

# In[ ]:


dataset_icd_path = f"{MAR_2022_INPUT_PATH}/2022_05_23__icd10_combined_redacted.txt"
dataset_opcs_path = f"{MAR_2022_INPUT_PATH}/2022_05_26__opcs_combined_redacted.txt"
dataset_diagnosis_snomed_path = f"{MAR_2022_INPUT_PATH}/GandH_PC_Diagnosis_202203191144_redacted_with_conceptIDs_ConfirmedOnly.csv"
dataset_problems_snomed_path = f"{MAR_2022_INPUT_PATH}/GandH_PC_Problems_202203191144_redacted_with_conceptIDs_ConfirmedOnly.csv"
dataset_procedures_snomed_path = f"{MAR_2022_INPUT_PATH}/GandH_PC_Procedures_202203191144_redacted_with_conceptIDs.csv"


# This is where we are saving each log and feather file per dataset. This is to store them so we don't need to process them again.

# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data").mkdir(parents=True, exist_ok=True)


# # ICD10 dataset
# 
# Unlike the primary care datasets, all the different datasets have different column headers in the raw form so it is necessary to have individual column maps.
# 

# In[ ]:


col_maps_icd = {"ICD10_code": "code", "ICD10_name": "term", "CodingDate": "date", "PseudoNHS": "nhs_number"}


# In[ ]:


dataset_icd = RawDataset(
    path=dataset_icd_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_dataset_icd = dataset_icd.process_dataset(deduplication_options=deduplication_options, 
                                                    column_maps=col_maps_icd)


# In[ ]:


processed_dataset_icd.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_ICD.arrow"
)


# In[ ]:


processed_dataset_icd.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_ICD_log.txt"
)


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_dataset_icd.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)


# In[ ]:


cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_ICD.arrow"
)
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_ICD_log.txt"
)


# ### OPCS dataset

# In[ ]:


col_maps_opcs = {"OPCS_code": "code", "OPCS_name": "term", "CodingDate": "date", "PseudoNHS": "nhs_number"}


# In[ ]:


dataset_opcs = RawDataset(
    path=dataset_opcs_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.OPCS.value
)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_opcs = dataset_opcs.process_dataset(\n    deduplication_options=deduplication_options,\n    column_maps=col_maps_opcs\n)\n')


# In[ ]:


processed_dataset_opcs.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_OPCS.arrow"
)


# In[ ]:


processed_dataset_opcs.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_OPCS_log.txt"
)


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


## cleaned_dataset has been used already, ?change to cleaned_dataset_opcs
cleaned_dataset = processed_dataset_opcs.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)


# In[ ]:


cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_OPCS.arrow"
)
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_OPCS_log.txt"
)


# 
# ### SNOMED
# 
# We are processing 3 different SNOMED datasets here. We will process and save them individually, then combine them, deduplicate and save this.
# 
# ### Diagnosis Snomed Dataset
# 
# Here we are loading the data. Now the data is loading the conceptId snomed codes as floats as they have some scientific notation in them. We do an extra step to convert these to integers immediately on loading.

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_diagnosis = RawDataset(\n    path=dataset_diagnosis_snomed_path,\n    dataset_type=DatasetType.BARTS_HEALTH.value,\n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', "## Why is the polar read with Barts Diagnoiis (2022) importing column DiscriptionID as int64 w/ no problem but only\n## importing conceptId as float64 requiring a recast to int64?\n\n## Note that this seems specific to this file (?to all Diagnosis files) as no recasting needed with Procedures\ndataset_diagnosis.data = dataset_diagnosis.data.with_columns(\n    dataset_diagnosis.data['conceptId'].cast(pl.Int64).alias('code')\n)\n")


# In[ ]:


col_maps_snomed = {
    "code": "code",
    "term": "term",
    "DiagDt": "date",
    "PseudoNHSNumber": "nhs_number"
}


# In[ ]:


get_ipython().run_line_magic('time', '')
processed_diagnosis = dataset_diagnosis.process_dataset(deduplication_options=deduplication_options,
                                                        column_maps=col_maps_snomed)


# In[ ]:


get_ipython().run_line_magic('time', '')
processed_diagnosis.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_SNOMED_diagnosis.arrow"
)
processed_diagnosis.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_SNOMED_diagnosis_log.txt"
)


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_line_magic('time', '')
cleaned_dataset = processed_diagnosis.remove_unrealistic_dates(date_start=date_start,
                                                                             date_end=date_end,
                                                                             before_born=True,
                                                                             demographic_dataset=demographics)


# In[ ]:


cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_diagnosis.arrow"
)
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_diagnosis_log.txt"
)


# ### Procedures Snomed Dataset

# In[ ]:


dataset_procedures = RawDataset(
    dataset_procedures_snomed_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value
)


# In[ ]:


procedure_map = {
    "PseudoNHSNumber": "nhs_number",
    "conceptId": "code",
    "ProcDt": "date"
}


# In[ ]:


processed_dataset_procedures = dataset_procedures.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=procedure_map
)


# In[ ]:


processed_dataset_procedures.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_SNOMED_procedures.arrow"
)
processed_dataset_procedures.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_SNOMED_procedures_log.txt"
)


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_dataset_procedures.remove_unrealistic_dates(date_start=date_start,
                                                                             date_end=date_end,
                                                                             before_born=True,
                                                                             demographic_dataset=demographics)


# In[ ]:


cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_procedures.arrow"
)
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_procedures_log.txt"
)


# ### Problems Snomed Dataset
# 
# The conceptId column contains the snomed code but because it has some Nulls in it, the dataframe is reading it in as strings. We are converting these to integers directly.
# 
# Notes added by SR/MS 06.12.24:
# 1. OK but also null in conceptId in Diagnoses and Procedure, in former, conceptId came in as f64 in latter comes in as i64!
# 2. Does Caroline mean "dataframe is reading it in as f64" (cf strings)

# In[ ]:


## When run on 06.12.2024, this gave us the following error message
# '''
# ComputeError: found more fields than defined in 'Schema'

# Consider setting 'truncate_ragged_lines=True'.
# '''
## Problem was tracked down to line 6010 which truncates early; it's the only problem line found
# dataset_problems = RawDataset(
#     dataset_problems_snomed_path, 
#     dataset_type=DatasetType.BARTS_HEALTH.value, 
#     coding_system=CodelistType.SNOMED.value
# )


# In[ ]:


line_to_exclude = "6010"
processed_file_name = (
    AnyPath(
        dataset_problems_snomed_path
    ).stem +
    f"_without_line_{line_to_exclude}" +
    AnyPath(
        dataset_problems_snomed_path
    ).suffix
)


# In[ ]:


barts_2022_03_preprocessing_command = (
    f"""sed '{line_to_exclude}d' """
    f"""\"{dataset_problems_snomed_path}\" > """
    f"""\"{PREPROCESSED_FILES_LOCATION}/{processed_file_name}\""""
)


# In[ ]:


subprocess.run(
    barts_2022_03_preprocessing_command,
    shell=True,
    check=True,
    capture_output=True,
    text=True
)


# In[ ]:


## In order to allow pipeline to run, we have created a file with no truncated line
dataset_problems = RawDataset(
    f"{PREPROCESSED_FILES_LOCATION}/{processed_file_name}",
    dataset_type=DatasetType.BARTS_HEALTH.value, 
    coding_system=CodelistType.SNOMED.value
)


# In[ ]:


## This line perhaps pertinent when using file with truncated line issue
## Recasting not longer necessary with problem line removed
## This kept:
## 1. To preserve original pipeline
## 2. to rename create 'code' column

dataset_problems.data = dataset_problems.data.with_columns(
    dataset_problems.data['conceptId']
    .cast(pl.Int64)
    .alias('code')
)


# In[ ]:


problems_map = {
    "PseudoNHSNumber": "nhs_number",
    "code": "code",
    "OnsetDate": "date"
}


# In[ ]:


processed_dataset_problems = dataset_problems.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=problems_map
)


# In[ ]:


processed_dataset_problems.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_SNOMED_problems.arrow"
)
processed_dataset_problems.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/processed_data/dataset_SNOMED_problems.txt"
)


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_dataset_problems.remove_unrealistic_dates(date_start=date_start,
                                                                             date_end=date_end,
                                                                             before_born=True,
                                                                             demographic_dataset=demographics)


# In[ ]:


cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_problems.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_problems.txt")


# ## Merge all the SNOMED datasets from March 2022 together
# 
# First we load all the datasets from the files that we created earlier. We merged them in and deduplicate.

# In[ ]:


dataset_diagnosis = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_diagnosis.arrow", 
                                     dataset_type=DatasetType.BARTS_HEALTH.value, 
                                     coding_system=CodelistType.SNOMED.value, 
                                     log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_diagnosis_log.txt")


# In[ ]:


dataset_problems = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_problems.arrow", 
                                    dataset_type=DatasetType.BARTS_HEALTH.value, 
                                    coding_system=CodelistType.SNOMED.value, 
                                    log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_problems.txt")


# In[ ]:


dataset_procedures = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_procedures.arrow", 
                                      dataset_type=DatasetType.BARTS_HEALTH.value, 
                                      coding_system=CodelistType.SNOMED.value, 
                                      log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_SNOMED_procedures_log.txt")


# In[ ]:


dataset_diagnosis.merge_with_dataset(dataset_procedures)


# In[ ]:


dataset_diagnosis.merge_with_dataset(dataset_problems)


# In[ ]:


# Now we deduplicate


# In[ ]:


dedup = dataset_diagnosis.deduplicate()


# In[ ]:


dedup.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/merged_SNOMED.arrow")


# In[ ]:


# Add all the logs from the datasets to this final log and sort


# In[ ]:


for l in dataset_problems.log:
    dedup.log.append(l)
    
for l in dataset_procedures.log:
    dedup.log.append(l)

dedup.log.sort()
dedup.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/merged_SNOMED_log.txt")


# ## 2nd cut of data - May 2023 Data
# 
# In this first set of data we have 2 ICD file, 2 OPCS file and 3 SNOMED files that relate to diagosis, procedures and problems. We will combine the datasets with the same "types" of codes into one dataset.

# In[ ]:


dataset_icd1_path = f"{MAY_2023_INPUT_PATH}/GH_APC_Diagnosis_202305071650.ascii.redacted.tab"
dataset_icd2_path = f"{MAY_2023_INPUT_PATH}/GH_OP_Diagnosis_202305071650.ascii.redacted.tab"
dataset_opcs1_path = f"{MAY_2023_INPUT_PATH}/GH_APC_OPCS_202305071650.ascii.redacted.tab"
dataset_opcs2_path = f"{MAY_2023_INPUT_PATH}/GH_OPA_OPCS_202305071651.ascii.redacted.tab"
dataset_diagnosis_snomed_path = f"{MAY_2023_INPUT_PATH}/GH_PC_Diagnosis_202305071654.ascii.redacted_ConfirmedOnly.tab"
dataset_problems_snomed_path = f"{MAY_2023_INPUT_PATH}/GH_PC_Problems_202305071654.ascii.redacted_ConfirmedOnly.tab"
dataset_procedures_snomed_path = f"{MAY_2023_INPUT_PATH}/GH_PC_Procedures_202305071654.ascii.redacted.tab"


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data").mkdir(parents=True, exist_ok=True)


# ### ICD10 Codes
# 
# We load both ICD datasets, clean and save them as feather files, and then merge them together, deduplicate this, and save again.

# In[ ]:


dataset_icd1 = RawDataset(
    path=dataset_icd1_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


col_mapping = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "ICD_Diagnosis_Cd": "code",
    "ICD_Diag_Desc": "term",
    "Activity_date": "date"
}


# In[ ]:


processed_dataset_icd1 = dataset_icd1.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_mapping
)


# In[ ]:


processed_dataset_icd1.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/apc_ICD.arrow")
processed_dataset_icd1.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/apc_ICD_log.txt")


# In[ ]:


dataset_icd2 = RawDataset(
    path=dataset_icd2_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_dataset_icd2 = dataset_icd2.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_mapping)


# In[ ]:


processed_dataset_icd2.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/op_ICD.arrow")
processed_dataset_icd2.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/op_ICD_log.txt")


# Merge the ICD dataset from May 2023 together

# In[ ]:


processed_dataset_icd1.merge_with_dataset(processed_dataset_icd2)


# In[ ]:


merged_icd = processed_dataset_icd1.deduplicate()


# In[ ]:


merged_icd.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/merged_ICD.arrow")


# In[ ]:


for log in processed_dataset_icd2.log:
    merged_icd.log.append(log)

merged_icd.log.sort()
merged_icd.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/merged_ICD_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = merged_icd.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)


# In[ ]:


cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_ICD.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_ICD_log.txt")


# ### OPCS

# In[ ]:


dataset_opcs1 = RawDataset(
    path=dataset_opcs1_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.OPCS.value
)


# In[ ]:


col_mapping = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "OPCS_Proc_Cd": "code",
    "Proc_Desc": "term",
    "Activity_date": "date"
}


# In[ ]:


processed_dataset_opcs1 = dataset_opcs1.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_mapping
)


# In[ ]:


processed_dataset_opcs1.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/apc_OPCS.arrow")
processed_dataset_opcs1.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/apc_OPCS_log.txt")


# In[ ]:


dataset_opcs2 = RawDataset(
    path=dataset_opcs2_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.OPCS.value
)


# In[ ]:


processed_dataset_opcs2 = dataset_opcs2.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_mapping
)


# In[ ]:


processed_dataset_opcs2.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/op_OPCS.arrow")
processed_dataset_opcs2.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/op_OPCS_log.txt")


# Merge the OPCS dataset from May 2023 together

# In[ ]:


processed_dataset_opcs1.merge_with_dataset(processed_dataset_opcs2)
merged_opcs = processed_dataset_opcs1.deduplicate()
merged_opcs.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/merged_OPCS.arrow")


# In[ ]:


for log in processed_dataset_opcs2.log:
    merged_opcs.log.append(log)

merged_opcs.log.sort()
merged_opcs.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/merged_OPCS_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = merged_opcs.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_OPCS.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_OPCS_log.txt")


# ### SNOMED
# 
# ### Diagnosis Snomed Dataset
# 
# Here we loading the snomed data for diagnoses. For some reason, there are 2 codes out of 389,794 that are not SNOMED codes - they are `G51.0` and `G51.0         ` with whitespace. I have removed these codes for now.

# In[ ]:


dataset_diagnosis = RawDataset(
    path=dataset_diagnosis_snomed_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value
)


# In[ ]:


dataset_diagnosis.log.append(f"{datetime.now()}: There are {dataset_diagnosis.data.shape[0]} rows")


# In[ ]:


dataset_diagnosis.data = dataset_diagnosis.data.filter(dataset_diagnosis.data['DiagCode'] != "G51.0")


# In[ ]:


dataset_diagnosis.data = dataset_diagnosis.data.filter(dataset_diagnosis.data['DiagCode'] != "G51.0          ")


# In[ ]:


dataset_diagnosis.log.append(f"{datetime.now()}: After removing ICD codes (G51.0) {dataset_diagnosis.data.shape[0]} rows")


# In[ ]:


dataset_diagnosis.data = dataset_diagnosis.data.with_columns(
    dataset_diagnosis.data['DiagCode'].cast(pl.Int64).alias('code'))


# In[ ]:


col_maps_snomed = {"code": "code", "Diagnosis": "term", "DiagDt": "date", "PseudoNHS_2023_04_24": "nhs_number"}


# In[ ]:


processed_diagnosis = dataset_diagnosis.process_dataset(deduplication_options=deduplication_options,
                                                        column_maps=col_maps_snomed)


# In[ ]:


processed_diagnosis.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_diagnosis.arrow")
processed_diagnosis.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_diagnosis_log.txt")


# ### Procedures
# 
# Procedures tab file has a number of different issues. First of all there are a mixtue of OPCS codes and SNOMED codes used. Secondaly the SNOMED codes have a white space at the end of each SNOMED code. E.g. `1489351015          `, this means it is being read in as a string. We need to do some pre-processing to account for this.
# 

# In[ ]:


preprocessed_data = pl.read_csv(dataset_procedures_snomed_path,
                                infer_schema_length=0,
                                null_values=['', ' ', 'NULL', "NA"],
                                separator="\t"
                               )


# In[ ]:


log = [f'{datetime.now()}: Load dataset from {dataset_diagnosis_snomed_path} for preprocessing. This step was needed to deal with some unexpected NAs, empty strings at the end of codes and ICD10 codes. There are {preprocessed_data.shape[0]} rows in original dataset. ']


# First we remove all the extra whitespace.

# In[ ]:


preprocessed_data = preprocessed_data.with_columns(preprocessed_data['ProcCD'].str.replace(r"\s+$", ""))


# In[ ]:


log.append(f"{datetime.now()}: Whitespace from the procedure code removed")


# Split into different datasets based on type of code from ProcType column.

# In[ ]:


opcs_procedures_preprocessed = preprocessed_data.filter(pl.col('ProcType') == "OPCS4")


# In[ ]:


opcs_log = log.copy()
opcs_log.append(f"{datetime.now()}: OPCS dataset separated from SNOMED dataset. This results in {opcs_procedures_preprocessed.shape[0]} rows")


# In[ ]:


snomed_procedures_preprocessed = preprocessed_data.filter(pl.col('ProcType') == "SNOMED CT")


# In[ ]:


snomed_log = log.copy()
snomed_log.append(f"{datetime.now()}: SNOMED dataset separated from OPCS dataset. This results in {snomed_procedures_preprocessed.shape[0]} rows")


# Now we deal with each dataset separately.

# In[ ]:


snomed_file_to_save_path = f"{PREPROCESSED_FILES_LOCATION}/snomed_procedures.csv"
snomed_logs_to_save_path = f"{PREPROCESSED_FILES_LOCATION}/snomed_procedures_log.txt"
opcs_file_to_save_path = f"{PREPROCESSED_FILES_LOCATION}/opcs_procedures.csv"
opcs_logs_to_save_path = f"{PREPROCESSED_FILES_LOCATION}/opcs_procedures_log.txt"


# In[ ]:


snomed_procedures_preprocessed.write_csv(snomed_file_to_save_path)
snomed_log.append(f"{datetime.now()}: Save dataset after preprocessing into {snomed_file_to_save_path}")
with open(snomed_logs_to_save_path, "w") as f:
    for line in snomed_log:
        f.write(line + "\n")


# In[ ]:


opcs_procedures_preprocessed.write_csv(opcs_file_to_save_path)
opcs_log.append(f"{datetime.now()}: Save dataset after preprocessing into {opcs_file_to_save_path}")
with open(opcs_logs_to_save_path, "w") as f:
    for line in opcs_log:
        f.write(line + "\n")


# ### Processing
# 
# We have now done all the pre-processing. We can load the data from file and process it.
# 
# We start with SNOMED.

# In[ ]:


dataset_procedures = RawDataset(
    path=snomed_file_to_save_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value
)


# In[ ]:


col_maps_snomed = {"ProcCD": "code", "ProcDetails": "term", "ProcDt": "date", "PseudoNHS_2023_04_24": "nhs_number"}


# In[ ]:


processed_procedures = dataset_procedures.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps_snomed
)


# In[ ]:


processed_procedures.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_procedures.arrow")
processed_procedures.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_procedures_log.txt")


# And now OPCS. Remember this is the OPCS dataset that has come from SNOMED.

# In[ ]:


dataset_procedures_opcs = RawDataset(
    path=opcs_file_to_save_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.OPCS.value
)


# In[ ]:


processed_procedures_opcs = dataset_procedures_opcs.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps_snomed
)


# In[ ]:


processed_procedures_opcs.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_OPCS_procedures.arrow")
processed_procedures_opcs.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_OPCS_procedures_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_procedures_opcs.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/dataset_OPCS_procedures.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/dataset_OPCS_procedures_log.txt")


# ### Problems
# 
# Again, the SNOMED codes have a white space at the end of each SNOMED code. E.g. 1489351015          , this means it is being read in as a string. We need to do some pre-processing to account for this.
# 
# ### Pre-processing
# 

# In[ ]:


preprocessed_data = pl.read_csv(dataset_problems_snomed_path,
                                infer_schema_length=0,
                                null_values=['', ' ', 'NULL', "NA"],
                                separator="\t"
                               )


# In[ ]:


logs = [f'{datetime.now()}: Load dataset from {dataset_problems_snomed_path} for preprocessing. This step was needed to deal with some unexpected NAs, empty strings at the end of codes and ICD10 codes. There are {preprocessed_data.shape[0]} rows in original dataset. ']


# In[ ]:


preprocessed_data = preprocessed_data.with_columns(preprocessed_data['ProbCode'].str.replace(r"\s+$", ""))
logs.append(f"{datetime.now()}: Whitespace from the ProbCode code removed")


# In[ ]:


snomed_file_to_save_path = f"{PREPROCESSED_FILES_LOCATION}/snomed_problems.csv"
snomed_logs_to_save_path = f"{PREPROCESSED_FILES_LOCATION}/snomed_problems_log.txt"


# In[ ]:


preprocessed_data.write_csv(snomed_file_to_save_path)


# In[ ]:


logs.append(f"{datetime.now()}: Save dataset after preprocessing into {snomed_file_to_save_path}")
with open(snomed_logs_to_save_path, "w") as f:
    for line in snomed_log:
        f.write(line + "\n")


# ### Processsing
# 
# Now we load this saved file and process as normal.

# In[ ]:


dataset_problems = RawDataset(
    path=snomed_file_to_save_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value
)


# In[ ]:


dataset_problems.data = dataset_problems.data.with_columns(
    dataset_problems.data['ProbCode']
    .cast(pl.Int64)
    .alias('code')
)


# In[ ]:


col_maps_snomed = {
    "code": "code",
    "Problem": "term",
    "OnsetDate": "date",
    "PseudoNHS_2023_04_24": "nhs_number"
}


# In[ ]:


processed_problems = dataset_problems.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps_snomed
)


# In[ ]:


processed_problems.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_problems.arrow")
processed_problems.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_problems_log.txt")


# ### Merge all snomed data together for May 2023
# 
# We merge the datasets together and then deduplicate the data. We also add the logs together.
# 
# Again we are loading from file just in case the cells got run out of order.

# In[ ]:


processed_diagnosis = ProcessedDataset(
    path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_diagnosis.arrow",
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value,
    log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_diagnosis_log.txt")


# In[ ]:


processed_procedures = ProcessedDataset(
    path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_procedures.arrow",
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value,
    log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_procedures_log.txt")


# In[ ]:


processed_problems = ProcessedDataset(
    path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_problems.arrow",
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.SNOMED.value,
    log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/dataset_SNOMED_problems_log.txt")


# We merge them together.

# In[ ]:


processed_problems.merge_with_dataset(processed_procedures)


# In[ ]:


processed_problems.merge_with_dataset(processed_diagnosis)


# In[ ]:


dedup = processed_problems.deduplicate()
dedup.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/merged_SNOMED.arrow")


# In[ ]:


for l in processed_procedures.log:
    dedup.log.append(l)
        
for l in processed_diagnosis.log:
    dedup.log.append(l)

dedup.log.sort()
dedup.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/processed_data/merged_SNOMED_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = dedup.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_SNOMED.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_SNOMED_log.txt")


# ## 3rd cut of data - Dec 2023 Data
# 
# In this 3rd set of data we have 2 ICD file, 2 OPCS file and 1 SNOMED file that relate to diagosis, procedures and problems. We will combine the datasets with the same "types" of codes into one dataset.

# In[ ]:


dataset_icd1_path = f"{DEC_2023_INPUT_PATH}/GH_APC_Diagnosis__20231211.ascii.redacted.tab"
dataset_icd2_path = f"{DEC_2023_INPUT_PATH}/GH_OP_Diagnosis__20231211.ascii.redacted.tab"
dataset_opcs1_path = f"{DEC_2023_INPUT_PATH}/GH_APC_OPCS__20231211.ascii.redacted.tab"
dataset_opcs2_path = f"{DEC_2023_INPUT_PATH}/GH_OPA_OPCS__20231211.ascii.redacted.tab"
dataset_snomed_path = f"{DEC_2023_INPUT_PATH}/GH_PC_Diagnosis_Problems_Procedures_mappedConceptIDs_ConfirmedOnly.csv"


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data").mkdir(parents=True, exist_ok=True)


# ### ICD10

# In[ ]:


dataset_icd1 = RawDataset(
    path=dataset_icd1_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


icd_col_map = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "ICD_Diagnosis_Cd": "code",
    "ICD_Diag_Desc": "term",
    "Activity_date": "date"
}


# In[ ]:


processed_icd_1 = dataset_icd1.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=icd_col_map
)


# In[ ]:


processed_icd_1.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/APC_ICD_10.arrow")


# In[ ]:


processed_icd_1.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/APC_ICD_10_log.txt")


# In[ ]:


dataset_icd2 = RawDataset(
    path=dataset_icd2_path,
    dataset_type=DatasetType.BARTS_HEALTH.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


icd_col_map = {
    "PseudoNHS_2023_11_08": "nhs_number",
    "ICD_Diagnosis_Cd": "code",
    "ICD_Diag_Desc": "term",
    "Activity_date": "date"
}


# In[ ]:


processed_icd_2 = dataset_icd2.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=icd_col_map
)


# In[ ]:


processed_icd_2.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/OP_ICD_10.arrow")
processed_icd_2.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/OP_ICD_10_log.txt")


# In[ ]:


processed_icd_1.merge_with_dataset(processed_icd_2)
merged_icd = processed_icd_1.deduplicate()
merged_icd.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/merged_ICD.arrow")

for log in processed_icd_2.log:
    merged_icd.log.append(log)

merged_icd.log.sort()
merged_icd.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/merged_ICD_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = merged_icd.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_ICD.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_ICD_log.txt")


# ### OPCS

# In[ ]:


opcs_1 = RawDataset(path=dataset_opcs1_path, dataset_type=DatasetType.BARTS_HEALTH.value, coding_system=CodelistType.OPCS.value)


# In[ ]:


col_map = {
    "PseudoNHS_2023_11_08": "nhs_number",
    "OPCS_Proc_Cd": "code",
    "OPCS_Proc_Dt": "date"
}


# In[ ]:


processed_opcs1 = opcs_1.process_dataset(deduplication_options=deduplication_options, column_maps=col_map)


# In[ ]:


processed_opcs1.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/APC_OPCS.arrow")
processed_opcs1.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/APC_OPCS_log.txt")


# In[ ]:


opcs_2 = RawDataset(path=dataset_opcs2_path, dataset_type=DatasetType.BARTS_HEALTH.value, coding_system=CodelistType.OPCS.value)


# In[ ]:


processed_opcs2 = opcs_2.process_dataset(deduplication_options=deduplication_options, column_maps=col_map)


# In[ ]:


processed_opcs2.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/OP_OPCS.arrow")
processed_opcs2.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/OP_OPCS_log.txt")


# Merged

# In[ ]:


processed_opcs1.merge_with_dataset(processed_opcs2)
merged_opcs = processed_opcs1.deduplicate()
merged_opcs.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/merged_OPCS.arrow")

for log in processed_opcs2.log:
    merged_opcs.log.append(log)

merged_opcs.log.sort()
merged_opcs.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/merged_OPCS_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = merged_opcs.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_OPCS.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_OPCS_log.txt")


# ### SNOMED
# 
# This dataset is slightly different as it has already been merged together and saved on the TRE. I do not have the original files. SOme of the SNOMED codes have been saved in the CSV with scientific notation (i.e row 17728 has a SNOMED code 6.92851E+14. For now I have dropped these. I have dropped the `SNOMED_descriptionID` column here as this seems to be where many of the errors with scientific notation come in and since we don't use this anyway.

# In[ ]:


snomed_data = RawDataset(path=dataset_snomed_path, dataset_type=DatasetType.BARTS_HEALTH.value, coding_system=CodelistType.SNOMED.value)


# In[ ]:


col_map = {
    "PseudoNHS_2023_11_08": "nhs_number",
    "SNOMED_conceptID_mapped": "code",
    "date": "date",
    "term": "term"
}


# In[ ]:


processed_snomed = snomed_data.process_dataset(deduplication_options=deduplication_options, column_maps=col_map)


# In[ ]:


processed_snomed.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/dataset_SNOMED.arrow")
processed_snomed.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/processed_data/dataset_SNOMED_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_snomed.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_SNOMED.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_SNOMED_log.txt")


# ###### 2024_12 cut start

# ## 4th cut of data - September 2024 Data
# 
# Suitable files were released from quarantine following redacting of identifying data by DvH in APril 2025
# 
# Files:
# 
# ```
#         "RDE_MSDS_Diagnosis.ascii.redacted.tab",
#         "RDE_PC_DIAGNOSIS.ascii.redacted.tab",
#         "RDE_OP_DIAGNOSIS.ascii.redacted.tab",
#         "RDE_PC_PROBLEMS.ascii.redacted.tab",
#         "RDE_PC_PROCEDURES.ascii.redacted.tab",
#         "RDE_OPA_OPCS.ascii.redacted.tab",
#         "RDE_APC_OPCS.ascii.redacted.tab", 
#         "RDE_APC_DIAGNOSIS.ascii.redacted.tab", 
#         "RDE_ALL_PROCEDURES.ascii.redacted.tab",  
#  ```
# 
# Here we use a slightly different approach than previous cuts, this is because the file formats have changed slightly.  Therefore, we extract all data, deduplicate them and then segregate them by codeset.  We then can import into these into this pipeline's usual per codeset frames.

# In[ ]:


files = {
    file.split(".", 1)[0]:
    f"{INPUT_LOCATION}/2024_09_ResearchDataset/" 
    + file 
    for file in [
        "RDE_MSDS_Diagnosis.ascii.redacted.tab",
        "RDE_PC_DIAGNOSIS.ascii.redacted.tab",
        "RDE_OP_DIAGNOSIS.ascii.redacted.tab",
        "RDE_PC_PROBLEMS.ascii.redacted.tab",
        "RDE_PC_PROCEDURES.ascii.redacted.tab",
        "RDE_OPA_OPCS.ascii.redacted.tab",
        "RDE_APC_OPCS.ascii.redacted.tab", 
        "RDE_APC_DIAGNOSIS.ascii.redacted.tab", 
        "RDE_ALL_PROCEDURES.ascii.redacted.tab",  
    ]
}


# In[ ]:


provenance_enum = pl.Enum(files.keys())


# In[ ]:


codesets_enum = pl.Enum(CodelistType)


# In[ ]:


HASH_COLUMN = (
    pl.struct(
        [
            pl.col("pseudo_nhs_number"), 
            pl.col("date"),
            pl.col("codeset"),
            pl.col("original_code"),
            pl.col("original_term"),
        ]
    ).hash()
    .alias("hash")
)


# In[ ]:


TARGET_OUTPUT_COLUMNS = [
    pl.col("pseudo_nhs_number"), 
    pl.col("date"),
    pl.col("codeset"),
    pl.col("original_code"),
    pl.col("original_term"),
    pl.col("provenance"),
]


# In[ ]:


TARGET_OUTPUT_COLUMNS_WITH_HASH = TARGET_OUTPUT_COLUMNS + [pl.col("hash")]


# In[ ]:


OUTPUT_DATAFRAMES = {}


# ### 1. RDE_MSDS_Diagnosis
# 
# ```
# Rows: 1
# Columns: 8
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ DiagScheme           <str> '06'
# $ Diagnosis            <str> '10151000132103'
# $ DiagDate             <str> '00/00/0000 00:00'
# $ LocalFetalID         <str> None
# $ FetalOrder           <str> None
# $ SnomedCD             <str> '10151000132103'
# $ DiagDesc             <str> 'Viral fever'
# ```

# In[ ]:


OUTPUT_DATAFRAMES['rde_msds_diagnosis'] = (
    pl.scan_csv(
        files["RDE_MSDS_Diagnosis"],
        separator="\t",
        infer_schema=False,
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("DiagDate").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        pl.lit("SNOMED").cast(codesets_enum).alias("codeset"),
        pl.col("Diagnosis").alias("original_code"),  # SNOMED codes, can be cast to .cast(pl.Int64) succesfully.
        pl.col("DiagDesc").alias("original_term"),  # Description of diagnosis
        pl.lit("RDE_MSDS_Diagnosis").cast(provenance_enum).alias("provenance")
#         pl.col("DiagScheme"),  # always "6"
#         pl.col("LocalFetalID"),  # null throughout
#         pl.col("FetalOrder"),  # null throughout
#         pl.col("SnomedCD"),  # Duplicate of Diagnosis. verified with .filter(pl.col("Diagnosis") != pl.col("SnomedCD")): SNOMED codes, can be cast to .cast(pl.Int64) succesfully.

    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)
# rde_msds_diagnosis #.write_csv("rde_msds_diagnosis.csv")


# ### 2. RDE_PC_DIAGNOSIS
# 
# #### RDE_PC_DIAGNOSIS does not have codes, we could consider recovering these from a lookup table
# 
# ```
# Rows: 1
# Columns: 10
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ Diagnosis            <str> 'Positional vertigo'
# $ Confirmation         <str> 'Differential'
# $ DiagDt               <str> '00/00/0000 00:00'
# $ Classification       <str> '.'
# $ ClinService          <str> 'Non-Specified'
# $ DiagType             <str> None
# $ DiagCode             <str> None
# $ Vocab                <str> None
# $ Axis                 <str> None
# ```

# In[ ]:


OUTPUT_DATAFRAMES["rde_pc_diagnosis"] = (
    pl.scan_csv(
        files["RDE_PC_DIAGNOSIS"],
        separator="\t",
        infer_schema=False,
    )
    .filter(
        pl.col("Confirmation").eq("Confirmed"),
#         pl.col("DiagCode").is_not_null(),   
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("DiagDt").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        pl.lit("SNOMED").cast(codesets_enum).alias("codeset"),
        pl.col("DiagCode").alias("original_code"),
        pl.col("Diagnosis").alias("original_term"),
        pl.lit("RDE_PC_DIAGNOSIS").cast(provenance_enum).alias("provenance")

#         pl.col('Confirmation'),
#         pl.col('Classification'),
#         pl.col('ClinService'),
#         pl.col('DiagType'),
#         pl.col('Vocab'),
#         pl.col('Axis'),
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)


# ### 3. RDE_OP_DIAGNOSIS
# ```
# Rows: 1
# Columns: 7
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ ICD_Diagnosis_Num    <str> '1'
# $ ICD_Diagnosis_Cd     <str> 'Z349'
# $ ICD_Diag_Desc        <str> '"Supervision of normal pregnancy, unspecified"'
# $ NHS_Number           <str> '00/00/0000 00:00'
# $ Activity_date        <str> '00/00/0000'
# $ CDS_Activity_Dt      <str> None
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES["rde_op_diagnosis"] = (
    pl.scan_csv(
        files["RDE_OP_DIAGNOSIS"],
        separator="\t",
        infer_schema=False,
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("Activity_date").str.to_date(format="%d/%m/%Y").alias("date"),
        pl.lit("ICD10").cast(codesets_enum).alias("codeset"),
        pl.col("ICD_Diagnosis_Cd").alias("original_code"),
        pl.col("ICD_Diag_Desc").alias("original_term"),
        pl.lit("RDE_OP_DIAGNOSIS").cast(provenance_enum).alias("provenance"),
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)
# rde_op_diagnosis


# ### 4. RDE_PC_PROBLEMS
# 
# Some file corruption is present (in the original file), e.g. Confirmation includes things like `" severe with psychotic feature"`
# 
# These can be overcome by selecting only:
# 1. `Confirmation` == `Confirmed`
# 2. `Vocab` == `SNOMED CT`
# 
# 
# `Problem` is always the same as `Annot_Disp`
# 
# `ProbCode` can have trailing spaces.
# 
# ```
# Rows: 1
# Columns: 13
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ Problem              <str> 'SARS-CoV'
# $ Annot_Disp           <str> 'SARS-CoV'
# $ Confirmation         <str> 'Confirmed'
# $ Classification       <str> 'Infection Risk'
# $ OnsetDate            <str> '2020-12-15 00:00'
# $ StatusDate           <str> '2020-12-16 00:00'
# $ Stat_LifeCycle       <str> 'Active'
# $ LifeCycleCancReson   <str> None
# $ Vocab                <str> 'SNOMED CT'
# $ Axis                 <str> 'Organism'
# $ SecDesc              <str> None
# $ ProbCode             <str> '2537007017          '
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES["rde_pc_problems"] = (
    pl.scan_csv(
        files["RDE_PC_PROBLEMS"],
        separator="\t",
        infer_schema=False,
    )
    .filter(
        pl.col("Confirmation").eq("Confirmed"),
        pl.col("Vocab").eq("SNOMED CT")
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("OnsetDate").str.to_date(format="%Y-%m-%d %H:%M").alias("date"), # note hypens separating date elements
        pl.lit("SNOMED").cast(codesets_enum).alias("codeset"),
        pl.col("ProbCode").str.strip_chars().alias("original_code"),
        pl.col("Problem").alias("original_term"),
        pl.lit("RDE_PC_PROBLEMS").cast(provenance_enum).alias("provenance"),
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)
# rde_pc_problems


# ### 5. RDE_PC_PROCEDURES
# 
# Issues:
# 
# 1. 10,685 SNOMED CT codes are in scientific notation "at source" (i.e. unrecoverable)
# 2. Some `ProcType` (i.e. `original_code`) have spaces, e.g. `"1761631000000114    "`
# 3. Some `EncType` have spaces, e.g. `"Inpatient                                    "`
# 3. `Comment` is always `null`
# 
# 
# ```
# Rows: 1
# Columns: 11
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ AdmissionDT          <str> '00/00/0000 00:00'
# $ DischargeDT          <str> '00/00/0000 00:00'
# $ TreatmentFunc        <str> 'Interventional Cardiology'
# $ Specialty            <str> 'Cardiology'
# $ ProcDt               <str> '00/00/0000 00:00'
# $ ProcDetails          <str> 'Cardiac rehabilitation - phase 1'
# $ ProcCD               <str> '1489351015'
# $ ProcType             <str> 'SNOMED CT'
# $ EncType              <str> 'Inpatient                                    '
# $ Comment              <str> None
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES['rde_pc_procedures'] = (
    pl.scan_csv(
        [
            files["RDE_PC_PROCEDURES"],
        ],
        separator="\t"
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("ProcType")
        .str.replace("SNOMED CT", "SNOMED")
        .str.replace("OPCS4", "OPCS")
        .cast(codesets_enum)
        .alias("codeset"),
#         pl.col("AdmissionDT"),
#         pl.col("DischargeDT"),
#         pl.col("TreatmentFunc"),
#         pl.col("Specialty"),
        pl.col("ProcDt").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        pl.col("ProcCD").alias("original_code"),
        pl.col("ProcDetails").alias("original_term"),
        pl.lit("RDE_PC_PROCEDURES").cast(provenance_enum).alias("provenance")
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)

# rde_pc_procedures


# ### 6. RDE_OPA_OPCS
# 
# ```
# Rows: 1
# Columns: 7
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ OPCS_Proc_Num        <i64> 1
# $ OPCS_Proc_Scheme_Cd  <i64> 2
# $ OPCS_Proc_Cd         <str> 'D152'
# $ Proc_Desc            <str> 'Suction clearance of middle ear'
# $ OPCS_Proc_Dt         <str> '00/00/0000 00:00'
# $ CDS_Activity_Dt      <str> '00/00/0000'
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES["rde_opa_opcs"] = (
    pl.scan_csv(
        [
            files["RDE_OPA_OPCS"],
        ],
        separator="\t"
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("OPCS_Proc_Dt").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        pl.lit("OPCS").cast(codesets_enum).alias("codeset"),
        pl.col("OPCS_Proc_Cd").alias("original_code"),
        pl.col("Proc_Desc").alias("original_term"),
        pl.lit("RDE_OPA_OPCS").cast(provenance_enum).alias("provenance")
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)

# rde_opa_opcs


# ### 7. RDE_APC_OPCS
# 
# ```
# Rows: 1
# Columns: 8
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ OPCS_Proc_Num        <i64> 3
# $ OPCS_Proc_Scheme_Cd  <i64> 2
# $ OPCS_Proc_Cd         <str> 'Z943'
# $ Proc_Desc            <str> 'Left sided operation'
# $ OPCS_Proc_Dt         <str> '00/00/0000 00:00'
# $ Activity_date        <str> '00/00/0000 00:00'
# $ CDS_Activity_Dt      <str> '00/00/0000 00:00'
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES["rde_apc_opcs"] = (
    pl.scan_csv(
        [
            files["RDE_APC_OPCS"],
        ],
        separator="\t"
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("OPCS_Proc_Dt").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        pl.lit("OPCS").cast(codesets_enum).alias("codeset"),
        pl.col("OPCS_Proc_Cd").alias("original_code"),
        pl.col("Proc_Desc").alias("original_term"),
        pl.lit("RDE_APC_OPCS").cast(provenance_enum).alias("provenance")
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)

# rde_opa_opcs


# ### 8. RDE_APC_DIAGNOSIS
# 
# ```
# Rows: 1
# Columns: 6
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ ICD_Diagnosis_Num    <i64> 1
# $ ICD_Diagnosis_Cd     <str> 'D561'
# $ ICD_Diag_Desc        <str> 'Beta thalassaemia'
# $ Activity_date        <str> '00/00/0000 00:00'
# $ CDS_Activity_Dt      <str> '00/00/0000 00:00'
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES["rde_apc_diagnosis"] = (
    pl.scan_csv(
        [
            files["RDE_APC_DIAGNOSIS"],
        ],
        separator="\t"
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("Activity_date").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        pl.lit("ICD10").cast(codesets_enum).alias("codeset"),
        pl.col("ICD_Diagnosis_Cd").alias("original_code"),
        pl.col("ICD_Diag_Desc").alias("original_term"),
        pl.lit("RDE_APC_DIAGNOSIS").cast(provenance_enum).alias("provenance")
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)

# rde_apc_diagnosis


# ### 9. RDE_ALL_PROCEDURES
# 
# Issues:
# 
# 1. In the original file, some fields have a triple quote at the start and end (i.e. `"""<some_text>"""`) which completely throws the polars parser.  There are two options:
#     1. preprocess the file to remove all double-quotes
#     2. parse with `quote_char` set to `None` and correct fields post-hoc. **We chose this option.**
# 2. One date is in a different invalid format (`"1899-12-30 00:00"`, i.e. separated by hyphens not forward slashes).  We reject those 2 rows.
#     
# 
# 
# ```
# Rows: 1
# Columns: 6
# $ PseudoNHS_2024-07-10 <str> 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# $ Procedure_Code       <str> '1489351015'
# $ Catalogue            <str> 'SNOMED'
# $ Code_text            <str> 'Cardiac rehabilitation - phase 1'
# $ Procedure_note       <str> 'Cardiac rehabilitation - phase 1'
# $ Procedure_date       <str> '00/00/0000 00:00'
# ```
# 

# In[ ]:


OUTPUT_DATAFRAMES["rde_all_procedures"] = (
    pl.scan_csv(
        [
            files["RDE_ALL_PROCEDURES"],
        ],
        separator="\t",
        quote_char=None,
    )
    .filter(
        pl.col("Procedure_date").ne("1899-12-30 00:00")
    )
    .select(
        pl.col("PseudoNHS_2024-07-10").alias("pseudo_nhs_number"),
        pl.col("Procedure_date").str.to_date(format="%d/%m/%Y %H:%M").alias("date"),
        (
            pl.col("Catalogue")
            .str.replace("ICD10WHO", "ICD10")
            .str.replace("SNOMED CT", "SNOMED")
            .str.replace("OPCS4", "OPCS")
            .cast(codesets_enum)
        ).alias("codeset"),
        pl.col("Procedure_Code").alias("original_code"),
        pl.col("Code_text").str.replace_all('"+', '').alias("original_term"),
        pl.lit("RDE_ALL_PROCEDURES").cast(provenance_enum).alias("provenance")
    )
    .with_columns(
        HASH_COLUMN       
    )
    .unique("hash")
    .select(
        TARGET_OUTPUT_COLUMNS_WITH_HASH
    )
#     .collect()
)

# rde_all_procedures


# ### 10. Now concat all 9 lazyframes into a single dataframe

# In[ ]:


rde_all_all = pl.concat(OUTPUT_DATAFRAMES.values()).collect()


# #### De-duplicate concatenated 2024_09 cut

# In[ ]:


height_before = rde_all_all.shape[0]


# In[ ]:


rde_all_all = (
    rde_all_all
    .unique("hash")
)


# In[ ]:


height_after = rde_all_all.shape[0]


# In[ ]:


print(f"{height_before - height_after} rows removed by de-duplicating.")


# ## Now export this final concatenated deduplicated version
# 
# 

# In[ ]:


rde_all_all.write_ipc(
    AnyPath(
        PREPROCESSED_FILES_LOCATION,
        "2024_09_Barts_RDE_ALL.arrow"
    )
)


# In[ ]:


for codeset in codesets_enum.categories:
    (
        rde_all_all
        .filter(
            pl.col("codeset").eq(codeset),
            pl.col("date").is_not_null(),
        )
        .with_columns(
            # in order to comply wit tretools specifications, we have to recast "date" as a string
            pl.col("date").cast(pl.Utf8)
        )
        .write_ipc(
            AnyPath(
                PREPROCESSED_FILES_LOCATION,
                f"2024_09_Barts_RDE_{codeset}.arrow"
            )
        )
    )


# In[ ]:


## Ick, but works
for codeset in codesets_enum.categories:
    globals()[f"dataset_{codeset.lower()}_path"] = AnyPath(
                PREPROCESSED_FILES_LOCATION,
                f"2024_09_Barts_RDE_{codeset}.arrow"
            ).__fspath__()


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data").mkdir(parents=True, exist_ok=True)


# ### ICD10

# In[ ]:


dataset_icd = RawDataset(
    path=dataset_icd10_path, 
    dataset_type=DatasetType.BARTS_HEALTH.value, 
    coding_system=CodelistType.ICD10.value,
)


# In[ ]:


col_map = {
    "pseudo_nhs_number": "nhs_number",
    "original_code": "code",
    "original_term": "term",
    "date": "date"
}


# In[ ]:


processed_icd = dataset_icd.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_map
)


# In[ ]:


processed_icd.write_to_feather(f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data/ALL_ICD_10.arrow")


# In[ ]:


processed_icd.write_to_log(f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data/ALL_ICD_10_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_icd.remove_unrealistic_dates(date_start=date_start, 
                                                      date_end=date_end,
                                                      before_born=True,
                                                      demographic_dataset=demographics)
cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_ICD.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_ICD_log.txt")


# ### OPCS

# In[ ]:


opcs = RawDataset(
    path=dataset_opcs_path, 
    dataset_type=DatasetType.BARTS_HEALTH.value, 
    coding_system=CodelistType.OPCS.value
)


# In[ ]:


processed_opcs = opcs.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_map
)


# In[ ]:


processed_opcs.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data/ALL_OPCS.arrow")
processed_opcs.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data/ALL_OPCS_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_opcs.remove_unrealistic_dates(
    date_start=date_start, 
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_OPCS.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_OPCS_log.txt")


# ### SNOMED

# In[ ]:


snomed_data = RawDataset(
    path=dataset_snomed_path, 
    dataset_type=DatasetType.BARTS_HEALTH.value, 
    coding_system=CodelistType.SNOMED.value
)


# In[ ]:


processed_snomed = snomed_data.process_dataset(deduplication_options=deduplication_options, column_maps=col_map)


# In[ ]:


processed_snomed.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data/dataset_SNOMED.arrow")
processed_snomed.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/processed_data/dataset_SNOMED_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_snomed.remove_unrealistic_dates(
    date_start=date_start, 
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_SNOMED.arrow")
cleaned_dataset.write_to_log(
    f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_SNOMED_log.txt")


# ###### 2024_09 cut end

# ## Merging all the data together for Barts Health
# 
# Here we are merging all the data together per coding system (i.e. ICD10, SNOMED and OPCS).
# 
# ### ICD 10 codes
# 

# In[ ]:


march_2022_icd = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_ICD.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.ICD10.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_ICD_log.txt")


# In[ ]:


may_2023_icd = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_ICD.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.ICD10.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_ICD_log.txt")


# In[ ]:


dec_2023_icd = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_ICD.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.ICD10.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_ICD_log.txt")


# In[ ]:


sep_2024_icd = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_ICD.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.ICD10.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_ICD_log.txt")


# In[ ]:


march_2022_icd.merge_with_dataset(may_2023_icd)


# In[ ]:


march_2022_icd.merge_with_dataset(dec_2023_icd)


# In[ ]:


march_2022_icd.merge_with_dataset(sep_2024_icd)


# In[ ]:


dedup_icd = march_2022_icd.deduplicate()


# In[ ]:


for log in may_2023_icd.log:
    dedup_icd.log.append(log)

for log in dec_2023_icd.log:
    dedup_icd.log.append(log)
    
for log in sep_2024_icd.log:
    dedup_icd.log.append(log)

dedup_icd.log.sort()


# In[ ]:


dedup_icd.write_to_feather(f"{MEGADATA_BARTS_LOCATION}/merged_ICD.arrow")
dedup_icd.write_to_log(f"{MEGADATA_BARTS_LOCATION}/merged_ICD_log.txt")


# ### OPCS codes

# In[ ]:


march_2022_opcs = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_OPCS.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.OPCS.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/dataset_OPCS_log.txt")


# In[ ]:


may_2023_opcs = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_OPCS.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.OPCS.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_OPCS_log.txt")


# In[ ]:


dec_2023_opcs = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_OPCS.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.OPCS.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_OPCS_log.txt")


# In[ ]:


sep_2024_opcs = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_OPCS.arrow",
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.OPCS.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_OPCS_log.txt")


# In[ ]:


march_2022_opcs.merge_with_dataset(may_2023_opcs)


# In[ ]:


march_2022_opcs.merge_with_dataset(dec_2023_opcs)


# In[ ]:


march_2022_opcs.merge_with_dataset(sep_2024_opcs)


# In[ ]:


dedup_opcs = march_2022_opcs.deduplicate()


# In[ ]:


for log in may_2023_opcs.log:
    dedup_opcs.log.append(log)
    
for log in dec_2023_opcs.log:
    dedup_opcs.log.append(log)

for log in sep_2024_opcs.log:
    dedup_opcs.log.append(log)

dedup_opcs.log.sort()


# In[ ]:


dedup_opcs.write_to_feather(f"{MEGADATA_BARTS_LOCATION}/merged_OPCS.arrow")
dedup_opcs.write_to_log(f"{MEGADATA_BARTS_LOCATION}/merged_OPCS_log.txt")


# ### SNOMED codes

# In[ ]:


march_2022_snomed = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/merged_SNOMED.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.SNOMED.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/march_2022/clean_processed_data/merged_SNOMED_log.txt")


# In[ ]:


may_2023_snomed = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_SNOMED.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.SNOMED.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/may_2023/clean_processed_data/merged_SNOMED_log.txt")


# In[ ]:


dec_2023_snomed = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_SNOMED.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.SNOMED.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/dec_2023/clean_processed_data/merged_SNOMED_log.txt")


# In[ ]:


sep_2024_snomed = ProcessedDataset(path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_SNOMED.arrow", 
                                  dataset_type=DatasetType.BARTS_HEALTH.value, 
                                  coding_system=CodelistType.SNOMED.value, 
                                  log_path=f"{PROCESSED_DATASETS_BARTS_LOCATION}/sep_2024/clean_processed_data/merged_SNOMED_log.txt")


# In[ ]:


march_2022_snomed.merge_with_dataset(may_2023_snomed)


# In[ ]:


march_2022_snomed.merge_with_dataset(dec_2023_snomed)


# In[ ]:


sep_2024_snomed.data = (
    # We have to do a little madness here to comply with expected tretools input
    sep_2024_snomed.data
    .with_columns(
        pl.col("code")
        .cast(pl.Int64, strict=False)
    )
    .filter(
        pl.col("code").is_not_null()
    )
)
march_2022_snomed.merge_with_dataset(sep_2024_snomed)


# In[ ]:


dedup_snomed = march_2022_snomed.deduplicate()


# In[ ]:


for log in may_2023_snomed.log:
    dedup_snomed.log.append(log)
    
for log in dec_2023_snomed.log:
    dedup_snomed.log.append(log)

for log in sep_2024_snomed.log:
    dedup_snomed.log.append(log)
     
dedup_snomed.log.sort()


# In[ ]:


dedup_snomed.write_to_feather(f"{MEGADATA_BARTS_LOCATION}/merged_SNOMED.arrow")
dedup_snomed.write_to_log(f"{MEGADATA_BARTS_LOCATION}/merged_SNOMED_log.txt")


# ## Mapping SNOMED to ICD
# 
# We also going to map the SNOMED data to ICD10 and merge with the ICD10 datasets. We already saved the mapping file in the previous notebook. We will load this first.
# 
# Note added 06.12.2024: Caroline's mapping files are in her /red/ directory, if full path to these is not given, these steps will only work if run from Caroline's directory.  Modified to give full path to look-up tables.
# 
# 2025-04-14: mapping file is generated in notebook \#2 
# 
# Issues:
# 1. SHould we move the look-tables to /library-red/reference_files/
# 2. What is the source of these lookup tables?  When were they last updated?

# In[ ]:


snomed_data = ProcessedDataset(path=f"{MEGADATA_BARTS_LOCATION}/merged_SNOMED.arrow", 
                               dataset_type=DatasetType.BARTS_HEALTH.value, 
                               coding_system=CodelistType.SNOMED.value,
                               log_path=f"{MEGADATA_BARTS_LOCATION}/merged_SNOMED_log.txt"
                              )


# In[ ]:





# In[ ]:


mapped_data = snomed_data.map_snomed_to_icd(
    mapping_file=f"{MAPPING_FILES_LOCATION}/processed_mapping_file.csv",
    snomed_col="conceptId",
    icd_col="mapTarget"
)


# In[ ]:


final_dedup = mapped_data.deduplicate()


# In[ ]:


final_dedup.write_to_feather(f"{MEGADATA_BARTS_LOCATION}/final_mapped_snomed_to_icd.arrow")
final_dedup.write_to_log(f"{MEGADATA_BARTS_LOCATION}/final_mapped_snomed_to_icd_log.txt")


# Now we merged with the ICD codes

# In[ ]:


icd_data = ProcessedDataset(path=f"{MEGADATA_BARTS_LOCATION}/merged_ICD.arrow", 
                               dataset_type=DatasetType.BARTS_HEALTH.value, 
                               coding_system=CodelistType.ICD10.value,
                               log_path=f"{MEGADATA_BARTS_LOCATION}/merged_ICD_log.txt"
                              )


# In[ ]:


snomed_to_icd_data =  ProcessedDataset(path=f"{MEGADATA_BARTS_LOCATION}/final_mapped_snomed_to_icd.arrow", 
                               dataset_type=DatasetType.BARTS_HEALTH.value, 
                               coding_system=CodelistType.ICD10.value,
                               log_path=f"{MEGADATA_BARTS_LOCATION}/final_mapped_snomed_to_icd_log.txt"
                              )


# In[ ]:


icd_data.merge_with_dataset(snomed_to_icd_data)


# In[ ]:


merged_final = icd_data.deduplicate()


# In[ ]:


merged_final.write_to_feather(f"{MEGADATA_BARTS_LOCATION}/merged_and_mapped_icd.arrow")


# In[ ]:


for log in snomed_to_icd_data.log:
    merged_final.log.append(log)

merged_final.log.sort()

merged_final.write_to_log(f"{MEGADATA_BARTS_LOCATION}/merged_and_mapped_icd_log.txt")


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("4-process-datasets-bradford")


# In[ ]:





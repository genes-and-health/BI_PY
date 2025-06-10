#!/usr/bin/env python
# coding: utf-8

# # 4-process-datasets-bradford -- Plan
# This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as feather files (binary) with their log files where appropriate. 
# 
# In this notebook, we do the following:
# 
# 1) Process Bradford datasets - ICD10
# 2) Process Bradford dataset - OPCS
# 3) Process Bradford datasets - SNOMED
# 4) Map the SNOMED data to ICD
# 
# 
# ## Processing Bradford Datasets
# The Bradford datasets are in SNOMED, ICD10 and OPCS. There are 3 "cuts" of data representing 3 time periods when the data was made available. Each dataset comprises of multiple CSV files. 
# 
# We process the datasets in the following way:
# 1) Load the data for each "type" of coding system per file
# 2) Deduplicate and process
# 3) Save as arrow file and with log file. 
# 4) Then we add in the demographic data created by the first notebook and remove unreaslistic data
# 5) Save this "clean" data
# 
# Once we have done this:
# 1) Reload each dataset from file and merge with other datasets of the same type for a time period (i.e. the same cut of the data)
# 2) Deduplicate and merge all the log files together
# 3) Save
# 
# Finally merge all the same types of dataset together, deduplicate and save. 
# 

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


# ### Imports
# This is where imports from tretools are pulled in. 

# In[ ]:


from datetime import datetime
from cloudpathlib import AnyPath

from tretools.datasets.raw_dataset import RawDataset
from tretools.datasets.dataset_enums.dataset_types import DatasetType
from tretools.codelists.codelist_types import CodelistType
from tretools.datasets.demographic_dataset import DemographicDataset
from tretools.datasets.processed_dataset import ProcessedDataset


# In[ ]:


import pandas as pd # Why Pandas? 
# Maybe because at the time of writing polars has limited read_xlsx support?
# if we were to use polars for the .xlsx imports, then the `fastexcel` package must be installed:
# """
# ModuleNotFoundError: required package 'fastexcel' not found.
# """
# when using `import polars as pl` and `pl.read_excel(...)`


# In[ ]:


INPUT_FOLDER = (
    "/genesandhealth/library-red/genesandhealth/phenotypes_rawdata/"
    "DSA__BradfordTeachingHospitals_NHSFoundation_Trust"
)


# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# In[ ]:


PROCESSED_DATASETS_LOCATION = f"{ROOT_LOCATION}/{VERSION}/processed_datasets"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"
PREPROCESSED_FILES_LOCATION = f"{ROOT_LOCATION}/{VERSION}/preprocessed_files"
MAPPING_FILES_LOCATION =  f"{ROOT_LOCATION}/{VERSION}/mapping_files"


# In[ ]:


PROCESSED_DATASETS_BRADFORD_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/bradford"
MEGADATA_BRADFORD_LOCATION = f"{MEGADATA_LOCATION}/bradford"

AnyPath(PROCESSED_DATASETS_BRADFORD_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(MEGADATA_BRADFORD_LOCATION).mkdir(parents=True, exist_ok=True)


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


# In[ ]:


# 4th cut 2024_12
# No useable binary trait data in this cut
# dec_2024_path = f"{INPUT_FOLDER}/2024_12_BTHNFT"

# 3rd cut 2023_05
may_2023_path = f"{INPUT_FOLDER}/2023_05_BTHNFT"

# 2nd cut 2022_06
june_2022_path = f"{INPUT_FOLDER}/2022_06_BTHNFT"

# 1st cut 2021_02
feb_2021_path = f"{INPUT_FOLDER}/2021_02_BTHFT_ICD10_OPCS"


# Load data and transform it
# 

# In[ ]:


demographic_file_path = f"{PROCESSED_DATASETS_LOCATION}/demographics/clean_demographics.arrow"


# In[ ]:


date_start=datetime.strptime("1910-01-01", "%Y-%m-%d")
date_end=datetime.today() # Was hardcoded as 2024-01-29.
demographics = DemographicDataset(path=demographic_file_path)


# In[ ]:


deduplication_options = ['nhs_number', 'code', 'date']


# ### 1st cut of data - Feb 2021 Data
# 
# In this first set of data we have 1 ICD file and 1 OPCS file. 
# 
# We do need to do some pre-processsing with these datasets as they are saved in Excel. This step will be removed with the next update of `tretools` but is included here for speed. I am doing this pre-processing step in pandas for now as I am just reading in the file and saving as a CSV, and polars is missing a library. This has been requested to the TRE team. 

# **Preprocessing**

# In[ ]:


# PREPROCESSED_DATASETS_PATH = f"{OUTPUT_FOLDER}/bradford/preprocessed_datasets/feb_2021"
# AnyPath(PREPROCESSED_DATASETS_PATH).mkdir(parents=True, exist_ok=True)


# In[ ]:


icd_pre_data = pd.read_excel(f"{feb_2021_path}/icd10_bfs_1578_2021-02-02_deident.xlsx")


# In[ ]:


icd_pre_data.to_csv(f"{PREPROCESSED_FILES_LOCATION}/Brad_icd_cut_1.csv")


# In[ ]:


opcs_pre_data = pd.read_excel(f"{feb_2021_path}/opcs_bfd_1578_2021-02-02_deident.xlsx")


# In[ ]:


opcs_pre_data.to_csv(f"{PREPROCESSED_FILES_LOCATION}/Brad_opcs_cut_1.csv")


# **ICD Processing**

# In[ ]:


FEB_2021_FOLDER = f"{PROCESSED_DATASETS_BRADFORD_LOCATION}/feb_2021/processed_data"
AnyPath(FEB_2021_FOLDER).mkdir(parents=True, exist_ok=True)
FEB_2021_CLEAN_FOLDER = f"{PROCESSED_DATASETS_BRADFORD_LOCATION}/feb_2021/clean_processed_data"
AnyPath(FEB_2021_CLEAN_FOLDER).mkdir(parents=True, exist_ok=True)


# In[ ]:


icd_data = RawDataset(path=f"{PREPROCESSED_FILES_LOCATION}/Brad_icd_cut_1.csv", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.ICD10.value)


# In[ ]:


col_maps = {
    "pseudonhs": "nhs_number",
    "icd10_code": "code",
    "icd10_name": "term",
    "episode_start_date": "date"
}


# In[ ]:


processed_icd_data = icd_data.process_dataset(deduplication_options=deduplication_options, column_maps=col_maps)


# In[ ]:


processed_icd_data.write_to_feather(
    f"{FEB_2021_FOLDER}/icd.arrow")
processed_icd_data.write_to_log(
    f"{FEB_2021_FOLDER}/icd_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_icd_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)


# In[ ]:


cleaned_dataset.write_to_feather(f"{FEB_2021_CLEAN_FOLDER}/icd.arrow")
cleaned_dataset.write_to_log(f"{FEB_2021_CLEAN_FOLDER}/icd_log.txt")


# **OPCS Processing**
# 
# Note here I have taken `episode_start_date` as the date, rather than `procedure` date. 

# In[ ]:


opcs_data = RawDataset(path=f"{PREPROCESSED_FILES_LOCATION}/Brad_opcs_cut_1.csv", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.OPCS.value)


# In[ ]:


col_maps = {
    "pseudonhs": "nhs_number",
    "opcs_code": "code",
    "opcs_procedure_description": "term",
    "episode_start_date": "date"
}


# In[ ]:


processed_opcs_data = opcs_data.process_dataset(deduplication_options=deduplication_options, column_maps=col_maps)


# In[ ]:


processed_opcs_data.write_to_feather(f"{FEB_2021_FOLDER}/opcs.arrow")
processed_opcs_data.write_to_log(f"{FEB_2021_FOLDER}/opcs_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_opcs_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{FEB_2021_CLEAN_FOLDER}/opcs.arrow")
cleaned_dataset.write_to_log(f"{FEB_2021_CLEAN_FOLDER}/opcs_log.txt")


# ### 2nd cut of data - June 2022 Data
# 
# In this second set of data we have 1 ICD file, 1 OPCS file and 2 SNOMED files (diagnosis and problems).  

# In[ ]:


JUNE_2022_FOLDER = f"{PROCESSED_DATASETS_BRADFORD_LOCATION}/june_2022/processed_data"
JUNE_2022_CLEAN_FOLDER = f"{PROCESSED_DATASETS_BRADFORD_LOCATION}/june_2022/cleaned_processed_data"
AnyPath(JUNE_2022_FOLDER).mkdir(parents=True, exist_ok=True)
AnyPath(JUNE_2022_CLEAN_FOLDER).mkdir(parents=True, exist_ok=True)


# **ICD Processing**

# In[ ]:


icd_data = RawDataset(path=f"{june_2022_path}/1578_gh_diagnoses_2022-06-10_redacted.tsv", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.ICD10.value)


# In[ ]:


col_maps = {
    "PseudoNHS": "nhs_number",
    "icd10_code": "code",
    "icd10_name": "term",
    "episode_start_date": "date"
}


# In[ ]:


processed_icd_data = icd_data.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_icd_data.write_to_feather(f"{JUNE_2022_FOLDER}/icd.arrow")
processed_icd_data.write_to_log(f"{JUNE_2022_FOLDER}/icd_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_icd_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{JUNE_2022_CLEAN_FOLDER}/icd.arrow")
cleaned_dataset.write_to_log(f"{JUNE_2022_CLEAN_FOLDER}/icd_log.txt")


# **OPCS Processing**

# In[ ]:


opcs_data = RawDataset(path=f"{june_2022_path}/1578_gh_procedures_2022-06-10_redacted.tsv", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.OPCS.value)


# In[ ]:


col_maps = {
    "PseudoNHS": "nhs_number",
    "opcs_code": "code",
    "opcs_procedure_description": "term",
    "episode_start_date": "date"
}


# In[ ]:


processed_opcs_data = opcs_data.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_opcs_data.write_to_feather(f"{JUNE_2022_FOLDER}/opcs.arrow")
processed_opcs_data.write_to_log(f"{JUNE_2022_FOLDER}/opcs_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_opcs_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{JUNE_2022_CLEAN_FOLDER}/opcs.arrow")
cleaned_dataset.write_to_log(f"{JUNE_2022_CLEAN_FOLDER}/opcs_log.txt")


# **SNOMED diagnosis**

# In[ ]:


diagnosis_data = RawDataset(path=f"{june_2022_path}/bradford_cerner_diagnoses_with_conceptIDs_14July2022edits_redacted.tsv", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.SNOMED.value)


# In[ ]:


col_maps = {
    "PseudoNHS": "nhs_number",
    "conceptId": "code",
    "term": "term",
    "date_of_diagnosis_entry": "date"
}


# In[ ]:


processed_diagnosis_data = diagnosis_data.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_diagnosis_data.write_to_feather(f"{JUNE_2022_FOLDER}/snomed_diagnosis.arrow")
processed_diagnosis_data.write_to_log(f"{JUNE_2022_FOLDER}/snomed_diagnosis_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_diagnosis_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{JUNE_2022_CLEAN_FOLDER}/snomed_diagnosis.arrow")
cleaned_dataset.write_to_log(f"{JUNE_2022_CLEAN_FOLDER}/snomed_diagnosis_log.txt")


# **SNOMED Problems**

# In[ ]:


prob_data = RawDataset(path=f"{june_2022_path}/bradford_cerner_problems_with_conceptIDs_14July2022edits_redacted.tsv", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.SNOMED.value)


# In[ ]:


col_maps = {
    "PseudoNHS": "nhs_number",
    "conceptId": "code",
    "term": "term",
    "date_of_problem_entry": "date"
}


# In[ ]:


processed_prob_data = prob_data.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_prob_data.write_to_feather(f"{JUNE_2022_FOLDER}/snomed_prob.arrow")
processed_prob_data.write_to_log(f"{JUNE_2022_FOLDER}/snomed_prob_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_prob_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{JUNE_2022_CLEAN_FOLDER}/snomed_prob.arrow")
cleaned_dataset.write_to_log(f"{JUNE_2022_CLEAN_FOLDER}/snomed_prob_log.txt")


# **Merged data**
# 
# Now we are going to merge the SNOMED datasets together. We are loading these from file to ensure they are loaded correctly. 

# In[ ]:


prob_data = ProcessedDataset(path=f"{JUNE_2022_CLEAN_FOLDER}/snomed_prob.arrow", 
                             dataset_type=DatasetType.BRADFORD.value,
                             coding_system=CodelistType.SNOMED.value, 
                             log_path=f"{JUNE_2022_CLEAN_FOLDER}/snomed_prob_log.txt")


# In[ ]:


diag_data = ProcessedDataset(path=f"{JUNE_2022_CLEAN_FOLDER}/snomed_diagnosis.arrow", 
                             dataset_type=DatasetType.BRADFORD.value,
                             coding_system=CodelistType.SNOMED.value, 
                             log_path=f"{JUNE_2022_CLEAN_FOLDER}/snomed_diagnosis_log.txt")


# In[ ]:


prob_data.merge_with_dataset(diag_data)


# In[ ]:


dedup_data = prob_data.deduplicate()


# In[ ]:


for log in diag_data.log:
    dedup_data.log.append(log)

dedup_data.log.sort()


# In[ ]:


dedup_data.write_to_feather(f"{JUNE_2022_CLEAN_FOLDER}/snomed_merged.arrow")


# In[ ]:


dedup_data.write_to_log(f"{JUNE_2022_CLEAN_FOLDER}/snomed_merged_log.txt")


# ### 3rd cut of data - May 2023 Data
# 
# In this 3rd set of data we have 1 ICD file, 1 OPCS file and 2 SNOMED files (diagnosis and problems).  Unlike the 1st cut of the data (excel) and the 2nd cut (tsv), these files are in tab files. 

# In[ ]:


MAY_2023_CLEAN_FOLDER = f"{PROCESSED_DATASETS_BRADFORD_LOCATION}/may_2023/clean_processed_data"
AnyPath(MAY_2023_CLEAN_FOLDER).mkdir(parents=True, exist_ok=True)
MAY_2023_FOLDER = f"{PROCESSED_DATASETS_BRADFORD_LOCATION}/may_2023/processed_data"
AnyPath(MAY_2023_FOLDER).mkdir(parents=True, exist_ok=True)


# **ICD Processing**

# In[ ]:


icd_data = RawDataset(path=f"{may_2023_path}/1578_gh_diagnoses_2023-06-09.ascii.redacted.tab", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.ICD10.value)


# In[ ]:


col_maps = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "icd10_code": "code",
    "icd10_name": "term",
    "episode_start_date": "date"
}


# In[ ]:


processed_icd_data = icd_data.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_icd_data.write_to_feather(f"{MAY_2023_FOLDER}/icd.arrow")
processed_icd_data.write_to_log(f"{MAY_2023_FOLDER}/icd_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_icd_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{MAY_2023_CLEAN_FOLDER}/icd.arrow")
cleaned_dataset.write_to_log(f"{MAY_2023_CLEAN_FOLDER}/icd_log.txt")


# **OPCS Processing**

# In[ ]:


opcs_data = RawDataset(path=f"{may_2023_path}/1578_gh_procedures_2023-06-09.ascii.redacted.tab", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.OPCS.value)


# In[ ]:


col_maps = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "opcs_code": "code",
    "opcs_procedure_description": "term",
    "episode_start_date": "date"
}


# In[ ]:


processed_opcs_data = opcs_data.process_dataset(deduplication_options, col_maps)
processed_opcs_data.write_to_feather(f"{MAY_2023_FOLDER}/opcs.arrow")
processed_opcs_data.write_to_log(f"{MAY_2023_FOLDER}/opcs_log.txt")


# In[ ]:


cleaned_dataset = processed_opcs_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{MAY_2023_CLEAN_FOLDER}/opcs.arrow")
cleaned_dataset.write_to_log(f"{MAY_2023_CLEAN_FOLDER}/opcs_log.txt")


# **SNOMED Diagnosis Processing**

# In[ ]:


diag = RawDataset(path=f"{may_2023_path}/1578_gh_cerner_diagnoses_2023-06-09.ascii.redacted.tab", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.SNOMED.value)


# In[ ]:


col_maps = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "DIAGNOSIS_ID": "code",
    "CONCEPT_NAME": "term",
    "date_of_diagnosis_entry": "date"
}


# In[ ]:


processed_diag = diag.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_diag.write_to_feather(f"{MAY_2023_FOLDER}/snomed_diagnosis.arrow")
processed_diag.write_to_log(f"{MAY_2023_FOLDER}/snomed_diagnosis_log.txt")


# In[ ]:


cleaned_dataset = processed_diag.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{MAY_2023_CLEAN_FOLDER}/snomed_diagnosis.arrow")
cleaned_dataset.write_to_log(f"{MAY_2023_CLEAN_FOLDER}/snomed_diagnosis_log.txt")


# **SNOMED Problems Processing**

# In[ ]:


prob = RawDataset(path=f"{may_2023_path}/1578_gh_cerner_problems_2023-06-09.ascii.redacted.tab", 
                      dataset_type=DatasetType.BRADFORD.value, 
                      coding_system=CodelistType.SNOMED.value)


# In[ ]:


col_maps = {
    "PseudoNHS_2023_04_24": "nhs_number",
    "PROBLEM_ID": "code",
    "problem_description": "term",
    "date_of_problem_entry": "date"
}


# In[ ]:


processed_prob = prob.process_dataset(deduplication_options, col_maps)


# In[ ]:


processed_prob.write_to_feather(f"{MAY_2023_FOLDER}/snomed_problems.arrow")
processed_prob.write_to_log(f"{MAY_2023_FOLDER}/snomed_problems_log.txt")


# In[ ]:


cleaned_dataset = processed_prob.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{MAY_2023_CLEAN_FOLDER}/snomed_problems.arrow")
cleaned_dataset.write_to_log(f"{MAY_2023_CLEAN_FOLDER}/snomed_problems_log.txt")


# **Merged data**
# 
# Now we are going to merge the SNOMED datasets together. We are loading these from file to ensure they are loaded correctly. 

# In[ ]:


prob_data = ProcessedDataset(path=f"{MAY_2023_CLEAN_FOLDER}/snomed_problems.arrow", 
                             dataset_type=DatasetType.BRADFORD.value,
                             coding_system=CodelistType.SNOMED.value, 
                             log_path=f"{MAY_2023_CLEAN_FOLDER}/snomed_problems_log.txt")


# In[ ]:


diag_data = ProcessedDataset(path=f"{MAY_2023_CLEAN_FOLDER}/snomed_diagnosis.arrow", 
                             dataset_type=DatasetType.BRADFORD.value,
                             coding_system=CodelistType.SNOMED.value, 
                             log_path=f"{MAY_2023_CLEAN_FOLDER}/snomed_diagnosis_log.txt")


# In[ ]:


prob_data.merge_with_dataset(diag_data)


# In[ ]:


dedup_data = prob_data.deduplicate()


# In[ ]:


for log in diag_data.log:
    dedup_data.log.append(log)

dedup_data.log.sort()


# In[ ]:


dedup_data.write_to_feather(f"{MAY_2023_CLEAN_FOLDER}/snomed_merged.arrow")
dedup_data.write_to_log(f"{MAY_2023_CLEAN_FOLDER}/snomed_merged_log.txt")


# ### 4th cut of data - Dec 2024 Data
# 
# At present, no useable binary trait data in this cut.  All cells pretaining to this cut are "raw"

# ###### 4th Cut end

# ## Merge all the data together of the same type
# 
# We are merging all the data together:
# 
# - ICD10 data
# - OPCS data
# - SNOMED data
# 
# This is across all the data cuts. 

# **ICD10 Dataset**
# 
# We are merging all the datasets together. We will load the data from file to start. 

# In[ ]:


icd_1 = ProcessedDataset(path=f"{FEB_2021_CLEAN_FOLDER}/icd.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{FEB_2021_CLEAN_FOLDER}/icd_log.txt")


# In[ ]:


icd_2 = ProcessedDataset(path=f"{JUNE_2022_CLEAN_FOLDER}/icd.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{JUNE_2022_CLEAN_FOLDER}/icd_log.txt")


# In[ ]:


icd_3 = ProcessedDataset(path=f"{MAY_2023_CLEAN_FOLDER}/icd.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{MAY_2023_CLEAN_FOLDER}/icd_log.txt")


# In[ ]:


icd_1.merge_with_dataset(icd_2)


# In[ ]:


icd_1.merge_with_dataset(icd_3)


# In[ ]:


dedup = icd_1.deduplicate()


# In[ ]:


for log in icd_2.log:
    dedup.log.append(log)
for log in icd_3.log:
    dedup.log.append(log)
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_BRADFORD_LOCATION}/icd.arrow")
dedup.write_to_log(f"{MEGADATA_BRADFORD_LOCATION}/icd_log.txt")


# **OPCS Dataset**
# 
# We are merging all the datasets together. We will load the data from file to start. 

# In[ ]:


opcs_1 = ProcessedDataset(path=f"{FEB_2021_CLEAN_FOLDER}/opcs.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{FEB_2021_CLEAN_FOLDER}/opcs_log.txt")


# In[ ]:


opcs_2 = ProcessedDataset(path=f"{JUNE_2022_CLEAN_FOLDER}/opcs.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{JUNE_2022_CLEAN_FOLDER}/opcs_log.txt")


# In[ ]:


opcs_3 = ProcessedDataset(path=f"{MAY_2023_CLEAN_FOLDER}/opcs.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{MAY_2023_CLEAN_FOLDER}/opcs_log.txt")


# In[ ]:


opcs_1.merge_with_dataset(opcs_2)
opcs_1.merge_with_dataset(opcs_3)


# In[ ]:


dedup = opcs_1.deduplicate()


# In[ ]:


for log in opcs_2.log:
    dedup.log.append(log)
for log in opcs_3.log:
    dedup.log.append(log)
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_BRADFORD_LOCATION}/opcs.arrow")
dedup.write_to_log(f"{MEGADATA_BRADFORD_LOCATION}/opcs_log.txt")


# **SNOMED Dataset**
# 
# We are merging all the datasets together. We will load the data from file to start. 

# In[ ]:


snomed_1 = ProcessedDataset(path=f"{JUNE_2022_CLEAN_FOLDER}/snomed_merged.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{JUNE_2022_CLEAN_FOLDER}/snomed_merged_log.txt")
snomed_2 = ProcessedDataset(path=f"{MAY_2023_CLEAN_FOLDER}/snomed_merged.arrow", 
                         dataset_type=DatasetType.BRADFORD.value, 
                         coding_system=CodelistType.ICD10.value, 
                         log_path=f"{MAY_2023_CLEAN_FOLDER}/snomed_merged_log.txt")


# In[ ]:


snomed_1.merge_with_dataset(snomed_2)
dedup = snomed_1.deduplicate()


# In[ ]:


for log in snomed_2.log:
    dedup.log.append(log)
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_BRADFORD_LOCATION}/snomed.arrow")
dedup.write_to_log(f"{MEGADATA_BRADFORD_LOCATION}/snomed_log.txt")


# **Map SNOMED to ICD10** 
# 
# Load the data from saved SNOMED dataset and map it from the mapping file. 
# 
# Note added 06.12.2024: Caroline's mapping files are in her /red/ directory, if full path to these is not given, these steps will only work if run from Caroline's directory.  Modified to give full path to look-up tables.
# 
# Issues:
# 1. Should we move the look-tables to /library-red/reference_files/
# 2. What is the source of these lookup tables?  When were they last updated?

# In[ ]:


snomed_data = ProcessedDataset(path=f"{MEGADATA_BRADFORD_LOCATION}/snomed.arrow", 
                               dataset_type=DatasetType.BRADFORD.value, 
                               coding_system=CodelistType.SNOMED.value,
                               log_path=f"{MEGADATA_BRADFORD_LOCATION}/snomed_log.txt"
                              )


# In[ ]:


mapped_data = snomed_data.map_snomed_to_icd(
    mapping_file=f"{MAPPING_FILES_LOCATION}/processed_mapping_file.csv",
    snomed_col="conceptId",
    icd_col="mapTarget"
)


# In[ ]:


final_dedup = mapped_data.deduplicate()


# In[ ]:


for log in mapped_data.log:
    final_dedup.log.append(log)


# In[ ]:


final_dedup.log.sort()


# In[ ]:


final_dedup.write_to_feather(f"{MEGADATA_BRADFORD_LOCATION}/final_mapped_snomed_to_icd.arrow")
final_dedup.write_to_log(f"{MEGADATA_BRADFORD_LOCATION}/final_mapped_snomed_to_icd_log.txt")


# In[ ]:


final_dedup.coding_system = CodelistType.ICD10.value


# **Merge with the ICD data and deduplicate**

# In[ ]:


icd_data = ProcessedDataset(path=f"{MEGADATA_BRADFORD_LOCATION}/icd.arrow", 
                               dataset_type=DatasetType.BRADFORD.value, 
                               coding_system=CodelistType.ICD10.value,
                               log_path=f"{MEGADATA_BRADFORD_LOCATION}/icd_log.txt"
                              )


# In[ ]:


icd_data.merge_with_dataset(final_dedup)


# In[ ]:


merged_dedup = icd_data.deduplicate()


# In[ ]:


for log in icd_data.log:
    merged_dedup.log.append(log)


# In[ ]:


merged_dedup.log.sort()


# In[ ]:


merged_dedup.write_to_feather(f"{MEGADATA_BRADFORD_LOCATION}/merged_and_mapped.arrow")
merged_dedup.write_to_log(f"{MEGADATA_BRADFORD_LOCATION}/merged_and_mapped_log.txt")


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("5-process-datasets-nhs-digital")


# In[ ]:





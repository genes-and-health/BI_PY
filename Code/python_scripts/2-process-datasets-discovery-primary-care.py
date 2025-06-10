#!/usr/bin/env python
# coding: utf-8

# # 2-process-datasets-discovery-primary-care - Plan
# This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as feather files (binary) with their log files where appropriate. 
# 
# In this first notebook, we do the following:
# 
# 1) Process primary care datasets - SNOMED 
# 2) Map primary care datasets to ICD 
# 
# ## Processing Primary Care Datasets
# The primary care datasets are in SNOMED. There are 5 "cuts" of data representing 5 time periods when the data was made available. Each dataset comprises of at least two CSV files - a procedures and an observation file - but some have more. 
# 
# We process datasets in the following way:
# 1) Load data from the CSV in each dataset
# 2) Deduplicate on code, nhs number and date ==> this means the same SNOMED code on a different date is preserved as a separate event for now
# 3) Save this dataset in its own folder
# 4) Load the demographic dataset and use the remove unrealistic dates method to remove rows where the event took place before 1st Jan 1910, after today's date (5th Dec 2024) or before the patient was born. 
# 5) Save this dataset
# 
# We do this processs for observations and procedure csv files separately. Then we:
# 
# 1) Reload the cleaned and deduplicated observation dataset from processed file made in step 5 above. 
# 2) Reload the cleaned and deduplicated procedures dataset from processed file made in step 5 above. 
# 3) Merge observations and procedures datasets together
# 4) Deduplicate this merged dataset on code, nhs number and date
# 5) Save this merged processed dataset under the date of the original dataset
# 
# Finally once we have done this to all the 5 time cuts of the data, we do the following:
# 
# 1) Merge all the processed datasets together
# 2) Deduplicate this "megafile"
# 3) Save as feather file

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


# In[ ]:


from cloudpathlib import AnyPath
import polars as pl
from datetime import datetime


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


# ### Paths
# 
# Here we are defining the paths that are going to be key in this notebook, and where appropriate we are making folders to store the processed datasets in. 

# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# In[ ]:


INPUT_LOCATION = "/genesandhealth/library-red/genesandhealth/phenotypes_rawdata/DSA__Discovery_7CCGs"


# In[ ]:


PROCESSED_DATASETS_LOCATION = f"{ROOT_LOCATION}/{VERSION}/processed_datasets"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"
DEMOGRAPHICS_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/demographics" # Created in pipeline notebook #1


# In[ ]:


# 2025-04-16: mapping file imported from Caroline Morton .../red/ directory
MAPPING_FILES_LOCATION =  f"{ROOT_LOCATION}/{VERSION}/mapping_files"


# Here we are making the folders to save all the files:

# In[ ]:


PROCESSED_DATASETS_PRIMARY_CARE_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/primary_care"
MEGADATA_PRIMARY_CARE_LOCATION = f"{MEGADATA_LOCATION}/primary_care"

AnyPath(PROCESSED_DATASETS_PRIMARY_CARE_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(MEGADATA_PRIMARY_CARE_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(MAPPING_FILES_LOCATION).mkdir(parents=True, exist_ok=True)


# We create the paths to the dataset original folders

# In[ ]:


april_2022_path = f"{INPUT_LOCATION}/2022_04_Discovery"
dec_2022_path = f"{INPUT_LOCATION}/2022_12_Discovery"
jan_2023_path = f"{INPUT_LOCATION}/2023_01_Discovery"
mar_2023_path = f"{INPUT_LOCATION}/2023_03_Discovery"
nov_2023_path = f"{INPUT_LOCATION}/2023_11_Discovery"
july_2024_path = f"{INPUT_LOCATION}/2024_07_Discovery"
dec_2024_path = f"{INPUT_LOCATION}/2024_12_Discovery"


# In[ ]:


DEMOGRAPHICS_FILE_LOCATION = f"{DEMOGRAPHICS_LOCATION}/clean_demographics.arrow"


# ### Imports
# This is where imports from tretools are pulled in. 

# In[ ]:


from tretools.datasets.raw_dataset import RawDataset
from tretools.datasets.processed_dataset import ProcessedDataset
from tretools.datasets.dataset_enums.dataset_types import DatasetType
from tretools.codelists.codelist_types import CodelistType
from tretools.datasets.demographic_dataset import DemographicDataset


# ## Load the data and transform it
# 
# Here we are defining the common variables that we will need. We also load the demographics data file that was created in the first notebook so that we can run the remove the unrealistic dates method. 

# In[ ]:


deduplication_options = ['nhs_number', 'code', 'date']


# In[ ]:


col_maps = {
    "original_code": "code",
    "original_term": "term",
    "clinical_effective_date": "date",
    "pseudo_nhs_number": "nhs_number"
}


# In[ ]:


demographics = DemographicDataset(path=DEMOGRAPHICS_FILE_LOCATION)


# In[ ]:


date_start=datetime.strptime("1910-01-01", "%Y-%m-%d")
# set end date to today
date_end=datetime.today()


# ### 1st cut of data - April 2022 Data
# 
# There are 4 different files in this dataset which we will merge together. There are 4 different files because according to the README, the volunteers in Tower Hamlets, Waltham Forest, Newham and City and Hackney, were kept in one dataset (observations and procedure files) and Barking, Havering, Redbridge Outer East London CCGs were kept in a different dataset. For reference, I will call the first dataset (Tower Hamlets, Waltham Forest, Newham and City and Hackney) one and the other one two. 

# In[ ]:


dataset_one_apr_2022_observations_path = f"{april_2022_path}/GNH_thwfnech-phase2-outfiles_merge/GNH_thwfnech_observations_output_dataset_20220423.csv"
dataset_one_apr_2022_procedures_path = f"{april_2022_path}/GNH_thwfnech-phase2-outfiles_merge/GNH_thwfnech_procedure_req_output_dataset_20220424.csv"
dataset_two_apr_2022_observations_path = f"{april_2022_path}/GNH_bhr-phase2-outfiles_merge/GNH_bhr_observations_output_dataset_20220412.csv"
dataset_two_apr_2022_procedures_path = f"{april_2022_path}/GNH_bhr-phase2-outfiles_merge/GNH_bhr_procedure_req_output_dataset_20220412.csv"


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data").mkdir(parents=True, exist_ok=True)


# **Dataset one observations processing**
# 
# We load the data, process it by deduplicating on code, nhs number and date and save, with a log file. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_obs = RawDataset(\n    path=dataset_one_apr_2022_observations_path,\n    dataset_type=DatasetType.PRIMARY_CARE.value,\n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = (\n    dataset_one_obs\n    .process_dataset(\n        deduplication_options=deduplication_options,\n        column_maps=col_maps\n    )\n)\n')


# In[ ]:


processed_dataset_one_obs.write_to_feather(
    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_thwfnech_observations.arrow"
)


# In[ ]:


processed_dataset_one_obs.write_to_log(
    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_thwfnech_observations_log.txt"
)


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = (\n    processed_dataset_one_obs\n    .remove_unrealistic_dates(\n        date_start=date_start,\n        date_end=date_end,\n        before_born=True,\n        demographic_dataset=demographics\n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '(\n    cleaned_dataset_one_obs\n    .write_to_feather(\n        f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_thwfnech_observations.arrow"\n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '(\n    cleaned_dataset_one_obs\n    .write_to_log(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_thwfnech_observations_log.txt")\n)\n')


# **Dataset one procedures processing**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_proc = RawDataset(\n    path=dataset_one_apr_2022_procedures_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = dataset_one_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_thwfnech_procedures.arrow"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_thwfnech_procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = processed_dataset_one_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_thwfnech_procedures.arrow"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_thwfnech_procedures_log.txt"\n)\n')


# **Dataset two observations processing**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_two_obs = RawDataset(\n    path=dataset_two_apr_2022_observations_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs = dataset_two_obs.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_bhr_observations.arrow"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_bhr_observations_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_processed_dataset_two_obs = processed_dataset_two_obs.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_processed_dataset_two_obs.write_to_feather(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_bhr_observations.arrow")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_processed_dataset_two_obs.write_to_log(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_bhr_observations_log.txt")\n')


# **Dataset two procedure processing**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_two_proc = RawDataset(\n    path=dataset_two_apr_2022_procedures_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc = dataset_two_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_bhr_procedures.arrow"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_bhr_procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_processed_dataset_two_proc = processed_dataset_two_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_processed_dataset_two_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_bhr_procedures.arrow"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_processed_dataset_two_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_bhr_procedures_log.txt"\n)\n')


# ### Merge all the data from April 2022 together 
# 
# Now we merge all the datasets into one dataset. We are going to load these from file to ensure that we are loading the right files as sometimes we run cells in different orders and that can cause issues. 

# Load from file

# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_thwfnech_observations.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_thwfnech_observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_thwfnech_procedures.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_thwfnech_procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_bhr_observations.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_bhr_observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/clean_processed_data/GNH_bhr_procedures.arrow",\n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/april_2022/processed_data/GNH_bhr_procedures_log.txt"\n)\n')


# Merge all the data together

# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.merge_with_dataset(processed_dataset_one_proc)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.merge_with_dataset(processed_dataset_two_obs)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.merge_with_dataset(processed_dataset_two_proc)\n')


# Deduplicate the data

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data = processed_dataset_one_obs.deduplicate()\n')


# We now merge all the logs from the creation of the datasets into the log, and then sort by timestamps. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_one_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_two_obs.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_two_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.log.sort()\n')


# Writing the data and logs to files to allow permanent access

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_apr_2022.txt")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/april_22.arrow")\n')


# ### 2nd cut of data - Dec 2022 Data
# 
# There are 4 different files in this dataset which we will merge together. There are 4 different files because according to the README, the volunteers in Tower Hamlets, Waltham Forest, Newham and City and Hackney, were kept in one dataset (observations and procedure files) and Barking, Havering, Redbridge Outer East London CCGs were kept in a different dataset. For reference, I will call the first dataset (Tower Hamlets, Waltham Forest, Newham and City and Hackney) one and the other one two. 

# In[ ]:


dataset_one_dec_2022_observations_path = f"{dec_2022_path}/GNH_thwfnech-phase2-outfiles_merge/cohort_gh2_observations_output_dataset_20221207.csv"
dataset_one_dec_2022_procedures_path = f"{dec_2022_path}/GNH_thwfnech-phase2-outfiles_merge/cohort_gh2_procedure_req_output_dataset_20221208.csv"
dataset_two_dec_2022_observations_path = f"{dec_2022_path}/GNH_bhr-phase2-outfiles_merge/gh2_observations_dataset_20221207.csv"
dataset_two_dec_2022_procedures_path = f"{dec_2022_path}/GNH_bhr-phase2-outfiles_merge/gh2_procedure_req_20221207.csv"


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data").mkdir(parents=True, exist_ok=True)


# **Dataset one obs**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_obs = RawDataset(\n    path=dataset_one_dec_2022_observations_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = dataset_one_obs.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_thwfnech_observations.arrow"\n)\nprocessed_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_thwfnech_observations_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = (\n    processed_dataset_one_obs\n    .remove_unrealistic_dates(\n        date_start=date_start,\n        date_end=date_end,\n        before_born=True,\n        demographic_dataset=demographics\n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_observations.arrow"\n)\ncleaned_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_observations_log.txt"\n)\n')


# **Dataset one procedures**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_proc = RawDataset(\n    path=dataset_one_dec_2022_procedures_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = dataset_one_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_thwfnech_procedures.arrow"\n)\nprocessed_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_thwfnech_procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = processed_dataset_one_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_procedures.arrow"\n)\ncleaned_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_procedures_log.txt"\n)\n')


# **Dataset two obs**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_two_obs = RawDataset(\n    path=dataset_two_dec_2022_observations_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs = dataset_two_obs.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_bhr_observations.arrow"\n)\nprocessed_dataset_two_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/NH_bhr_observations_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_two_obs = (\n    processed_dataset_two_obs\n    .remove_unrealistic_dates(\n        date_start=date_start,\n        date_end=date_end,\n        before_born=True,\n        demographic_dataset=demographics\n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_two_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_observations.arrow")\ncleaned_dataset_two_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_observations_log.txt")\n')


# **Dataset two proc**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_two_proc = RawDataset(\n    path=dataset_two_dec_2022_procedures_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc = dataset_two_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_bhr_procedures.arrow"\n)\nprocessed_dataset_two_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/processed_data/GNH_bhr_procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_two_proc = processed_dataset_two_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_two_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_procedures.arrow"\n)\ncleaned_dataset_two_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_procedures_log.txt"\n)\n')


# Now we merge all the datasets into one. For this we will merge `dataset_one_proc`, `dataset_two_obs` and `dataset_two_proc` into `dataset_one_obs`. We will reload them from file to be safe. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_observations.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_procedures.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_thwfnech_procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_obs = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_observations.arrow",\n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_two_proc = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_procedures.arrow",\n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2022/clean_processed_data/GNH_bhr_procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.merge_with_dataset(processed_dataset_one_proc)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.merge_with_dataset(processed_dataset_two_obs)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.merge_with_dataset(processed_dataset_two_proc)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.log.append(f"{datetime.now()}: The merged datasets has {processed_dataset_one_obs.data.shape[0]} rows")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data = processed_dataset_one_obs.deduplicate()\n')


# We now merge all the logs from the creation of the datasets into the log, and then sort by timestamps. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_one_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_two_obs.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_two_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.log.sort()\n')


# Writing the data and logs to files to allow permanent access

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_dec_2022.txt")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/dec_22.arrow")\n')


# ### 3rd cut of data - Jan 2023 Data \[Corrupted, Skipped\]
# 
# 2025-04-14: In previous run of BI_PY, this note was present: "The data had an error in procedures and observations according to the readme so this has been removed." but (in heading)refered to Dec 2022 cut which is fine.  Jan 2023 cut is however corrupted and should be skipped. 
# 

# ### 4th cut of data - March 2023 Data
# 
# There are 2 different files - observations and procedures for this cut of data. 

# In[ ]:


dataset_one_march_2023_observations_path = f"{mar_2023_path}/gh3_observations.csv"
dataset_one_march_2023_procedures_path = f"{mar_2023_path}/gh3_procedure_req.csv"


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data").mkdir(parents=True, exist_ok=True)


# **Dataset one obs**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_obs = RawDataset(path=dataset_one_march_2023_observations_path, dataset_type=DatasetType.PRIMARY_CARE.value, coding_system=CodelistType.SNOMED.value)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = dataset_one_obs.process_dataset(deduplication_options=deduplication_options, column_maps=col_maps)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/processed_data/observations.arrow")\nprocessed_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/processed_data/observations_log.txt")\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = (\n    processed_dataset_one_obs.remove_unrealistic_dates(\n        date_start=date_start,\n        date_end=date_end,\n        before_born=True,\n        demographic_dataset=demographics\n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/observations.arrow")\ncleaned_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/observations_log.txt")\n')


# **Dataset one procedures**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_proc = RawDataset(path=dataset_one_march_2023_procedures_path, dataset_type=DatasetType.PRIMARY_CARE.value, coding_system=CodelistType.SNOMED.value)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = dataset_one_proc.process_dataset(deduplication_options=deduplication_options, column_maps=col_maps)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/processed_data/procedures.arrow"\n)\nprocessed_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/processed_data/procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = processed_dataset_one_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/procedures.arrow"\n)\ncleaned_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/procedures_log.txt"\n)\n')


# Now we merge all the datasets into one. For this we will merge `dataset_one_proc` into `dataset_one_obs`. We will load them file. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/observations.arrow", \n    coding_system=CodelistType.SNOMED.value,\n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/procedures.arrow", \n    coding_system=CodelistType.SNOMED.value,\n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/march_2023/clean_processed_data/procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs.merge_with_dataset(cleaned_dataset_one_proc)\n')


# We now merge all the logs from the creation of the datasets into the log, and then sort by timestamps. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data = cleaned_dataset_one_obs.deduplicate()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_one_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.log.sort()\n')


# Writing the data and logs to files to allow permanent access

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_march_23.txt")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/march_2023.arrow")\n')


# ### 5th cut of data - Nov 2023 Data
# 
# There are 2 different files - observations and procedures for this cut of data. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_nov_2023_observations_path = f"{nov_2023_path}/gh3_observations.csv"\ndataset_one_nov_2023_procedures_path = f"{nov_2023_path}/gh3_procedure_req.csv"\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/processed_data").mkdir(parents=True, exist_ok=True)\nAnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data").mkdir(parents=True, exist_ok=True)\n')


# **Dataset one obs**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_obs = RawDataset(\n    path=dataset_one_nov_2023_observations_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = dataset_one_obs.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/processed_data/observations.arrow"\n)\nprocessed_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/processed_data/observations_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = processed_dataset_one_obs.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/observations.arrow"\n)\ncleaned_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/observations_log.txt"\n)\n')


# **Dataset one procedures**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_proc = RawDataset(\n    path=dataset_one_nov_2023_procedures_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = dataset_one_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/processed_data/procedures.arrow"\n)\nprocessed_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/processed_data/procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = processed_dataset_one_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/procedures.arrow"\n)\ncleaned_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/procedures_log.txt"\n)\n')


# Now we merge all the datasets into one. For this we will merge `dataset_one_proc` into `dataset_one_obs`. We are loading them from file into the datasets. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'obs_data = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/observations.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'proc_data = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/procedures.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/nov_2023/clean_processed_data/procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'obs_data.merge_with_dataset(proc_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data = obs_data.deduplicate()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_one_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.log.sort()\n')


# Writing the data and logs to files to allow permanent access

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_nov_23.txt")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/nov_2023.arrow")\n')


# ### 6th cut of data - July 2024 Data
# 
# There are 2 different files - observations and procedures for this cut of data. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_jul_2024_observations_path = f"{july_2024_path}/gh3_observations.csv"\ndataset_one_jul_2024_procedures_path = f"{july_2024_path}/gh3_procedure_req.csv"\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/processed_data").mkdir(parents=True, exist_ok=True)\nAnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data").mkdir(parents=True, exist_ok=True)\n')


# **Dataset one obs**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_obs = RawDataset(\n    path=dataset_one_jul_2024_observations_path,\n    dataset_type=DatasetType.PRIMARY_CARE.value,\n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = dataset_one_obs.process_dataset(\n    deduplication_options=deduplication_options,\n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/processed_data/observations.arrow"\n)\nprocessed_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/processed_data/observations_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = processed_dataset_one_obs.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/observations.arrow"\n)\ncleaned_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/observations_log.txt"\n)\n')


# **Dataset one procedures**
# 
# 2025-04-14: incorrect file specified here; replaced from `dataset_one_nov_2023_procedures_path` to `dataset_one_jul_2024_procedures_path`

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_proc = RawDataset(\n    path=dataset_one_jul_2024_procedures_path, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = dataset_one_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/processed_data/procedures.arrow")\nprocessed_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/processed_data/procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = processed_dataset_one_proc.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/procedures.arrow"\n)\ncleaned_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/procedures_log.txt"\n)\n')


# Now we merge all the datasets into one. For this we will merge `dataset_one_proc` into `dataset_one_obs`. We are loading them from file into the datasets. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'obs_data = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/observations.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'proc_data = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/procedures.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/jul_2024/clean_processed_data/procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'obs_data.merge_with_dataset(proc_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data = obs_data.deduplicate()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_one_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.log.sort()\n')


# Writing the data and logs to files to allow permanent access

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_jul_24.txt")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/jul_2024.arrow")\n')


# ### 7th cut of data - December 2024 Data
# 
# There are 2 different files - observations and procedures for this cut of data. 

# In[ ]:


dataset_one_dec_2024_observations_path = f"{dec_2024_path}/gh3_observations.csv"
dataset_one_dec_2024_procedures_path = f"{dec_2024_path}/gh3_procedure_req.csv"


# In[ ]:


AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/processed_data").mkdir(parents=True, exist_ok=True)
AnyPath(f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data").mkdir(parents=True, exist_ok=True)


# **Dataset one obs**

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_obs = RawDataset(\n    path=dataset_one_dec_2024_observations_path,\n    dataset_type=DatasetType.PRIMARY_CARE.value,\n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs = dataset_one_obs.process_dataset(\n    deduplication_options=deduplication_options,\n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/processed_data/observations.arrow")\nprocessed_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/processed_data/observations_log.txt")\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs = processed_dataset_one_obs.remove_unrealistic_dates(\n    date_start=date_start,\n    date_end=date_end,\n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_obs.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/observations.arrow"\n)\ncleaned_dataset_one_obs.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/observations_log.txt"\n)\n')


# **Dataset one procedures**
# 
# 2025-04-14: incorrect file specified here; replaced from `dataset_one_nov_2023_procedures_path` to `dataset_one_jul_2024_procedures_path`

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dataset_one_proc = RawDataset(\n    path=dataset_one_dec_2024_procedures_path,\n    dataset_type=DatasetType.PRIMARY_CARE.value,\n    coding_system=CodelistType.SNOMED.value\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc = dataset_one_proc.process_dataset(\n    deduplication_options=deduplication_options, \n    column_maps=col_maps\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'processed_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/processed_data/procedures.arrow"\n)\nprocessed_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/processed_data/procedures_log.txt"\n)\n')


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 5th Dec 2024) and save this into a cleaned_processed_data folder

# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc = processed_dataset_one_proc.remove_unrealistic_dates(\n    date_start=date_start, \n    date_end=date_end, \n    before_born=True,\n    demographic_dataset=demographics\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'cleaned_dataset_one_proc.write_to_feather(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/procedures.arrow"\n)\ncleaned_dataset_one_proc.write_to_log(\n    f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/procedures_log.txt"\n)\n')


# Now we merge all the datasets into one. For this we will merge `dataset_one_proc` into `dataset_one_obs`. We are loading them from file into the datasets. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'obs_data = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/observations.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/observations_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'proc_data = ProcessedDataset(\n    path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/procedures.arrow", \n    coding_system=CodelistType.SNOMED.value, \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    log_path=f"{PROCESSED_DATASETS_PRIMARY_CARE_LOCATION}/dec_2024/clean_processed_data/procedures_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'obs_data.merge_with_dataset(proc_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data = obs_data.deduplicate()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in processed_dataset_one_proc.log:\n    deduplicated_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.log.sort()\n')


# Writing the data and logs to files to allow permanent access

# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_dec_24.txt")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'deduplicated_data.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/dec_2024.arrow")\n')


# ### Merge all the primary care datasets together
# 
# Here we are merging all the 6 datasets together (7 cuts but one invalid) and then deduplicate this. We will read in each cleaned, deduplicated file and merge them into each other. Once they are all merged, we will deduplicate them. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/april_22.arrow", \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value, \n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_apr_2022.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'dec_2022_data = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/dec_22.arrow", \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value, \n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_dec_2022.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'mar_2023_data = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/march_2023.arrow", \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value, \n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_march_23.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'nov_2023_data = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/nov_2023.arrow", \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value, \n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_nov_23.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'jul_2024_data = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/jul_2024.arrow",\n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value, \n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_jul_24.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'dec_2024_data = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/dec_2024.arrow", \n    dataset_type=DatasetType.PRIMARY_CARE.value, \n    coding_system=CodelistType.SNOMED.value, \n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/log_dec_24.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data.merge_with_dataset(dec_2022_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data.merge_with_dataset(mar_2023_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data.merge_with_dataset(nov_2023_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data.merge_with_dataset(jul_2024_data)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data.merge_with_dataset(dec_2024_data)\n')


# We then merge all the log files together, and sort based on timestamp.

# In[ ]:


get_ipython().run_cell_magic('time', '', 'for log in dec_2022_data.log:\n    apr_2022_data.log.append(log)\n    \nfor log in mar_2023_data.log:\n    apr_2022_data.log.append(log)\n        \nfor log in nov_2023_data.log:\n    apr_2022_data.log.append(log)\n    \nfor log in jul_2024_data.log:\n    apr_2022_data.log.append(log)\n    \nfor log in dec_2024_data.log:\n    apr_2022_data.log.append(log)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'apr_2022_data.log.sort()\n')


# Now we deduplicate and save the resulting file. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'final_dataset = apr_2022_data.deduplicate()\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'final_dataset.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/final_merged_data.arrow")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'final_dataset.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/final_log.txt")\n')


# ## Map SNOMED codes to ICD10
# 
# For the ICD10 3 digit binary traits, we want all the snomed codes (wherever possible) to be mapped to ICD10 codes. For this we need to load the mapping file, map the data and save as a new file. 
# 
# Our process for the mapping of SNOMED codes to ICD10 is:
# 1) Load merged dataset from feather - This contains all the primary care data deduplicated. They are SNOMED at this points. 
# 2) Load the CSV file that maps SNOMED to ICD10. See note 2 below. 
# 3) Map data to ICD10 and drop rows that do not map. i.e. no ICD10 code exists for that SNOMED code
# 4) Remove extra columns, and rename ICD10 column to code. Drop SNOMED code column. 
# 5) Deduplicate the data - This is needed as there are many SNOMED codes that map to just one ICD10 code as SNOMED is much more verbose. 
# 6) Save as feather file
# 
# NOTE 1:  
# The removal of unrealistic dates has already taken place on the data here so we do not need to repeat this step. 
# 
# NOTE 2: 
# Some of the long SNOMED codes that were created by the R code that generates the mapping file, saves them with scientific notation, i.e. a number with an E3 in it. We need to do a preprocessing step to load this data as strings (UTf.8) and then convert these to integrers. This is done in the preprocessing step below. 

# **Load the mapping file**
# 
# We are assigning a specific schema of strings to allow us to load the numbers with scientific notation. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'mapping_file = AnyPath(\n    "/genesandhealth/library-red/genesandhealth",\n    "phenotypes_curated/version008_2024_02",\n    "3digitICD10/snomed-to-icd-mapping/snomed_to_icd_map.tsv"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'schema = {\n    "conceptId": pl.Utf8,\n    "mapTarget": pl.Utf8,\n    "ICD10_3digit": pl.Utf8\n}\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'mapping_data = pl.read_csv(mapping_file, separator="\\t", schema=schema)\n')


# Here we are converting the strings to floats and then integers which will remove the scientific notation. 

# In[ ]:


get_ipython().run_cell_magic('time', '', "mapping_data = mapping_data.with_columns(pl.col('conceptId').cast(pl.Float64).cast(pl.Int64))\n")


# In[ ]:


get_ipython().run_cell_magic('time', '', 'mapping_data.write_csv(\n    AnyPath(\n        MAPPING_FILES_LOCATION,\n        "processed_mapping_file.csv"\n    )\n)\n')


# Now we actually do the mapping. We first need to load out feather file and log file. The feather file is the final merged file from above. We are loading from memory as doing this over a day. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'final_dataset = ProcessedDataset(\n    path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/final_merged_data.arrow",\n    dataset_type=DatasetType.PRIMARY_CARE.value,\n    coding_system=CodelistType.SNOMED.value,\n    log_path=f"{MEGADATA_PRIMARY_CARE_LOCATION}/final_log.txt"\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '\n# 2025-04-14: the .map_snomed_to_icd tretools function applies\n# an inner join, i.e, only snomed codes which exist both in\n# `final_dataset` and `mapping_file` are preserved\n# I.e. approx 4m rows kep from approx 66m row (nb. lot fever unique obvs)\n\nmapped_data = (\n    final_dataset\n    .map_snomed_to_icd(\n        mapping_file=f"{MAPPING_FILES_LOCATION}/processed_mapping_file.csv", \n        snomed_col="conceptId",\n        icd_col="mapTarget"\n    )\n)\n')


# We now do a final deduplicate. 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dedup = mapped_data.deduplicate()\n')


# Finally we write the merged and mapped dataset to feather and the log to a text file. 
# 
# **We do not need to 're-merge' this files with any other native ICD-10 dataframes, primary care data are only SNOMED** 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'dedup.write_to_feather(f"{MEGADATA_PRIMARY_CARE_LOCATION}/final_mapped_data.arrow")\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', 'dedup.write_to_log(f"{MEGADATA_PRIMARY_CARE_LOCATION}/final_mapped_log.txt")\n')


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("3-process-datasets-barts-health")


# In[ ]:





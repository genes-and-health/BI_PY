#!/usr/bin/env python
# coding: utf-8

# # 5-process-datasets-nhs-digital -- Plan
# This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as feather files (binary) with their log files where appropriate. 
# 
# In this notebook, we do the following:
# 
# 1) Process NHS digital datasets - ICD10
# 2) Merge all the data together
# 
# 
# ## Processing NHS Digital Datasets
# The NHS digital datasets are in ICD10. For now, we are only using the latest version. 
# 
# We process the datasets in the following way:
# 1) Load the dataset in turn
# 2) Expand the wide files into multirow files so that one person can have multiple rows. 
# 3) Deduplicate 
# 4) Save
# 
# We will then remove unrealistic dates with previously saved Demographic data from notebook 1, and save again. 
# 
# For some datasets, in particular the very wide datasets, there are a few issues which required some preprocessing. These steps will be removed in future where possible, when the custom library is updated. Where preprocessing has been required, a log file is created and the final preprocessed dataset is saved in a folder called preprocessing. 
# 
# **As of May 2025, we now include ECDS data**

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


# ### Imports
# 
# This is where imports from tretools are pulled in. 

# In[ ]:


from cloudpathlib import AnyPath
import polars as pl
from datetime import datetime

from tretools.datasets.raw_dataset import RawDataset
from tretools.datasets.dataset_enums.dataset_types import DatasetType
from tretools.codelists.codelist_types import CodelistType
from tretools.datasets.demographic_dataset import DemographicDataset
from tretools.datasets.processed_dataset import ProcessedDataset


# In[ ]:


# Override of tretools to accommodate NHSDIgital ECDS data

import pkg_resources
import json
def override_expand_cols_to_rows(self, hes_subtype: str, config_path: str = None):
        # check the dataset type
        if self.dataset_type != DatasetType.NHS_DIGITAL.value:
            raise NotImplementedError("This method is only implemented for NHS Digital datasets")

        # check if has overwritten config

        # if not, check default config
        if config_path is None:
            if hes_subtype == "ECDS":
                config = {
                    "nhs_number": "STUDY_ID",
                    "date": "ARRIVAL_DATE",
                    "column_to_expand": [
                        'CHIEF_COMPLAINT',
                        'COMORBIDITIES_1', 
                        'COMORBIDITIES_10',
                        'COMORBIDITIES_2',
                        'COMORBIDITIES_3',
                        'COMORBIDITIES_4',
                        'COMORBIDITIES_5',
                        'COMORBIDITIES_6',
                        'COMORBIDITIES_7',
                        'COMORBIDITIES_8',
                        'COMORBIDITIES_9',
                        'DIAGNOSIS_CODE_1',
                        'DIAGNOSIS_CODE_10',
                        'DIAGNOSIS_CODE_11',
                        'DIAGNOSIS_CODE_12',
                        'DIAGNOSIS_CODE_2',
                        'DIAGNOSIS_CODE_3',
                        'DIAGNOSIS_CODE_4',
                        'DIAGNOSIS_CODE_5',
                        'DIAGNOSIS_CODE_6',
                        'DIAGNOSIS_CODE_7',
                        'DIAGNOSIS_CODE_8',
                        'DIAGNOSIS_CODE_9',
                    ],
                }
            else:
                resource_path = "/usr/local/lib/python3.9/dist-packages/tretools/datasets/"
                if hes_subtype == "CIV_REG":
                    resource_path = resource_path + "configs/NHS_D/civ_reg.json"
                elif hes_subtype == "APC":
                    resource_path = resource_path + "configs/NHS_D/apc.json"
                elif hes_subtype == "OP":
                    resource_path = resource_path + "configs/NHS_D/op.json"
                # this loads the default config from the package files. We get these 
                # from the tretools package, which is installed in the environment
                # config_path = pkg_resources.resource_filename(__name__, resource_path)
                config_path = resource_path
                config = json.loads(open(config_path).read())

        # log the shape of the data
        self.log.append(f"{datetime.now()}: Data shape before expanding wide columns into rows: {self.data.shape}")

        # unpivot the data
        self.data = (
            self.data.lazy()
            .unpivot(
                on=config["column_to_expand"],
                value_name="code",
                index=[config["nhs_number"], config["date"]], 
            )
            .drop_nulls()
            .filter(pl.col("code") != "")
            .collect()
        )
        
        # drop the variable column
        self.data = self.data.drop("variable")
        self._standarise_column_names(
            column_maps={
                config["nhs_number"]: "nhs_number",
                config["date"]: "date",
                "code": "code"
            }
        )

        # log the action
        self.log.append(f"{datetime.now()}: Data shape after expanding wide columns into rows: {self.data.shape}")


# In[ ]:


RawDataset._expand_cols_to_rows = override_expand_cols_to_rows


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


# ## Paths

# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# In[ ]:


PROCESSED_DATASETS_LOCATION = f"{ROOT_LOCATION}/{VERSION}/processed_datasets"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"
PREPROCESSED_FILES_LOCATION = f"{ROOT_LOCATION}/{VERSION}/preprocessed_files"


# In[ ]:


PROCESSED_DATASETS_NHS_DIGITAL_LOCATION = f"{PROCESSED_DATASETS_LOCATION}/nhs_digital"
MEGADATA_NHS_DIGITAL_LOCATION = f"{MEGADATA_LOCATION}/nhs_digital"


# In[ ]:


#INPUT_FOLDER = "/genesandhealth/nhsdigital-sublicence-library-red/DSA__NHSDigitalNHSEngland"
INPUT_FOLDER = "/genesandhealth/nhsdigital-sublicence-red/DSA__NHSDigitalNHSEngland"


# In[ ]:


AnyPath(PROCESSED_DATASETS_NHS_DIGITAL_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(MEGADATA_NHS_DIGITAL_LOCATION).mkdir(parents=True, exist_ok=True)


# We create the paths to the dataset original folders

# In[ ]:


## New on 10.04.2025:
MAR_2025_INPUT_PATH = f"{INPUT_FOLDER}/2025_03"

## New on 06.12.2024:
OCT_2024_INPUT_PATH = f"{INPUT_FOLDER}/2024_10"

# Previous cuts:
JULY_2023_INPUT_PATH = f"{INPUT_FOLDER}/2023_07"
SEPT_2021_INPUT_PATH = f"{INPUT_FOLDER}/2021_09"



# ## Load the data and transform it

# In[ ]:


deduplication_options = ['nhs_number', 'code', 'date']


# In[ ]:


col_maps = {
    "nhs_number": "nhs_number",
    "code": "code",
    "date": "date"
}


# In[ ]:


demographic_file_path = f"{PROCESSED_DATASETS_LOCATION}/demographics/clean_demographics.arrow"


# In[ ]:


date_start=datetime.strptime("1910-01-01", "%Y-%m-%d")
# date_end=datetime.strptime("2024-01-29", "%Y-%m-%d")  #  MS commented out
date_end=datetime.today() #  MS happy to hear reasons why not.
demographics = DemographicDataset(path=demographic_file_path)


# ### 1st cut of data - Sept 2021 Data
# 
# In this first set of data, we have 4 different files. 
# 
# No ECDS data for this cut

# In[ ]:


SEP_2021_OUTPUT_PATH= f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/sept_2021/processed_data"
AnyPath(SEP_2021_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
SEP_2021_OUTPUT_CLEAN_PATH= f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/sept_2021/clean_processed_data"
AnyPath(SEP_2021_OUTPUT_CLEAN_PATH).mkdir(parents=True, exist_ok=True)


# In[ ]:


civreg_path = f"{SEPT_2021_INPUT_PATH}/FILE0138006_NIC338864_CIVREG_MORT_.txt"
ae_path = f"{SEPT_2021_INPUT_PATH}/NIC338864_HES_AE_all_2021_11_25.txt"
apc_path = f"{SEPT_2021_INPUT_PATH}/NIC338864_HES_APC_all_2021_11_25.txt"
op_path = f"{SEPT_2021_INPUT_PATH}/NIC338864_HES_OP_all_2021_11_25.txt"


# **Civ Registration Data**

# In[ ]:


civreg_data = RawDataset(
    path=civreg_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_civreg_data = civreg_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="CIV_REG"
)


# In[ ]:


processed_civreg_data.write_to_feather(f"{SEP_2021_OUTPUT_PATH}/civ_reg.arrow")


# In[ ]:


processed_civreg_data.write_to_log(f"{SEP_2021_OUTPUT_PATH}/civ_reg_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder. *MS note:* Previously end_date was hardcoded to 29th Jan 2024

# In[ ]:


cleaned_dataset = processed_civreg_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)


# In[ ]:


cleaned_dataset.write_to_feather(f"{SEP_2021_OUTPUT_CLEAN_PATH}/civ_reg.arrow")
cleaned_dataset.write_to_log(f"{SEP_2021_OUTPUT_CLEAN_PATH}/civ_reg_log.txt")


# **APC Data**

# In[ ]:


apc_data = RawDataset(
    path=apc_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_apc_data = apc_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="APC"
)


# In[ ]:


processed_apc_data.write_to_feather(f"{SEP_2021_OUTPUT_PATH}/apc.arrow")
processed_apc_data.write_to_log(f"{SEP_2021_OUTPUT_PATH}/apc_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after 29th Jan 2024) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_apc_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{SEP_2021_OUTPUT_CLEAN_PATH}/apc.arrow")
cleaned_dataset.write_to_log(f"{SEP_2021_OUTPUT_CLEAN_PATH}/apc_log.txt")


# **OP Data**

# Processing

# In[ ]:


op_data = RawDataset(
    path=op_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_op_data = op_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="OP"
)


# In[ ]:


processed_op_data.write_to_feather(f"{SEP_2021_OUTPUT_PATH}/op.arrow")
processed_op_data.write_to_log(f"{SEP_2021_OUTPUT_PATH}/op_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_op_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{SEP_2021_OUTPUT_CLEAN_PATH}/op.arrow")
cleaned_dataset.write_to_log(f"{SEP_2021_OUTPUT_CLEAN_PATH}/op_log.txt")


# ### Merge these data together
# 
# Now we merge all these datasets together and deduplicate. We load from file so that we can be assured that the correct dataset is loaded as sometimes the cells can be run manually in the wrong order. 

# In[ ]:


civ_data = ProcessedDataset(
    path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/civ_reg.arrow",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value,
    log_path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/civ_reg_log.txt"
)


# In[ ]:


apc_data = ProcessedDataset(
    path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/apc.arrow",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value,
    log_path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/apc_log.txt"
)


# In[ ]:


op_data = ProcessedDataset(
    path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/op.arrow",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value,
    log_path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/op_log.txt"
)


# Merge the files together

# In[ ]:


civ_data.merge_with_dataset(apc_data)
civ_data.merge_with_dataset(op_data)


# Deduplicate on code, date and nhs number

# In[ ]:


dedup = civ_data.deduplicate()


# Add all the logs together and sort. 

# In[ ]:


for log in apc_data.log:
    dedup.log.append(log)
    
for log in op_data.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{SEP_2021_OUTPUT_CLEAN_PATH}/merged_data.arrow")
dedup.write_to_log(f"{SEP_2021_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# ### 2nd cut of data - July 2023 Data
# 
# In this second set of data we have 5 files. 
# 
# Note added 06.12.2024: Seems like previously some files pre-concatenated (.e.g. NIC338864_HES_APC_1997to2023.txt) these are no longer available. Possibly owing to missing 2022 data.

# In[ ]:


JULY_2023_OUTPUT_PATH = f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/july_2023/processed_data" # was previously named `july_path`
AnyPath(JULY_2023_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
JULY_2023_OUTPUT_CLEAN_PATH = f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/july_2023/clean_processed_data"
AnyPath(JULY_2023_OUTPUT_CLEAN_PATH).mkdir(parents=True, exist_ok=True)


# In[ ]:


civreg_path = f"{JULY_2023_INPUT_PATH}/FILE0179979_NIC338864_CIVREG_MORT_.txt"
apc_path = f"{JULY_2023_INPUT_PATH}/HES" 
op_path = f"{JULY_2023_INPUT_PATH}/HES" 
cancer_path = f"{JULY_2023_INPUT_PATH}/FILE0190666_NIC338864_Cancer_.txt"
ecds_path = f"{JULY_2023_INPUT_PATH}/ECDS"


# **Civ Registration Data**

# In[ ]:


civreg_data = RawDataset(
    path=civreg_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_civreg_data = civreg_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="CIV_REG"
)


# In[ ]:


processed_civreg_data.write_to_feather(f"{JULY_2023_OUTPUT_PATH}/civ_reg.arrow")


# In[ ]:


processed_civreg_data.write_to_log(f"{JULY_2023_OUTPUT_PATH}/civ_reg_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_civreg_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{JULY_2023_OUTPUT_CLEAN_PATH}/civ_reg.arrow")
cleaned_dataset.write_to_log(f"{JULY_2023_OUTPUT_CLEAN_PATH}/civ_reg_log.txt")


# **APC Data**
# 
# 2025-04-14 We need a fudge here, at present the pipeline expects a single APC file, we create one by concatenating `NIC338864_HES_AE_YYYY99.txt` file**s** with `nic338864_hes_apc_202299.csv` file.
# 
# I have not found a way to include both .txt files and .csv file in one AnyPath glob like operation (case_sensitive=False appears not to work/be implemented)
# 
# Also, this is not as easy as it seems as you get different d-types with the .txt and the .csv
# 
# ```
#  ('KEY', '.txt dtype', '.csv dtype')
# [('ADMISTAT', 'String', 'Int64'),
#  ('BEDYEAR', 'String', 'Int64'),
#  ('EPIDUR', 'String', 'Int64'),
#  ('IMD04', 'String', 'Float64'),
#  ('IMD04C', 'String', 'Float64'),
#  ('IMD04ED', 'String', 'Float64'),
#  ('IMD04EM', 'String', 'Float64'),
#  ('IMD04HD', 'String', 'Float64'),
#  ('IMD04HS', 'String', 'Float64'),
#  ('IMD04I', 'String', 'Float64'),
#  ('IMD04IA', 'String', 'Float64'),
#  ('IMD04IC', 'String', 'Float64'),
#  ('IMD04LE', 'String', 'Float64'),
#  ('IMD04RK', 'String', 'Int64'),
#  ('MAINSPEF', 'String', 'Int64'),
#  ('NEOCARE', 'String', 'Int64'),
#  ('SUSRECID', 'String', 'Int64'),
#  ('TRETSPEF', 'String', 'Int64')]
#  ```
#  
# The solutions is to force all d-types to string with infer_schema=False and to force the column order prior to the concat
# 
# We have manually checked that on 2025-04-24, all the APC files (.txt and .csv) had the same (**220!**) columns.

# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        apc_path,
        "NIC338864_HES_APC_*.txt"
    ),
    infer_schema=False
#     schema=lf_temp_csv_schema
)
column_order = lf_temp_txt.collect_schema().names()


# In[ ]:


lf_temp_csv = pl.scan_csv(
    AnyPath(
        apc_path,
        "nic338864_hes_apc_*.csv"
    ),
    infer_schema=False
)
# lf_temp_csv_schema = lf_temp_csv.collect_schema()


# In[ ]:


## Expect this to pass
assert(
    set(lf_temp_csv.collect_schema().names()) == 
    set(lf_temp_txt.collect_schema().names())
)


# In[ ]:


lf_temp_all = pl.concat(
    [
        lf_temp_txt,
        lf_temp_csv.select(column_order)
    ]
)


# In[ ]:


# PREPROCESSED_APC_2023_07_LOCATION = (
#     f"{PREPROCESSED_FILES_LOCATION}/nic338864_hes_apc_2023_07_all.csv"
# ) 
lf_temp_all.sink_csv(
    AnyPath(PREPROCESSED_FILES_LOCATION,"nic338864_hes_apc_2023_07_all.csv")
)


# In[ ]:


del(lf_temp_txt, lf_temp_csv, lf_temp_all, column_order)


# In[ ]:


apc_data = RawDataset(
#     path=apc_path,
    path=f"{PREPROCESSED_FILES_LOCATION}/nic338864_hes_apc_2023_07_all.csv",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_apc_data = apc_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="APC"
)


# In[ ]:


processed_apc_data.write_to_feather(f"{JULY_2023_OUTPUT_PATH}/apc.arrow")
processed_apc_data.write_to_log(f"{JULY_2023_OUTPUT_PATH}/apc_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_apc_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{JULY_2023_OUTPUT_CLEAN_PATH}/apc.arrow")
cleaned_dataset.write_to_log(f"{JULY_2023_OUTPUT_CLEAN_PATH}/apc_log.txt")


# **OP Data**
# 
# Same rigmarole with OP data

# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        apc_path,
        "NIC338864_HES_OP_*.txt"
    ),
    infer_schema=False
#     schema=lf_temp_csv_schema
)
column_order = lf_temp_txt.collect_schema().names()


# In[ ]:


lf_temp_csv = pl.scan_csv(
    AnyPath(
        apc_path,
        "nic338864_hes_op_*.csv"
    ),
    infer_schema=False
)
# lf_temp_csv_schema = lf_temp_csv.collect_schema()


# In[ ]:


## Expect this to pass
assert(
    set(lf_temp_csv.collect_schema().names()) == 
    set(lf_temp_txt.collect_schema().names())
)


# In[ ]:


lf_temp_all = pl.concat(
    [
        lf_temp_txt,
        lf_temp_csv.select(column_order)
    ]
)


# In[ ]:


lf_temp_all.sink_csv(
    AnyPath(f"{PREPROCESSED_FILES_LOCATION}/nic338864_hes_op_2023_07_all.csv")
)


# In[ ]:


del(lf_temp_txt, lf_temp_csv, lf_temp_all, column_order)


# In[ ]:


op_data = RawDataset(
#     path=op_path,
    path=f"{PREPROCESSED_FILES_LOCATION}/nic338864_hes_op_2023_07_all.csv",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_op_data = op_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="OP"
)


# In[ ]:


processed_op_data.write_to_feather(f"{JULY_2023_OUTPUT_PATH}/op.arrow")
processed_op_data.write_to_log(f"{JULY_2023_OUTPUT_PATH}/op_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_op_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{JULY_2023_OUTPUT_CLEAN_PATH}/op.arrow")
cleaned_dataset.write_to_log(f"{JULY_2023_OUTPUT_CLEAN_PATH}/op_log.txt")


# **Cancer Registry Data**
# 
# This is a 28 column txt file received in early Feb 2024 - missing in the original data. 
# 
# Unfortunately there is no date of diagnosis, only a year. We are therefore setting the "date" of diagnosis as the 1st Jan of that year. This means that we will include as many people as possible. However when we combine with demographic data to get rid of unrealistic dates, we are removing events that happen up to the first year of life (i.e. if you are born on 31st Nov, and your cancer of that year is diagnosed in Dec of the same year, when we set this diagnosis date as 1st Jan, it will get cleaned from the data as being before you were born). 
# 
# The second problem or caveat here is that the age at diagnosis will be slightly incorrect. Up to 364 days out of sync. 
# 
# **2025-04-15:  I have changed the date of assignment to be 2nd July, i.e. the middle of the year.  This means that the age at diagnosis will still be slightly incorrect but only up to 183 days out of synch.**
# 
# Logs will be added to describe this process manually below. 

# In[ ]:


ca_data = RawDataset(
    path=cancer_path,
    dataset_type="ca",
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


file_year = datetime.fromtimestamp(AnyPath(cancer_path).stat().st_ctime).year


# In[ ]:


cutoff = file_year % 2000


# In[ ]:


ca_data.log.append(f"{datetime.now()}: There are {ca_data.data.shape[0]} rows in the data. The dates are all integers. We will convert these to the 1st Jan of that year - so 16 becomes 2016-01-01")


# In[ ]:


ca_data.data = (
    ca_data.data
    .with_columns(
        # TODO min of current year of script run and cutoff
        pl.when(pl.col("CANCER_REGISTRATION_YEAR") <= cutoff) # Hack. cutoff based on `cancer_path` creation* date. *metadata-last-modified date
        .then((pl.col("CANCER_REGISTRATION_YEAR") + 2000).cast(pl.Utf8))
        .otherwise((pl.col("CANCER_REGISTRATION_YEAR") + 1900).cast(pl.Utf8))
        .alias("full_year") 
    )
    .with_columns(
        pl.concat_str(
            pl.col("full_year"),
            pl.lit("-07-02")
        )
#         .cast(pl.Date) # to reinstate one day. We should work dates as dates as soon as we can. 
#         # The pipeline currently expects this date as a string at this stage.
        .alias("full_date") # this is the cancer registration full date
    )
)


# In[ ]:


ca_data.log.append(f"{datetime.now()}: There are now {ca_data.data.shape[0]} rows in the data")


# In[ ]:


deduplication_options = ['nhs_number', 'code', 'date']


# In[ ]:


col_maps = {
    "CANCER_SITE": "code", 
    "full_date": "date", 
    "Study_ID": "nhs_number"
}


# In[ ]:


processed_ca_data = ca_data.process_dataset(
    deduplication_options=deduplication_options, 
    column_maps=col_maps
)


# In[ ]:


processed_ca_data.write_to_feather(f"{JULY_2023_OUTPUT_PATH}/ca.arrow")
processed_ca_data.write_to_log(f"{JULY_2023_OUTPUT_PATH}/ca_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_ca_data.remove_unrealistic_dates(date_start=date_start,
                                                                 date_end=date_end,
                                                                 before_born=True,
                                                                 demographic_dataset=demographics)
cleaned_dataset.write_to_feather(f"{JULY_2023_OUTPUT_CLEAN_PATH}/ca.arrow")
cleaned_dataset.write_to_log(f"{JULY_2023_OUTPUT_CLEAN_PATH}/ca_log.txt")


# **ECDS Data**

# This is a "new" venture.
# 
# #### Again we need to preprocess files to a single .csv
# 
# For 2023_07 cut, there are only .txt files (i.e not .txt files + .csv file).  The separator is the pipe symbol `|`
# 
# Unfortunately, even within the glob based scan_csv, there are inconsistencies in field dtypes (principally String vs Float).  So easier again to cast all the str with `infer_schema=False` argument.

# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        ecds_path,
        "*ECDS*.txt"
    ),
    separator="|",
    infer_schema=False
)


# In[ ]:


PREPROCESSED_ECDS_2023_07_LOCATION = (
    f"{PREPROCESSED_FILES_LOCATION}/nic338864_ecds_2023_07_all.csv"
) 
lf_temp_txt.sink_csv(
    AnyPath(PREPROCESSED_ECDS_2023_07_LOCATION)
)


# In[ ]:


ecds_data = RawDataset(
#     path=op_path,
    path=PREPROCESSED_ECDS_2023_07_LOCATION,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.SNOMED.value,
)


# In[ ]:


col_maps = {
#     "CANCER_SITE": "code", 
    "ARRIVAL_DATE": "date", 
    "STUDY_ID": "nhs_number"
}


# In[ ]:


processed_ecds_data = ecds_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="ECDS",
)


# In[ ]:


processed_ecds_data.write_to_feather(f"{JULY_2023_OUTPUT_PATH}/ecds.arrow")
processed_ecds_data.write_to_log(f"{JULY_2023_OUTPUT_PATH}/ecds_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_ecds_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{JULY_2023_OUTPUT_CLEAN_PATH}/ecds.arrow")
cleaned_dataset.write_to_log(f"{JULY_2023_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# ### Merge these data together
# 
# Now we merge all these datasets together and deduplicate. We load from file so that we can be assured that the correct dataset is loaded as sometimes the cells can be run manually in the wrong order. 

# In[ ]:


civ_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/civ_reg.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/civ_reg_log.txt")


# In[ ]:


apc_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/apc.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/apc_log.txt")


# In[ ]:


op_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/op.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/op_log.txt")


# In[ ]:


ca_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/ca.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/ca_log.txt")


# In[ ]:


ecds_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/ecds.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.SNOMED.value, 
                            log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# In[ ]:


civ_data.merge_with_dataset(apc_data)


# In[ ]:


civ_data.merge_with_dataset(op_data)


# In[ ]:


civ_data.merge_with_dataset(ca_data)


# ## Note
# 
# ECDS data are SNOMED, others are ICD10; only same codeset data can be merged with `merge_with_dataset`
# 
# ECDS data are merge later in pipeline

# In[ ]:


dedup = civ_data.deduplicate()


# In[ ]:


for log in apc_data.log:
    dedup.log.append(log)
    
for log in op_data.log:
    dedup.log.append(log)
    
for log in ca_data.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{JULY_2023_OUTPUT_CLEAN_PATH}/merged_data.arrow")
dedup.write_to_log(f"{JULY_2023_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# ###### 2024_10  START ######

# ### 3rd cut of data - October 2024 Data
# 
# In this third cut of data we have n files. 
# 

# In[ ]:


OCT_2024_OUTPUT_PATH = f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/october_2024/processed_data"
AnyPath(OCT_2024_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
OCT_2024_OUTPUT_CLEAN_PATH = f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/october_2024/clean_processed_data"
AnyPath(OCT_2024_OUTPUT_CLEAN_PATH).mkdir(parents=True, exist_ok=True)


# In[ ]:


civreg_path = f"{OCT_2024_INPUT_PATH}/FILE0220457_NIC338864_CIVREG_MORT_.txt"
apc_path = f"{OCT_2024_INPUT_PATH}/HES/FILE0220459_NIC338864_HES_APC_202399.txt" 
op_path  = f"{OCT_2024_INPUT_PATH}/HES/FILE0220461_NIC338864_HES_OP_202399.txt" 
cancer_path = f"{OCT_2024_INPUT_PATH}/FILE0220456_NIC338864_Cancer_.txt"
ecds_path = f"{OCT_2024_INPUT_PATH}/ECDS/NIC338864_ECDS_202399.txt"


# **Civ Registration Data**

# In[ ]:


civreg_data = RawDataset(
    path=civreg_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_civreg_data = civreg_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="CIV_REG"
)


# In[ ]:


processed_civreg_data.write_to_feather(f"{OCT_2024_OUTPUT_PATH}/civ_reg.arrow")


# In[ ]:


processed_civreg_data.write_to_log(f"{OCT_2024_OUTPUT_PATH}/civ_reg_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_civreg_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{OCT_2024_OUTPUT_CLEAN_PATH}/civ_reg.arrow")
cleaned_dataset.write_to_log(f"{OCT_2024_OUTPUT_CLEAN_PATH}/civ_reg_log.txt")


# **APC Data**

# In[ ]:


apc_data = RawDataset(
    path=apc_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_apc_data = apc_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="APC"
)


# In[ ]:


processed_apc_data.write_to_feather(f"{OCT_2024_OUTPUT_PATH}/apc.arrow")
processed_apc_data.write_to_log(f"{OCT_2024_OUTPUT_PATH}/apc_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_apc_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{OCT_2024_OUTPUT_CLEAN_PATH}/apc.arrow")
cleaned_dataset.write_to_log(f"{OCT_2024_OUTPUT_CLEAN_PATH}/apc_log.txt")


# **OP Data**

# In[ ]:


op_data = RawDataset(
    path=op_path,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_op_data = op_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="OP"
)


# In[ ]:


processed_op_data.write_to_feather(f"{OCT_2024_OUTPUT_PATH}/op.arrow")
processed_op_data.write_to_log(f"{OCT_2024_OUTPUT_PATH}/op_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_op_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{OCT_2024_OUTPUT_CLEAN_PATH}/op.arrow")
cleaned_dataset.write_to_log(f"{OCT_2024_OUTPUT_CLEAN_PATH}/op_log.txt")


# **Cancer Registry Data**
# 
# This is a 28 column txt file received in early Feb 2024 - missing in the original data. 
# 
# Unfortunately there is no date of diagnosis, only a year. We are therefore setting the "date" of diagnosis as the 1st Jan of that year. This means that we will include as many people as possible. However when we combine with demographic data to get rid of unrealistic dates, we are removing events that happen up to the first year of life (i.e. if you are born on 31st Nov, and your cancer of that year is diagnosed in Dec of the same year, when we set this diagnosis date as 1st Jan, it will get cleaned from the data as being before you were born). 
# 
# The second problem or caveat here is that the age at diagnosis will be slightly incorrect. Up to 364 days out of sync. 
# 
# **2025-04-15:  I have changed the date of assignment to be 2nd July, i.e. the middle of the year.  This means that the age at diagnosis will still be slightly incorrect but only up to 183 days out of synch.**
# 
# Logs will be added to describe this process manually below. 

# In[ ]:


ca_data = RawDataset(
    path=cancer_path,
    dataset_type="ca",
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


file_year = datetime.fromtimestamp(AnyPath(cancer_path).stat().st_ctime).year


# In[ ]:


cutoff = file_year % 2000


# In[ ]:


ca_data.log.append(f"{datetime.now()}: There are {ca_data.data.shape[0]} rows in the data. The dates are all integers. We will convert these to the 1st Jan of that year - so 16 becomes 2016-01-01")


# In[ ]:


ca_data.data = (
    ca_data.data
    .with_columns(
        # TODO min of current year of script run and cutoff
        pl.when(pl.col("CANCER_REGISTRATION_YEAR") <= cutoff) # Hack. cutoff based on `cancer_path` creation* date. *metadata-last-modified date
        .then((pl.col("CANCER_REGISTRATION_YEAR") + 2000).cast(pl.Utf8))
        .otherwise((pl.col("CANCER_REGISTRATION_YEAR") + 1900).cast(pl.Utf8))
        .alias("full_year") 
    )
    .with_columns(
        pl.concat_str(
            pl.col("full_year"),
            pl.lit("-07-02")
        )
#         .cast(pl.Date) # to reinstate one day. We should work dates as dates as soon as we can. 
#         # The pipeline currently expects this date as a string at this stage.
        .alias("full_date") # this is the cancer registration full date
    )
)


# In[ ]:


ca_data.log.append(f"{datetime.now()}: There are now {ca_data.data.shape[0]} rows in the data")


# In[ ]:


deduplication_options = ['nhs_number', 'code', 'date']


# In[ ]:


col_maps = {
    "CANCER_SITE": "code", 
    "full_date": "date", 
    "Study_ID": "nhs_number"
}


# In[ ]:


processed_ca_data = ca_data.process_dataset(
    deduplication_options=deduplication_options, 
    column_maps=col_maps
)


# In[ ]:


processed_ca_data.write_to_feather(f"{OCT_2024_OUTPUT_PATH}/ca.arrow")
processed_ca_data.write_to_log(f"{OCT_2024_OUTPUT_PATH}/ca_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_ca_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{OCT_2024_OUTPUT_CLEAN_PATH}/ca.arrow")
cleaned_dataset.write_to_log(f"{OCT_2024_OUTPUT_CLEAN_PATH}/ca_log.txt")


# **ECDS Data**

# This is a "new" venture.
# 
# #### Again we need to preprocess files to a single .csv
# 
# For 2024_10 cut, there is a single .txt file.  The separator is the pipe symbol `|`
# 
# Unfortunately, even within the glob based scan_csv, there are inconsistencies in field dtypes (principally String vs Float).  So easier again to cast all the str with `infer_schema=False` argument.

# In[ ]:


ecds_path


# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        ecds_path
#         "*ECDS*.txt"
    ),
    separator="|",
    infer_schema=False
)


# In[ ]:


PREPROCESSED_ECDS_2024_10_LOCATION = (
    f"{PREPROCESSED_FILES_LOCATION}/nic338864_ecds_2024_10_all.csv"
) 
lf_temp_txt.sink_csv(
    AnyPath(PREPROCESSED_ECDS_2024_10_LOCATION)
)


# In[ ]:


ecds_data = RawDataset(
#     path=op_path,
    path=PREPROCESSED_ECDS_2024_10_LOCATION,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.SNOMED.value,
)


# In[ ]:


col_maps = {
#     "CANCER_SITE": "code", 
    "ARRIVAL_DATE": "date", 
    "STUDY_ID": "nhs_number"
}


# In[ ]:


processed_ecds_data = ecds_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="ECDS",
)


# In[ ]:


processed_ecds_data.write_to_feather(f"{OCT_2024_OUTPUT_PATH}/ecds.arrow")
processed_ecds_data.write_to_log(f"{OCT_2024_OUTPUT_PATH}/ecds_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_ecds_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{OCT_2024_OUTPUT_CLEAN_PATH}/ecds.arrow")
cleaned_dataset.write_to_log(f"{OCT_2024_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# ### Merge these data together
# 
# Now we merge all these datasets together and deduplicate. We load from file so that we can be assured that the correct dataset is loaded as sometimes the cells can be run manually in the wrong order. 

# In[ ]:


civ_data = ProcessedDataset(path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/civ_reg.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/civ_reg_log.txt")


# In[ ]:


apc_data = ProcessedDataset(path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/apc.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/apc_log.txt")


# In[ ]:


op_data = ProcessedDataset(path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/op.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/op_log.txt")


# In[ ]:


ca_data = ProcessedDataset(path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/ca.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/ca_log.txt")


# In[ ]:


civ_data.merge_with_dataset(apc_data)


# In[ ]:


civ_data.merge_with_dataset(op_data)


# In[ ]:


civ_data.merge_with_dataset(ca_data)


# In[ ]:


dedup = civ_data.deduplicate()


# In[ ]:


for log in apc_data.log:
    dedup.log.append(log)
    
for log in op_data.log:
    dedup.log.append(log)
    
for log in ca_data.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{OCT_2024_OUTPUT_CLEAN_PATH}/merged_data.arrow")
dedup.write_to_log(f"{OCT_2024_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# ###### 2024_10 END ######

# ###### 2025_03 START ######

# ### 4th cut of data - March 2025 Data
# 
# 2025-04-15: In this fourth cut of data we only have HES and ECDS data; there are no mortality/civic_reg or cancer data
# 

# In[ ]:


MAR_2025_OUTPUT_PATH = f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/march_2025/processed_data"
AnyPath(MAR_2025_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
MAR_2025_OUTPUT_CLEAN_PATH = f"{PROCESSED_DATASETS_NHS_DIGITAL_LOCATION}/march_2025/clean_processed_data"
AnyPath(MAR_2025_OUTPUT_CLEAN_PATH).mkdir(parents=True, exist_ok=True)


# In[ ]:


apc_path = f"{MAR_2025_INPUT_PATH}/HES/" 
op_path = f"{MAR_2025_INPUT_PATH}/HES/" 
ecds_path = f"{MAR_2025_INPUT_PATH}/ECDS/"


# **APC Data**

# #### Again we need to preprocess files to a single .csv
# 
# For 2025_03 cut, there are only .txt files (i.e not .txt files + .csv file).  The separator is the pipe symbol `|`
# 
# Unfortunately, even within the glob based scan_csv, there are inconsistencies in field dtypes (principally String vs Float).  So easier again to cast all to strings with `infer_schema=False` argument.

# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        apc_path,
        "FILE*_NIC338864_HES_APC_*.txt"
    ),
    separator="|",
    infer_schema=False
)


# In[ ]:


PREPROCESSED_APC_2025_03_LOCATION = (
    f"{PREPROCESSED_FILES_LOCATION}/nic338864_hes_apc_2025_03_all.csv"
) 
lf_temp_txt.sink_csv(
    AnyPath(PREPROCESSED_APC_2025_03_LOCATION)
)


# In[ ]:


del(lf_temp_txt)


# In[ ]:


apc_data = RawDataset(
#     path=apc_path,
    path=PREPROCESSED_APC_2025_03_LOCATION,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_apc_data = apc_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="APC"
)


# In[ ]:


processed_apc_data.write_to_feather(f"{MAR_2025_OUTPUT_PATH}/apc.arrow")
processed_apc_data.write_to_log(f"{MAR_2025_OUTPUT_PATH}/apc_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_apc_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{MAR_2025_OUTPUT_CLEAN_PATH}/apc.arrow")
cleaned_dataset.write_to_log(f"{MAR_2025_OUTPUT_CLEAN_PATH}/apc_log.txt")


# **OP Data**

# #### Again we need to preprocess files to a single .csv
# 
# For 2025_03 cut, there are only .txt files (i.e not .txt files + .csv file).  The separator is the pipe symbol `|`
# 
# Unfortunately, even within the glob based scan_csv, there are inconsistencies in field dtypes (principally String vs Float).  So easier again to cast all the str with `infer_schema=False` argument.

# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        apc_path,
        "FILE*_NIC338864_HES_OP_*.txt"
    ),
    separator="|",
    infer_schema=False
)


# In[ ]:


PREPROCESSED_OP_2025_03_LOCATION = (
    f"{PREPROCESSED_FILES_LOCATION}/nic338864_hes_op_2025_03_all.csv"
) 
lf_temp_txt.sink_csv(
    AnyPath(PREPROCESSED_OP_2025_03_LOCATION)
)


# In[ ]:


del(lf_temp_txt)


# In[ ]:


op_data = RawDataset(
#     path=op_path,
    path=PREPROCESSED_OP_2025_03_LOCATION,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value
)


# In[ ]:


processed_op_data = op_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="OP"
)


# In[ ]:


processed_op_data.write_to_feather(f"{MAR_2025_OUTPUT_PATH}/op.arrow")
processed_op_data.write_to_log(f"{MAR_2025_OUTPUT_PATH}/op_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_op_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{MAR_2025_OUTPUT_CLEAN_PATH}/op.arrow")
cleaned_dataset.write_to_log(f"{MAR_2025_OUTPUT_CLEAN_PATH}/op_log.txt")


# **ECDS Data**

# This is a "new" venture.
# 
# #### Again we need to preprocess files to a single .csv
# 
# For 2025_03 cut, there are only .txt files (i.e not .txt files + .csv file).  The separator is the pipe symbol `|`
# 
# Unfortunately, even within the glob based scan_csv, there are inconsistencies in field dtypes (principally String vs Float).  So easier again to cast all the str with `infer_schema=False` argument.

# In[ ]:


lf_temp_txt = pl.scan_csv(
    AnyPath(
        ecds_path,
        "*ECDS*.txt"
    ),
    separator="|",
    infer_schema=False
)


# In[ ]:


PREPROCESSED_ECDS_2025_03_LOCATION = (
    f"{PREPROCESSED_FILES_LOCATION}/nic338864_ecds_2025_03_all.csv"
) 
lf_temp_txt.sink_csv(
    AnyPath(PREPROCESSED_ECDS_2025_03_LOCATION)
)


# In[ ]:


ecds_data = RawDataset(
#     path=op_path,
    path=PREPROCESSED_ECDS_2025_03_LOCATION,
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.SNOMED.value,
)


# In[ ]:


col_maps = {
#     "CANCER_SITE": "code", 
    "ARRIVAL_DATE": "date", 
    "STUDY_ID": "nhs_number"
}


# In[ ]:


processed_ecds_data = ecds_data.process_dataset(
    deduplication_options=deduplication_options,
    column_maps=col_maps,
    nhs_digital_subtype="ECDS",
)


# In[ ]:


processed_ecds_data.write_to_feather(f"{MAR_2025_OUTPUT_PATH}/ecds.arrow")
processed_ecds_data.write_to_log(f"{MAR_2025_OUTPUT_PATH}/ecds_log.txt")


# We now remove the unrealistic data (i.e. event before birth, before 1st Jan 1910 or after \[run date of script\]) and save this into a cleaned_processed_data folder

# In[ ]:


cleaned_dataset = processed_ecds_data.remove_unrealistic_dates(
    date_start=date_start,
    date_end=date_end,
    before_born=True,
    demographic_dataset=demographics
)

cleaned_dataset.write_to_feather(f"{MAR_2025_OUTPUT_CLEAN_PATH}/ecds.arrow")
cleaned_dataset.write_to_log(f"{MAR_2025_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# ### Merge these data together
# 
# Now we merge all these datasets together and deduplicate. We load from file so that we can be assured that the correct dataset is loaded as sometimes the cells can be run manually in the wrong order. 

# In[ ]:


apc_data = ProcessedDataset(path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/apc.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/apc_log.txt")


# In[ ]:


op_data = ProcessedDataset(path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/op.arrow", 
                            dataset_type=DatasetType.NHS_DIGITAL.value, 
                            coding_system=CodelistType.ICD10.value, 
                            log_path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/op_log.txt")


# In[ ]:


apc_data.merge_with_dataset(op_data)


# In[ ]:


dedup = apc_data.deduplicate()


# In[ ]:


for log in apc_data.log:
    dedup.log.append(log)
    
for log in op_data.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MAR_2025_OUTPUT_CLEAN_PATH}/merged_data.arrow")
dedup.write_to_log(f"{MAR_2025_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# ###### 2025_03 END ######

# ### Merging all NHS Digital data together \[ICD10\]
# 
# Now we have 4 merged datasets - from Sept 2021, July 2023, October 2024 and from March 2025. We are going to merged these into each other and deduplicate. 

# In[ ]:


sept_2021_data = ProcessedDataset(path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/merged_data.arrow", 
                                coding_system=CodelistType.ICD10.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{SEP_2021_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# In[ ]:


july_2023_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/merged_data.arrow", 
                                coding_system=CodelistType.ICD10.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# In[ ]:


oct_2024_data = ProcessedDataset(path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/merged_data.arrow", 
                                coding_system=CodelistType.ICD10.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# In[ ]:


mar_2025_data = ProcessedDataset(path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/merged_data.arrow", 
                                coding_system=CodelistType.ICD10.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/merged_data_log.txt")


# In[ ]:


sept_2021_data.merge_with_dataset(july_2023_data)


# In[ ]:


sept_2021_data.merge_with_dataset(oct_2024_data)


# In[ ]:


sept_2021_data.merge_with_dataset(mar_2025_data)


# In[ ]:


dedup = sept_2021_data.deduplicate()


# In[ ]:


for log in july_2023_data.log:
    dedup.log.append(log)
    
for log in oct_2024_data.log:
    dedup.log.append(log)
    
for log in mar_2025_data.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_ICD10.arrow")


# In[ ]:


dedup.write_to_log(f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_ICD10_log.txt")


# ### Merging all NHS Digital data together \[SNOMED\]
# 
# We need to do this because all other MEGADATA files generated herein are ICD10 and ECDS is SNOMED
# 
# NB. there is no ECDS for Sept 2021.

# In[ ]:


july_2023_data = ProcessedDataset(path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/ecds.arrow", 
                                coding_system=CodelistType.SNOMED.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{JULY_2023_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# In[ ]:


oct_2024_data = ProcessedDataset(path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/ecds.arrow", 
                                coding_system=CodelistType.SNOMED.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{OCT_2024_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# In[ ]:


mar_2025_data = ProcessedDataset(path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/ecds.arrow", 
                                coding_system=CodelistType.SNOMED.value, 
                                dataset_type=DatasetType.NHS_DIGITAL.value, 
                                log_path=f"{MAR_2025_OUTPUT_CLEAN_PATH}/ecds_log.txt")


# In[ ]:


july_2023_data.merge_with_dataset(oct_2024_data)


# In[ ]:


july_2023_data.merge_with_dataset(mar_2025_data)


# In[ ]:


dedup = july_2023_data.deduplicate()


# In[ ]:


for log in oct_2024_data.log:
    dedup.log.append(log)
    
for log in mar_2025_data.log:
    dedup.log.append(log)
    
dedup.log.sort()


# In[ ]:


dedup.write_to_feather(f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_SNOMED.arrow")


# In[ ]:


dedup.write_to_log(f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_SNOMED_log.txt")


# **Map SNOMED to ICD10** 
# 
# Load the data from saved SNOMED dataset and map it from the mapping file. 
# 
# Note added 06.12.2024: Caroline's mapping files are in her /red/ directory, if full path to these is not given, these steps will only work if run from Caroline's directory.  Modified to give full path to look-up tables.
# 
# Issues:
# 1. Should we move the look-tables to /library-red/reference_files/
# 2. What is the source of these lookup tables?  When were they last updated?
# 
# 
# **Note 2025-05-12: Issue of ICD10 4 digit discussed w/ DvH and SF; agreed to limit all ICD10 data versions to 3digit.  Also verfied that SNOMED_to_ICD10 table maps each SNOMED code to a single ICD10 3digit code (m:1) but to multiple ICD10 4 digit codes (m:m).**

# In[ ]:


snomed_data = ProcessedDataset(
    path=f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_SNOMED.arrow",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.SNOMED.value,
    log_path=f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_SNOMED_log.txt"
)


# In[ ]:


# Alas we need to recast snomed_data's code field as an Int64 to work with TRE tool's merging function
snomed_data.data = (
    snomed_data.data
    .with_columns(
        pl.col("code").cast(pl.Int64, strict=True)
    )
)


# In[ ]:


mapped_data = snomed_data.map_snomed_to_icd(
    mapping_file=f"{ROOT_LOCATION}/{VERSION}/mapping_files/processed_mapping_file.csv",
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


final_dedup.write_to_feather(f"{MEGADATA_NHS_DIGITAL_LOCATION}/final_mapped_snomed_to_icd.arrow")
final_dedup.write_to_log(f"{MEGADATA_NHS_DIGITAL_LOCATION}/final_mapped_snomed_to_icd_log.txt")


# In[ ]:


final_dedup.coding_system = CodelistType.ICD10.value


# **Merge with the ICD data and deduplicate**

# In[ ]:


icd_data = ProcessedDataset(
    path=f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_ICD10.arrow",
    dataset_type=DatasetType.NHS_DIGITAL.value,
    coding_system=CodelistType.ICD10.value,
    log_path=f"{MEGADATA_NHS_DIGITAL_LOCATION}/nhs_d_merged_ICD10_log.txt"
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


merged_dedup.write_to_feather(f"{MEGADATA_NHS_DIGITAL_LOCATION}/merged_and_mapped.arrow")
merged_dedup.write_to_log(f"{MEGADATA_NHS_DIGITAL_LOCATION}/merged_and_mapped_log.txt")


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("6-merge-datasets-notebook")


# In[ ]:





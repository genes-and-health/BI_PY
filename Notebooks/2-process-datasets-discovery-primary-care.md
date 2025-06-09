# `2-process-datasets-discovery-primary-care.ipynb`

## Coding system(s)

* SNOMED-CT

## Data cuts

There are 7 "cuts" of data representing 7 time periods when the data was made available. Each dataset comprises of at least two CSV files - a procedures and an observation file - but some have more. At present, we only used the observation and procedures files.

* 2024_12: Observations, Procedures
* 2024_07: Observations, Procedures
* 2023_11: Observations, Procedures
* 2023_03: Observations, Procedures
* 2023_01: Corrupted data, not used
* 2022_12: Observations, Procedures
* 2022_04: Observations, Procedures

## Process

This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as .arrow files (binary) with their log files where appropriate. This notebook:

1. Processes primary care datasets (SNOMED codes)
2. Where possible, maps SNOMED codes to ICD-10 codes

Processing Primary Care Datasets The primary care datasets are in SNOMED. We process datasets in the following way:

1. Load data from the CSV in each dataset
2. Deduplicate on code, nhs number and date ==> this means the same SNOMED code on a different date is preserved as a separate event for now
3. Save this dataset in its own folder
4. Load the demographic dataset and use the remove unrealistic dates method to remove rows where the event took place before 1st Jan 1910, after the run date or before the patient was born.
5. Save this dataset

We do this process for observations and procedure csv files separately. Then we:

1. Reload the cleaned and deduplicated observation dataset from processed file made in step 5 above.
2. Reload the cleaned and deduplicated procedures dataset from processed file made in step 5 above.
3. Merge observations and procedures datasets together
4. Deduplicate this merged dataset on code, nhs number and date
5. Save this merged processed dataset under the date of the original dataset

Finally once we have done this to all the 7 time cuts of the data, we do the following:

1. Merge all the processed datasets together
2. Deduplicate this "megafile"
3. Save as .arrow file

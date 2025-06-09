# ` 3-process-datasets-barts-health.ipynb`

## Coding system(s)
* SNOMED-CT, ICD-10, OPCS
  
## Data cuts

There are 4 "cuts" of data.

* 2024_09: ICD10 (`RDE_APC_DIAGNOSIS`, `RDE_OP_DIAGNOSIS`), OPCS (`RDE_APC_OPCS`, `RDE_OPA_OPCS`), SNOMED (`RDE_MSDS`, `RDE_PC_PROBLEMS`, `RDE_PC_PROCEDURES`, `RDE_ALL_PROCEDURES`)
* 2023_12: ICD10 (`GH_APC_Diagnosis`, `GH_OP_Diagnosis`), OPCS (`GH_APC_OPCS`, `GH_OP_OPCS`), SNOMED (`GandH_PC_Diagnosis_Problems_Procedures`)
* 2023_05: ICD10 (`GH_APC_Diagnosis`, `GH_OP_Diagnosis`), OPCS (`GH_APC_OPCS`, `GH_OP_OPCS`), SNOMED (`GandH_PC_Diagnosis`, `GandH_PC_Problems`, `GandH_PC_Procedures`)
* 2022_03: ICD10 (`2022_05_23_icd10_combined_redacted.txt`), OPCS (`2022_05_23_opcs_combined_redacted.txt`), SNOMED (`GandH_PC_Diagnosis`, `GandH_PC_Problems`, `GandH_PC_Procedures`)

## Process

In third second notebook, we do the following:

1. Process Barts healthcare datasets - SNOMED
2. Process Barts healthcare dataset - ICD10
3. Process Barts healthcare dataset - OPCS
4. Map SNOMED datasets to ICD and combine with ICD10 dataset

The barts healthcare datasets are in SNOMED, ICD10 and OPCS. There are 5 "cuts" of data representing 5 time periods when the data was made available. Each dataset comprises of multiple CSV files.

We process the datasets in the following way:

1. Load the data for each "type" of coding system per file
2. Deduplicate and process
3. Save as arrow file and with log file.
4. We then remove all unrealistic dates via the dataset that we created in the first notebook
5. Save this cleaned data file with log

Once all the datasets have been created:

1. Reload each dataset from file and merge with other datasets of the same type for a time period (i.e. the same cut of the data)
2. Deduplicate and merge all the log files together
3. Save in processed merged data folder

Finally merge all the same types of dataset together, deduplicate and save.



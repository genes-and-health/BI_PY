# ` 4-process-datasets-bradford.ipynb`

## Coding system(s)
* SNOMED-CT, ICD-10, OPCS
  
## Data cuts

There are 4 "cuts" of data.

* 2024_12: _no useable data in this cut_
* 2023_05: ICD10 (`_gh_diagnoses_`), OPCS (`_gh_procedures_`), SNOMED (`_gh_cerner_diagnoses_`, `_gh_cerner_problems_`)
* 2022_06: ICD10 (`_gh_diagnoses_`), OPCS (`_gh_procedures_`), SNOMED (`bradford_cerner_diagnoses_`, `bradford_cerner_problems_`)
* 2021_02: ICD10 (`icd10_bfs_`), OPCS (`opcs_bfd_`)

## Process

We process the datasets in the following way:

1. Load the data for each "type" of coding system per file
2. Deduplicate and process
3. Save as arrow file and with log file.
4. Then we add in the demographic data created by the first notebook and remove unrealistic data
5. Save this "clean" data

Once we have done this:

1. Reload each dataset from file and merge with other datasets of the same type for a time period (i.e. the same cut of the data)
2. Deduplicate and merge all the log files together
3. Save

Finally merge all the same types of dataset together, deduplicate and save.

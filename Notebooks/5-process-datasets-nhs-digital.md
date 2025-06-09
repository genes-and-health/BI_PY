# `5-process-datasets-nhs-digital.ipynb`

## Codesets
* ICD-10 \[all non-ECDS\]
* SNOMED-CT \[ECDS\]

## Data cuts
* 2025_03: HES_APC, HES_OP, ECDS
* 2024_10: CIVREG, HES_APC, HES_OP, CANCER, ECDS
* 2023_07: CIVREG, HES_APC, HES_OP, CANCER, ECDS
* 2021_09: CIVREG, HES_APC, HES_OP

## ECDS data

As of this release (version010), we now use ECDS (SNOMED-CT) data in `BI_PY`.  Because all other NHS Digital data sources used employ ICD-10, we collectively process all ECDS data together in the last step of the notebook.

## Process

We process the datasets in the following way:

* Load the dataset in turn
* Expand the wide files into multirow files so that one person can have multiple rows.
* Deduplicate
* Save

We will then remove unrealistic dates with previously saved Demographic data from notebook 1, and save again.

For some datasets, in particular the very wide datasets, there are a few issues which required some preprocessing. These steps will be removed in future where possible, when the custom library is updated. Where pre-processing has been required, the final pre-processed dataset is saved in a folder called `preprocessed_files`.

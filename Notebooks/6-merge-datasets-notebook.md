# `6-merge-datasets-notebook.ipynb`

## Codesets

* ICD-10, SNOMED-CT, OPCS
* *and* SNOMED mapped to ICD-10

## Data

There are no "cuts" here, this notebook uses the output of the primary care, Barts Trust, Bradford Trust and NHS Digital data-processing notebooks.

## Process

In this notebook, we upload the data files created in notebooks 2 to 5. We are aiming to get all the data of the same type together so we end up with (files in bold are in the `.../BI_PY/megadata/` directory):

* `ICD10 dataset`: \["Native" = `barts_icd` + `bratford_icd` + `nhs_d_icd` (there is no native ICD-10 data in `primary_care`)\] => **`icd_only.arrow`** 
* `OPCS dataset`: \[`barts_icd` + `bratford_icd` (only sources of OPCS codes are secondary care)\] => **`opcs_only.arrow`**
* `SNOMED dataset`: \[`primary_snomed` + `barts_icd` + `bratford_icd` + `nhs_d_icd`\] => **`snomed_only.arrow`**

In addition to this, we have mapped the SNOMED datasets to ICD10 and we will merge these in as well so we end up with:

* `SNOMED_MAPPED_AND_ICD dataset`: \[**`icd_only.arrow`** + `mapped_data_primary_icd` + `mapped_data_barts_icd` + `mapped_data_bradford_icd` + `mapped_data_nhs_digital_icd`\] => **`icd_and_mapped_snomed.arrow`**

The **`icd_and_mapped_snomed.arrow`** is processed (truncated to 3 characters) to produce **`icd_and_mapped_snomed_3_digit_deduplication.arrow`**


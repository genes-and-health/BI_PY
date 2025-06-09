# `8-custom-phenotypes.ipynb`

## Codesets

* ICD-10, SNOMED-CT and OPCS
* Please note that when considering custom phenotypes, we do **NOT** used ICD-10 codes obtained through mapping (i.e. we use only "native" ICD-10 codes)

## Data

Custom phenotypes are defined in the `.../BI_PY/inputs/GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv` file.
Individual to code mapping comes from **`icd_only.arrow`** + **`opcs_only.arrow`** + **`snomed_only.arrow`**
**`icd_and_mapped_snomed.arrow`**

## Process

This notebook creates individual trait files and regenie input files and co-variate files  for custom phenotypes.

We import the ICD-10, SNOMED-CT and OPCS mapping dataframes and join these to megadata files.
We then deduplicate and "tidy-up" and save one individual_trait_file per phenotype.

## individual trait files

The custom phenotype individual trait file .csv files have 8 columns:
* `nhs_number`: 64-char pseudo_NHS_number
* `phenotype`: custom phenotype name (e.g. `Hypertension`, `GNH0018_EssentialHypertension_summary_report.csv`)
* `date`: date of **earliest recorded**
* `code`: code for **earliest recorded**, can be an ICD-10 code (e.g. `I10X1), an OPCS code (e.g. `K752`) or a SNOMED code (e.g. `394659003`)
* `term`: text description of the earliest recorded code (e.g "Angina pectoris, unspecified")
* `all_codes`: all custom phenotype codes for individual found for this trait in this individual -- pipe-symbol separated (e.g. `I259 | 53741008 | I209 | 194828000 | K752 | I251`)
* `all_coding_systems`: all coding systems represented -- pipe-symbol separated (e.g. `ICD10 | OPCS4 | SNOMED_ConceptID`)
* `gender`: `M` or `F`
* `dob`: patient dob (birthday always set to 1st of month)
* `age_at_event`: age at first recorded diagnosis in years (fractional), e.g. 54.2
* `age_range`: binned age (e.g. 35-44, 55-64) 

## regenie files
regenie files are created for the 51k GWAS and 55k ExWAS datasets; each has an associated **age at first diagnosis** co-variate file:

51k GWAS:
* `2025_05_custom_phenotypes_regenie_51koct2024_65A_Topmed.tsv`
* `2025_05_regenie_51koct2024_65A_Topmed_Binary_custom_phenotypes_age_at_first_diagnosis_megawide.tsv`

55k ExWAS:
* `2025_05_custom_phenotypes_regenie_55k_BroadExomeIDs.tsv` (regenie input file)
* `2025_05_regenie_55k_BroadExomeIDs_Binary_custom_phenotypes_age_at_first_diagnosis_megawide-digit_ICD-10_age_at_first_diagnosis_megawide.tsv` (co-variate file)

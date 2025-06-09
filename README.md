# `BI_PY` pipeline: a python pipeline for binary trait attribution in Genes & Health

<img src="GNH%20TD%20Logo.png" alt="G&H Team Data logo" width=25%>

## Authors

* Stuart Rison
* Mike Samuels

Based on the ptyhon/treetools pipeline written by Caroline Morton and Saeed Bidi and contributions from:
* Sarah Finer
* Sam Hodgson
* Ben Jacobs
* Stravoula Kanoni
* Rohini Mathur
* Caroline Morton
* Daniel Stow
* David van Heel
* Julia Zollner.
* and other G&H Team Data members

## Summary

The Genes & Health (G&H) `BI_PY` pipeline extracts and processes binary trait data from G&H phenotype data.  Relevant codes may be SNOMED-CT, ICD-10 or OPCS codes.

The pipeline creates files and covariate files suitable for `regenie` \[G/Ex\]WAS analysis, as well as generic files for each binary trait at a _per individual_ level (one row per individual summarising the individual's earliest applicable code and age at first diagnosis).

## Phenotype data
The pipeline imports G&H phenotype data in `.../library-red/phenotypes_rawdata/`.  These data are from the following sources:

<details>
   
<summary>Binary phenotype data sources (click arrow to expand)</summary>

1. **DSA__BartHealth_NHS_Trust**: Secondary care data from the Barts Health NHS Trust \[North East London: ~40,000 individuals with data\]
2. **DSA__BradfordTeachingHospitals_NHSFoundation_Trust**: Secondary care data from the Bradford Teaching Hospitals NHS Trust \[Bradford and environs: ~1,700 individuals with data\]
3. **DSA__Discovery_7CCGs**: Primary care data from the North East London ICS \[North East London: ~45,000 individuals with data\]
4. **DSA_NHSDigital**: Data from from NHS Digital (NHSD) \[England-wide: ~TBC individuals with data].  Data files vary with each cut of NHSD but include one or more of: i) **civil registration data**, ii) **HES APC data**, iii) **HES OP data**, iv) **cancer registry data**, v) **ECDS data**

</details>

The source datafiles required are reasonably small **and `BI_PY` does not copy over any raw data from `../library-red/` to the ivm (cf. `QUANT_PY` which does)**.

The pipeline is constituted of 8 formal sequential python notebooks (`NB#1`, `NB#2`, etc.):
* **NB#1:** [1-create-clean-demographics-notebook.ipynb](Notebooks/1-create-clean-demographics-notebook)
* **NB#2:** 2-process-datasets-discovery-primary-care.ipynb
* **NB#3:** 3-process-datasets-barts-health.ipynb
* **NB#4:** 4-process-datasets-bradford.ipynb
* **NB#5:** 5-process-datasets-nhs-digital.ipynb
* **NB#6:** 6-merge-datasets-notebook.ipynb
* **NB#7:** 7-three-and-four-digit-ICD.ipynb
* **NB#8:** 8-custom-phenotypes.ipynb

The notebooks can be found in the [code](code) directory.

## Input files

The pipeline requires a single imput file.  This file is a `.csv` file which gives a set of SNOMED +/- ICD-10 +/- OPCS codes which constitute a phenotype.  The current version of this input file is `GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv`.  This file can be found in the [inputs](inputs) directory.

The custom codelist .csv file has 4 columns: `phenotype`, `code`, `name`, `comment`.

<details>
   
<summary>"GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv" file extract</summary>
  
```
phenotype, code, name, comment
GNH0002_CoronaryArteryDisease_narrow,I200,ICD10,Unstable angina,
GNH0002_CoronaryArteryDisease_narrow,I201,ICD10,Angina pectoris with documented spasm,
GNH0002_CoronaryArteryDisease_narrow,I208,ICD10,Other forms of angina pectoris,
[...]
GNH0002_CoronaryArteryDisease_narrow,K401,OPCS4,Saphenous vein graft replacement of one coronary artery,
GNH0002_CoronaryArteryDisease_narrow,K402,OPCS4,Saphenous vein graft replacement of two coronary arteries,
GNH0002_CoronaryArteryDisease_narrow,K403,OPCS4,Saphenous vein graft replacement of three coronary arteries,
[...]
GNH0002_CoronaryArteryDisease_narrow,I753000,SNOMED ConceptID,Old myocardial infarction,
GNH0002_CoronaryArteryDisease_narrow,22298000,SNOMED ConceptID,Heart attack,
GNH0002_CoronaryArteryDisease_narrow,22298000,SNOMED ConceptID,Myocardial infarction,
```

</details>

In `GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv` there are **285 binary trait codelists** from various users:
* **MULTIPLY-initiative**: 202 traits; e.g. Ankylosing_spondylitis \[ICD-10, SNOMED-CT, OPCS\]
* **GenomicsPLC/Consortium**: 16 traits; e.g. GNH0005_MyocardialInfarction_extended \[ICD-10, SNOMED-CT, OPCS\]
* **Bespoke researcher/research group**: 42 traits; e.g. MGH_MitralValveProlapse \[ICD-10 and/or SNOMED-CT and/or OPCS\]
* **NEW! NHS Primary Care Domain refsets**: 25 traits; e.g. QOF_CKD_COD \[SNOMED-CT only\]

## Locations/paths naming convention

1. We do not use relative paths.
2. We do not explicitly use the word FOLDER in the naming, so `MEGADATA_LOCATION`, not `MEGADATA_FOLDER_LOCATION`.
3. Locations and paths are in `UPPER_CASE`.
4. When referred to as `_LOCATION`, the variable contain a string with the path.
5. When referred to as `_PATH`, the variable is an `AnyPath` path object.
6. The folder order is "what it is" / "Where it's from" so, for example megadata/primary_care not primary_care/megadata; so `MEGADATA_PRIMARY_CARE_LOCATION` or `PROCESSED_DATASETS_PRIMARY_CARE_PATH`

## Pipeline steps
It is advisable to run the pipeline on a VM with lots of memory, typically an `n2d-highmem` 32 processor VM with 256Gb memory.

The pipeline is constituted of a series of independent python Jupyter notebooks.  They can be run individually but they are best run sequentially and contemporaneously.  To this effect, running the last cell in the notebook will save and close the current notebook and automatically open the next notebook.

Each notebook is described below.

> [!TIP]
> Many intermediary files are available in [`.arrow` format](https://arrow.apache.org/overview/)
>
> _(This link does not automatically open in a new window. Use CTRL+click (on Windows and Linux) or CMD+click (on MacOS) to open the link in a new window)_
> 

## `1-create-clean-demographics-notebook.ipynb`

This notebook is for processing the demographic dataset into a usable format. This is the first step in our process because when we come to cleaning and processing the other data that we have (notebook 2 to 5), we want to remove unrealistic data for example, events that happen before birth or in the future.

In this first notebook, we take in 2 different reference datasets that allow us to map demographic information (age and sex) to nhs_numbers.  We process these and save the output as a `.arrow` file for use in subsequent `BI_PY`
The reference datasets are:
* `2025_02_01__Megalinkage_forTRE.csv` which links ExWAS and GWAS identifiers to `pseudo_nhs_number`s
* `QMUL__Stage1Questionnaire/2025_04_25__S1QSTredacted.csv` which clarifies volunteer age and gender.

## `2-process-datasets-discovery-primary-care.ipynb`

### Coding system(s)
* SNOMED-CT
  
### Data cuts

There are 7 "cuts" of data representing 7 time periods when the data was made available. Each dataset comprises of at least two CSV files - a procedures and an observation file - but some have more.  At present, we only used the observation and procedures files.

* 2024_12: Observations, Procedures
* 2024_07: Observations, Procedures
* 2023_11: Observations, Procedures
* 2023_03: Observations, Procedures
* 2023_01: _Corrupted data, not used_
* 2022_12: Observations, Procedures
* 2022_04: Observations, Procedures

### Process

This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as `.arrow` files (binary) with their log files where appropriate.
This notebook: 
1. Processes primary care datasets (SNOMED codes)
2. Where possible, maps SNOMED codes to ICD-10 codes

Processing Primary Care Datasets
The primary care datasets are in SNOMED. 
We process datasets in the following way:

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
3. Save as `.arrow` file

## ` 3-process-datasets-barts-health.ipynb`

### Coding system(s)
* SNOMED-CT, ICD-10, OPCS
  
### Data cuts

There are 4 "cuts" of data.

* 2024_09: ICD10 (`RDE_APC_DIAGNOSIS`, `RDE_OP_DIAGNOSIS`), OPCS (`RDE_APC_OPCS`, `RDE_OPA_OPCS`), SNOMED (`RDE_MSDS`, `RDE_PC_PROBLEMS`, `RDE_PC_PROCEDURES`, `RDE_ALL_PROCEDURES`)
* 2023_12: ICD10 (`GH_APC_Diagnosis`, `GH_OP_Diagnosis`), OPCS (`GH_APC_OPCS`, `GH_OP_OPCS`), SNOMED (`GandH_PC_Diagnosis_Problems_Procedures`)
* 2023_05: ICD10 (`GH_APC_Diagnosis`, `GH_OP_Diagnosis`), OPCS (`GH_APC_OPCS`, `GH_OP_OPCS`), SNOMED (`GandH_PC_Diagnosis`, `GandH_PC_Problems`, `GandH_PC_Procedures`)
* 2022_03: ICD10 (`2022_05_23_icd10_combined_redacted.txt`), OPCS (`2022_05_23_opcs_combined_redacted.txt`), SNOMED (`GandH_PC_Diagnosis`, `GandH_PC_Problems`, `GandH_PC_Procedures`)

### Process

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

## ` 4-process-datasets-bradford.ipynb`

### Coding system(s)
* SNOMED-CT, ICD-10, OPCS
  
### Data cuts

There are 4 "cuts" of data.

* 2024_12: _no useable data in this cut_
* 2023_05: ICD10 (`_gh_diagnoses_`), OPCS (`_gh_procedures_`), SNOMED (`_gh_cerner_diagnoses_`, `_gh_cerner_problems_`)
* 2022_06: ICD10 (`_gh_diagnoses_`), OPCS (`_gh_procedures_`), SNOMED (`bradford_cerner_diagnoses_`, `bradford_cerner_problems_`)
* 2021_02: ICD10 (`icd10_bfs_`), OPCS (`opcs_bfd_`)

### Process

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

## `5-process-datasets-nhs-digital.ipynb`

### Codesets
* ICD-10 \[all non-ECDS\]
* SNOMED-CT \[ECDS\]

### Data cuts
* 2025_03: HES_APC, HES_OP, ECDS
* 2024_10: CIVREG, HES_APC, HES_OP, CANCER, ECDS
* 2023_07: CIVREG, HES_APC, HES_OP, CANCER, ECDS
* 2021_09: CIVREG, HES_APC, HES_OP

#### ECDS data

As of this release (version010), we now use ECDS (SNOMED-CT) data in `BI_PY`.  Because all other NHS Digital data sources used employ ICD-10, we collectively process all ECDS data together in the last step of the notebook.

### Process

We process the datasets in the following way:

Load the dataset in turn
Expand the wide files into multirow files so that one person can have multiple rows.
Deduplicate
Save
We will then remove unrealistic dates with previously saved Demographic data from notebook 1, and save again.

For some datasets, in particular the very wide datasets, there are a few issues which required some preprocessing. These steps will be removed in future where possible, when the custom library is updated. Where preprocessing has been required, the final pre-processed dataset is saved in a folder called `preprocessed_files`.

## `6-merge-datasets-notebook.ipynb`

### Codesets

* ICD-10, SNOMED-CT, OPCS
* *and* SNOMED mapped to ICD-10

### Data

There are no "cuts" here, this notebook uses the output of the primary care, Barts Trust, Bradford Trust and NHS Digital data-processing notebooks.

### Process

In this notebook, we upload the data files created in notebooks 2 to 5. We are aiming to get all the data of the same type together so we end up with (files in bold are in the `.../BI_PY/megadata/` directory):

* `ICD10 dataset`: \["Native" = `barts_icd` + `bratford_icd` + `nhs_d_icd` (there is no native ICD-10 data in `primary_care`)\] => **`icd_only.arrow`** 
* `OPCS dataset`: \[`barts_icd` + `bratford_icd` (only sources of OPCS codes are secondary care)\] => **`opcs_only.arrow`**
* `SNOMED dataset`: \[`primary_snomed` + `barts_icd` + `bratford_icd` + `nhs_d_icd`\] => **`snomed_only.arrow`**

In addition to this, we have mapped the SNOMED datasets to ICD10 and we will merge these in as well so we end up with:

* `SNOMED_MAPPED_AND_ICD dataset`: \[**`icd_only.arrow`** + `mapped_data_primary_icd` + `mapped_data_barts_icd` + `mapped_data_bradford_icd` + `mapped_data_nhs_digital_icd`\] => **`icd_and_mapped_snomed.arrow`**

The **`icd_and_mapped_snomed.arrow`** is processed (truncated to 3 characters) to produce **`icd_and_mapped_snomed_3_digit_deduplication.arrow`**

## `7-three-and-four-digit-ICD.ipynb`

### Codesets

* ICD-10 *and* SNOMED mapped to ICD-10

### Data

**`icd_and_mapped_snomed.arrow`**

### Process

This notebook creates individual trait files for ICD-10 3-digit and ICD-10 4-digit codes and regenie input files and co-variate files for ICD-10 3-digit codes.

#### individual trait files

The individual trait file .csv files have 8 columns: `nhs_number`, `date`, `code`, `age_at_event`, `dataset_type`, `codelist_type`, `gender`, `age_range`.

<details>
   
<summary>Individual trait file extract</summary>

```
nhs_number,date,code,age_at_event,dataset_type,codelist_type,gender,age_range
00...................................................18,20XX-XX-XX,E66,##.#,merged,ICD10,M,55-64
44...................................................20,20XX-XX-XX,E66,##.#,merged,ICD10,F,16-24
BC...................................................33,20XX-XX-XX,E66,##.#,merged,ICD10,F,45-54
9B...................................................86,20XX-XX-XX,E66,##.#,merged,ICD10,F,25-34
FD...................................................28,20XX-XX-XX,E66,##.#,merged,ICD10,M,65-74
94...................................................34,20XX-XX-XX,E66,##.#,merged,ICD10,F,35-44
6E...................................................03,20XX-XX-XX,E66,##.#,merged,ICD10,M,45-54
A1...................................................32,20XX-XX-XX,E66,##.#,merged,ICD10,F,25-34
```
</details>

#### regenie files
regenie files are created for the 51k GWAS and 55k ExWAS datasets; each has an associated **age at first diagnosis** co-variate file:

51k GWAS:
* `2025_05_icd10_3d_regenie_51koct2024_65A_Topmed.tsv` (regenie input file)
* `2025_05_regenie_51koct2024_65A_Topmed_Binary_3-digit_ICD-10_age_at_first_diagnosis_megawide.tsv` (co-variate file)

55kExWAS:
* `2025_05_icd10_3d_regenie_55k_BroadExomeIDs.tsv`
* `2025_05_regenie_55k_BroadExomeIDs_Binary_3-digit_ICD-10_age_at_first_diagnosis_megawide.tsv`

#### ICD-10 clean-up procedure

The ICD-10 codes in **`icd_and_mapped_snomed.arrow`** are "cleaned-up".  

> [!TIP]
> Letter suffixes after ICD-10 codes are not necessarily invalid, they are used for additional indication such as diagnostic certainty or affected side of body.
> See: [https://gesund.bund.de/en/icd-code-search/g01](https://gesund.bund.de/en/icd-code-search/g01)
> 
> Additional indicators:
> On medical documents, the ICD code is often appended by letters that indicate the diagnostic certainty or the affected side of the body.
> * G: Confirmed diagnosis
> * V: Tentative diagnosis
> * Z: Condition after
> * A: Excluded diagnosis
> 
> * L: Left
> * R: Right
> * B: Both sides

We have some codes appended with "D" which seems to be invalid (although appears in Google searches). We could (and indeed should) simply remove terminal B-Z characters and if terminal character is A delete the row as this represents an "Excluded diagnosis".

The ICD-10 field clean-up is therefore performed as follows:
* Remove all spaces
* Exclude icd10 = "NA" rows
* Exclude icd10 codes < 3 char length (minimum valid icd10 is 3 chars)
* Exclude icd10 codes not starting with a letter
* Exclude icd10 codes ending with an "A" rows; "A" suffixes represent "Excluded diagnosis"
* Remove B-Z characters at end of icd10 code
* Remove "X" and "." and "-"
* Format to "XXX.X" if dots=True
* Keeping up to 4 meaningful characters

## `8-custom-phenotypes.ipynb`

### Codesets

* ICD-10, SNOMED-CT and OPCS
* Please note that when considering custom phenotypes, we do **NOT** used ICD-10 codes obtained through mapping (i.e. we use only "native" ICD-10 codes)

### Data

Custom phenotypes are defined in the `.../BI_PY/inputs/GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv` file.
Individual to code mapping comes from **`icd_only.arrow`** + **`opcs_only.arrow`** + **`snomed_only.arrow`**
**`icd_and_mapped_snomed.arrow`**

### Process

This notebook creates individual trait files and regenie input files and co-variate files  for custom phenotypes.

We import the ICD-10, SNOMED-CT and OPCS mapping dataframes and join these to megadata files.
We then deduplicate and "tidy-up" and save one individual_trait_file per phenotype.

#### individual trait files

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

#### regenie files
regenie files are created for the 51k GWAS and 55k ExWAS datasets; each has an associated **age at first diagnosis** co-variate file:

51k GWAS:
* `2025_05_custom_phenotypes_regenie_51koct2024_65A_Topmed.tsv`
* `2025_05_regenie_51koct2024_65A_Topmed_Binary_custom_phenotypes_age_at_first_diagnosis_megawide.tsv`

55k ExWAS:
* `2025_05_custom_phenotypes_regenie_55k_BroadExomeIDs.tsv` (regenie input file)
* `2025_05_regenie_55k_BroadExomeIDs_Binary_custom_phenotypes_age_at_first_diagnosis_megawide-digit_ICD-10_age_at_first_diagnosis_megawide.tsv` (co-variate file)

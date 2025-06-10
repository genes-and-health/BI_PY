# `BI_PY`: a python pipeline for binary trait attribution in Genes & Health

<img src="GNH%20TD%20Logo.png" alt="G&H Team Data logo" width=25%>

## Authors

* Stuart Rison
* Mike Samuels

Derived from the python/tre-tools pipeline written by Caroline Morton and Saeed Bidi and contributions from:
| <!-- --> | <!-- --> | <!-- --> |
|----------|----------|----------|
| Sarah Finer | Sam Hodgson | Ben Jacobs |
| Stravoula Kanoni | Rohini Mathur | Daniel Stow |
| David van Heel | Julia Zollner | and other G&H Team Data members |

## Summary

The Genes & Health (G&H) `BI_PY` pipeline extracts and processes binary trait data from G&H phenotype data.  Relevant codes may be SNOMED-CT, ICD-10 or OPCS codes.  `BI_PY` "runs" a list of such per-binary-phenotype codes against any code associated with a G&H volunteer.  If the volunteer has one or more such codes, they are associated with the relevant binary phenotye --allowing case-control type analyses against volunteers not associated with the bianry phenotype.

The pipeline creates files and covariate files suitable for `regenie` \[G/Ex\]WAS analysis, as well as generic files for each binary trait at a _per individual_ level (one row per individual summarising the individual's earliest applicable code and age at first diagnosis).

The pipeline is constituted of 8 formal sequential python notebooks (`NB#1`, `NB#2`, etc.) each described separately:

* [**1-create-clean-demographics-notebook**](Notebooks/1-create-clean-demographics-notebook.md) \[code: [.ipynb](Code/notebooks/1-create-clean-demographics-notebook.ipynb) | [.py](Code/python_scripts/1-create-clean-demographics-notebook.py)\] 
* [**2-process-datasets-discovery-primary-care**](Notebooks/2-process-datasets-discovery-primary-care.md) \[code: [.ipynb](Code/notebooks/2-process-datasets-discovery-primary-care.ipynb) | [.py](Code/python_scripts/2-process-datasets-discovery-primary-care)\] 
* [**3-process-datasets-barts-health**](Notebooks/3-process-datasets-barts-health.md) \[code: [.ipynb](Code/notebooks/3-process-datasets-barts-health.ipynb) | [.py](Code/python_scripts/3-process-datasets-barts-health.py)\] 
* [**4-process-datasets-bradford**](Notebooks/4-process-datasets-bradford.md) \[code: [.ipynb](Code/notebooks/4-process-datasets-bradford.ipynb) | [.py](Code/python_scripts/4-process-datasets-bradford.py)\] 
* [**5-process-datasets-nhs-digital**](Notebooks/5-process-datasets-nhs-digital.md) \[code: [.ipynb](Code/notebooks/5-process-datasets-nhs-digital.ipynb) | [.py](Code/python_scripts/5-process-datasets-nhs-digital.py)\] 
* [**6-merge-datasets-notebook**](Notebooks/6-merge-datasets-notebook.md) \[code: [.ipynb](Code/notebooks/6-merge-datasets-notebook.ipynb) | [.py](Code/python_scripts/6-merge-datasets-notebook.py)\] 
* [**7-three-and-four-digit-ICD**](Notebooks/7-three-and-four-digit-ICD.md) \[code: [.ipynb](Code/notebooks/7-three-and-four-digit-ICD.ipynb) | [.py](Code/python_scripts/7-three-and-four-digit-ICD.py)\] 
* [**8-custom-phenotypes**](Notebooks/8-custom-phenotypes.md) \[code: [.ipynb](Code/notebooks/8-custom-phenotypes-individual-trait-files-and-regenie.ipynb) | [.py](Code/python_scripts/8-custom-phenotypes-individual-trait-files-and-regenie.py)\] 

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

## Input files

The pipeline requires a single input file.  This file is a `.csv` file which gives a set of SNOMED +/- ICD-10 +/- OPCS codes which constitute a phenotype.  The current version of this input file is `GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv`.  This file can be found in the [inputs](inputs) directory.

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

Click [`GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv`](https://docs.google.com/spreadsheets/d/1ipwdF2j_owfr_QbkDYk1rk0TW3KtdfQYVQn-Vf-o38s/edit?usp=sharing) to access a Google document detailing the binary trait codelists (tab `v010_2025_05_BinaryTraits_Custom_Codelists`)

## Output files

All final output is saved to an `outputs` directory at the root level of the `/versionxyz-YYYY-mm/BI_PY` directory 

If `outputs` is not present, the pipeline creates the directory at the root level of the `/versionxyz-YYYY-mm/BI_PY` directory when required.

The `outputs` directory hierarchy is as follows:

```
outputs  
├── custom_phenotypes  
│   ├── individual_trait_files  
│   └── regenie  
└── icd10  
    ├── individual_trait_files  
    │   ├── 3_digit_icd  
    │   └── 4_digit_icd  
    └── regenie
```

## Locations/paths naming convention

1. We do not use relative paths.
2. We do not explicitly use the word FOLDER in the naming, so `MEGADATA_LOCATION`, not `MEGADATA_FOLDER_LOCATION`.
3. Locations and paths are in `UPPER_CASE`.
4. When referred to as `_LOCATION`, the variable contain a string with the path.
5. When referred to as `_PATH`, the variable is an `AnyPath` path object.
6. The folder order is "what it is" / "Where it's from" so, for example megadata/primary_care not primary_care/megadata; so `MEGADATA_PRIMARY_CARE_LOCATION` or `PROCESSED_DATASETS_PRIMARY_CARE_PATH`

## Running the pipeline
It is advisable to run the pipeline on a VM with lots of memory, typically an `n2d-highmem` 32 processor VM with 256Gb memory.

The pipeline is constituted of a series of independent python Jupyter notebooks.  They can be run individually but they are best run sequentially and contemporaneously.  To this effect, running the last cell in the notebook will save and close the current notebook and automatically open the next notebook.

> [!TIP]
> Many intermediary files are available in [`.arrow` format](https://arrow.apache.org/overview/)
>
> _(This link does not automatically open in a new window. Use CTRL+click (on Windows and Linux) or CMD+click (on MacOS) to open the link in a new window)_
> 


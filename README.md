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
* **NB#1:** 1-create-clean-demographics-notebook.ipynb
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
3. Location and path are in `UPPER_CASE`.
4. When referred to as `_LOCATION`, the variable contain a string with the path.
5. When referred to as `_PATH`, the variable is an `AnyPath` path object.
6. The folder order is "what it is" / "Where it's from" so, for example megadata/primary_care not primary_care/megadata; so `MEGADATA_PRIMARY_CARE_LOCATION` or `PROCESSED_DATASETS_PRIMARY_CARE_PATH`

## Pipeline steps
It is advisable to run the pipeline on a VM with lots of memory, typically an `n2d-highmem` 32 processor VM with 256Gb memory.

The pipeline is constituted of a series of independent python Jupyter notebooks.  They can be run individually but they are best run sequentially and contemporaneously.  To this effect, running the list cell in the notebook will save and close the current notebook and automatically open the next notebook.

Each notebook is described below.

> [!TIP]
> Many intermediary files are available in [`.arrow` format](https://arrow.apache.org/overview/)
>
> _(This link does not automatically open in a new window. Use CTRL+click (on Windows and Linux) or CMD+click (on MacOS) to open the link in a new window)_
> 

### `1-create-clean-demographics-notebook.ipynb`

This notebook is for processing the demographic dataset into a usable format. This is the first step in our process because when we come to cleaning and processing the other data that we have (notebook 2 to 5), we want to remove unrealistic data for example, events that happen before 1910 or in the future.

In this first notebook, we take in 2 different reference datasets that allow us to map demographic information (age and sex) to nhs_numbers.  We process these and save the output as a `.arrows` file for use in subsequent `BI_PY`
The reference datasets are:
* `2025_02_01__Megalinkage_forTRE.csv` (which links ExWAS and GWAS identifiers to `pseudo_nhs_number`s)
* `QMUL__Stage1Questionnaire/2025_04_25__S1QSTredacted.csv` which clarifies volunteer age and gender.

### `2-process-datasets-discovery-primary-care.ipynb`

This notebook is for processing the various datasets we have. We take in raw datasets, and we output processed datasets as `.arrow` files (binary) with their log files where appropriate.
This notebook: 
1. Processes primary care datasets (SNOMED codes)
2. Where possible, maps SNOMED codes to ICD-10 codes

Processing Primary Care Datasets
The primary care datasets are in SNOMED. There are 7 "cuts" of data representing 7 time periods when the data was made available. Each dataset comprises of at least two CSV files - a procedures and an observation file - but some have more.
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

### ` 3-process-datasets-barts-health.ipynb`

In third second notebook, we do the following:

1. Process Barts healthcare datasets - SNOMED
2. Process Barts healthcare dataset - ICD10
3. Process Barts healthcare datatst - OPCS
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

### ` 4-process-datasets-bradford.ipynb`

The Bradford datasets are in SNOMED, ICD10 and OPCS. There are 3 "cuts" of data representing 3 time periods when the data was made available. Each dataset comprises of multiple CSV files.

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

# GRAVEYARD BELOW
NB. processing of Bradford data now takes place in notebook #4.

STEP 1: Import phenotype files with appropriate pre-processing
`R` is very good at handling "raggedness" but in doing so, it makes assumptions.  This can lead to the "wrong" data ending in a column.  Python can also import .csv/.tsv/.tab files and make assumptions about the seprators/raggedness/column data type but in `QUANT_PY` this is intentionally and explicitly avoided.  This means that some files need to be pre-processed.  This take the form of one or more of the following pre-processing operations:

<details>

   <summary>Pre-processing operations</summary>
   
   1. **Commas in double-quotes stripping**: Exclude any row with double quoted text with one or more commas in in.  This excludes <2% of rows but means that the parsing behaviour is consistent and predictable.
   2. **Exclude "unterminated" double-quotes**: Some rows include a double-quote not paired with a second double-quote before the next separator.  In such cases, the importing functions often "glob" all text in subsequent rows until another double-quote is found.  Therefore, these rows are excluded.
   3. **Excluded rows with non-standard number of fields**: some rows may have additional/fewer separators either intentionally or erroneously creating additional/deleting fields.  `QUANT_PY` rejects any lines with a non-standard number of separators.
   4. **Strip double-quote**: This can be applied to non comma-delimited data files.  In some such files, double-quotes can appear singly ("), doubly ("") or even triply (""")
      
</details>

Processed files are listed in [Appendix A](#appendix-a-list-of-processed-phenotype-files).

#### Intermediary `.arrow` files
These can be useful for debugging purposes or for researchers interested in phenotypic data from a specific source.

<details>

   <summary>Per provenance `.arrow` files</summary>
   
   These can be found in the following directories (with an examplar `.arrow` file listed for each directory:
   * **`.../data/primary_care/arrow/`**: `2024_12_Discover_path.arrow`
   * **`.../data/secondary_care/arrow`**: `2023_05_Bradford_measurements.arrow`
   * **`.../data/secondary_care/nda`**: `2025_04_formatted_nda.arrow`

</details>

### STEP 2: Progressively merge files to create COMBO file

Files are merged in the following order (with deduplication after every merge operation).  Again, intermediate file directories are available as listed:
1. Primary care data + NDA data (primary care data): **`.../data/combined_datasets/`**: `YYYY_MM_Combined_primary_care.arrow`
2. Barts data + Bradford data (secondary care data): **`.../data/combined_datasets/`**: `YYYY_MM_Combined_secondary_care.arrow`

Finally, primary and secondary care data are merged.

> [!TIP]
> The final output of the multiple merges is considered a key output of the pipeline and is therefore stored in the **`.../outputs/`** directory:
> 
>  **`../outputs/`**: `YYYY_MM_Combined_all_sources.arrow`
>
> The dataframe is referred to as the **COMBO**.

On 2025-04-01, the **COMBO** `2025_04_Combined_all_sources.arrow` was `78,582,474` rows long.

### STEP 3: Add hospitalisation status column

#### Import HES data

Admitted Patient Care (APC) episodes are extracted from HES data pulls of 2021-09, 2023-07, 2024-10, and 2025-03. HES APC data are imported, cleaned up and deduplicated.

> [!TIP]
> The output of the HES APC pulls merges and deduplication are in the **`.../data/combined_datasets/`** directory:
> 
>  **`.../data/combined_datasets/`**: `YYYY_MM_Combined_HES.arrow`

#### Flagging COMBO result dates

The HES dataset is used to define three APC `region_types`:
* **APC**: a date span for the APC
* **buffer_before**: a date span of 14d prior to addmission date
* **buffer_after**: a date span of 14d after discharge date

**COMBO** test results are flagged to none (`null`) if they fall out of the above listed three region types, or to one or more of the region types, by joining the APC data to **COMBO**.  For example, a date may exist within an APC period (flagged as `["APC"]`), or within an APC and a buffer_before (for example if the date falls both within an APC and within the buffer_before of a subsequent APC; flagged as `["APC", "buffer_before"]`).

By extension, test result dates can be classifed in one of 11 (some non-mutually exclusive) categories.

<details>
   <summary>Test result data categories</summary>
   
   1. **`IN_APC_ONLY`**: test results collected in an actual hospitalisation episode not overlapping with the buffer of another APC
   2. **`IN_APC_ANY`**: test results collected in an actual hospitalisation episode (which may overlap another APC's 14d buffer)
   3. **`IN_BUFFER_BEFORE_ONLY`**: test results collected within the 14d prior to a hospitalisation episode (not overlapping with another APC or another APC's buffer)
   4. **`IN_BUFFER_BEFORE_ANY`**: test results collected within the 14d prior to a hospitalisation episode (may overlap with an APC)
   5. **`IN_BUFFER_AFTER_ONLY`**: test results collected within the 14d following a hospitalisation episode (not overlapping with another APC or another APC's buffer)
   6. **`IN_BUFFER_AFTER_ANY`**: test results collected within the 14d following a hospitalisation episode (may overlap with an APC)
   7. **`IN_BUFFERS_ONLY`**: test results collected _either_ within the 14d prior to, or following, a hospitalisation episode _and_ not during the actual hospitalisation period
   8. **`IN_BUFFERS_ANY`**: test results collected _either_ within the 14d prior to, or following, a hospitalisation episode (may overlap with one or more APCs)
   9. **`IN_TOTAL_EXCLUSION_ZONE`**: test results collected within a period from 14 days prior to a hospitalisation episode to 14 days following a hospitalisation episode _including_ the hospitalisation period
   10. **`OUT_OF_APC`**: test results collected outside of any APC hospitalisation episode
   11.  **`OUT_OF_TOTAL_EXCLUSION_ZONE`**: test results collected outside of any buffered hospitalisation episode (hospitalisation episode + 14 days either side)   

</details>

In practice, only three possible date statuses are considered: "all" (all quantitative results), "out_hospital" (i.e. `OUT_OF_TOTAL_EXCLUSION_ZONE`) and "in_hospital" (i.e. `IN_TOTAL_EXCLUSION_ZONE`) 

### STEP 4: Perform unit conversions and flag out-of-range **COMBO** results

COMBO is joined to a denormalised traits dataframe (`traits_features` x `trait_aliases`) which identifies COMBO row with traits to extract, their target units and their valid range.  **Unit conversions are performed where possible and applicable** and final results are flagged as:

* **`below_min`**: result lower than the minimum value set for this trait.  These will subsequently be excluded.
* **`ok`**: result within valid range for this trait.
* **`above_max`**: result higher than the maximum value set for this trait.  These will subsequently be excluded.

> [!NOTE]
> HbA1c values in `%` (percentages) are converted to values in `millimol/mol` using the following equation: $mmol\/mol \[IFCC\] =  (10.93*percentage \[NGSP/UKPD\]) - 23.50$  
> See [National Glycohemoglobin Standardization Program](https://ngsp.org/ifccngsp.asp) 
> _(This link does not automatically open in a new window. Use CTRL+click (on Windows and Linux) or CMD+click (on MacOS) to open the link in a new window)_ 
> 
> Because percentages apply to traits other than HbA1c, this conversion cannot be performed using the `unit_conversions.csv` and needs to be "hard-coded" in the pipeline.  

### Step 5: COMBO restricted to valid pseudoNHS numbers and valid demographics

When volunteers take part in stage 1 of Genes & Health, their questionnaire and consent form is labelled with the ID number on the Oragene saliva tube (style: `15001502031604`). These Oragene IDs are then used to label genetic samples (e.g. GSA chip or exome seq). They also label the Questionnaire (aka `S1QT`). Some people have taken part twice (or more than twice) over the years in Genes & Health, and will have a different Oragene ID each time.  The **OrageneID** is the link to genetic data, the **pseudoNHSnumber** is the link to phenotypic data.

Step 5 uses a `YYYY_MM_DD_MegaLinkage_forTRE.csv`&trade; source file to allow these linkages.

<details>
   <summary>MegaLinkage&trade; file columns</summary>
   
   * **OrageneID**: 14 digit unique OrageneID
   * **Number of OrageneIDs with this NHS number (i.e. taken part twice or more)**: 1, 2, 3 or 4.  Typically 1 (single participation), 2 (~10% individuals), 3 (~0.7% individuals), 4 (~0.05% individuals)
   * **S1QST gender**: 1=male, 2=female
   * **HasValidNHS**: "yes", "no"
   * **pseudonhs_2024-07-10**: pseudoNHSnumber
   * **51176GSA-T0PMedr3 Jan2024release**: GSA identifier (OrageneID_GSAID_RunID; OrageneID as above + '\_' + GSAID = 12digit identified + '\_' + RunID = row/col in format RxxCxx where x = digit)
   * **44628exomes_release_2023-JUL-07**: Broad Institute Exome sequencing ID (= GNH-+OrageneID)
   * **55273exomes_release_2024-OCT-08**: Broad Institute Exome sequencing ID (= GNH-+OrageneID)
   
</details>

<details>
   <summary>Extract from <code>2025_02_10_MegaLinkage_forTRE.csv</code>&trade; [redacted]</summary>

   ```
   OrageneID,Number of OrageneIDs with this NHS number (i.e. taken part twice or more),S1QST gender,HasValidNHS,pseudonhs_2024-07-10,51176GSA-T0PMedr3 Jan2024release,44628exomes_release_2023-JUL-07,55273exomes_release_2024-OCT-08
   1xxxxxxxxxx2,1,1,yes,2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx0,1xxxxxxxxxx2_2xxxxxxxxxx2_Rxxxx2,GH-1xxxxxxxxxx2,GMH-1xxxxxxxxxx2
   1xxxxxxxxxx0,1,2,yes,8xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx0,1xxxxxxxxxx0_2xxxxxxxxxx2_Rxxxx2,GH-1xxxxxxxxxx2,GMH-1xxxxxxxxxx0
   1xxxxxxxxxx2,1,1,yes,9xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx8,1xxxxxxxxxx2_2xxxxxxxxxx2_Rxxxx2,GH-1xxxxxxxxxx2,GMH-1xxxxxxxxxx2
   1xxxxxxxxxx0,1,2,yes,8xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx5,1xxxxxxxxxx0_2xxxxxxxxxx2_Rxxxx2,GH-1xxxxxxxxxx2,GMH-1xxxxxxxxxx0
   1xxxxxxxxxx0,1,1,yes,0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx8,1xxxxxxxxxx0_2xxxxxxxxxx2_Rxxxx2,GH-1xxxxxxxxxx2,GMH-1xxxxxxxxxx0
   1xxxxxxxxxx7,1,1,yes,8xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx0,1xxxxxxxxxx7_2xxxxxxxxxx2_Rxxxx1,GH-1xxxxxxxxxx7,GMH-1xxxxxxxxxx7
   1xxxxxxxxxx8,1,2,yes,4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx4,1xxxxxxxxxx8_2xxxxxxxxxx2_Rxxxx2,GH-1xxxxxxxxxx8,GMH-1xxxxxxxxxx8
   ```

</details>

#### Filter to valid pseudoNHS number
There are approximately 1,000 rows excluded by this pseudoNHS validation.
Possible reasons:

1. subject asked to be removed/withdrawn
2. subject died
3. subject had multiple pseudoNHSnumbers which have been merged

#### Filter to valid demographics
Use `MegaLinkage file`$trade; to link to Stage 1 questionnaire data found in `/library-red/genesandhealth/phenotype_raw_data/QMUL__Stage1Questionnaire`.  This gives patient MONTH/YEAR of birth.  All volunteers are assumed to be born on the first day of a month.  Results are excluded if age at test is less than or equal to zero (i.e. result before birth of volunteer), or the result is dated beyond the date of the run execution (i.e. results dated to the future).

Results obtained prior to 16 years of age are also excluded.

#### Filter to within-range values
Only rows with a `range_position` (as defined in [STEP 4](#step-4-perform-unit-conversions-and-flag-out-of-range-combo-results)) equal to `ok` (cf. `below_min` and `above_max`) are kept.

### STEP 6: Window data in 10-day windows
Because the same quantitative result can come from multiple sources with a similar but non-identical date (e.g. a secondary care result is registered in an individual's primary care record with the date it was received in primary care rather than the actual test result date), the quantitative data are "windowed".

Non-overlapping 10-day windows are applied and any identical test results within these windows are deduplicated even if the result dates differ.  The earliest instance of the result is kept.

### STEP 7: Generate output files
These can all be found in the **`.../outputs/`** directory

## OUTPUT FILES
The following files are generated from the QCed **COMBO** generated in **STEP 6**

1. **per trait files** \[`../outputs/individual_trait_files/`; subdirectories: `in_hospital`, `out_hospital`, `all`\]:
     - **`_{trait}_readings_at_unique_timepoints.csv`**: one validated result per row (columns: `pseudo_nhs_number, trait, unit, value, date, gender, age_at_test, minmax_outlier`) 
     - **`_{trait}_per_individual_stats.csv`**: one row per volunteer (`pseudo_nhs_number, trait, median, mean, max, min, earliest, latest, number_observations`)
2. **per trait plots** \[`../outputs/individual_trait_plots/`; subdirectories: `in_hospital`, `out_hospital`, `all`\]:
      - **`_{trait}_{setting}.svg`**: Histograms of trait log10(values) for trait separated M and F listing median, mean, min, max, number individuals, number observations
3. **regenie files** \[`../outputs/regenie/`; subdirectories: `in_hospital`, `out_hospital`, `all` and `covariate_files`\]:
      - **`_{trait}_{setting}_[regenie_51|regenie_55].tsv`**: regenie files for 51kGWAS and 55kExome analyses
      - **`./covariate_files/_{setting}_[regenie_51|regenie_55]_megawide.tsv`**: regenie covariate files allowing age at test analyses (cf. age on joining Genes and Health)
4. **reference COMBO files** \[`../outputs/reference_combo_files/`\]:
      - **`_Combined_all_sources.arrow`**: the "raw" merger of primary, secondary and NDA data.  No QC, no restriction to the 111 traits extracted in `version010_2025_04`
      - **`_Combined_traits_NHS_and_demographics_restircted_pre_10d_windowing`**: above file processed to limit to valid NHS number, valid demographics and valid values but _not_ windowed (end of **STEP 5**)
      - **`_Combined_traits_NHS_and_demographics_restircted_post_10d_windowing`**: above file processed to limit to valid NHS number, valid demographics and valid values _and_ windowed (end of **STEP 6**)


# Appendix A: List of processed phenotype files
```
# Primary care
.../DSA_Discovery_7CCGs/2022_04_Discovery/GNH_thw/nech-phase2-outfiles_merge/GNH_thw/nech_observations_output_dataset_20220423.csv
.../DSA_Discovery_7CCGs/2022_04_Discovery/GNH_bhr-phase2-outfiles_merge/GNH_bhr_observations_output_dataset_20220412.csv
.../DSA_Discovery_7CCGs/2022_12_Discovery/GNH_thw/nech-phase2-outfiles_merge/gh2_observations_output_dataset_20221207.csv
.../DSA_Discovery_7CCGs/2022_12_Discovery/GNH_bhr-phase2-outfiles_merge/gh2_observations_dataset_20221207.csv
.../DSA_Discovery_7CCGs/2023_04_Discovery/gh3_observations.csv
.../DSA_Discovery_7CCGs/2023_11_Discovery/gh3_observations.csv
.../DSA_Discovery_7CCGs/2024_04_Discovery/gh3_observations.csv
.../DSA_Discovery_7CCGs/2024_12_Discovery/gh3_observations.csv

# NDA
.../DSA_NHSDigitalNHSEngland/2024_10/NDA/NIC338864_NDA_BMI.txt
.../DSA_NHSDigitalNHSEngland/2024_10/NDA/NIC338864_NDA_CHOL.txt
.../DSA_NHSDigitalNHSEngland/2024_10/NDA/NIC338864_NDA_HBA1C.txt
.../DSA_NHSDigitalNHSEngland/2024_10/NDA/NIC338864_NDA_BP.txt

# Secondary care -- Bradford
.../DSA_BradfordTeachingHospitals_NHSFoundation_Trust/2023_05_BTHFT/1578_gh_lab_results_2023-06-09_noCR.ascii.redacted.tab
.../DSA_BradfordTeachingHospitals_NHSFoundation_Trust/2024_12_BTHFT/1578_gh_lab_results_2024-12-05.ascii.redacted.tab
.../DSA_BradfordTeachingHospitals_NHSFoundation_Trust/2022_06_BTHFT/1578_gh_cerner_measurements_2022-06-10_redacted.tsv
.../DSA_BradfordTeachingHospitals_NHSFoundation_Trust/2024_12_BTHFT/1578_gh_cerner_measurements_2024-12-05.ascii.redacted.tab

# Secondary care -- Barts
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Candida_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/SHBG_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Fasting_Glucose.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/WhiteBloodCellCount_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Monocytes_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Basophil_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/HDL_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/DHEA_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Random_Glucose.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/TotalCholesterol_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Insulin_Antibodies.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/BileAcidSerum_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/ApolipoproteinB100_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/TriglycerideSerum_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Testosterone_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/RBC_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Lymphocytes_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Eosinophil_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/MCV_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/LDL_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/ApolipoproteinA1_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Neutrophils_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Haematocrit_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/MCHC_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Platelet_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Oestradiol_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/GAD_Antibodies.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/HbA1c_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2021_04_PathologyLab/Prolactin_April2021.csv
.../DSA_BartsHealth_NHS_Trust/2022_03_ResearchDatasetv1.5/GH_Pathology_20220319T143_redacted_noHistopathologyReport.csv
.../DSA_BartsHealth_NHS_Trust/2023_05_ResearchDatasetv1.5/GH_Pathology_20230517T061.ascii_redacted.nohisto.tab
.../DSA_BartsHealth_NHS_Trust/2023_12_ResearchDatasetv1.6/GH_Pathology_20231218.ascii.nohisto.redacted2.tab
.../DSA_BartsHealth_NHS_Trust/2024_09_ResearchDataset/RDE_Pathology_ascii.nohisto.redacted2.csv
.../DSA_BartsHealth_NHS_Trust/2023_05_ResearchDatasetv1.5/GandH_Measurements_20230512T304.ascii.redacted.tab
.../DSA_BartsHealth_NHS_Trust/2023_12_ResearchDatasetv1.6/GandH_Measurements_20240423.ascii.redacted2.tab
.../DSA_BartsHealth_NHS_Trust/2024_09_ResearchDataset/RDE_Measurements.ascii.redacted2.tab
```

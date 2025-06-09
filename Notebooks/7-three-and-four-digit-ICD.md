# `7-three-and-four-digit-ICD.ipynb`

## Codesets

* ICD-10 *and* SNOMED mapped to ICD-10

## Data

**`icd_and_mapped_snomed.arrow`**

## Process

This notebook creates individual trait files for ICD-10 3-digit and ICD-10 4-digit codes and regenie input files and co-variate files for ICD-10 3-digit codes.

## individual trait files

The individual trait file .csv files have 8 columns: `nhs_number`, `date`, `code`, `age_at_event`, `dataset_type`, `codelist_type`, `gender`, `age_range`.

<details>
   
<summary>Individual trait file extract</summary>

```
nhs_number,date,code,age_at_event,dataset_type,codelist_type,gender,age_range
00...................................................18,20XX-XX-XX,E66,#.#,merged,ICD10,M,55-64
44...................................................20,20XX-XX-XX,E66,#.#,merged,ICD10,F,16-24
BC...................................................33,20XX-XX-XX,E66,#.#,merged,ICD10,F,45-54
9B...................................................86,20XX-XX-XX,E66,#.#,merged,ICD10,F,25-34
FD...................................................28,20XX-XX-XX,E66,#.#,merged,ICD10,M,65-74
94...................................................34,20XX-XX-XX,E66,#.#,merged,ICD10,F,35-44
6E...................................................03,20XX-XX-XX,E66,#.#,merged,ICD10,M,45-54
A1...................................................32,20XX-XX-XX,E66,#.#,merged,ICD10,F,25-34
```
</details>

## regenie files
regenie files are created for the 51k GWAS and 55k ExWAS datasets; each has an associated **age at first diagnosis** co-variate file:

51k GWAS:
* `2025_05_icd10_3d_regenie_51koct2024_65A_Topmed.tsv` (regenie input file)
* `2025_05_regenie_51koct2024_65A_Topmed_Binary_3-digit_ICD-10_age_at_first_diagnosis_megawide.tsv` (co-variate file)

55kExWAS:
* `2025_05_icd10_3d_regenie_55k_BroadExomeIDs.tsv`
* `2025_05_regenie_55k_BroadExomeIDs_Binary_3-digit_ICD-10_age_at_first_diagnosis_megawide.tsv`

## ICD-10 clean-up procedure

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

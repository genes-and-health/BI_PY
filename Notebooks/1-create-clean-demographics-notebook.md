# `1-create-clean-demographics-notebook.ipynb`

This notebook is for processing the demographic dataset into a usable format. This is the first step in our process because when we come to cleaning and processing the other data that we have (notebook 2 to 5), we want to remove unrealistic data for example, events that happen before birth or in the future.

In this first notebook, we take in 2 different reference datasets that allow us to map demographic information (age and sex) to nhs_numbers.  We process these and save the output as a `.arrow` file for use in subsequent `BI_PY`
The reference datasets are:
* `2025_02_01__Megalinkage_forTRE.csv` which links ExWAS and GWAS identifiers to `pseudo_nhs_number`s
* `QMUL__Stage1Questionnaire/2025_04_25__S1QSTredacted.csv` which clarifies volunteer age and gender.

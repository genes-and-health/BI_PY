#!/usr/bin/env python
# coding: utf-8

# ## 8-custom-phenotypes-individual-trait-files-and-regenie -- Plan
# 
# This notebook does not use tretools either to generate `individual_trait_files` or to generate `regenie` files.  Instead it uses "pure" polars which means tit is a lot faster and easier to "debug" and check.
# 
# We import the ICD-10, SNOMED-CT and OPCS mapping dataframes and join these to `megadata` files.
# 
# We then deduplicate and "tidy-up" and save one `individual_trait_file` per phenotype.
# 
# We uses a non-tretools method to create both `individual_trait_file`s and the `regenie` files
# 
# We discovered an issue with `GenesAndHealth_custombinary_codelist_v010_2025_05v3.csv` (non-standard characters and duplicated lines), these were corrected and now we use `GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv`
# 
# We have verifed that our methods and the TREtools based methods create the same output **if individuals with no assigned phenotypes (i.e. 0 out of 287 custom phenotypes assigned) are excluded**
# 
# On 2025-05-23, there were 287 custom phenotypes.

# In[ ]:


VERSION = 'version010_2025_05_SR'


# In[ ]:


# version = "version010"
mon = "05"
yr = "2025"


# In[ ]:


import polars as pl
from cloudpathlib import AnyPath
from datetime import datetime
import itertools


# ### Scripting for automated next notebook initation

# In[ ]:


from IPython.display import Javascript


# In[ ]:


def redirect_to_next_notebook_in_pipeline(other_notebook):
    
    js_code = f"""
    if (typeof Jupyter !== 'undefined' && Jupyter.notebook && Jupyter.notebook.kernel) {{
        // only runs when cell is executed, not from cached output
        console.log("Redirecting to next notebook in pipeline...");
        Jupyter.notebook.save_checkpoint();
        Jupyter.notebook.session.delete();
        
        setTimeout(function() {{
            window.location.href = '{other_notebook}.ipynb';
        }}, 1500)
    }} else {{
        console.log("Found cached output. Not an active notebook context. Skipping redirect.")
    }}
    """
    display(Javascript(js_code))


# In[ ]:


# polars namespace additions

# In subsequent version this code may be integrated with the establisted TRE Tools package

@pl.api.register_lazyframe_namespace("TRE")
class TRETools:
    def __init__(self, lzdf: pl.LazyFrame) -> None:
        self._lzdf = lzdf
        
    def unique_with_logging(self, *args, label: str = "Unique", **kwargs) -> pl.LazyFrame:
        before = self._lzdf.select(pl.first()).collect().height
        filtered_lzdf = self._lzdf.unique(*args, **kwargs)
        after = filtered_lzdf.select(pl.first()).collect().height
        
        if before > 0:
            change = ((after - before) / before) * 100
            change_str = f" ({'+' if change > 0 else ''}{change:.1f}%)"
        
        unchanged = " (row count unchanged)" if after == before else ""
        
        print(f"[{label}: on {args}] Before unique: {before} rows, After unique: {after} rows{unchanged}{change_str}")
        return filtered_lzdf    
    
    def filter_with_logging(self, *args, label: str = "Filter", **kwargs) -> pl.LazyFrame:
        before = self._lzdf.select(pl.first()).collect().height
        filtered_lzdf = self._lzdf.filter(*args, **kwargs)
        after = filtered_lzdf.select(pl.first()).collect().height
        
        if before > 0:
            change = ((after - before) / before) * 100
            change_str = f" ({'+' if change > 0 else ''}{change:.1f}%)"
        
        unchanged = " (row count unchanged)" if after == before else ""
        print(f"[{label}] Before filter: {before} rows, After filter: {after} rows{unchanged}{change_str}")
        return filtered_lzdf
    
    def join_with_logging(
        self,
        other: pl.LazyFrame,
        *args,
        how: str = "inner",
        label: str = "Join",
        **kwargs
    ) -> pl.LazyFrame:
        left_before = self._lzdf.select(pl.first()).collect().height
        right_before = other.select(pl.first()).collect().height
        joined_lzdf = self._lzdf.join(other, *args, how=how, **kwargs)
        after = joined_lzdf.select(pl.first()).collect().height
        
        if left_before > 0:
            change = ((after - left_before) / left_before) * 100
            change_str = f" ({'+' if change > 0 else ''}{change:.1f}%)" if abs(change) > 1.00 else f" ({after - left_before} rows {' removed' if change < 0 else ' added'})"
        
        unchanged = " (row count unchanged)" if after == left_before else ""
        print(f"[{label}] Join type: {how.upper()}")
        print(f"[{label}] Left: {left_before} rows, Right: {right_before} rows -> After: {after} rows{unchanged}{change_str}")
        return joined_lzdf


# **Paths to files**

# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# #### Data in

# In[ ]:


INPUTS_LOCATION = f"{ROOT_LOCATION}/{VERSION}/inputs"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"


# In[ ]:


CUSTOM_CODELIST_INPUT_FILE_LOCATION = (
    f"{INPUTS_LOCATION}/GenesAndHealth_custombinary_codelist_v010_2025_05v4.csv"
)
CUSTOM_CODELIST_INPUT_FILE_PATH = AnyPath(
    CUSTOM_CODELIST_INPUT_FILE_LOCATION
)


# #### Data out

# In[ ]:


OUTPUTS_CUSTOM_PHENOTYPES_LOCATION = f"{ROOT_LOCATION}/{VERSION}/outputs/custom_phenotypes"
OUTPUTS_CUSTOM_PHENOTYPES_PATH = AnyPath(
    OUTPUTS_CUSTOM_PHENOTYPES_LOCATION
)


# In[ ]:


OUTPUTS_CUSTOM_PHENOTYPES_PATH.mkdir(parents=True, exist_ok=True)


# In[ ]:


OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION = f"{OUTPUTS_CUSTOM_PHENOTYPES_LOCATION}/individual_trait_files/"

OUTPUTS_REGENIE_FILES_LOCATION = f"{OUTPUTS_CUSTOM_PHENOTYPES_LOCATION}/regenie/"
OUTPUTS_REGENIE_FILES_TEMP_LOCATION = f"{OUTPUTS_REGENIE_FILES_LOCATION}/temp/"



# In[ ]:


AnyPath(OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION).mkdir(parents=True, exist_ok=True)

AnyPath(OUTPUTS_REGENIE_FILES_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(OUTPUTS_REGENIE_FILES_TEMP_LOCATION).mkdir(parents=True, exist_ok=True)


# In[ ]:


MEGA_LINKAGE_PATH = AnyPath(
    "/genesandhealth/library-red/genesandhealth",
    "2025_02_10__MegaLinkage_forTRE.csv" # actually tab-delimited
)


# ## Instantiate Custom Phenotype Mapping

# In[ ]:


coding_system_enum = pl.Enum(["ICD10", "OPCS4", "SNOMED_ConceptID"])


# In[ ]:


custom_phenotype_mapping = (
    pl.scan_csv(
        CUSTOM_CODELIST_INPUT_FILE_PATH
    )
    .rename({"term":"coding_system"})
    .select(
        pl.col("code"),
        pl.col("coding_system").cast(coding_system_enum),
        pl.col("phenotype"),
        pl.col("name").alias("term")
    )
)


# **Note added by SB 2024-03-26**
# 
# There appears to be a problem with the ICD codelist for phenotype `MGH_MajorAdverseVascularLimbEvent`. It has unfeasible codes in it: `0Y6N0Z5` and others. For now, I have manually removed this (sic) lines from the codelist. 
# 
# The MGH_CKD lists also have extra whitespaces which have been removed.
# 
# **Note added by SR 2025-04-22**
# 
# I cannot find any mention of `MGH_MajorAdverseVascularLimbEvent` in the large codelist.  Perhaps SB deleted all lines pertaining to `MGH_MajorAdverseVascularLimbEvent`
# 
# 1. We still see some non-standard white spaces in MGH_CKD, e.g. `MGH_CKD,N182<0xa0>,ICD10,"Chronic kidney disease, stage 2"` (\&nbsp;)
# 2. We updated the principal code list to include:  
#     a. Aniruddh Patel's updated `MGH_MajorAdverseVascularLimbEvent` code list  
#     b. Joe Gafton's Skin problem code list.  We did this by processing their files and appending the processed version to the principal list to create `GenesAndHealth_custombinary_codelist_v010_2025-04-22v1.csv`  
# 
# The "code" to get the extra lines is in the `Code graveyard` below. 
# 
# **Note added by SR 2025-05-16**
# 
# 1. We added QOF `_COD` codesets pertaining to relevant primary care managed conditions (e.g. `QOF_CHD_COD` (Coronary heart disease), `QOF_AST_COD` (Asthma)

# In[ ]:


from tretools.datasets.demographic_dataset import DemographicDataset


# In[ ]:


demographic_file_path = f"{ROOT_LOCATION}/{VERSION}/processed_datasets/demographics/clean_demographics.arrow"


# In[ ]:


demographics = DemographicDataset(path=demographic_file_path)


# In[ ]:


def _calculate_demographics_standalone(lf: pl.LazyFrame, demographics: DemographicDataset) -> pl.LazyFrame:
        print("STANDALONE _calculate_demographics (similar to that of tretools.counter.counter)")
        
        gender_map = {1: "M", 2: "F"}

        # Merge the first events data with demographics together
        first_events_plus_demographics = (
            lf
            .join(
                demographics.data.lazy(),
                on="nhs_number", 
                how="inner"
            )
            .with_columns(
                ((pl.col("date") - pl.col("dob")).dt.total_days() / 365.25)
                .round(1)
                .alias("age_at_event")
            )
            .with_columns([
                pl.col("age_at_event").cut( # ?<16
                    [16, 25, 35, 45, 55, 65, 75, 85], 
                    labels=["<16", "16-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-84", "85+"]
                )
                .alias("age_range"),
                pl.col("gender")
                .replace_strict(gender_map)
            ])
#             .select(
#                 pl.col("nhs_number"), 
#                 pl.col("phenotype"),
#                 pl.col("code"),
#                 pl.col("term"),
#                 pl.col("coding_system"),
#                 pl.col("date"), 
#                 pl.col("age_at_event"), 
#                 pl.col("gender"), 
#                 pl.col("age_range")
#             )
        )

#         self.log.append(f"{datetime.now()}: Demographic data added to the report")
        return first_events_plus_demographics


# In[ ]:


custom_mapped_combo = (
    pl.concat(
        [
# We could, but do not, include ICD10 codes obtained via mapping of SNOMED for definition of the
# custom phenotypes.  This is because we posit that if a valid ICD10 is obtained via mapping then
# the original SNOMED ought to be part of the codelist (i.e. direct attribution not indirect attribution)
#             (
#
#                 pl.scan_ipc(
#                     f"{MEGADATA_LOCATION}/icd_and_mapped_snomed.arrow",
#                 )
#                 .with_columns(
#                     pl.lit("ICD10")
#                     .cast(coding_system_enum)
#                     .alias("coding_system")
#                 )
#             ),
            (
                pl.scan_ipc(
                    f"{MEGADATA_LOCATION}/icd_only.arrow",
                )
                .with_columns(
                    pl.lit("ICD10")
                    .cast(coding_system_enum)
                    .alias("coding_system")
                )
            ),
            (
                pl.scan_ipc(
                    f"{MEGADATA_LOCATION}/opcs_only.arrow",
                )
                .with_columns(
                    pl.lit("OPCS4")
                    .cast(coding_system_enum)
                    .alias("coding_system")
                )
            ),
            (
                pl.scan_ipc(
                    f"{MEGADATA_LOCATION}/snomed_only.arrow",
                )
                .with_columns(
                    pl.lit("SNOMED_ConceptID")
                    .cast(coding_system_enum)
                    .alias("coding_system"),
                    pl.col("code").cast(pl.Utf8)
                )
            )
        ]
    )
#     .with_columns(
#         pl.len().over(["nhs_number", "code", "date"]).alias("dup_count")
#     )
    .join(
        custom_phenotype_mapping,
        on=["code", "coding_system"],
        how="inner",
    )
    .group_by(["nhs_number", "phenotype"])
    .agg(
        pl.col("date").min(),
        pl.col("code").explode().unique().alias("all_codes"),
        pl.col("coding_system").explode().unique().alias("all_coding_systems"),
        pl.col("code").filter(pl.col("date") == pl.col("date").min()).first().alias("code"),
        pl.col("term").filter(pl.col("date") == pl.col("date").min()).first().alias("term"),
        
    )
    .with_columns(
        pl.col("all_codes").list.join(" | "),
        pl.col("all_coding_systems").cast(pl.List(pl.Utf8)).list.join(" | ")
    )
    .sort(["nhs_number", "date", "code", ])
    .pipe(_calculate_demographics_standalone, demographics=demographics)
    .select(
        pl.col('nhs_number'),
        pl.col('phenotype'),
        pl.col('date'),
        pl.col('code'),
        pl.col('term'),
        pl.col('all_codes'),
        pl.col('all_coding_systems'),
        pl.col('gender'),
        pl.col('dob'),
        pl.col('age_at_event'),
        pl.col('age_range'),
    )
#     .collect()
)


# In[ ]:


get_ipython().run_cell_magic('time', '', '## Partition by custom_phenotype\ncustom_mapped_combo_phenotype_dict = (\n    custom_mapped_combo\n    .collect()\n    .partition_by(\n        "phenotype",\n        as_dict=True\n    )\n)\n')


# ## Write individual custom phenotype (aka trait) files

# In[ ]:


get_ipython().run_cell_magic('time', '', '# sorted for clarity, not efficiency\nfor i, ((phenotype, ), df) in enumerate(sorted(custom_mapped_combo_phenotype_dict.items())):\n    print(f"{i+1}. {phenotype}", end=", ")\n    (\n        df\n        .lazy()\n        .sink_csv(\n            AnyPath(\n                OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION,\n                f"{yr}_{mon}_{phenotype}_summary_report.csv"\n            ),\n        )\n    )\n        \n')


# ## Create phenotype reports

# cf. version080 
# 
# `.../custom_phenotypes/overall_summary_report_README.md`
# 

# In[ ]:


def generate_custom_phenotypes_count_summary(lf: pl.LazyFrame) -> None:
    report_filename = f"custom_phenotypes_count_summary.md"
    report_path = AnyPath(OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION, report_filename)
    
    df = (
        lf
        .select(
            pl.col("phenotype").value_counts()
        )
        .unnest("phenotype")
        .join(
            custom_phenotype_mapping.select(pl.col("phenotype").unique()),
            on="phenotype",
            how="right"
        )
        .sort("phenotype")

        .select(
            pl.col("phenotype"),
            pl.col("count").fill_null(0),
        )
        .collect()
    )
    
    max_phenotype_char_length = df.select(pl.col("phenotype").str.len_chars().max()).item()
#     print(f"max_phenotype_char_length: {max_phenotype_char_length}")
    
    with open(report_path, "w") as f:
        print(f"""# Custom Phenotype Count Summary Report

## Generated by Genes and Health *BI_PY* pipeline

Generated by the BI_PY `8-custom-phenotypes-individual-trait-files-and-regenie` notebook.

## Report Generation Date and Time

This report was generated at {datetime.today().strftime(format="%d %B %Y %H:%M:%S")}.
 
## Report Overview

This summary report provides an overview of the counts associated with each phenotype across all datasets:

- Discovery Primary Care
- Barts Health
- Bradford
- NHS Digital

## Summary
""", file=f)

    with pl.Config(
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_cell_numeric_alignment="RIGHT",
        fmt_str_lengths=max_phenotype_char_length,
        tbl_rows=-1,
    ):
        with open(report_path, "a") as f:
            print(
                df,
                file=f
            )
    print(f"Report `{report_filename}` generated.")


# In[ ]:


generate_custom_phenotypes_count_summary(lf=custom_mapped_combo)


# # regenie

# ## Generate 55k ExWAS - regenie input file (i.e. not covariate file)

# In[ ]:


valid_regenie_55k = (
    pl.scan_csv(
        MEGA_LINKAGE_PATH,
        infer_schema=False,
        new_columns=[
            "OrageneID",
            "Number of OrageneIDs with this NHS number (i.e. taken part twice or more)",
            "s1qst_gender",
            "HasValidNHS",
            "pseudo_nhs_number",
            "gsa_id",
            "44028exomes_release_2023-JUL-07",
            "exome_id",
        ]
    )
    .TRE
    .filter_with_logging(
        pl.col("exome_id").is_not_null(),
        pl.col("pseudo_nhs_number").is_not_null(), # there are some rows with NON-NULL exome_id but NULL pseudo_nhs_number
        label="Only include NON-NULL exome_id and NON-NULL pseudo_nhs_number for 55k Regenie"
    )
    .TRE
    .filter_with_logging(
        pl.col("OrageneID").is_not_null(),
        label="Sanity check to ensure no NULL OrageneID. row count should remain unchanged"
    )
    .TRE
    .unique_with_logging(
        ["pseudo_nhs_number"],
        label="Sanity check: row count should remain unchanged when uniquing by pseudo_nhs_number"
    )
    .TRE
    .unique_with_logging(
        ["OrageneID"],
        label="Sanity check: row count should remain unchanged when uniquing by OrageneID"
    )
    .select(
        pl.col("pseudo_nhs_number"),
        pl.col("exome_id").alias("IID")
    )
    .sort(by="IID")
)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'combo_custom_phenotypes_55k_dict = (\n    custom_mapped_combo\n    .TRE\n    .join_with_logging(\n        valid_regenie_55k.select(\n            pl.col("pseudo_nhs_number"),\n            pl.col("IID"),\n        ),\n        left_on="nhs_number",\n        right_on="pseudo_nhs_number",\n        how="inner",\n        label="restrict to pseudo_NHS_numbers with ExWAS"\n    )\n    .with_columns(\n        pl.lit("1").alias("FID")\n    )\n    .select( ## We use the AgeAtFirstDiagnosis columns for covariate file generation later in pipeline\n        pl.col("FID"),\n        pl.col("IID"),\n        pl.col("phenotype"),\n        pl.col("age_at_event").round(1).alias("AgeAtFirstDiagnosis"),\n        pl.col("age_at_event").pow(2).round(1).alias("AgeAtFirstDiagnosis_Squared"),\n    )\n    .sort(by="IID")\n    .set_sorted("IID")\n    .collect()\n    .partition_by(\n        "phenotype",\n        as_dict=True,\n        \n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '# [UPDATE: Fixed in Polars 1.26]. We have identifed possible bug with .sink_csv where header line separator is not changed to specified separator\n# i.e. header remains comma-separated while non-header rows are tab-delimited\n# We have found that if we use .write_csv instead, we work around this issue  \n# until we update polars for permanent fix.\n\n(\n    pl.concat(\n        [\n            valid_regenie_55k\n            .with_columns(\n                pl.lit("1")\n                .alias("FID")\n            )\n            .select(\n                pl.col("FID"),\n                pl.col("IID")\n            ),\n            *[\n                valid_regenie_55k\n                .select(\n                    pl.col("IID")\n                )\n                .join(\n                    lf\n                    .lazy(), \n                    on="IID", \n                    how="left"\n                )\n                .with_columns(\n                     pl.col("phenotype")\n                    .is_not_null()\n                    .cast(pl.Int8)\n                    .cast(pl.Utf8)\n                    .alias(phenotype)\n                )\n                .select(\n                    pl.col("IID"),\n                    pl.col(phenotype)\n                )\n\n                for (phenotype, ), lf in sorted(combo_custom_phenotypes_55k_dict.items())\n            ]\n        ],\n    how="align"\n    )\n    .with_columns(\n        pl.sum_horizontal(\n            pl.all()\n            .exclude(["FID", "IID"])\n            .cast(pl.Int8, strict=False)\n        )\n        .alias("indv_pheno_count")\n    )\n    .filter(pl.col("indv_pheno_count")>0)\n    .select(\n        pl.exclude("indv_pheno_count")\n    )\n    .sort("IID")\n    .collect()  # see note above\n    .write_csv(  # see note above\n        AnyPath(\n            OUTPUTS_REGENIE_FILES_LOCATION,\n            f"{yr}_{mon}_custom_phenotypes_regenie_55k_BroadExomeIDs.tsv"\n        ),\n        separator="\\t",\n        null_value="0"\n    )\n)\n')


# ## Generate 51k GWAS - regenie input file (i.e. not covariate file)

# In[ ]:


valid_regenie_51k = (
    pl.scan_csv(
        MEGA_LINKAGE_PATH,
        infer_schema=False,
        new_columns=[
            "OrageneID",
            "Number of OrageneIDs with this NHS number (i.e. taken part twice or more)",
            "s1qst_gender",
            "HasValidNHS",
            "pseudo_nhs_number",
            "gsa_id",
            "44028exomes_release_2023-JUL-07",
            "exome_id",
        ]
    )
    .TRE
    .filter_with_logging(
        pl.col("gsa_id").is_not_null(),
        pl.col("pseudo_nhs_number").is_not_null(), # there are some rows with NON-NULL exome_id but NULL pseudo_nhs_number
        label="Only include NON-NULL gsa_id and NON-NULL pseudo_nhs_number for 51k Regenie"
    )
    .TRE
    .filter_with_logging(
        pl.col("OrageneID").is_not_null(),
        label="Sanity check to ensure no NULL OrageneID. row count should remain unchanged"
    )
    .TRE
    .unique_with_logging(
        ["pseudo_nhs_number"],
        label="Sanity check: row count should remain unchanged when uniquing by pseudo_nhs_number"
    )
    .TRE
    .unique_with_logging(
        ["OrageneID"],
        label="Sanity check: row count should remain unchanged when uniquing by OrageneID"
    )
    .select(
        pl.col("pseudo_nhs_number"),
        pl.col("gsa_id").alias("IID")
    )
)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'combo_custom_phenotypes_51k_dict = (\n    custom_mapped_combo\n    .TRE\n    .join_with_logging(\n        valid_regenie_51k.select(\n            pl.col("pseudo_nhs_number"),\n            pl.col("IID"),\n        ),\n        left_on="nhs_number",\n        right_on="pseudo_nhs_number",\n        how="inner",\n        label="restrict to pseudo_NHS_numbers with GWAS"\n    )\n    .with_columns(\n        pl.lit("1").alias("FID")\n    )\n    .select(\n        pl.col("FID"),\n        pl.col("IID"),\n        pl.col("phenotype"),\n        pl.col("age_at_event").round(1).alias("AgeAtFirstDiagnosis"),\n        pl.col("age_at_event").pow(2).round(1).alias("AgeAtFirstDiagnosis_Squared"),\n    )\n    .sort(by="IID")\n    .set_sorted("IID")\n    .collect()\n    .partition_by(\n        "phenotype",\n        as_dict=True,\n        \n    )\n)\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '# [UPDATE: Fixed in Polars 1.26]. We have identifed possible bug with .sink_csv where header line separator is not changed to specified separator\n# i.e. header remains comma-separated while non-header rows are tab-delimited\n# We have found that if we use .write_csv instead, we work around this issue  \n# until we update polars for permanent fix.\n\n(\n    pl.concat(\n        [\n            valid_regenie_51k\n            .with_columns(\n                pl.lit("1")\n                .alias("FID")\n            )\n            .select(\n                pl.col("FID"),\n                pl.col("IID")\n            ),\n            *[\n                valid_regenie_51k\n                .select(\n                    pl.col("IID")\n                )\n                .join(\n                    lf\n                    .lazy(), \n                    on="IID", \n                    how="left"\n                )\n                .with_columns(\n                     pl.col("phenotype")\n                    .is_not_null()\n                    .cast(pl.Int8)\n                    .cast(pl.Utf8)\n                    .alias(phenotype)\n                )\n                .select(\n                    pl.col("IID"),\n                    pl.col(phenotype)\n                )\n\n                for (phenotype, ), lf in sorted(combo_custom_phenotypes_51k_dict.items())\n            ]\n        ],\n    how="align"\n    )\n    .with_columns(\n        pl.sum_horizontal(\n            pl.all()\n            .exclude(["FID", "IID"])\n            .cast(pl.Int8, strict=False)\n        )\n        .alias("indv_pheno_count")\n    )\n    .filter(pl.col("indv_pheno_count")>0)\n    .select(\n        pl.exclude("indv_pheno_count")\n    )\n    .sort("IID")\n    .collect()  # see note above\n    .write_csv(  # see note above\n        AnyPath(\n            OUTPUTS_REGENIE_FILES_LOCATION,\n            f"{yr}_{mon}_custom_phenotypes_regenie_51koct2024_65A_Topmed.tsv"\n        ),\n        separator="\\t",\n        null_value="0"\n    )\n)\n')


# ## Now generate covariate files (AgeAtFirstDiagnosis)

# ## 55k ExWAS

# In[ ]:


get_ipython().run_cell_magic('time', '', '# [UPDATE: Fixed in Polars 1.26]. We have identifed possible bug with .sink_csv where header line separator is not changed to specified separator\n# i.e. header remains comma-separated while non-header rows are tab-delimited\n# We have found that if we use .write_csv instead, we work around this issue  \n# until we update polars for permanent fix.\n\n# temp_cov_55k = (\n(\n    pl.concat(\n        [\n            valid_regenie_55k\n            .with_columns(\n                pl.lit("1")\n                .alias("FID")\n            )\n            .select(\n                pl.col("FID"),\n                pl.col("IID")\n            ),\n            *[\n                valid_regenie_55k\n                .select(\n                    pl.col("IID"),\n                )\n                .join(\n                    lf\n                    .lazy()\n                    .select(\n                        pl.col("IID"),\n                        pl.col("AgeAtFirstDiagnosis").alias(f"AgeAtFirstDiagnosis.{phenotype}"),\n                        pl.col("AgeAtFirstDiagnosis_Squared").alias(f"AgeAtFirstDiagnosis_Squared.{phenotype}")\n                    ), \n                    on="IID", \n                    how="left"\n                )\n\n                for (phenotype, ), lf in sorted(combo_custom_phenotypes_55k_dict.items())\n            ],\n        ],\n    how="align"\n    )\n    .with_columns(\n            (\n                pl.sum_horizontal(\n                    pl.all()\n                    .exclude(["FID", "IID"])\n                    .is_not_null()\n                )\n                .cast(pl.Boolean)\n                .alias("indv_has_ge_1_phenotypes")\n            )\n    )\n    .sort("IID")\n    .filter(\n        pl.col("indv_has_ge_1_phenotypes")\n    )\n    .collect()  # see note above\n    .write_csv(  # see note above\n        AnyPath(\n            OUTPUTS_REGENIE_FILES_LOCATION,\n            f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_custom_phenotypes_age_at_first_diagnosis_megawide.tsv"\n        ),\n        separator="\\t",\n        null_value="0"\n    )\n)\n')


# ## 51k GWAS covariate file

# In[ ]:


get_ipython().run_cell_magic('time', '', '# [UPDATE: Fixed in Polars 1.26]. We have identifed possible bug with .sink_csv where header line separator is not changed to specified separator\n# i.e. header remains comma-separated while non-header rows are tab-delimited\n# We have found that if we use .write_csv instead, we work around this issue  \n# until we update polars for permanent fix.\n\n# temp_cov_51k = (\n(\n    pl.concat(\n        [\n            valid_regenie_51k\n            .with_columns(\n                pl.lit("1")\n                .alias("FID")\n            )\n            .select(\n                pl.col("FID"),\n                pl.col("IID")\n            ),\n            *[\n                valid_regenie_51k\n                .select(\n                    pl.col("IID"),\n                )\n                .join(\n                    lf\n                    .lazy()\n                    .select(\n                        pl.col("IID"),\n                        pl.col("AgeAtFirstDiagnosis").alias(f"AgeAtFirstDiagnosis.{phenotype}"),\n                        pl.col("AgeAtFirstDiagnosis_Squared").alias(f"AgeAtFirstDiagnosis_Squared.{phenotype}")\n                    ), \n                    on="IID", \n                    how="left"\n                )\n\n                for (phenotype, ), lf in sorted(combo_custom_phenotypes_51k_dict.items())\n            ],\n        ],\n    how="align"\n    )\n    .with_columns(\n            (\n                pl.sum_horizontal(\n                    pl.all()\n                    .exclude(["FID", "IID"])\n                    .is_not_null()\n                )\n                .cast(pl.Boolean)\n                .alias("indv_has_ge_1_phenotypes")\n            )\n    )\n    .sort("IID")\n    .filter(\n        pl.col("indv_has_ge_1_phenotypes")\n    )\n    .collect()  # see note above\n    .write_csv(  # see note above\n        AnyPath(\n            OUTPUTS_REGENIE_FILES_LOCATION,\n            f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_custom_phenotypes_age_at_first_diagnosis_megawide.tsv"\n        ),\n        separator="\\t",\n        null_value="0"\n    )\n)\n')


# In[ ]:


print("That's all folks!")


# ### Run next cell to initiate next notebook

# In[ ]:


# redirect_to_next_notebook_in_pipeline("change_to_next_notebook_if_applicable")


# In[ ]:





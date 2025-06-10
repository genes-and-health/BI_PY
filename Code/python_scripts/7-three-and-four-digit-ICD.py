#!/usr/bin/env python
# coding: utf-8

# ## Three and Four digit ICD 10 analysis
# 
# ### In previous version of pipeline (version008) there were separate notebook for ICD-10 3-digit (old notebook 7) and ICD-10 4-digit (old notebook 9) output generation.  This new notebook does both and therefore replaces old notebook 7; old notebook 9 is no longer needed.
# 
# This notebook does 4 steps:
#     
# 1) Loads the mapped dataset (containing codes for 3 and 4 digit ICD), and sorts them 
# into 3 digit code if ends in X (e.g. A01X), has a dot (e.g. A01.) or a - (e.g.A01-). If it is a 4 digit code, or more, it is turned into A01.1. We will also for the sake of time create ICD10 codes without dots (A011) in case need these in the future. 
# 2) Creates per ICD-10 (phenotype) individuals lists
# 3) Creates 2 regenie files (\[G|Ex\]WAS) \[limited to 3-digit ICD-10\]
# 4) Creates a Phenotype count summary

# In[ ]:


import polars as pl
import itertools

from datetime import datetime
from cloudpathlib import AnyPath, CloudPath

from tretools.datasets.processed_dataset import ProcessedDataset
from tretools.codelists.codelist_types import CodelistType
from tretools.datasets.demographic_dataset import DemographicDataset

# Plotting packages
import altair as alt
alt.data_transformers.enable("vegafusion")
alt.renderers.enable("svg")


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


GNH_PALETTE = {
    "COBALT_BLUE": "#32449b",
    "EMERALD_GREEN": "#45c086",
    "MAGENTA": "#c44887",
    "PEACH_ORANGE": "#ff8070",
    "INDIGO_PURPLE": "#312849",
}


# In[ ]:


# version = "version010"
mon = "05"
yr = "2025"


# In[ ]:


VERSION = 'version010_2025_05_SR'


# In[ ]:


ROOT_LOCATION = "/home/ivm/BI_PY"


# In[ ]:


PROCESSED_DATASETS_LOCATION = f"{ROOT_LOCATION}/{VERSION}/processed_datasets"
PREPROCESSED_FILES_LOCATION = f"{ROOT_LOCATION}/{VERSION}/preprocessed_files"
MEGADATA_LOCATION = f"{ROOT_LOCATION}/{VERSION}/megadata"


# In[ ]:


MEGA_LINKAGE_PATH = AnyPath(
    "/genesandhealth/library-red/genesandhealth",
    "2025_02_10__MegaLinkage_forTRE.csv" # actually tab-delimited
)


# In[ ]:


OUTPUTS_LOCATION = f"{ROOT_LOCATION}/{VERSION}/outputs/icd10"


# In[ ]:


OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION = f"{OUTPUTS_LOCATION}/individual_trait_files/"
OUTPUTS_3D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION = f"{OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION}/3_digit_icd/"
OUTPUTS_4D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION = f"{OUTPUTS_INDIVIDUAL_TRAIT_FILES_LOCATION}/4_digit_icd/"

OUTPUTS_REGENIE_FILES_LOCATION = f"{OUTPUTS_LOCATION}/regenie/"
OUTPUTS_REGENIE_FILES_TEMP_LOCATION = f"{OUTPUTS_REGENIE_FILES_LOCATION}/temp/"



# In[ ]:


AnyPath(OUTPUTS_3D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(OUTPUTS_4D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION).mkdir(parents=True, exist_ok=True)

AnyPath(OUTPUTS_REGENIE_FILES_LOCATION).mkdir(parents=True, exist_ok=True)
AnyPath(OUTPUTS_REGENIE_FILES_TEMP_LOCATION).mkdir(parents=True, exist_ok=True)


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


# ### Load the ICD10 Data
# 
# Here we are loading the ICD10 data that has already been processed. The data has untruncated raw ICD codes that are from the data or mapped from SNOMED. 

# In[ ]:


mapped_data = ProcessedDataset(path=f"{MEGADATA_LOCATION}/icd_and_mapped_snomed.arrow", 
                               dataset_type="MERGED", 
                               coding_system=CodelistType.ICD10.value, 
                               log_path=f"{MEGADATA_LOCATION}/icd_and_mapped_snomed_log.txt")


# ### `clean_icd10()` can be `.pipe`d into a polars LazyFrame to clean the ICD-10 codes column
# 
# Letters after code are not necessarily invalid, they are used for additional indication such as diagnostic certainty or affected side of body.
# 
# See: https://gesund.bund.de/en/icd-code-search/g01
# 
# ```
# Additional indicator
# On medical documents, the ICD code is often appended by letters that indicate the diagnostic certainty or the affected side of the body.
# 
# G: Confirmed diagnosis
# V: Tentative diagnosis
# Z: Condition after
# A: Excluded diagnosis
# 
# L: Left
# R: Right
# B: Both sides
# ```
# 
# We have some codes appended with "D" which seems to be invalid (alhtough appears in Google searches).  We could (and indeed should) simply remove terminal B-Z characters and if terminal character is A **delete the row** as this represents an "Excluded diagnosis".
# 
# 

# In[ ]:


def clean_icd10(lf: pl.LazyFrame, icd10_column: str = "code") -> pl.LazyFrame:
    """
    Cleans an ICD-10 column by:
    - Removing all spaces
    - Excluding icd10 = "NA" rows
    - Excluding icd10 code <3 char length (minimum valid icd10 is 3 chars)
    - Excluding icd10 codes not starting with a letter
    - Excluding icd10 ending with an "A" rows; "A" suffixes represent "Excluded diagnosis"
    - Removing B-Z characters at end of icd10 code
    - Removing "X" and "." and "-"
    - Formatting to "XXX.X" if dots=True
    - Keeping up to 4 meaningful characters
        
    Args:
        lzdf (pl.LazyFrame): The input LazyFrame containing the ICD-10 column to clean
        icd10_column (str): Name of the column containing ICD-10 codes
        [not longer has arguments dots as outputs a pl.LazyFrame with both dotted and undotted 4 digit ICD10]
        
    Returns:
        pl.LazyFrame: the modified LazyFrame with cleaned ICD-10 codes
    """
    return (
        lf
        .with_columns(
            pl.col(icd10_column)
            .str.replace_all(" ","")
        )
        .filter( # eliminiate rows with inappropriate codes
            pl.col(icd10_column).ne("NA"),
            pl.col(icd10_column).ne("-1"),
            pl.col(icd10_column).str.len_chars() >= 3,
            pl.col(icd10_column).str.contains("^[A-Z]"),
            ~pl.col(icd10_column).str.contains("A$"),
        )
        .with_columns( # create icd_10_new (invalid character processed code)
            pl.col(icd10_column)
            .str.replace(r"[B-Z]$", "")
            .str.replace_all(r"[\.-]","")  # Remove any `.` and `-`
            .str.replace(r"^(.+)X(.*)","$1$2") # Remove `X` somewhere other than in the first position
            .alias(f"{icd10_column}_new")
        )
        .with_columns( # create 3-digit version of icd10_new
            pl.col(f"{icd10_column}_new")
            .str.slice(0,3)
            .alias(f"{icd10_column}_new_3d")
        )
        .with_columns( # create both dotted and undotted version of icd10_new
            pl.when(pl.col(f"{icd10_column}_new").str.slice(3, 1).ne("")) #  4 or more characters
            .then(
                pl.concat_str(
                    pl.col(f"{icd10_column}_new").str.slice(0, 3),
                    pl.lit("."),
                    pl.col(f"{icd10_column}_new").str.slice(3, 1)
                ).alias(f"{icd10_column}_new_4d")
            ),
            pl.when(pl.col(f"{icd10_column}_new").str.slice(3, 1).ne("")) #  4 or more characters
            .then(
                pl.col(f"{icd10_column}_new").str.slice(0, 4),
            ).alias(f"{icd10_column}_new_4d_undotted")
#             .fill_null(pl.col(f"{icd10_column}_new"))  # Ensure 3-char codes remain unchanged
# #             .alias(icd10_column)
#             .alias("cleaned_code")
        )
        
        
#         .pipe(lambda lzdf: print(lzdf.collect().height) or lzdf)  # before unique() 7581082 rows
        .unique()
#         .pipe(lambda lzdf: print(lzdf.collect().height) or lzdf)  # after unique() 7385200 rows
    )


# # Generate individual_trait_files and regenie files

# ## Import demographics (created in Workbook 1)

# In[ ]:


demographic_file_path = f"{PROCESSED_DATASETS_LOCATION}/demographics/clean_demographics.arrow"
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
            .select(
                pl.col("nhs_number"), 
                pl.col("code"), 
                pl.col("date"), 
                pl.col("age_at_event"), 
                pl.col("gender"), 
                pl.col("age_range")
            )
        )

#         self.log.append(f"{datetime.now()}: Demographic data added to the report")
        return first_events_plus_demographics


# ### Creating the codelists
# 
# We want to get every variation of A01 to Q99.9. This includes all 3 digit possibilities (such as A01, A02, B21 etc), and all 4 digit variations (such as A01.0, A01.1, A01.2).

# In[ ]:


from itertools import product

def generate_icd10_codes(icd_length: int, with_dot=True) -> dict:
    # Avoids the need for nested loops
    
    if icd_length == 3:
        # 17 letter x 99 numbers (01-99) = 1683 codes
        letters = [chr(c) for c in range(ord("A"), ord("Q") + 1)]
        numbers = [f"{i:02d}" for i in range(1, 100)]

        return [f"{l}{n}" for l, n in product(letters, numbers)]
    elif icd_length == 4:
        # 17 letter x 99 numbers (01-99) x 10 sub-digits (0-9) = 16830 codes
        letters = [chr(c) for c in range(ord("A"), ord("Q") + 1)]
        numbers = [f"{i:02d}" for i in range(1, 100)]
        decimals = [*[f".{i}" for i in range(10)]] if with_dot else [""]
        
        return [f"{l}{n}{d}" for l, n, d in product(letters, numbers, decimals)]
    else:
        raise ValueError(f"generate_combo_icd10: `icd_length` of {icd_length} not recognised.  Try 3 or 4.")


# In[ ]:


get_ipython().run_cell_magic('time', '', 'def generate_combo_icd10(icd_length: int) -> pl.LazyFrame:\n    if icd_length == 4:\n        code_column = "code_new_4d"\n    elif icd_length == 3:\n        code_column = "code_new_3d"\n    else:\n        raise ValueError(f"generate_combo_icd10: `icd_length` of {icd_length} not recognised.  Try 3 or 4.")\n    return (\n        mapped_data.data\n        .lazy()\n        .pipe(clean_icd10)\n        .join(\n            pl.LazyFrame({"code": generate_icd10_codes(icd_length=icd_length)}),\n            left_on=code_column,\n            right_on="code",\n            how="semi"\n        )\n        .group_by(\n            pl.col("nhs_number"),\n            pl.col(code_column).alias("code")\n        )\n        .agg(\n            pl.col("date").min()\n        )\n        .pipe(_calculate_demographics_standalone, demographics=demographics)\n        .with_columns(\n            pl.lit("merged").alias("dataset_type"),\n            pl.lit("ICD10").alias("codelist_type"),\n        )\n        .select(\n            pl.col("nhs_number"),\n            pl.col("date"),\n            pl.col("code"),\n            pl.col("age_at_event"),\n            pl.col("dataset_type"),\n            pl.col("codelist_type"),\n            pl.col("gender"),\n            pl.col("age_range"),\n        )\n    )\n')


# ### Create per ICD-10 3 digit lists of individuals

# In[ ]:


combo_icd10_3d = generate_combo_icd10(icd_length=3)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'phenotypes_3d_dict = (\n    combo_icd10_3d\n    .collect()\n    .partition_by("code", as_dict=True)\n)\n')


# ### Write individual_trait_files (ICD10 3-digit)

# In[ ]:


get_ipython().run_cell_magic('time', '', '# sorted for clarity, not efficiency\nfor i, ((phenotype, ), df) in enumerate(sorted(phenotypes_3d_dict.items())):\n    print(f"{i+1}. {phenotype}", end=", ")\n    (\n        df\n        .lazy()\n        .sink_csv(\n            AnyPath(\n                OUTPUTS_3D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION,\n                f"{yr}_{mon}_{phenotype}_summary_report.csv"\n            ),\n        )\n    )\n        \n')


# ### Create per ICD-10 4 digit lists of individuals

# In[ ]:


combo_icd10_4d = generate_combo_icd10(icd_length=4)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'phenotypes_4d_dict = (\n    combo_icd10_4d\n    .collect()\n    .partition_by("code", as_dict=True)\n)\n')


# ### Write individual_trait_files (ICD10 4-digit)

# In[ ]:


get_ipython().run_cell_magic('time', '', '# sorted for clarity, not efficiency\nfor i, ((phenotype, ), df) in enumerate(sorted(phenotypes_4d_dict.items())):\n    print(f"{i+1}. {phenotype}", end=", ")\n    (\n        df\n        .lazy()\n        .sink_csv(\n            AnyPath(\n                OUTPUTS_4D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION,\n                f"{yr}_{mon}_{phenotype}_summary_report.csv"\n            ),\n        )\n    )\n        \n')


# # Now create regenie files
# 
# We have decided **NOT** to generate _regenie_ files for ICD-10 4-digits (as per previous `BI_PY` versions)

# ## Create valid pseudo_nhs_number lists
# 
# * 55k (ExWAS)
# * 51k (GWAS)

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
)


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
)


# ## Write ICD-10 3-digit regenie files

# ### 51k GWAS

# In[ ]:


get_ipython().run_cell_magic('time', '', '\n## NB following join of combo_icd10_3d w/ valid_regenie_51k we lose 5 traits\n## Lost traits are \'A35\', \'A65\', \'F59\', \'H45\', \'J62\'\ncombo_icd10_3d_trait_51k_dict = (\n    combo_icd10_3d\n    .TRE\n    .join_with_logging(\n        valid_regenie_51k.select(\n            pl.col("pseudo_nhs_number"),\n            pl.col("gsa_id"),\n        ),\n        left_on="nhs_number",\n        right_on="pseudo_nhs_number",\n        how="inner",\n        label="restrict to pseudo_NHS_numbers with GWAS"\n    )\n    .with_columns(\n        pl.lit("1").alias("FID")\n    )\n    .select(\n        pl.col("FID"),\n        pl.col("gsa_id").alias("IID"),\n        pl.col("code"),\n        pl.col("age_at_event").round(1).alias("AgeAtFirstDiagnosis"),\n        pl.col("age_at_event").pow(2).round(1).alias("AgeAtFirstDiagnosis_Squared"),\n    )\n    .sort(by="IID")\n    .set_sorted("IID")\n    .collect()\n    .partition_by(\n        "code",\n        as_dict=True,\n        \n    )\n)\n')


# ## Process `combo_icd10_3d_51k_trait_dict` in batches
# 
# Otherwise we get the following polars warning:
# 
# > **UserWarning:** encountered expression deeper than 512 elements; this may overflow the stack, consider refactoring
# 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'batch_size = 48  # anything above ~90 causes a "deeper than 512 elements" warning; loose testing suggests 48 best\n\n# Split the dictionary items into batches\nnum_batches = (len(combo_icd10_3d_trait_51k_dict) + batch_size - 1) // batch_size  # Ceiling division\n\nfor batch_idx in range(num_batches):\n    # Get the current batch of items\n    batch_start = batch_idx * batch_size\n    batch_end = min((batch_idx + 1) * batch_size, len(combo_icd10_3d_trait_51k_dict))\n    current_batch = dict(itertools.islice(sorted(combo_icd10_3d_trait_51k_dict.items()), batch_start, batch_end))\n\n    # Process the current batch\n    pl.concat(\n        [\n            df\n            .lazy()\n            .with_columns(\n                pl.lit(1).alias(trait).cast(pl.Enum(["0", "1"]))\n            )\n            .select(\n                pl.col("FID"),\n                pl.col("IID"),\n                pl.col(trait)\n            )\n        for (trait, ), df in current_batch.items()\n        ],\n    how="align").sink_parquet(\n        AnyPath(\n            OUTPUTS_REGENIE_FILES_TEMP_LOCATION,\n            f"{yr}_{mon}_icd10_3d_regenie_51koct2024_65A_Topmed_batch{batch_idx+1}.parquet"\n        ),\n    )\n    \n    print(f"Processed batch {batch_idx+1}/{num_batches} ({len(current_batch)} items; Start: {batch_start}, End: {batch_end-1})")\n')


# In[ ]:


# Now concatenate all batch files

concatenated_parquets_51k = (
    pl.concat(
        # now concat the batches
        [
            pl.scan_parquet(
                AnyPath(
                    OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                    f"{yr}_{mon}_icd10_3d_regenie_51koct2024_65A_Topmed_batch{i+1}.parquet"
                )
            ) 
        for i in range(num_batches)
        ],
        how="align"
    )
)


# In[ ]:


# We keep a storage and use efficient .parquet just in case
(
    concatenated_parquets_51k
    .sink_parquet(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_icd10_3d_regenie_51koct2024_65A_Topmed.parquet"
        ),
    )
)


# In[ ]:


# We have identifed possible bug with .sink_csv where header line separator is not changed to specified separator
# i.e. header remains comma-separated while non-header rows are tab-delimited
# We have found that if we use .write_csv instead, we resolve this issue

(
    concatenated_parquets_51k
    .collect()
    .write_csv(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_icd10_3d_regenie_51koct2024_65A_Topmed.tsv"
        ),
        separator="\t",
        null_value="0"
    )
)


# In[ ]:


chart_51k = (
    alt.Chart(
        concatenated_parquets_51k
        .with_columns(
            pl.sum_horizontal(pl.exclude(["FID", "IID"]).cast(pl.Int8)).alias("sum_traits")
        )
        .select(
            pl.col("IID"),
            pl.col("sum_traits")
        )
        .collect(),
        title=f"51k GWAS phenotype counts per individual",
        width=650,
    )
    .mark_bar(
        fill=GNH_PALETTE["EMERALD_GREEN"],
    )
    .encode(
        alt.X("sum_traits:Q").bin(maxbins=48).title(f"ICD10 3digit conditions assigned"),#.scale(type="log"),
        alt.Y("count()").title("Number of individuals"),
    )
)
chart_51k.save(
    AnyPath(
        OUTPUTS_REGENIE_FILES_LOCATION,
        "GWAS-51k-icd-10-3-digit-phenotypes-per-individual-distribution.svg"
    )
)
chart_51k


# ### 55k ExWAS

# In[ ]:


get_ipython().run_cell_magic('time', '', '\n## NB following join of combo_icd10_3d w/ valid_regenie_55k we lose no traits\n## Lost traits are: N/A\ncombo_icd10_3d_trait_55k_dict = (\n    combo_icd10_3d\n    .TRE\n    .join_with_logging(\n        valid_regenie_55k.select(\n            pl.col("pseudo_nhs_number"),\n            pl.col("exome_id"),\n        ),\n        left_on="nhs_number",\n        right_on="pseudo_nhs_number",\n        how="inner",\n        label="restrict to pseudo_NHS_numbers with ExWAS"\n    )\n    .with_columns(\n        pl.lit("1").alias("FID")\n    )\n    .select(\n        pl.col("FID"),\n        pl.col("exome_id").alias("IID"),\n        pl.col("code"),\n        pl.col("age_at_event").round(1).alias("AgeAtFirstDiagnosis"),\n        pl.col("age_at_event").pow(2).round(1).alias("AgeAtFirstDiagnosis_Squared"),\n    )    \n    .sort(by="IID")\n    .set_sorted("IID")\n    .collect()\n    .partition_by(\n        "code",\n        as_dict=True,\n        \n    )\n)\n')


# ## Process `combo_icd10_3d_55k_trait_dict` in batches
# 
# Otherwise we get the following polars warning:
# 
# > UserWarning: encountered expression deeper than 512 elements; this may overflow the stack, consider refactoring
# 

# In[ ]:


get_ipython().run_cell_magic('time', '', 'batch_size = 48  # anything above ~90 causes a "deeper than 512 elements" warning\n\n# Split the dictionary items into batches\nnum_batches = (len(combo_icd10_3d_trait_55k_dict) + batch_size - 1) // batch_size  # Ceiling division\n\nfor batch_idx in range(num_batches):\n    # Get the current batch of items\n    batch_start = batch_idx * batch_size\n    batch_end = min((batch_idx + 1) * batch_size, len(combo_icd10_3d_trait_55k_dict))\n    current_batch = dict(itertools.islice(sorted(combo_icd10_3d_trait_55k_dict.items()), batch_start, batch_end))\n\n    # Process the current batch\n    pl.concat(\n        [\n            df\n            .lazy()\n            .with_columns(\n                pl.lit(1).alias(trait).cast(pl.Enum(["0", "1"]))\n            )\n            .select(\n                pl.col("FID"),\n                pl.col("IID"),\n                pl.col(trait)\n            )\n        for (trait, ), df in current_batch.items()\n        ],\n    how="align").sink_parquet(\n        AnyPath(\n            OUTPUTS_REGENIE_FILES_TEMP_LOCATION,\n            f"{yr}_{mon}_icd10_3d_regenie_55k_BroadExomeIDs_batch{batch_idx+1}.parquet"\n        ),\n    )\n    \n    print(f"Processed batch {batch_idx+1}/{num_batches} ({len(current_batch)} items; Start: {batch_start}, End: {batch_end-1})")\n')


# In[ ]:


# Now concatenate all batch files

concatenated_parquets_55k = (
    pl.concat(
        # now concat the batches
        [
            pl.scan_parquet(
                AnyPath(
                    OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                    f"{yr}_{mon}_icd10_3d_regenie_55k_BroadExomeIDs_batch{i+1}.parquet"
                )
            ) 
        for i in range(num_batches)
        ],
        how="align"
    )
)


# In[ ]:


# We keep a storage and use efficient .parquet just in case
(
    concatenated_parquets_55k
    .sink_parquet(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_icd10_3d_regenie_55k_BroadExomeIDs.parquet"
        ),
    )
)


# In[ ]:


# We have identifed possible bug with .sink_csv where header line separator is not changed to specified separator
# i.e. header remains comma-separated while non-header rows are tab-delimited
# We have found that if we use .write_csv instead, we resolve this issue

(
    concatenated_parquets_55k
    .collect()
    .write_csv(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_icd10_3d_regenie_55k_BroadExomeIDs.tsv"
        ),
        separator="\t",
        null_value="0"
    )
)


# In[ ]:


chart_55k = (
    alt.Chart(
        concatenated_parquets_55k       
        .with_columns(
            pl.sum_horizontal(pl.exclude(["FID", "IID"]).cast(pl.Int8)).alias("sum_traits")
        )
        .select(
            pl.col("IID"),
            pl.col("sum_traits")
        )
        .collect(),
        title=f"55k ExWAS phenotype counts per individual",
        width=650,
    )
    .mark_bar(
        fill=GNH_PALETTE["COBALT_BLUE"],
    )
    .encode(
        alt.X("sum_traits:Q").bin(maxbins=48).title(f"ICD10 3digit conditions assigned"),#.scale(type="log"),
        alt.Y("count()").title("Number of individuals"),
    )
)
chart_55k.save(
    AnyPath(
        OUTPUTS_REGENIE_FILES_LOCATION,
        "ExWAS-55k-icd-10-3-digit-phenotypes-per-individual-distribution.svg"
    )
)
chart_55k


# In[ ]:


chart_51k_and_55k = (
    (
        alt.Chart(
            concatenated_parquets_55k       
            .with_columns(
                pl.sum_horizontal(pl.exclude(["FID", "IID"]).cast(pl.Int8)).alias("sum_traits")
            )
            .select(
                pl.col("IID"),
                pl.col("sum_traits")
            )
            .collect(),
    #         title=f"55k ExWAS phenotype counts per individual",
            width=650,
        )
        .mark_bar(
            fill=GNH_PALETTE["COBALT_BLUE"],
        )
        .encode(
            alt.X("sum_traits:Q").bin(maxbins=48).title(f"ICD10 3digit conditions assigned"),#.scale(type="log"),
            alt.Y("count()").title("Number of individuals"),
        )
    )

    +

    (
        alt.Chart(
            concatenated_parquets_51k
            .with_columns(
                pl.sum_horizontal(pl.exclude(["FID", "IID"]).cast(pl.Int8)).alias("sum_traits")
            )
            .select(
                pl.col("IID"),
                pl.col("sum_traits")
            )
            .collect(),
            title=f"51k GWAS (green) vs 55k ExWAS (blue) phenotype counts per individual",
            width=650,
        )
        .mark_bar(
            fill=GNH_PALETTE["EMERALD_GREEN"],
            opacity=0.8,
        )
        .encode(
            alt.X("sum_traits:Q").bin(maxbins=48).title(f"ICD10 3digit conditions assigned"),#.scale(type="log"),
            alt.Y("count()").title("Number of individuals"),
        )
    )
)
chart_51k_and_55k.save(
    AnyPath(
        OUTPUTS_REGENIE_FILES_LOCATION,
        "GWAS-and-ExWAS-icd-10-3-digit-phenotypes-per-individual-distribution.svg"
    )
)
chart_51k_and_55k


# ## Generate covariate regenie files

# ### 51k covariate (3-digit ICD-10)

# In[ ]:


get_ipython().run_cell_magic('time', '', 'batch_size = 16  # anything above ~90 causes a "deeper than 512 elements" warning\n\n# Split the dictionary items into batches\nnum_batches = (len(combo_icd10_3d_trait_51k_dict) + batch_size - 1) // batch_size  # Ceiling division\n\nfor batch_idx in range(num_batches):\n   # Get the current batch of items\n   batch_start = batch_idx * batch_size\n   batch_end = min((batch_idx + 1) * batch_size, len(combo_icd10_3d_trait_51k_dict))\n   current_batch = dict(itertools.islice(sorted(combo_icd10_3d_trait_51k_dict.items()), batch_start, batch_end))\n\n   # Process the current batch\n   (\n       pl.concat([\n           df\n           .lazy()\n           .group_by(["FID", "IID"])\n           .agg(\n               pl.col("AgeAtFirstDiagnosis").min().round(1).alias(f"AgeAtFirstDiagnosis.{phenotype}"),\n               pl.col("AgeAtFirstDiagnosis_Squared").min().round(1).alias(f"AgeAtFirstDiagnosis_Squared.{phenotype}"),\n           )\n\n           for (phenotype, ), df in sorted(current_batch.items())\n       ],\n       how="align")\n       .sink_parquet(\n           AnyPath(\n               OUTPUTS_REGENIE_FILES_TEMP_LOCATION, \n               f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_3-digit_ICD-10_age_at_test_megawide_batch{batch_idx+1}.parquet"\n           ),\n       )\n   )\n   \n   print(f"Processed batch {batch_idx+1}/{num_batches} ({len(current_batch)} items; Start: {batch_start}, End: {batch_end-1})")\n      \n')


# In[ ]:


# Now concatenate all batch files

concatenated_51k_covariate_parquets = (
    pl.concat(
        # now concat the batches
        [
            pl.scan_parquet(
                AnyPath(
                    OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                    f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_3-digit_ICD-10_age_at_test_megawide_batch{i+1}.parquet"
                )
            ) 
        for i in range(num_batches)
        ],
        how="align"
    )
)


# In[ ]:


(
    concatenated_51k_covariate_parquets
    .collect()
    .write_csv(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_3-digit_ICD-10_age_at_first_diagnosis_megawide.tsv",
            
        ),
        separator="\t",
        null_value="NA"
    )
)


# ### 51k covariate (4-digit ICD-10)
# 
# Not generated but code below works if needed (>100k columns; 4.0GB file)
 %%time
batch_size = 16  # anything above ~90 causes a "deeper than 512 elements" warning

# Split the dictionary items into batches
num_batches = (len(combo_icd10_4d_trait_51k_dict) + batch_size - 1) // batch_size  # Ceiling division

for batch_idx in range(num_batches):
    # Get the current batch of items
    batch_start = batch_idx * batch_size
    batch_end = min((batch_idx + 1) * batch_size, len(combo_icd10_4d_trait_51k_dict))
    current_batch = dict(itertools.islice(sorted(combo_icd10_4d_trait_51k_dict.items()), batch_start, batch_end))

    # Process the current batch
    (
        pl.concat([
            df
            .lazy()
            .group_by(["FID", "IID"])
            .agg(
                pl.col("AgeAtTest").min().round(1).alias(f"AgeAtTest.{phenotype}.min"),
                pl.col("AgeAtTest_Squared").min().round(1).alias(f"AgeAtTest_Squared.{phenotype}.min"),
                pl.col("AgeAtTest").median().round(1).alias(f"AgeAtTest.{phenotype}.median"),
                pl.col("AgeAtTest_Squared").median().round(1).alias(f"AgeAtTest_Squared.{phenotype}.median"),
                pl.col("AgeAtTest").max().round(1).alias(f"AgeAtTest.{phenotype}.max"),
                pl.col("AgeAtTest_Squared").max().round(1).alias(f"AgeAtTest_Squared.{phenotype}.max"),
            )

            for (phenotype, ), df in sorted(current_batch.items())
        ],
        how="align")
        .sink_parquet(
            AnyPath(
                OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_4-digit_ICD-10_age_at_test_megawide_batch{batch_idx+1}.parquet"
            ),
        )
    )
    
    print(f"Processed batch {batch_idx+1}/{num_batches} ({len(current_batch)} items; Start: {batch_start}, End: {batch_end-1})")
       # Now concatenate all batch files

concatenated_4d_51k_covariate_parquets = (
    pl.concat(
        # now concat the batches
        [
            pl.scan_parquet(
                AnyPath(
                    OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                    f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_4-digit_ICD-10_age_at_test_megawide_batch{i+1}.parquet"
                )
            ) 
        for i in range(num_batches)
        ],
        how="align"
    )
)%%time
# concatenated_covariate_parquets.select(pl.col("FID")).collect().shape
concatenated_4d_51k_covariate_parquets.collect().write_parquet(
    AnyPath(
        OUTPUTS_REGENIE_FILES_LOCATION,
        f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_4-digit_ICD-10_age_at_test_megawide.parquet",
    )
%%time
(
    concatenated_4d_51k_covariate_parquets.collect().write_csv(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_regenie_51koct2024_65A_Topmed_Binary_4-digit_ICD-10_age_at_test_megawide.tsv",
            
        ),
        separator="\t",
        null_value="NA"
    )
)
# ### 55k covariate (3-digit ICD-10)

# In[ ]:


get_ipython().run_cell_magic('time', '', 'batch_size = 16  # anything above ~90 causes a "deeper than 512 elements" warning\n\n# Split the dictionary items into batches\nnum_batches = (len(combo_icd10_3d_trait_55k_dict) + batch_size - 1) // batch_size  # Ceiling division\n\nfor batch_idx in range(num_batches):\n   # Get the current batch of items\n   batch_start = batch_idx * batch_size\n   batch_end = min((batch_idx + 1) * batch_size, len(combo_icd10_3d_trait_55k_dict))\n   current_batch = dict(itertools.islice(sorted(combo_icd10_3d_trait_55k_dict.items()), batch_start, batch_end))\n\n   # Process the current batch\n   (\n       pl.concat([\n           df\n           .lazy()\n           .group_by(["FID", "IID"])\n           .agg(\n               pl.col("AgeAtFirstDiagnosis").min().round(1).alias(f"AgeAtFirstDiagnosis.{phenotype}"),\n               pl.col("AgeAtFirstDiagnosis_Squared").min().round(1).alias(f"AgeAtFirstDiagnosis_Squared.{phenotype}"),\n           )\n\n           for (phenotype, ), df in sorted(current_batch.items())\n       ],\n       how="align")\n       .sink_parquet(\n           AnyPath(\n               OUTPUTS_REGENIE_FILES_TEMP_LOCATION, \n               f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_3-digit_ICD-10_age_at_test_megawide_batch{batch_idx+1}.parquet"\n           ),\n       )\n   )\n   \n   print(f"Processed batch {batch_idx+1}/{num_batches} ({len(current_batch)} items; Start: {batch_start}, End: {batch_end-1})")\n      \n')


# In[ ]:


# Now concatenate all batch files

concatenated_covariate_parquets = (
    pl.concat(
        # now concat the batches
        [
            pl.scan_parquet(
                AnyPath(
                    OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                    f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_3-digit_ICD-10_age_at_test_megawide_batch{i+1}.parquet"
                )
            ) 
        for i in range(num_batches)
        ],
        how="align"
    )
)


# In[ ]:


(
    concatenated_covariate_parquets.collect().write_csv(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_3-digit_ICD-10_age_at_first_diagnosis_megawide.tsv",
            
        ),
        separator="\t",
        null_value="NA"
    )
)


# ### 55k covariate (4-digit ICD-10)
# 
# Not generated but code below works if needed (>100k columns; 4.3GB file... and that's a lot of WAPPEs)
 %%time
batch_size = 16  # anything above ~90 causes a "deeper than 512 elements" warning

# Split the dictionary items into batches
num_batches = (len(combo_icd10_4d_trait_55k_dict) + batch_size - 1) // batch_size  # Ceiling division

for batch_idx in range(num_batches):
    # Get the current batch of items
    batch_start = batch_idx * batch_size
    batch_end = min((batch_idx + 1) * batch_size, len(combo_icd10_4d_trait_55k_dict))
    current_batch = dict(itertools.islice(sorted(combo_icd10_4d_trait_55k_dict.items()), batch_start, batch_end))

    # Process the current batch
    (
        pl.concat([
            df
            .lazy()
            .group_by(["FID", "IID"])
            .agg(
                pl.col("AgeAtTest").min().round(1).alias(f"AgeAtTest.{phenotype}.min"),
                pl.col("AgeAtTest_Squared").min().round(1).alias(f"AgeAtTest_Squared.{phenotype}.min"),
                pl.col("AgeAtTest").median().round(1).alias(f"AgeAtTest.{phenotype}.median"),
                pl.col("AgeAtTest_Squared").median().round(1).alias(f"AgeAtTest_Squared.{phenotype}.median"),
                pl.col("AgeAtTest").max().round(1).alias(f"AgeAtTest.{phenotype}.max"),
                pl.col("AgeAtTest_Squared").max().round(1).alias(f"AgeAtTest_Squared.{phenotype}.max"),
            )

            for (phenotype, ), df in sorted(current_batch.items())
        ],
        how="align")
        .sink_parquet(
            AnyPath(
                OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_4-digit_ICD-10_age_at_test_megawide_batch{batch_idx+1}.parquet"
            ),
        )
    )
    
    print(f"Processed batch {batch_idx+1}/{num_batches} ({len(current_batch)} items; Start: {batch_start}, End: {batch_end-1})")
       # Now concatenate all batch files

concatenated_4d_55k_covariate_parquets = (
    pl.concat(
        # now concat the batches
        [
            pl.scan_parquet(
                AnyPath(
                    OUTPUTS_REGENIE_FILES_TEMP_LOCATION, 
                    f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_4-digit_ICD-10_age_at_test_megawide_batch{i+1}.parquet"
                )
            ) 
        for i in range(num_batches)
        ],
        how="align"
    )
)%%time
# concatenated_covariate_parquets.select(pl.col("FID")).collect().shape
concatenated_4d_55k_covariate_parquets.collect().write_parquet(
    AnyPath(
        OUTPUTS_REGENIE_FILES_LOCATION,
        f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_4-digit_ICD-10_age_at_test_megawide.parquet",
    )
%%time
(
    concatenated_4d_55k_covariate_parquets.collect().write_csv(
        AnyPath(
            OUTPUTS_REGENIE_FILES_LOCATION,
            f"{yr}_{mon}_regenie_55k_BroadExomeIDs_Binary_4-digit_ICD-10_age_at_test_megawide.tsv",
            
        ),
        separator="\t",
        null_value="NA"
    )
)
# ## Create phenotype reports

# cf. version080 
# 
# `.../3-digit-ICD/overall_summary_report_README.md`
# 

# In[ ]:


def generate_icd_phenotypes_count_summary(icd_length=3) -> None:
    if not 3 <= icd_length <= 4:
        raise ValueError("`icd_length` must be 3 or 4")
    df_dict = {3: combo_icd10_3d, 4: combo_icd10_4d}
    output_location_dict = {
        3: OUTPUTS_3D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION,
        4: OUTPUTS_4D_ICD_INDIVIDUAL_TRAIT_FILES_LOCATION
    }
    report_filename = f"icd_{icd_length}_digit_phenotypes_count_summary.md"
    report_path = AnyPath(output_location_dict[icd_length], report_filename)
    with open(report_path, "w") as f:
        print(f"""# ICD-10 {icd_length}-digit Phenotype Count Summary Report

## Generated by Genes and Health *BI_PY* pipeline

Generated by the BI_PY `7-three-and-four-digit-ICD.ipynb` notebook.

## Report Generation Date and Time

This report was generated at {datetime.today().strftime(format="%d %B %Y %H:%M:%S")}.
 
## Report Overview

This summary report provides an overview of the counts associated with each phenotype across all datasets:

- Discovery Primary Care (where SNOMED CT has a mapping to ICD-10 {icd_length}-digit)
- Barts Health
- Bradford
- NHS Digital

## Summary
""", file=f)

    with pl.Config(
        tbl_formatting="MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_cell_numeric_alignment="RIGHT",
        tbl_rows=-1,
    ):
        with open(report_path, "a") as f:
            print(
                df_dict[icd_length]
            #     .head(5)
                .select(
                    pl.col("code").value_counts()
                )
                .unnest("code")
                .join(
                    pl.LazyFrame({"code": generate_icd10_codes(icd_length=icd_length)}),
                    on="code",
                    how="right"
                )
                .sort("code")

                .select(
                    pl.col("code").alias("phenotype"),
                    pl.col("count").fill_null(0),
                )
                .collect(),
                file=f
            )
    print(f"Report `{report_filename}` generated.")


# In[ ]:


generate_icd_phenotypes_count_summary(3)


# In[ ]:


generate_icd_phenotypes_count_summary(4)


# # Tidy up by removing temp files and temp directory

# In[ ]:


def AnyPath_rmtree(ap: AnyPath) -> None:
    '''
    Adapted from https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    '''
    if ap.is_file():
        print(f"Deleting file '{ap.name}'")
        ap.unlink()
    else:
        for child in ap.iterdir():
            AnyPath_rmtree(child)
        print(f"Removing directory '{ap.name}'")
        ap.rmdir()
        


# In[ ]:


AnyPath_rmtree(AnyPath(OUTPUTS_REGENIE_FILES_TEMP_LOCATION))


# ### Run next cell to initiate next notebook

# In[ ]:


redirect_to_next_notebook_in_pipeline("8-custom-phenotypes-individual-trait-files-and-regenie")


# In[ ]:





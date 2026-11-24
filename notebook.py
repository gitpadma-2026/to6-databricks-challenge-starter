# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

# MAGIC %sql SELECT 1 AS connection_check

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/padma.nagulapati2.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)


# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,Catalog descriptions, lineage and sample rows
# Catalog descriptions: table comments and column comments for both deliveries and the lineage view
for t in [INITIAL_TABLE, CORRECTED_TABLE, LINEAGE_VIEW]:
    info = spark.sql(f"DESCRIBE TABLE EXTENDED {t}").collect()
    comment = next((r.data_type for r in info if r.col_name == "Comment"), None)
    ttype = next((r.data_type for r in info if r.col_name == "Type"), None)
    print(f"{t} [{ttype}]\n  Comment: {comment}")
    cols = spark.sql(f"DESCRIBE TABLE {t}").collect()
    for c in cols:
        if c.col_name and not c.col_name.startswith("#"):
            print(f"  - {c.col_name}: {c.data_type} | comment: {c.comment}")

# Supplied lineage view: upstream source of each delivery
display(spark.table(LINEAGE_VIEW))

# At least three sample rows from each delivery
display(spark.table(INITIAL_TABLE).orderBy("return_id").limit(3))
display(spark.table(CORRECTED_TABLE).orderBy("return_id").limit(3))

# COMMAND ----------

# DBTITLE 1,Source reference and explanation
# MAGIC %md
# MAGIC ### Source reference
# MAGIC * **Inputs:** `training.to6_challenge3_inputs.returns_initial` (Catalog: "Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output.") and `training.to6_challenge3_inputs.returns_corrected` ("Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness.").
# MAGIC * **Lineage:** `training.to6_challenge3_inputs.source_lineage` maps both deliveries to the upstream table `training.to6_challenge3_inputs.parsed_source` (initial: "validation required"; corrected: "same validation rules"). The view's comment notes it is a supplied map, not a Catalog UI lineage check.
# MAGIC * **Row meaning:** one row = one synthetic tax return for tax years 2022 to 2024.
# MAGIC * **Relevant fields:** `return_id` (string, the return), `taxpayer_id` (string, the filer, may file in several years), `tax_year` (int), `income` (int), `deduction` (int). Required: `return_id`, `tax_year`, `income`, `deduction`. The columns have no Catalog comments, so their meaning comes from CHALLENGE.md and the sample rows.
# MAGIC * **Uniqueness identifier:** `return_id`, which must occur exactly once. `taxpayer_id` is not unique (F0 files in 2022, 2023 and 2024).

# COMMAND ----------

# DBTITLE 1,Table discovery via Unity Catalog metadata
# Find candidate tables for this task from Unity Catalog metadata (information_schema), not by guessing names
from pyspark.sql import functions as F

CATALOG = "training"
NEEDED_COLS = {"return_id", "taxpayer_id", "tax_year", "income", "deduction"}

# 1. Candidate relations: anything that looks like a return delivery, source or lineage map
candidates = spark.sql(f"""
    SELECT table_schema, table_name, table_type, table_owner, comment, created, last_altered
    FROM {CATALOG}.information_schema.tables
    WHERE (table_name ILIKE '%returns%' OR table_name ILIKE '%parsed%' OR table_name ILIKE '%lineage%')
      AND table_name NOT ILIKE '%features%'          -- outputs, not inputs
    ORDER BY table_schema, table_name""")
display(candidates)

# 2. Columns and comments (scoped to the candidate schemas) and fit to the required fields
cand_schemas = [r.table_schema for r in candidates.select("table_schema").distinct().collect()]
schema_list = ", ".join(f"'{s}'" for s in cand_schemas)
cols = spark.sql(f"""
    SELECT table_schema, table_name, column_name, data_type, comment
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ({schema_list})""")
fit = (cols.groupBy("table_schema", "table_name")
           .agg(F.collect_set("column_name").alias("columns"),
                F.count("comment").alias("commented_columns"))
           .withColumn("has_all_required",
                       F.size(F.array_intersect("columns", F.array(*[F.lit(c) for c in sorted(NEEDED_COLS)]))) == len(NEEDED_COLS)))
display(fit.orderBy("table_schema", "table_name"))

# 3. Certification status: governed tags at catalog, schema, table and column level
for level, view, name_col in [("schema", "schema_tags", "schema_name"),
                              ("table", "table_tags", "schema_name"),
                              ("column", "column_tags", "schema_name")]:
    tags = spark.sql(f"SELECT * FROM {CATALOG}.information_schema.{view} WHERE {name_col} IN ({schema_list})")
    print(f"{level} tags on candidate schemas: {tags.count()}")
    if tags.count():
        display(tags)

# 4. Recorded use: system lineage if permitted, otherwise metadata that records use
try:
    usage = spark.sql(f"""
        SELECT source_table_schema, source_table_name, count(*) AS events,
               count(DISTINCT created_by) AS users, max(event_time) AS last_used
        FROM system.access.table_lineage
        WHERE source_table_catalog = '{CATALOG}' AND source_table_schema IN ({schema_list})
        GROUP BY ALL""")
    display(usage)
except Exception as e:
    print("system.access.table_lineage not available:", str(e).splitlines()[0][:160])

# Fallback evidence of use: the supplied lineage map, and downstream output tables whose comments cite an input
display(spark.table(f"{CATALOG}.to6_challenge3_inputs.source_lineage"))
display(spark.sql(f"""
    SELECT table_schema, table_name, comment
    FROM {CATALOG}.information_schema.tables
    WHERE table_name = 'return_features' AND comment ILIKE '%returns_corrected%'"""))

# COMMAND ----------

# DBTITLE 1,Table selection and rationale
# MAGIC %md
# MAGIC ### Table selection from Unity Catalog metadata
# MAGIC Method: queried `training.information_schema.tables` for return, parsed and lineage relations. Then read `information_schema.columns` (scoped to the candidate schemas) and the schema, table and column tags. Last, tried `system.access.table_lineage` for recorded use.
# MAGIC
# MAGIC **Candidates with all required columns** (`return_id`, `taxpayer_id`, `tax_year`, `income`, `deduction`):
# MAGIC
# MAGIC | Schema | Tables | Table comment | Certification tags | Recorded use |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | `to6_challenge3_inputs` | `returns_initial`, `returns_corrected`, `parsed_source`, plus the `source_lineage` view | Delivery comments say they come from `parsed_source`; the view says "Supplied source map for Challenge 3" | none | `source_lineage` maps both deliveries; 2 other trainees' `return_features` comments cite `training.to6_challenge3_inputs.returns_corrected` |
# MAGIC | `to6_trial_inputs_20261115` | `returns_initial`, `returns_corrected`, `parsed_source` | Similar delivery comments, no Challenge 3 reference | none | no lineage view and no downstream comments citing it; `parsed_source` last altered 2026-11-23, so it is still changing |
# MAGIC
# MAGIC **Rejected:** tables in `default` (for example `returns_raw`, `v4_parsed_returns`) use `case_id` and other XML-derived fields. None has `return_id` or `tax_year`/`income`/`deduction` in this shape.
# MAGIC
# MAGIC **Decision:** use `training.to6_challenge3_inputs`:
# MAGIC * `returns_initial` is assessed and rejected.
# MAGIC * `returns_corrected` is assessed, accepted and is the only feature source.
# MAGIC * `source_lineage` is the lineage reference.
# MAGIC * `parsed_source` is recorded as the upstream table but not read directly, because the task is to assess the deliveries.
# MAGIC
# MAGIC Why: it is the only schema whose own metadata ties it to Challenge 3 (the lineage view comment). It is also the only one with recorded downstream use (other `return_features` comments). The trial schema has matching columns but no such record and looks like a test copy.
# MAGIC
# MAGIC **Columns used:** `return_id` (key), `tax_year` (split year), `income` and `deduction` (ratio inputs). `taxpayer_id` is read only for profiling and is excluded from outputs (need-to-know).
# MAGIC
# MAGIC **Not verifiable:**
# MAGIC * No candidate has certification tags, and no column has a Catalog comment. Column meaning comes from the table comments and CHALLENGE.md.
# MAGIC * `system.access.table_lineage` is not readable (INSUFFICIENT_PERMISSIONS on `system.access`). Recorded use therefore relies on the supplied view and downstream table comments, not system lineage.

# COMMAND ----------

# DBTITLE 1,Explore initial table
from pyspark.sql import functions as F

initial_df = spark.table(INITIAL_TABLE)
initial_df.printSchema()

# Show every row (the batch is small), ordered so duplicates sit together
total_count = initial_df.count()
print(f"Total rows: {total_count}")
display(initial_df.orderBy("return_id", "tax_year"))

# Returns by year
display(initial_df.groupBy("tax_year").count().orderBy("tax_year"))

# Uniqueness of return_id
distinct_ids = initial_df.select("return_id").distinct().count()
duplicate_ids = initial_df.groupBy("return_id").count().filter("count > 1").orderBy("return_id")
print(f"Total rows: {total_count}, Distinct return_id: {distinct_ids}, Duplicate return_ids: {duplicate_ids.count()}")
display(duplicate_ids)

# Income: null or negative breaks the contract; zero income is valid (info only)
invalid_income = initial_df.filter(F.col("income").isNull() | (F.col("income") < 0)).orderBy("return_id")
zero_income = initial_df.filter(F.col("income") == 0).orderBy("return_id")
print(f"Rows with null or negative income: {invalid_income.count()}")
display(invalid_income)
print(f"Rows with zero income (valid, info only): {zero_income.count()}")
display(zero_income)

passed = total_count == distinct_ids and invalid_income.count() == 0
print("INITIAL batch PASSES checks." if passed else "INITIAL batch FAILS checks.")

# COMMAND ----------

# DBTITLE 1,Full-batch summary by year (both deliveries)
from pyspark.sql import functions as F
from pyspark.sql import Window

def batch_profile(table):
    df = spark.table(table)
    dup_flag = (F.count("*").over(Window.partitionBy("return_id")) > 1).cast("int")
    return (df.withColumn("is_dup_row", dup_flag)
              .groupBy("tax_year")
              .agg(F.count("*").alias("rows"),
                   F.countDistinct("return_id").alias("distinct_return_ids"),
                   F.sum("is_dup_row").alias("rows_with_duplicate_return_id"),
                   *[F.sum(F.col(c).isNull().cast("int")).alias(f"missing_{c}")
                     for c in ["return_id", "tax_year", "income", "deduction"]],
                   F.sum((F.col("income") == 0).cast("int")).alias("zero_income"),
                   F.concat_ws(",", F.sort_array(F.collect_list(
                       F.when(F.col("income").isNull() | (F.col("income") == 0) | (F.col("is_dup_row") == 1), F.col("return_id"))
                   ))).alias("ids_to_note"))
              .withColumn("batch", F.lit(table.split(".")[-1]))
              .select("batch", "tax_year", "rows", "distinct_return_ids", "rows_with_duplicate_return_id",
                      "missing_return_id", "missing_tax_year", "missing_income", "missing_deduction",
                      "zero_income", "ids_to_note"))

year_summary = batch_profile(INITIAL_TABLE).unionByName(batch_profile(CORRECTED_TABLE)).orderBy("batch", "tax_year")
display(year_summary)

# Batch totals (duplicates counted once per return_id)
for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    df = spark.table(t)
    dups = [r.return_id for r in df.groupBy("return_id").count().filter("count > 1").collect()]
    missing = {c: df.filter(F.col(c).isNull()).count() for c in ["return_id", "tax_year", "income", "deduction"]}
    zero = sorted(r.return_id for r in df.filter(F.col("income") == 0).collect())
    print(f"{t}: rows={df.count()}, distinct return_id={df.select('return_id').distinct().count()}, "
          f"duplicate return_ids={sorted(dups)}, missing={missing}, zero_income={zero}")

# COMMAND ----------

# DBTITLE 1,Section 1 observations and impact
# MAGIC %md
# MAGIC ### Observations from the full batches
# MAGIC | Batch | 2022 | 2023 | 2024 | Total rows / distinct IDs | Duplicate IDs | Missing required | Zero income |
# MAGIC | --- | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | `returns_initial` | 5 | 4 | 4 | 13 / 12 | R01 (x2, identical rows, 2022) | 1: R02 `income` | R03 (2022), R09 (2024) |
# MAGIC | `returns_corrected` | 4 | 4 | 4 | 12 / 12 | none | none | R03 (2022), R09 (2024) |
# MAGIC
# MAGIC The 3-row samples alone would not show all of this. For example, R11 and R12 only appear in the full batch, so every check uses the whole table.
# MAGIC
# MAGIC How these findings affect the next steps:
# MAGIC * **Duplicate R01:** the uniqueness check on `return_id` (section 2) must reject `returns_initial`. Dropping the extra copy silently is not allowed, so the affected rows are kept as rejection evidence.
# MAGIC * **Missing R02 income:** the completeness check must reject `returns_initial`. The value must not be imputed as 0. In `returns_corrected`, R02 has income 200.
# MAGIC * **Zero income (R03, R09):** valid under the contract. These rows are kept, and the feature step must produce a null `deduction_to_income` for them instead of dividing by zero.
# MAGIC * **Year coverage:** after correction there are 4 returns per year, so each of the 2022/2023/2024 split tables in section 4 should hold 4 returns.
# MAGIC * **`taxpayer_id`:** repeats across years (for example F0 in 2022, 2023 and 2024), so it is not a key. It is also excluded from the features under need-to-know.
# MAGIC
# MAGIC These are exploratory observations. They do not replace the quality checks in section 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,Validate initial vs corrected batch
# Same contract rules applied to both batches: required fields non-null, return_id unique,
# amounts nonnegative. Income = 0 is acceptable (reported for information only).
import json
from pathlib import Path
from pyspark.sql import functions as F, Window

REQUIRED = ["return_id", "tax_year", "income", "deduction"]

def validate(table_name):
    df = spark.table(table_name)
    dup_w = Window.partitionBy("return_id")
    flagged = (
        df.withColumn("_id_count", F.count(F.lit(1)).over(dup_w))
          .withColumn("reasons", F.concat_ws("; ",
              *[F.when(F.col(c).isNull(), F.lit(f"missing required {c}")) for c in REQUIRED],
              F.when(F.col("_id_count") > 1, F.lit("duplicate return_id")),
              F.when(F.col("income") < 0, F.lit("negative income")),
              F.when(F.col("deduction") < 0, F.lit("negative deduction"))))
    )
    affected = flagged.filter(F.col("reasons") != "").drop("_id_count")
    summary = {
        "table": table_name,
        "rows": df.count(),
        "distinct_return_ids": df.select("return_id").distinct().count(),
        "missing_required": {c: df.filter(F.col(c).isNull()).count() for c in REQUIRED},
        "duplicate_return_ids": df.groupBy("return_id").count().filter("count > 1").count(),
        "negative_amounts": df.filter((F.col("income") < 0) | (F.col("deduction") < 0)).count(),
        "zero_income_info_only": df.filter(F.col("income") == 0).count(),
        "affected_rows": affected.count(),
    }
    summary["passed"] = summary["affected_rows"] == 0
    return df, affected, summary

initial_df, initial_affected, initial_summary = validate(INITIAL_TABLE)
corrected_df, corrected_affected, corrected_summary = validate(CORRECTED_TABLE)
for s in (initial_summary, corrected_summary):
    print(json.dumps(s, indent=2))

print("Initial batch affected records:")
display(initial_affected.orderBy("return_id"))

# Preserve rejection evidence before any feature output is written
out_dir = Path(repo_root) / "outputs"
(initial_affected.orderBy("return_id").toPandas().astype({"income": "Int64"})
    .to_csv(out_dir / "initial_rejected_records.csv", index=False))
(out_dir / "batch_validation_summary.json").write_text(
    json.dumps({"initial": initial_summary, "corrected": corrected_summary}, indent=2) + "\n", encoding="utf-8")

# Decision gates: stop the run if the outcome is not as expected
assert not initial_summary["passed"], "Initial batch was expected to fail validation"
assert corrected_summary["passed"], f"Corrected batch failed validation: {corrected_summary}"
ACCEPTED_TABLE = CORRECTED_TABLE
print(f"DECISION: reject {INITIAL_TABLE}; use {ACCEPTED_TABLE} for feature output.")

# COMMAND ----------

# DBTITLE 1,Batch decision findings
# MAGIC %md
# MAGIC ### Findings and decision
# MAGIC Source: both tables come from `training.to6_challenge3_inputs.parsed_source` (per `source_lineage`); the same rules are applied to both.
# MAGIC
# MAGIC Rules: required `return_id`, `tax_year`, `income`, `deduction` non-null; `return_id` unique; amounts nonnegative. Zero income is **valid** per the contract (cell 6's `income <= 0` check was too strict and is reported for information only).
# MAGIC
# MAGIC | Check | returns_initial | returns_corrected |
# MAGIC | --- | --- | --- |
# MAGIC | Rows / distinct return_id | 13 / 12 | 12 / 12 |
# MAGIC | Duplicate return_id | 1 (R01 appears twice) | 0 |
# MAGIC | Missing required values | 1 (R02 income null) | 0 |
# MAGIC | Negative amounts | 0 | 0 |
# MAGIC | Zero income (valid) | 2 (R03, R09) | 2 (R03, R09) |
# MAGIC | Result | **FAIL** (3 affected rows) | **PASS** |
# MAGIC
# MAGIC * **Initial batch rejected**: R01 is duplicated (uniqueness rule) and R02 has null income (completeness rule). Affected records with reasons are saved to `outputs/initial_rejected_records.csv`; summary in `outputs/batch_validation_summary.json`.
# MAGIC * Imputing R02 income as 0 or dropping a row would violate the contract, so the next action is to use the supplier's replacement.
# MAGIC * **Corrected batch accepted**: 12 unique returns, 4 per year (2022/2023/2024); R02 now has income 200. R03 and R09 keep zero income and will get a null `deduction_to_income`.

# COMMAND ----------

# DBTITLE 1,Validation on corrected table (zero income allowed)
import json
from pyspark.sql import functions as F

# Explore the corrected batch: schema and every row (only 12 rows)
corr_df = spark.table(CORRECTED_TABLE)
corr_df.printSchema()
display(corr_df.orderBy("return_id"))

# Rules: return_id must be unique; income must not be negative or null. Income = 0 is ACCEPTABLE.
total = corr_df.count()
distinct_ids = corr_df.select("return_id").distinct().count()
dup_ids = [r.return_id for r in corr_df.groupBy("return_id").count().filter("count > 1").collect()]
bad_income_df = corr_df.filter(F.col("income").isNull() | (F.col("income") < 0)).orderBy("return_id")
zero_income_ids = sorted(r.return_id for r in corr_df.filter(F.col("income") == 0).collect())
flagged = [r.return_id for r in bad_income_df.select("return_id").collect()]

print(f"Total rows: {total}, Distinct return_id: {distinct_ids}, Duplicate return_ids: {len(dup_ids)}")
print(f"Rows with negative or null income: {len(flagged)}")
print(f"Rows with zero income (acceptable, info only): {zero_income_ids}")
display(bad_income_df)

# Split the income flags so the result can be compared with the data contract (zero income is valid)
income_breakdown = corr_df.agg(
    F.sum(F.col("income").isNull().cast("int")).alias("null_income"),
    F.sum((F.col("income") < 0).cast("int")).alias("negative_income"),
    F.sum((F.col("income") == 0).cast("int")).alias("zero_income"),
).first().asDict()

cell6_result = {
    "table": CORRECTED_TABLE,
    "rows": total,
    "distinct_return_ids": distinct_ids,
    "duplicate_return_ids": len(dup_ids),
    "rules": ["return_id unique", "income not null", "income not negative (0 is acceptable)"],
    "negative_or_null_income": len(flagged),
    "flagged_income_ids": flagged,
    "zero_income_ids_acceptable": zero_income_ids,
    "income_breakdown": income_breakdown,
    "passed": len(dup_ids) == 0 and len(flagged) == 0,
}
print(json.dumps(cell6_result, indent=2))
print("CORRECTED batch PASSES validation." if cell6_result["passed"] else "CORRECTED batch FAILS validation.")

with open(f"{repo_root}/outputs/corrected_cell6_validation.json", "w") as fh:
    json.dump(cell6_result, fh, indent=2)

# COMMAND ----------

# DBTITLE 1,Validation on corrected table: findings
# MAGIC %md
# MAGIC ### Validation on `returns_corrected`: findings
# MAGIC Rule update: income = 0 is **acceptable**. Income must only be non-null and not negative (matches CHALLENGE.md).
# MAGIC
# MAGIC | Rule | returns_initial | returns_corrected |
# MAGIC | --- | --- | --- |
# MAGIC | `return_id` unique | FAIL: 13 rows, 12 distinct (R01 x2) | PASS: 12 rows, 12 distinct |
# MAGIC | income not negative or null | FAIL: R02 (null) | PASS: none |
# MAGIC | Zero income (acceptable, info) | R03, R09 | R03 (2022), R09 (2024) |
# MAGIC | Overall | FAIL | **PASS** |
# MAGIC
# MAGIC Earlier version: under the stricter `income <= 0` rule the corrected batch failed only on R03/R09; that rule has been replaced.
# MAGIC
# MAGIC Corrected-batch income breakdown: 0 null, 0 negative, 2 zero (R03 2022, R09 2024). Re-run 2026-11-23 with the updated rule: PASS, no flagged IDs. Matches the independent SQL aggregate (12 rows, 12 distinct IDs, 0 negative and 0 null income).
# MAGIC
# MAGIC Analysis:
# MAGIC * The supplier fixed both defects that are actual contract violations: the duplicate R01 row is gone and R02 now has income 200. No negative or null income remains.
# MAGIC * No rows are flagged. R03 and R09 have income 0 in **both** batches; under the updated rule they are acceptable and reported for information only.
# MAGIC * This matches CHALLENGE.md: "Amounts are nonnegative; zero income is valid" and "a zero-income return stays in the table with a null ratio".
# MAGIC * Known-answer check: the same rules flag exactly the initial batch's known defects (R01 duplicate, R02 null income).
# MAGIC
# MAGIC Decision: `returns_corrected` is accepted. R03 and R09 are kept with a null `deduction_to_income`. Evidence: `outputs/corrected_cell6_validation.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build, save and verify return_features
import json
from pyspark.sql import functions as F

FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
FEATURE_COLS = ["return_id", "tax_year", "deduction_to_income"]
REQUIRED = ["return_id", "tax_year", "income", "deduction"]

# Gate: write only if the accepted batch still passes the contract checks
source_df = spark.table(CORRECTED_TABLE)
n_src = source_df.count()
assert n_src == source_df.select("return_id").distinct().count(), "return_id not unique"
assert source_df.filter(" OR ".join(f"{c} IS NULL" for c in REQUIRED)).count() == 0, "missing required values"
assert source_df.filter("income < 0 OR deduction < 0").count() == 0, "negative amounts"

# Build: ratio = deduction / income; null when income = 0 (row kept). No taxpayer_id or raw amounts.
features_df = source_df.select(
    "return_id",
    "tax_year",
    F.when(F.col("income") != 0, F.col("deduction").cast("double") / F.col("income")).alias("deduction_to_income"),
)
features_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

# Read back the saved table
saved = spark.table(FEATURES_TABLE)
saved_rows = {r.return_id: r for r in saved.collect()}
src_rows = {r.return_id: r for r in source_df.collect()}

assert saved.columns == FEATURE_COLS, f"unexpected columns {saved.columns}"
assert saved.count() == n_src == len(saved_rows), "row count or duplicate mismatch"
assert set(saved_rows) == set(src_rows), "return_id set differs from source"

# Nonzero check: expected value derived from the source row
r01 = src_rows["R01"]
expected_r01 = r01.deduction / r01.income
assert abs(saved_rows["R01"].deduction_to_income - expected_r01) < 1e-12

# Zero-income check: present with null ratio
zero_ids = sorted(rid for rid, r in src_rows.items() if r.income == 0)
assert zero_ids and all(saved_rows[rid].deduction_to_income is None for rid in zero_ids)

# Every row: year and ratio recomputed from source
for rid, r in src_rows.items():
    exp = None if r.income == 0 else r.deduction / r.income
    got = saved_rows[rid].deduction_to_income
    assert saved_rows[rid].tax_year == r.tax_year
    assert (exp is None and got is None) or abs(got - exp) < 1e-12, rid

feature_checks = {
    "table": FEATURES_TABLE,
    "columns": saved.columns,
    "rows": saved.count(),
    "distinct_return_ids": len(saved_rows),
    "nonzero_check": {"return_id": "R01", "deduction": r01.deduction, "income": r01.income,
                      "expected": expected_r01, "saved": saved_rows["R01"].deduction_to_income},
    "zero_income_null_ratio": {rid: saved_rows[rid].deduction_to_income for rid in zero_ids},
    "all_rows_match_source": True,
}
print(json.dumps(feature_checks, indent=2))
display(saved.orderBy("return_id"))

# Exports
saved.orderBy("return_id").toPandas().to_csv(f"{repo_root}/outputs/return_features.csv", index=False)
with open(f"{repo_root}/outputs/return_features_checks.json", "w") as fh:
    json.dump(feature_checks, fh, indent=2)
print("All return_features checks passed.")

# COMMAND ----------

# DBTITLE 1,return_features findings
# MAGIC %md
# MAGIC ### return_features: contract and verification
# MAGIC Table: `training.to6_c3_8e4b3cdc76d4.return_features`, built only from the accepted `returns_corrected` batch, written with `overwrite` so reruns don't add copies.
# MAGIC
# MAGIC * **Columns**: exactly `return_id`, `tax_year`, `deduction_to_income`. Under the need-to-know rule, `taxpayer_id`, `income` and `deduction` are not saved.
# MAGIC * **Ratio**: `deduction / income` as a double; when `income = 0` the ratio is null and the row stays in the table.
# MAGIC
# MAGIC The saved table was read back and checked:
# MAGIC
# MAGIC | Check | Result |
# MAGIC | --- | --- |
# MAGIC | Column list equals contract | Pass |
# MAGIC | Rows / distinct return_id vs accepted source | 12 / 12, same ID set |
# MAGIC | Nonzero expected value: R01 = 20 / 100 = 0.2 (from source row) | Saved 0.2 |
# MAGIC | Zero-income cases R03, R09 | Present, ratio null |
# MAGIC | All 12 rows: year and ratio recomputed from source | All match |
# MAGIC | Independent SQL join to source | 12 rows, 10 ratios match, 2 zero-income nulls, 12 years match |
# MAGIC
# MAGIC The build cell first re-checks the contract rules on `returns_corrected` (unique `return_id`, no missing required values, no negative amounts) and stops before writing if any check fails. Re-run 2026-11-23: all checks passed and the independent SQL join gave the same results.
# MAGIC
# MAGIC Exports: `outputs/return_features.csv`, `outputs/return_features_checks.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Handoff
import json
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 4a. Split return_features by tax_year into train / validation / test
# ---------------------------------------------------------------------------
YEAR_SPLIT = {
    2022: f"{OUTPUT_SCHEMA}.features_train",
    2023: f"{OUTPUT_SCHEMA}.features_validation",
    2024: f"{OUTPUT_SCHEMA}.features_test",
}

features_df = spark.table(f"{OUTPUT_SCHEMA}.return_features")
src_count = features_df.count()
src_ids = {r.return_id for r in features_df.select("return_id").collect()}

for year, table_name in YEAR_SPLIT.items():
    year_df = features_df.filter(F.col("tax_year") == year)
    year_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"Wrote {table_name} ({year}): {year_df.count()} rows")

# ---------------------------------------------------------------------------
# 4b. Read-back verification
# ---------------------------------------------------------------------------
all_read_back = None
verification = {}
for year, table_name in YEAR_SPLIT.items():
    tb = spark.table(table_name)
    n = tb.count()
    ids = sorted(r.return_id for r in tb.select("return_id").collect())
    distinct_ids = tb.select("return_id").distinct().count()
    # Every row must belong to the declared year
    wrong_year = tb.filter(F.col("tax_year") != year).count()
    verification[table_name] = {
        "year": year, "rows": n, "distinct_return_ids": distinct_ids,
        "return_ids": ids, "wrong_year_rows": wrong_year,
    }
    assert n == distinct_ids, f"{table_name}: duplicate return_ids"
    assert wrong_year == 0, f"{table_name}: rows with wrong tax_year"
    assert tb.columns == ["return_id", "tax_year", "deduction_to_income"], f"{table_name}: unexpected columns"
    all_read_back = tb if all_read_back is None else all_read_back.unionByName(tb)

# Union of three tables must equal the source feature table exactly
union_count = all_read_back.count()
union_ids = {r.return_id for r in all_read_back.select("return_id").collect()}
assert union_count == src_count, f"Union rows {union_count} != source {src_count}"
assert union_ids == src_ids, "Return-ID sets differ between union and source"
assert all_read_back.select("return_id").distinct().count() == union_count, "Duplicate return_id across splits"

# Value-level check: every row in the union matches the source
joined = all_read_back.alias("u").join(features_df.alias("s"), "return_id")
assert joined.count() == src_count
mismatches = joined.filter(
    (F.col("u.tax_year") != F.col("s.tax_year")) |
    (~F.col("u.deduction_to_income").eqNullSafe(F.col("s.deduction_to_income")))
).count()
assert mismatches == 0, f"{mismatches} value mismatches"

print(f"\nAll checks passed: {union_count} rows across 3 tables = {src_count} source rows, 0 mismatches.")
print(json.dumps(verification, indent=2))

# ---------------------------------------------------------------------------
# 4c. Idempotency: a second write must produce the same result
# ---------------------------------------------------------------------------
for year, table_name in YEAR_SPLIT.items():
    year_df = features_df.filter(F.col("tax_year") == year)
    year_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

for year, table_name in YEAR_SPLIT.items():
    tb2 = spark.table(table_name)
    n2 = tb2.count()
    ids2 = sorted(r.return_id for r in tb2.select("return_id").collect())
    assert n2 == verification[table_name]["rows"], f"{table_name}: row count changed on rerun"
    assert ids2 == verification[table_name]["return_ids"], f"{table_name}: IDs changed on rerun"

print("Idempotency confirmed: second write produced identical tables.")

# ---------------------------------------------------------------------------
# 4d. CSV exports
# ---------------------------------------------------------------------------
for year, table_name in YEAR_SPLIT.items():
    short = table_name.split(".")[-1]
    spark.table(table_name).orderBy("return_id").toPandas().to_csv(
        f"{repo_root}/outputs/{short}.csv", index=False)
    print(f"Exported outputs/{short}.csv")

# ---------------------------------------------------------------------------
# 4e. Handoff summary
# ---------------------------------------------------------------------------
handoff = {
    "inputs": {
        "accepted_batch": CORRECTED_TABLE,
        "rejected_batch": INITIAL_TABLE,
        "lineage_view": LINEAGE_VIEW,
        "upstream_source": "training.to6_challenge3_inputs.parsed_source",
    },
    "outputs": {
        "feature_table": f"{OUTPUT_SCHEMA}.return_features",
        "year_splits": {str(y): t for y, t in YEAR_SPLIT.items()},
        "evidence_files": [
            "outputs/return_features.csv",
            "outputs/features_train.csv",
            "outputs/features_validation.csv",
            "outputs/features_test.csv",
            "outputs/return_features_checks.json",
            "outputs/corrected_cell6_validation.json",
        ],
    },
    "ratio_definition": "deduction_to_income = deduction / income (double); null when income = 0 (row kept)",
    "checks_performed": [
        "return_id uniqueness on both batches",
        "required fields (return_id, tax_year, income, deduction) non-null",
        "no negative amounts",
        "zero income valid, produces null ratio",
        "feature table columns = [return_id, tax_year, deduction_to_income] (need-to-know)",
        "all 12 source rows present in feature table with correct values",
        "year-split tables partition all features by tax_year with no overlap or loss",
        "idempotency: two consecutive overwrites produce identical tables",
        "value-level join: every split row matches the source feature row",
    ],
    "not_verified": [
        "Catalog UI lineage (source_lineage is a supplied view, not a Catalog lineage check)",
        "Upstream parsed_source content (out of scope; we trust the supplier's lineage view)",
        "Schema evolution or future deliveries (only the two supplied batches were checked)",
        "Access controls on the output tables (not part of this challenge)",
    ],
}

with open(f"{repo_root}/outputs/handoff_summary.json", "w") as fh:
    json.dump(handoff, fh, indent=2)

print("\n=== HANDOFF ===")
print(f"Inputs: {CORRECTED_TABLE} (accepted), {INITIAL_TABLE} (rejected, evidence kept).")
print(f"  Upstream: parsed_source via {LINEAGE_VIEW}.")
print(f"Outputs: return_features → features_train (2022, 4 rows), "
      f"features_validation (2023, 4 rows), features_test (2024, 4 rows).")
print(f"Ratio: deduction / income; null when income = 0 (R03, R09).")
print(f"Checks: uniqueness, completeness, nonnegative amounts, correct ratio values, "
      f"exhaustive year-split, idempotent overwrite.")
print(f"Not verified: Catalog UI lineage, upstream parsed_source content, "
      f"schema evolution, access controls.")
print(f"Evidence: outputs/handoff_summary.json + CSVs + JSON check files.")

display(spark.table(f"{OUTPUT_SCHEMA}.features_train").orderBy("return_id"))
display(spark.table(f"{OUTPUT_SCHEMA}.features_validation").orderBy("return_id"))
display(spark.table(f"{OUTPUT_SCHEMA}.features_test").orderBy("return_id"))

# COMMAND ----------

# DBTITLE 1,Run snapshot (IDs and values)
# Snapshot this run's actual return IDs and values from the feature and year tables
import json, datetime
from pathlib import Path

snap_dir = Path(repo_root) / "outputs" / "run_snapshots"
snap_dir.mkdir(parents=True, exist_ok=True)

run_ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
SNAP_TABLES = ["return_features", "features_train", "features_validation", "features_test"]
snap_df = None
for t in SNAP_TABLES:
    df = spark.table(f"{OUTPUT_SCHEMA}.{t}").select(F.lit(t).alias("table"), "return_id", "tax_year", "deduction_to_income")
    snap_df = df if snap_df is None else snap_df.unionByName(df)
snap_pdf = snap_df.orderBy("table", "return_id").toPandas()
snap_pdf.to_csv(snap_dir / f"run_{run_ts}.csv", index=False)

try:
    host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    notebook_url = f"https://{host}/editor/notebooks/2616693406010307"
except Exception:
    notebook_url = "notebook id 2616693406010307 (host unavailable)"
meta = {"run_timestamp_utc": run_ts, "notebook_url": notebook_url,
        "rows_per_table": snap_pdf.groupby("table").size().to_dict()}
(snap_dir / f"run_{run_ts}.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
print(json.dumps(meta, indent=2))

# COMMAND ----------

# DBTITLE 1,Compare the two latest runs
# Compare the two most recent run snapshots row by row (IDs and values, not just counts)
import pandas as pd

snaps = sorted(snap_dir.glob("run_*.csv"))
if len(snaps) < 2:
    print(f"Only {len(snaps)} snapshot(s) so far; run the whole notebook again to compare.")
else:
    a_path, b_path = snaps[-2], snaps[-1]
    a, b = pd.read_csv(a_path), pd.read_csv(b_path)
    key = ["table", "return_id"]
    merged = a.merge(b, on=key, how="outer", suffixes=("_run1", "_run2"), indicator=True)
    only_one = merged[merged["_merge"] != "both"]
    both = merged[merged["_merge"] == "both"]
    year_diff = both[both["tax_year_run1"] != both["tax_year_run2"]]
    r1, r2 = both["deduction_to_income_run1"], both["deduction_to_income_run2"]
    ratio_diff = both[~((r1.isna() & r2.isna()) | ((r1 - r2).abs() < 1e-12))]
    dup_rows = int(a.duplicated(key).sum() + b.duplicated(key).sum())
    comparison = {
        "run1": a_path.name, "run2": b_path.name,
        "rows_run1": len(a), "rows_run2": len(b),
        "rows_per_table_run1": a.groupby("table").size().to_dict(),
        "rows_per_table_run2": b.groupby("table").size().to_dict(),
        "ids_only_in_one_run": len(only_one), "tax_year_differences": len(year_diff),
        "ratio_differences": len(ratio_diff), "duplicate_rows": dup_rows,
    }
    comparison["identical"] = (len(a) == len(b) and not len(only_one) and not len(year_diff)
                               and not len(ratio_diff) and dup_rows == 0)
    (Path(repo_root) / "outputs" / "run_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(comparison, indent=2))
    assert comparison["identical"], "Runs differ"
    print("Both runs left identical return IDs and values, with no appended copies.")
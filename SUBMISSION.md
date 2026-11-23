# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Inputs and outputs

* Inputs: `training.to6_challenge3_inputs.returns_initial` (rejected), `training.to6_challenge3_inputs.returns_corrected` (accepted).
* Lineage: `training.to6_challenge3_inputs.source_lineage` shows both are deliveries from `training.to6_challenge3_inputs.parsed_source`.
* One row = one synthetic tax return; `return_id` is the uniqueness key; `taxpayer_id` is the filer (may span years).
* Output `training.to6_c3_8e4b3cdc76d4.return_features`: one row per accepted return, columns exactly
  `return_id`, `tax_year`, `deduction_to_income` (no `taxpayer_id` or raw amounts, per need-to-know).
* Year tables (built from the saved `return_features`, overwrite mode): `training.to6_c3_8e4b3cdc76d4.features_train`
  (2022: R01-R04), `training.to6_c3_8e4b3cdc76d4.features_validation` (2023: R05-R08),
  `training.to6_c3_8e4b3cdc76d4.features_test` (2024: R09-R12). Read-back: 4 rows each, 0 wrong-year rows,
  union = 12 rows / 12 distinct IDs = `return_features`, 0 value mismatches (confirmed by an independent SQL
  union/join). A second consecutive overwrite in the same cell left identical IDs.
* Ratio: `deduction_to_income = deduction / income`; null when income is 0.

## Checks and decisions

Source references: Catalog comments on both input tables and `source_lineage` (notebook section 1).

Rules applied identically to both batches (notebook section 2, cell "Validate initial vs corrected batch"):
required `return_id`, `tax_year`, `income`, `deduction` non-null; `return_id` unique; amounts nonnegative.
Zero income is valid and is not a rejection reason.

| Check | returns_initial | returns_corrected |
| --- | --- | --- |
| Rows / distinct return_id | 13 / 12 | 12 / 12 |
| Duplicate return_id | 1 (R01 x2) | 0 |
| Missing required values | 1 (R02 income null) | 0 |
| Negative amounts | 0 | 0 |
| Zero income (valid) | R03, R09 | R03, R09 |
| Result | FAIL (3 affected rows) | PASS |

* Initial rejection: R01 duplicated (uniqueness) and R02 missing income (completeness). Affected records
  with reasons: `outputs/initial_rejected_records.csv`; full summary: `outputs/batch_validation_summary.json`.
  Imputing income as 0 or dropping rows was not acceptable, so the replacement batch is used.
* Replacement: `returns_corrected` passes all checks; 4 returns per year (2022, 2023, 2024); R02 income = 200.
* Validation on `returns_corrected` (notebook cell "Validation on corrected table (zero income allowed)"):
  rules are unique `return_id` and income not null or negative; income = 0 is acceptable. Result: PASS
  (12 rows, 12 distinct IDs; 0 null, 0 negative income). Zero-income R03 (2022) and R09 (2024) are accepted
  and kept with a null ratio. The earlier stricter `income <= 0` rule (which flagged only R03/R09) was
  replaced on 2026-11-23. Evidence: `outputs/corrected_cell6_validation.json`.
* Feature build (notebook section 3, cell "Build, save and verify return_features"): from `returns_corrected`
  only, after the contract checks pass again; ratio computed only when income is not 0 (otherwise null);
  written with overwrite (no appends). Re-run 2026-11-23 with all checks passing.
* Nonzero expected value: R01 source row deduction 20 / income 100 = 0.2; saved value 0.2.
* Saved-table readback: columns equal the contract; 12 rows, 12 distinct IDs, same ID set as the source;
  zero-income R03 and R09 present with null ratio; year and ratio for all 12 rows recomputed from the source
  and matched. An independent SQL join to the source confirmed 10 matching ratios, 2 zero-income nulls
  and 12 matching years. Evidence: `outputs/return_features_checks.json`.

## Two successful runs

Notebook: https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/2616693406010307 (interactive runs, all code cells top to bottom).

| Run | Snapshot timestamp (UTC) | Retained IDs and values |
| --- | --- | --- |
| 1 | 2026-11-23 22:21:15 | `outputs/run_snapshots/run_2026-11-23T22-21-15Z.csv` (+ `.json` metadata) |
| 2 | 2026-11-23 22:23:28 | `outputs/run_snapshots/run_2026-11-23T22-23-28Z.csv` (+ `.json` metadata) |

Each snapshot holds every `return_id`, `tax_year` and `deduction_to_income` from `return_features` (12 rows)
and `features_train` / `features_validation` / `features_test` (4 rows each), 24 rows total.
Comparison (`outputs/run_comparison.json`, notebook cell "Compare the two latest runs"): outer join on
(table, return_id): 0 IDs in only one run, 0 `tax_year` differences, 0 ratio differences (nulls matched as
equal), 0 duplicate rows, identical = true. The two snapshot files were also compared directly and are
byte-identical.

* Year assignment: the Handoff cell asserts 0 rows with the wrong `tax_year` in each split table.
* One copy of every accepted return: the union of the three split tables equals `return_features`
  (12 rows, 12 distinct IDs, 0 value mismatches), which equals the 12 accepted source returns.
* No appended copies: all tables are written with overwrite; row counts per table are unchanged across
  both runs (12 / 4 / 4 / 4).

## Final files and limitations

List final exports under outputs/, distinguishing practice files. State anything not verified.

* Practice (ungraded): `outputs/connection-practice.json`.
* Rejection evidence: `outputs/initial_rejected_records.csv`, `outputs/batch_validation_summary.json`.
* Validation on corrected batch (zero income allowed): `outputs/corrected_cell6_validation.json`.
* Feature table export: `outputs/return_features.csv`, checks: `outputs/return_features_checks.json`.
* Split-table exports: `outputs/features_train.csv`, `outputs/features_validation.csv`, `outputs/features_test.csv`;
  handoff summary: `outputs/handoff_summary.json`.
* Two-run evidence: `outputs/run_snapshots/` (2 CSV + 2 JSON), `outputs/run_comparison.json`.

Not verified: Catalog UI lineage beyond the supplied `source_lineage` view; contents of upstream
`parsed_source`; future deliveries or schema changes; GitHub commit contents (verify after pushing).
This task does not add model labels or assess prediction-time leakage.

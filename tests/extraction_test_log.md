# Extraction Validation Test Log

**Date:** 2026-02-24 03:37 UTC
**Extractor model:** deepseek-r1:32b
**Spec:** Autonomy in Surgical Robotics: A Systematic Review v1.0
**Total elapsed:** 1218.3s

## Success Criteria

| Criterion | Target | Actual | Pass |
|-----------|--------|--------|------|
| Valid JSON (all papers) | 3/3 | 3/3 | YES |
| Source snippet verify rate | >80% | 100% | YES |
| Time per paper | <10 min | all OK | YES |
| **Overall** | | | **PASS** |

## Per-Paper Results

### Shademan 2016 (easy) — PASS

| Metric | Value |
|--------|-------|
| Parser | docling |
| Parse time | 16.2s |
| Pass 1 time | 213.8s |
| Pass 2 time | 163.0s |
| Total time | 393.0s |
| JSON valid | YES |
| Fields extracted | 15 |
| Snippets verified | 15/15 (100%) |

**Snippet Verification Detail:**

| Field | Exact | Normalized | Token Overlap | Verified |
|-------|-------|------------|---------------|----------|
| study_design | N | N | 100% | YES |
| sample_size | N | N | 100% | YES |
| robot_platform | N | Y | 100% | YES |
| autonomy_level | N | N | 100% | YES |
| task_performed | N | N | 100% | YES |
| accuracy_metric | N | N | 100% | YES |
| accuracy_value | N | N | 100% | YES |
| completion_time | N | N | 100% | YES |
| safety_events | Y | Y | 100% | YES |
| comparison_arm | N | N | 100% | YES |
| tissue_type | N | N | 100% | YES |
| fda_status | N | N | 100% | YES |
| cognitive_load | Y | Y | 0% | YES |
| sensor_modality | N | N | 100% | YES |
| learning_method | Y | Y | 0% | YES |

### Saeidi 2022 (medium) — PASS

| Metric | Value |
|--------|-------|
| Parser | docling |
| Parse time | 23.0s |
| Pass 1 time | 219.9s |
| Pass 2 time | 160.7s |
| Total time | 403.6s |
| JSON valid | YES |
| Fields extracted | 15 |
| Snippets verified | 15/15 (100%) |

**Snippet Verification Detail:**

| Field | Exact | Normalized | Token Overlap | Verified |
|-------|-------|------------|---------------|----------|
| study_design | N | N | 100% | YES |
| sample_size | N | N | 100% | YES |
| robot_platform | Y | Y | 100% | YES |
| autonomy_level | N | N | 100% | YES |
| task_performed | N | N | 100% | YES |
| accuracy_metric | N | N | 100% | YES |
| accuracy_value | N | N | 100% | YES |
| completion_time | N | N | 100% | YES |
| safety_events | N | N | 100% | YES |
| comparison_arm | N | N | 100% | YES |
| tissue_type | N | N | 100% | YES |
| fda_status | Y | Y | 0% | YES |
| cognitive_load | Y | Y | 0% | YES |
| sensor_modality | N | N | 100% | YES |
| learning_method | N | N | 100% | YES |

### Kim 2025 (hard) — PASS

| Metric | Value |
|--------|-------|
| Parser | docling |
| Parse time | 18.4s |
| Pass 1 time | 221.1s |
| Pass 2 time | 182.1s |
| Total time | 421.7s |
| JSON valid | YES |
| Fields extracted | 15 |
| Snippets verified | 15/15 (100%) |

**Snippet Verification Detail:**

| Field | Exact | Normalized | Token Overlap | Verified |
|-------|-------|------------|---------------|----------|
| study_design | N | N | 100% | YES |
| sample_size | N | Y | 100% | YES |
| robot_platform | N | N | 100% | YES |
| autonomy_level | N | N | 100% | YES |
| task_performed | N | N | 100% | YES |
| accuracy_metric | N | N | 100% | YES |
| accuracy_value | N | N | 100% | YES |
| completion_time | N | Y | 100% | YES |
| safety_events | N | N | 100% | YES |
| comparison_arm | N | Y | 100% | YES |
| tissue_type | N | N | 100% | YES |
| fda_status | N | Y | 100% | YES |
| cognitive_load | Y | Y | 0% | YES |
| sensor_modality | N | N | 100% | YES |
| learning_method | N | N | 100% | YES |

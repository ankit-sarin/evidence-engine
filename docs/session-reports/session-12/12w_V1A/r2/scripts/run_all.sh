#!/bin/bash
# 12w-V1A resume Q4: every combination, sequentially, one job.
M=/home/ankitsarin/scratch/12w-v1a/r2/measure2.sh
$M head
$M A /home/ankitsarin/scratch/12w-v1a/r2/diffs/A_one_builder.diff
$M A_V1 /home/ankitsarin/scratch/12w-v1a/r2/diffs/A_one_builder.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V1_runtime_only.diff
$M A_V2 /home/ankitsarin/scratch/12w-v1a/r2/diffs/A_one_builder.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V2_hash_only.diff
$M A_V1p /home/ankitsarin/scratch/12w-v1a/r2/diffs/A_plus_V1prime.diff
$M A_V2p /home/ankitsarin/scratch/12w-v1a/r2/diffs/A_plus_V2prime.diff
$M A_V3p /home/ankitsarin/scratch/12w-v1a/r2/diffs/A_plus_V3prime.diff
$M B /home/ankitsarin/scratch/12w-v1a/r2/diffs/B_structural_test.diff
$M B_V1 /home/ankitsarin/scratch/12w-v1a/r2/diffs/B_structural_test.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V1_runtime_only.diff
$M B_V2 /home/ankitsarin/scratch/12w-v1a/r2/diffs/B_structural_test.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V2_hash_only.diff
$M B_V3 /home/ankitsarin/scratch/12w-v1a/r2/diffs/B_structural_test.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V3_both.diff
$M C /home/ankitsarin/scratch/12w-v1a/r2/diffs/C_hash_over_runtime.diff
$M C_V1 /home/ankitsarin/scratch/12w-v1a/r2/diffs/C_hash_over_runtime.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V1_runtime_only.diff
$M C_V2 /home/ankitsarin/scratch/12w-v1a/r2/diffs/C_hash_over_runtime.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V2_hash_only.diff
$M C_V2p /home/ankitsarin/scratch/12w-v1a/r2/diffs/C_plus_V2prime.diff
$M C_V3 /home/ankitsarin/scratch/12w-v1a/r2/diffs/C_hash_over_runtime.diff /home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/diffs/V3_both.diff
echo ALL_DONE

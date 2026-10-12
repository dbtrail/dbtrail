#!/usr/bin/env bash
# The false-green tripwires of the MySQL integration suite: tests that must
# have PASSED, not merely not failed. A test that skips (no server, a build
# tag that drifted, a source in the wrong mode) leaves no failure behind, so
# a green run alone does not say it ran.
#
#   scripts/mysql-integration-key-tests.sh <log>
#
# <log> is every shard's test output together (what `collect` in
# scripts/mysql-integration-shard.sh writes): the tests live in different
# shards. Run by the `integration` job of ci.yml and the `integration-mysql`
# job of release.yaml, from this one file. The list used to be written in
# both workflows, and the release's copy had fallen to 7 of these.
set -euo pipefail
log="${1:?usage: mysql-integration-key-tests.sh <log>}"
[ -s "$log" ] || { echo "FATAL: the test log $log is missing or empty" >&2; exit 1; }

grep -q -- '--- PASS: TestEndToEnd_fullPipeline' "$log" \
  || { echo "FATAL: MySQL end-to-end integration test did not run — false-green tripwire" >&2; exit 1; }
grep -q -- '--- PASS: TestCachingSha2ColdAuthSeam' "$log" \
  || { echo "FATAL: caching_sha2 cold-auth seam test did not run — false-green tripwire" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationGTIDReconnectAfterCommitIndexesOnce ' "$log" \
  || { echo "FATAL: GTID reconnect exactly-once test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationGTIDCrashBeforeFirstFileCheckpointIndexesOnce ' "$log" \
  || { echo "FATAL: GTID first-checkpoint crash test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationGTIDResetKeepsOlderHistory ' "$log" \
  || { echo "FATAL: GTID reset history-survival test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationGTIDCutMidTransactionIndexesOnce ' "$log" \
  || { echo "FATAL: GTID cut-mid-transaction test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationGTIDFreshStartCheckpointsAtOnce ' "$log" \
  || { echo "FATAL: GTID fresh-start checkpoint test did not run (false-green tripwire)" >&2; exit 1; }
for name in \
  TestIntegrationGTIDResetMasterRestartBeforeRenumberingPasses \
  TestIntegrationGTIDResetMasterRestartAfterRenumberingPasses \
  TestIntegrationGTIDResetMasterCrashRightAfterRestart \
  TestIntegrationGTIDResetMasterWhileStreaming \
  TestIntegrationGTIDResetMasterFileNameReused \
  TestIntegrationGTIDCheckpointFromAnotherServer; do
  grep -q -- "--- PASS: $name " "$log" \
    || { echo "FATAL: GTID capture across a source RESET ($name) did not run (false-green tripwire)" >&2; exit 1; }
done
grep -q -- '--- PASS: TestIntegrationCrashRestartExactlyOnce_position' "$log" \
  || { echo "FATAL: crash→restart exactly-once test did not run — false-green tripwire" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationPositionResetMasterKeepsCapturedRows ' "$log" \
  || { echo "FATAL: position-mode RESET MASTER keep-rows test did not run: is the source in position mode? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationPositionResetRegrownPastCheckpoint ' "$log" \
  || { echo "FATAL: position-mode RESET MASTER regrown-past-the-checkpoint test did not run: is the source in position mode? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestOne_SourceFlavor_mysqlDeclaredMariaDBRefuses ' "$log" \
  || { echo "FATAL: source-flavor refusal test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestOne_SourceFlavor_mysqlDetectedWhenUndeclared ' "$log" \
  || { echo "FATAL: source-flavor detection test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationFlashbackRoutedUnchangedRealCapture_2085 ' "$log" \
  || { echo "FATAL: read routing by table over a real capture did not run: is the GTID source in the shard that runs it? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationFlashbackCaptureWatermark_realGTIDSets_2085 ' "$log" \
  || { echo "FATAL: the capture watermark test over real GTID sets did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationSQLCompareGeneratedNames ' "$log" \
  || { echo "FATAL: read-routing generated-column names test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationFlashbackRoutedSessionFunction_2127 ' "$log" \
  || { echo "FATAL: read routing after a session change inside a stored function did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationSQLCompareTemporalColumns ' "$log" \
  || { echo "FATAL: read-routing date-column arithmetic test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationSQLCompareCopyWords ' "$log" \
  || { echo "FATAL: read-routing copy-reserved words test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationBinlogOrderRecoverAndReconstruct2156_mysql ' "$log" \
  || { echo "FATAL: the applied-recovery-script test over a real capture did not run: is binary logging on? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationLatestPerKeyBinlogOrder2156_mysql ' "$log" \
  || { echo "FATAL: the verify and _snapshot latest-change test over a real capture did not run: is binary logging on? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationLatestPerPKThreePlacesAgree2156 ' "$log" \
  || { echo "FATAL: the latest-per-key agreement test (MySQL, DuckDB, Go) did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationRemainingSites2156_mysql ' "$log" \
  || { echo "FATAL: the verify --check recover and _flashback binary-log-order test over a real capture did not run: is binary logging on? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationCascadeBinlogOrder2156_mysql ' "$log" \
  || { echo "FATAL: the applied recover-cascade binary-log-order test over a real capture did not run: is binary logging on? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestStreamLoop_unmappableRowReachesThePersistedLedger ' "$log" || { echo "FATAL: unmappable-row skip ledger test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestRefresh_archivedHours_quietTableReadsNoArchive_lateChangeApplied_2152 ' "$log" \
  || { echo "FATAL: the refresh over archived hours (quiet table, late change) did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestRefresh_aRunThatDidNotCheckKeepsEachTablesPosition_2152 ' "$log" \
  || { echo "FATAL: the per-table archive record test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestPerformRotation_reArchiveAfterUploadFailureIsStampedNow_2152 ' "$log" \
  || { echo "FATAL: the re-archive after a failed upload test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationLateEventArchivedBeforeTheNextUpdateIsStillFetched ' "$log" \
  || { echo "FATAL: the late event archived before the next snapshot update test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestRefresh_afterTheBinlogNumberingStartsOver_refusesUntilAFullSnapshot ' "$log" \
  || { echo "FATAL: the refresh after a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestRunReconstruct_atBeforeABinlogRenumbering_2182 ' "$log" \
  || { echo "FATAL: reconstruct --at before a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestSnapshot_afterTheBinlogNumberingStartsOver_2174 ' "$log" \
  || { echo "FATAL: _snapshot after a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestVerifyBaselinePair_afterTheBinlogNumberingStartsOver_2174 ' "$log" \
  || { echo "FATAL: verify after a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationCopyUnchanged_numberingStartedOver_2160 ' "$log" \
  || { echo "FATAL: read routing after a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationCopyUnchanged_lateChangeArchivedAndDropped_2187 ' "$log" \
  || { echo "FATAL: read routing after a late change was archived and dropped did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestCascade_afterTheBinlogNumberingStartsOver_2177 ' "$log" \
  || { echo "FATAL: cascade recovery after a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestCheckNumbering_archivedWindow_2186 ' "$log" \
  || { echo "FATAL: the binlog renumbering check over archived hours did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestSnapshot_archivedWindow_2186 ' "$log" \
  || { echo "FATAL: _snapshot over archived hours after a binlog numbering that started over did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestCheckNumberingContinues_2160 ' "$log" || { echo "FATAL: the binlog renumbering check did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationVerifyLiveSourceUnderWrites2150_mysql ' "$log" \
  || { echo "FATAL: live-source verify under continuous writes did not run: is the GTID source in the shard that runs internal/streamrun? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationVerifyLiveSourceAnchorFallbacks2150 ' "$log" \
  || { echo "FATAL: the live-source verify anchor fallbacks test did not run: is the GTID source in the shard that runs internal/streamrun? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationSnapshotCutWalkIsPruned2150 ' "$log" \
  || { echo "FATAL: the live-source verify cut walk partition-pruning test did not run (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationVerifyLiveSourceUnpausedByDefault2150 ' "$log" \
  || { echo "FATAL: live-source verify without --pause-writes (the default) did not run: is the GTID source in the shard that runs internal/streamrun? (false-green tripwire)" >&2; exit 1; }
grep -q -- '--- PASS: TestIntegrationVerifyLiveSourceQuietAfterBaseline2150 ' "$log" \
  || { echo "FATAL: live-source verify on a quiet source after a baseline did not run: is the GTID source in the shard that runs internal/streamrun? (false-green tripwire)" >&2; exit 1; }

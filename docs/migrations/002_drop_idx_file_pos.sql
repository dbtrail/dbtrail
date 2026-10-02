-- 002_drop_idx_file_pos.sql
--
-- OPERATOR-FACING ONLY. This file is NOT auto-applied by `bintrail init`,
-- the agent, or `indexer.EnsureSchema(db)`. Apply manually per tenant
-- against the index database with the bintrail DB selected, e.g.:
--
--   mysql -u<user> -p<pass> -h<host> <index_db> < 002_drop_idx_file_pos.sql
--
-- Same convention as 001_create_tables.sql (see header on that file).
--
-- Removes the dead idx_file_pos index from binlog_events. The index was
-- declared at project inception (bintrail-spec.md:215, mid-Mar 2026) but
-- the query/recover paths shipped using gtid, event_timestamp, and pk_hash
-- — never binlog_file or start_pos. Confirmed by:
--
--   - performance_schema fetch counts: 0 fetches over 23.6 days on a
--     production tenant with normal traffic.
--   - exhaustive grep across nethalo/dbtrail and dbtrail/bintrail: no
--     query filters binlog_events by binlog_file or start_pos.
--
-- See nethalo/dbtrail#1199 for the full audit, including phased rollout
-- (Phase 1 marked the index INVISIBLE on the demo tenant 2026-04-08;
-- Phase 2 ships this migration; Phase 4 is the physical drop applied
-- across the fleet after 1 week of soak).
--
-- Idempotent: the IF EXISTS check is required because some tenants will
-- have the index marked INVISIBLE (still present) and others may already
-- have it dropped via Phase 4 manual application.

SET @stmt := IF(
    EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'binlog_events'
          AND index_name = 'idx_file_pos'
    ),
    'ALTER TABLE binlog_events DROP INDEX idx_file_pos',
    'SELECT ''idx_file_pos already dropped, skipping'' AS notice'
);
PREPARE drop_idx_stmt FROM @stmt;
EXECUTE drop_idx_stmt;
DEALLOCATE PREPARE drop_idx_stmt;

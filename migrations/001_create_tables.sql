-- Bintrail index database DDL
-- Reference schema: tables are created by `bintrail init`, not by executing
-- this file directly. The binlog_events partition list is generated dynamically
-- by the init command based on the --partitions flag.
--
-- Database: binlog_index (configurable via --index-dsn)

-- ─────────────────────────────────────────────────────────────────────────────
-- Main events table (range-partitioned by event_timestamp)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS binlog_events (
    event_id        BIGINT UNSIGNED  AUTO_INCREMENT,
    binlog_file     VARCHAR(255)     NOT NULL,
    start_pos       BIGINT UNSIGNED  NOT NULL,
    end_pos         BIGINT UNSIGNED  NOT NULL,
    event_timestamp DATETIME         NOT NULL,
    gtid            VARCHAR(255)     DEFAULT NULL,
    connection_id   INT UNSIGNED     DEFAULT NULL COMMENT 'MySQL connection ID (pseudo_thread_id) that produced this event',
    schema_name     VARCHAR(64)      NOT NULL,
    table_name      VARCHAR(64)      NOT NULL,
    event_type      TINYINT UNSIGNED NOT NULL COMMENT '1=INSERT, 2=UPDATE, 3=DELETE',
    pk_values       VARCHAR(512)     NOT NULL  COMMENT 'PK values in ordinal order, pipe-delimited. e.g. 12345 or 12345|2',
    pk_hash         VARCHAR(64)      AS (SHA2(pk_values, 256)) STORED,
    changed_columns JSON             DEFAULT NULL COMMENT 'list of columns that changed (UPDATEs only)',
    row_before      JSON             DEFAULT NULL COMMENT 'full row before image (UPDATE, DELETE)',
    row_after       JSON             DEFAULT NULL COMMENT 'full row after image (INSERT, UPDATE)',
    PRIMARY KEY (event_id, event_timestamp),
    INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
    INDEX idx_pk_hash    (schema_name, table_name, pk_hash, event_timestamp),
    INDEX idx_gtid       (gtid)
) ENGINE=InnoDB
  PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (
    -- Hourly partitions are created by `bintrail init --partitions N`.
    -- The naming convention is p_YYYYMMDDHH; each partition holds events
    -- with event_timestamp < the following hour. TO_SECONDS() is timezone-independent.
    -- Example (48 hours from 2026-02-19 00:00):
    --   PARTITION p_2026021900 VALUES LESS THAN (TO_SECONDS('2026-02-19 01:00:00')),
    --   PARTITION p_2026021901 VALUES LESS THAN (TO_SECONDS('2026-02-19 02:00:00')),
    --   ...
    --   PARTITION p_2026022023 VALUES LESS THAN (TO_SECONDS('2026-02-21 00:00:00')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
  );

-- Query pattern for PK lookup (always use both columns to guard hash collisions):
--   SELECT * FROM binlog_events
--   WHERE schema_name = 'mydb'
--     AND table_name  = 'orders'
--     AND pk_hash     = SHA2('12345', 256)
--     AND pk_values   = '12345'
--     AND event_timestamp BETWEEN ... AND ...;

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema snapshot table
-- Stores table/column metadata from information_schema at snapshot time.
-- Used by the indexer to map binlog column ordinals to column names.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_snapshots (
    id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_id      INT UNSIGNED NOT NULL,          -- groups all rows of one snapshot
    snapshot_time    DATETIME     NOT NULL,           -- time the snapshot was taken (set by Go)
    schema_name      VARCHAR(64)  NOT NULL,
    table_name       VARCHAR(64)  NOT NULL,
    column_name      VARCHAR(64)  NOT NULL,
    ordinal_position INT UNSIGNED NOT NULL,
    column_key       VARCHAR(3)   NOT NULL COMMENT 'PRI, UNI, MUL, or empty',
    data_type        VARCHAR(64)  NOT NULL,
    is_nullable      VARCHAR(3)   NOT NULL,
    column_default   TEXT         DEFAULT NULL,
    is_generated     TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 if STORED or VIRTUAL generated column',
    INDEX idx_snapshot_id    (snapshot_id),
    INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB;
-- Note: snapshot_id is NOT the auto-increment row ID. It is allocated by the
-- snapshot command via MAX(snapshot_id)+1 and shared by all rows of a snapshot.
-- NewResolver(db, 0) loads the latest snapshot; NewResolver(db, N) loads snapshot N.

-- ─────────────────────────────────────────────────────────────────────────────
-- Foreign key constraints table
-- Stores FK relationships captured from INFORMATION_SCHEMA.KEY_COLUMN_USAGE
-- joined with REFERENTIAL_CONSTRAINTS. Populated by `bintrail snapshot` using
-- the same snapshot_id as schema_snapshots.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fk_constraints (
    snapshot_id              INT UNSIGNED NOT NULL,
    constraint_name          VARCHAR(64)  NOT NULL,
    schema_name              VARCHAR(64)  NOT NULL,
    table_name               VARCHAR(64)  NOT NULL,
    column_name              VARCHAR(64)  NOT NULL,
    ordinal_position         INT          NOT NULL,
    referenced_schema_name   VARCHAR(64)  NOT NULL,
    referenced_table_name    VARCHAR(64)  NOT NULL,
    referenced_column_name   VARCHAR(64)  NOT NULL,
    delete_rule              VARCHAR(16)  NOT NULL DEFAULT '' COMMENT 'ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION); empty for pre-cascade-recovery snapshots',
    update_rule              VARCHAR(16)  NOT NULL DEFAULT '' COMMENT 'ON UPDATE rule; empty for pre-cascade-recovery snapshots',
    PRIMARY KEY (snapshot_id, schema_name, constraint_name, ordinal_position)
) ENGINE=InnoDB;
-- One row per FK column mapping. Composite FKs produce multiple rows with
-- increasing ordinal_position. The snapshot_id ties FK data to the column
-- metadata in schema_snapshots taken at the same time.

-- ─────────────────────────────────────────────────────────────────────────────
-- Server identity tables
-- bintrail_id is the stable, Bintrail-generated UUID for each MySQL server.
-- Identity resolution rules absorb individual component changes (UUID regen,
-- host migration, username rotation) while preserving the bintrail_id.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bintrail_servers (
    bintrail_id       CHAR(36)          NOT NULL,
    server_uuid       CHAR(36)          NOT NULL,
    host              VARCHAR(255)      NOT NULL,
    port              SMALLINT UNSIGNED NOT NULL DEFAULT 3306,
    username          VARCHAR(255)      NOT NULL,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    decommissioned_at TIMESTAMP    NULL DEFAULT NULL,
    PRIMARY KEY (bintrail_id),
    INDEX idx_server_uuid    (server_uuid),
    INDEX idx_host_port_user (host, port, username)
) ENGINE=InnoDB;
-- Note: no foreign keys — bintrail never uses FKs.

CREATE TABLE IF NOT EXISTS bintrail_server_changes (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    bintrail_id   CHAR(36)        NOT NULL,
    field_changed ENUM('server_uuid','host','port','username') NOT NULL,
    old_value     VARCHAR(255)    NOT NULL,
    new_value     VARCHAR(255)    NOT NULL,
    detected_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_bintrail_id (bintrail_id),
    INDEX idx_detected_at (detected_at)
) ENGINE=InnoDB;
-- Append-only audit trail. Never delete or update rows in this table.

-- ─────────────────────────────────────────────────────────────────────────────
-- Index state table
-- Tracks which binlog files have been indexed to prevent re-processing.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS index_state (
    binlog_file    VARCHAR(255) PRIMARY KEY,
    file_size      BIGINT UNSIGNED NOT NULL,
    last_position  BIGINT UNSIGNED NOT NULL COMMENT 'last parsed byte position in the file',
    events_indexed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status         ENUM('in_progress','completed','failed') NOT NULL,
    started_at     DATETIME NOT NULL,
    completed_at   DATETIME DEFAULT NULL,
    error_message  TEXT     DEFAULT NULL
) ENGINE=InnoDB;

-- ─────────────────────────────────────────────────────────────────────────────
-- Snapshot ID allocator (#844)
-- Dedicated AUTO_INCREMENT counter for schema_snapshots.snapshot_id. InnoDB's
-- AUTO_INCREMENT allocation is a lightweight, statement-duration lock (not a
-- row/gap lock held for a transaction's lifetime), so concurrent snapshot
-- writers (the watch daemon's DDL hook, a manual `bintrail snapshot`, and the
-- console baseline trigger) allocate distinct ids without ever deadlocking —
-- unlike an earlier `SELECT MAX(snapshot_id)+1 ... FOR UPDATE` design, which
-- reliably deadlocked under 3+ concurrent writers. One row is inserted, never
-- deleted, per snapshot ever allocated; see internal/metadata.DDLSnapshotIDSeq.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS snapshot_id_seq (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB;

-- ─────────────────────────────────────────────────────────────────────────────
-- Rotation policy (#1709)
-- The built-in rotation retention this index was created under, recorded once
-- when the index is created and read only while the operator sets none
-- (--rotate-retain / BINTRAIL_ROTATE_RETAIN / the console's rotation settings).
-- Changing the built-in default in a later release therefore moves the indexes
-- created from then on, and never shortens the window an existing index has
-- been running on. NO ROW (or no table) means an index created before this
-- record existed: it keeps 30d, the default every such index ran under, and
-- stays under the upgrade guard. See internal/indexer.DDLRotationPolicy.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rotation_policy (
    id             INT UNSIGNED PRIMARY KEY DEFAULT 1,
    initial_retain VARCHAR(16)  NOT NULL,
    recorded_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT rotation_policy_single_row CHECK (id = 1)
) ENGINE=InnoDB;

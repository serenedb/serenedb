SELECT
    S.pid AS pid, S.datid AS datid, S.datname AS datname,
    S.relid AS relid,
    S.command AS command,
    S.io_type AS "type",
    S.bytes_processed AS bytes_processed,
    S.bytes_total AS bytes_total,
    S.tuples_processed AS tuples_processed,
    0::BIGINT AS tuples_excluded,
    0::BIGINT AS tuples_skipped
FROM sdb_progress S WHERE S.command IN ('COPY FROM', 'COPY TO')

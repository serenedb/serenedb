SELECT
    S.pid AS pid, S.datid AS datid, S.datname AS datname,
    S.relid AS relid,
    S.current_relid AS index_relid,
    S.command AS command,
    S.phase AS phase,
    0::BIGINT AS lockers_total,
    0::BIGINT AS lockers_done,
    0::BIGINT AS current_locker_pid,
    0::BIGINT AS blocks_total,
    0::BIGINT AS blocks_done,
    S.tuples_total AS tuples_total,
    S.tuples_processed AS tuples_done,
    0::BIGINT AS partitions_total,
    0::BIGINT AS partitions_done
FROM sdb_progress S WHERE S.command IN ('CREATE INDEX', 'REINDEX')

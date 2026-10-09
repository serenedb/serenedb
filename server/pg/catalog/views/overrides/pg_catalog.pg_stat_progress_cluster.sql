SELECT
    S.pid AS pid,
    S.datid AS datid,
    S.datname AS datname,
    S.relid AS relid,
    S.command AS command,
    S.phase AS phase,
    S.current_relid AS cluster_index_relid,
    S.tuples_processed AS heap_tuples_scanned,
    0::BIGINT AS heap_tuples_written,
    S.steps_total AS heap_blks_total,
    S.step AS heap_blks_scanned,
    0::BIGINT AS index_rebuild_count
FROM sdb_progress S WHERE S.command = 'CLUSTER'

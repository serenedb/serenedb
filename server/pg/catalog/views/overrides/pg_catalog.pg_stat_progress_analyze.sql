SELECT
    S.pid AS pid, S.datid AS datid, S.datname AS datname,
    S.relid AS relid,
    S.phase AS phase,
    0::BIGINT AS sample_blks_total,
    0::BIGINT AS sample_blks_scanned,
    0::BIGINT AS ext_stats_total,
    0::BIGINT AS ext_stats_computed,
    S.items_total AS child_tables_total,
    S.items_processed AS child_tables_done,
    S.current_relid AS current_child_table_relid,
    0::double precision AS delay_time
FROM sdb_progress S WHERE S.command = 'ANALYZE'

SELECT
    S.pid AS pid, S.datid AS datid, S.datname AS datname,
    S.relid AS relid,
    S.phase AS phase,
    S.steps_total AS heap_blks_total, S.step AS heap_blks_scanned,
    0::BIGINT AS heap_blks_vacuumed, 0::BIGINT AS index_vacuum_count,
    0::BIGINT AS max_dead_tuple_bytes, 0::BIGINT AS dead_tuple_bytes,
    0::BIGINT AS num_dead_item_ids, S.items_total AS indexes_total,
    S.items_processed AS indexes_processed,
    0::double precision AS delay_time
FROM sdb_progress S WHERE S.command = 'VACUUM'

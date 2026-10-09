SELECT
    S.pid AS pid,
    S.phase AS phase,
    S.bytes_total AS backup_total,
    S.bytes_processed AS backup_streamed,
    S.items_total AS tablespaces_total,
    S.items_processed AS tablespaces_streamed
FROM sdb_progress S WHERE S.command = 'BASEBACKUP'

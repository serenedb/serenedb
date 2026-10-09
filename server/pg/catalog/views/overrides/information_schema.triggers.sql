SELECT CAST(current_database() AS sql_identifier) AS trigger_catalog,
       CAST(n.nspname AS sql_identifier) AS trigger_schema,
       CAST(t.tgname AS sql_identifier) AS trigger_name,
       CAST(em.text AS character_data) AS event_manipulation,
       CAST(current_database() AS sql_identifier) AS event_object_catalog,
       CAST(n.nspname AS sql_identifier) AS event_object_schema,
       CAST(c.relname AS sql_identifier) AS event_object_table,
       CAST(
         -- To determine action order, partition by schema, table,
         -- event_manipulation (INSERT/DELETE/UPDATE), ROW/STATEMENT (1),
         -- BEFORE/AFTER (66), then order by trigger name.  It's preferable
         -- to partition by view output information_schema.columns, so that query constraints
         -- can be pushed down below the window function.
         rank() OVER (PARTITION BY CAST(n.nspname AS sql_identifier),
                                   CAST(c.relname AS sql_identifier),
                                   em.num,
                                   t.tgtype & 1,
                                   t.tgtype & 66
                                   ORDER BY t.tgname)
         AS cardinal_number) AS action_order,
       CAST(
         CASE WHEN pg_has_role(c.relowner, 'USAGE')
           THEN (regexp_match(pg_get_triggerdef(t.oid), E'.{35,} WHEN \\((.+)\\) EXECUTE FUNCTION'))[1]
           ELSE null END
         AS character_data) AS action_condition,
       CAST(
         regexp_extract(pg_get_triggerdef(t.oid),
                        '(?s) FOR EACH (?:ROW|STATEMENT) (.*)$', 1)
         AS character_data) AS action_statement,
       CAST(
         -- hard-wired reference to TRIGGER_TYPE_ROW
         CASE t.tgtype & 1 WHEN 1 THEN 'ROW' ELSE 'STATEMENT' END
         AS character_data) AS action_orientation,
       CAST(
         -- hard-wired refs to TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSTEAD
         CASE t.tgtype & 66 WHEN 2 THEN 'BEFORE' WHEN 64 THEN 'INSTEAD OF' ELSE 'AFTER' END
         AS character_data) AS action_timing,
       CAST(tgoldtable AS sql_identifier) AS action_reference_old_table,
       CAST(tgnewtable AS sql_identifier) AS action_reference_new_table,
       CAST(null AS sql_identifier) AS action_reference_old_row,
       CAST(null AS sql_identifier) AS action_reference_new_row,
       CAST(null AS time_stamp) AS created

FROM pg_namespace n, pg_class c, pg_trigger t,
     -- hard-wired refs to TRIGGER_TYPE_INSERT, TRIGGER_TYPE_DELETE,
     -- TRIGGER_TYPE_UPDATE; we intentionally omit TRIGGER_TYPE_TRUNCATE
     (VALUES (4, 'INSERT'),
             (8, 'DELETE'),
             (16, 'UPDATE')) AS em (num, text)

WHERE n.oid = c.relnamespace
      AND c.oid = t.tgrelid
      AND t.tgtype & em.num <> 0
      AND NOT t.tgisinternal
      AND (NOT pg_is_other_temp_schema(n.oid))
      AND (pg_has_role(c.relowner, 'USAGE')
           -- SELECT privilege omitted, per SQL standard
           OR has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
           OR has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES') )

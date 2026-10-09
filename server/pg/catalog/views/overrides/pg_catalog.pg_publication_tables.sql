SELECT
    P.pubname AS pubname,
    N.nspname AS schemaname,
    C.relname AS tablename,
    ( SELECT array_agg(a.attname ORDER BY a.attnum)
      FROM pg_attribute a
      WHERE a.attrelid = GPT.relid AND
            -- TODO(mbkkt): restore ANY() once DuckDB supports correlated UNNEST
            list_has(GPT.attrs, a.attnum)
    ) AS attnames,
    pg_get_expr(GPT.qual, GPT.relid) AS rowfilter
FROM pg_publication P,
     LATERAL pg_get_publication_tables(P.pubname) GPT,
     pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE C.oid = GPT.relid

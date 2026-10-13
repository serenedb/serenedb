-- https://sqlite.org/lang_with.html
-- https://sqlite.org/lang_corefunc.html
-- https://sqlite.org/series.html
-- https://sqlite.org/lang_expr.html
-- https://sqlite.org/datatype3.html
-- https://sqlite.org/json1.html

WITH RECURSIVE
     src AS ( SELECT '>>[++.<+++.<]++++++.>+++.' AS code )
   , params AS ( SELECT LENGTH(code) AS len FROM src )
   , tokens AS ( SELECT gs.value AS i
                      , substr(src.code, gs.value, 1) AS token
                   FROM params
                      , src
                      , generate_series(0, params.len, 1) gs(value) )
   , bracket_match(pos, stack, pairs) AS ( SELECT 0 AS pos
                                                , json_array() AS stack
                                                , json('{}') AS pairs
                                             FROM tokens
                                            UNION ALL
                                           SELECT tokens.i + 1 AS pos
                                                , CASE tokens.token
                                                  WHEN '[' THEN json_insert(stack, '$[#]', to_json(tokens.i))
                                                  WHEN ']' THEN json_remove(stack, '$[#-1]')
                                                  ELSE stack
                                                   END AS stack
                                                , CASE tokens.token
                                                  WHEN ']' THEN json_insert(pairs, '$.' || json_extract(stack, -1), to_json(tokens.i))
                                                  ELSE pairs
                                                   END AS pairs
                                             FROM bracket_match AS bm
                                             JOIN tokens ON tokens.i = bm.pos )
   , evaluate(pc, dt, mem, output) AS ( SELECT 0
                                             , 0
                                             , json_array()
                                             , json_array()
                                          FROM tokens
                                         UNION ALL
                                        SELECT CASE tokens.token
                                               WHEN '[' THEN CASE
                                                             WHEN coalesce(json_extract( evaluate.mem, evaluate.dt )::INTEGER, 0) = 0
                                                             THEN json_extract( (SELECT pairs FROM bracket_match ORDER BY pos DESC LIMIT 1)
                                                                                     , evaluate.pc::text )::INTEGER + 1
                                                             ELSE evaluate.pc + 1
                                                             END
                                               WHEN ']' THEN CASE
                                                             WHEN coalesce(json_extract( evaluate.mem, evaluate.dt )::INTEGER, 0) != 0
                                                             THEN json_extract( (SELECT pairs FROM bracket_match ORDER BY pos DESC LIMIT 1)
                                                                                     , evaluate.pc::text )::INTEGER + 1
                                                             ELSE evaluate.pc + 1
                                                             END
                                               ELSE evaluate.pc + 1
                                               END AS pc
                                             , CASE tokens.token
                                               WHEN '<' THEN evaluate.dt - 1
                                               WHEN '>' THEN evaluate.dt + 1
                                               ELSE evaluate.dt
                                                END AS dt
                                             , CASE tokens.token
                                               WHEN '+' THEN json_set( evaluate.mem
                                                                     , '$[' || evaluate.dt || ']'
                                                                     , to_json(coalesce( json_extract( evaluate.mem
                                                                                             , evaluate.dt )::INTEGER, 0 ) + 1) )
                                               WHEN '-' THEN json_set( evaluate.mem
                                                                     , '$[' || evaluate.dt || ']'
                                                                     , to_json(coalesce( json_extract( evaluate.mem
                                                                                             , evaluate.dt )::INTEGER, 0 ) - 1) )
                                               ELSE json_insert( evaluate.mem
                                                               , '$[' || evaluate.dt || ']'
                                                               , to_json(0))
                                               END AS mem
                                             , CASE tokens.token
                                               WHEN '.' THEN json_insert( evaluate.output
                                                                        , '$[#]'
                                                                        , json_extract( evaluate.mem
                                                                                      , evaluate.dt ) )
                                               ELSE evaluate.output
                                               END AS output
                                         FROM evaluate
                                         JOIN tokens ON tokens.i = evaluate.pc
                                        WHERE evaluate.pc <= (SELECT len FROM params))
SELECT * FROM evaluate;

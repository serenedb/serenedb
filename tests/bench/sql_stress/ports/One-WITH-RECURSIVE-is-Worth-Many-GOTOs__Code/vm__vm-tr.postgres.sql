
-- # of VM runs (~ invocations)
\set N :invocations


SELECT
(
    WITH :MODE run("rec?",
                   "res",
                   "ip",
                   "regs") AS
    (
        SELECT True,
               NULL :: numeric,
               0 AS "ip_1",
               "regs"
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "res",
                           "ip",
                           "regs"),
              LATERAL
              (SELECT "ifresult3".*
               FROM (LATERAL
                     (SELECT "RTE0".*
                              FROM program AS "RTE0"("loc", "opc", "reg1", "reg2", "reg3")
                              WHERE "RTE0"."loc" = "ip"
                     ) AS "ins_2"
                     LEFT OUTER JOIN
                     (LATERAL
                      (SELECT ("ip" + 1) :: int4 AS "ip_3") AS "let1"("ip_3")
                      LEFT OUTER JOIN
                      LATERAL
                       ((SELECT True,
                               NULL :: numeric,
                               "ip_3",
                               (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), ("ins_2".reg2))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_27"
                         WHERE ("ins_2".opc) = 'lod')
                          UNION ALL
                        (SELECT "ifresult7".*
                         FROM ((SELECT True,
                                         NULL :: numeric,
                                         "ip_3",
                                         (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), (("regs")[("ins_2".reg2)::int]))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_4"
                                 WHERE ("ins_2".opc) = 'mov')
                                  UNION ALL
                                (SELECT "ifresult11".*
                                 FROM ((SELECT "ifresult13".*
                                         FROM (LATERAL
                                               (SELECT (("regs")[("ins_2".reg1)::int]) = (("regs")[("ins_2".reg2)::int]) AS "q17_19"
                                               ) AS "let12"("q17_19")
                                               LEFT OUTER JOIN
                                               LATERAL
                                               ((SELECT True,
                                                         NULL :: numeric,
                                                         "ins_2".reg3 :: int4 AS "ip_23",
                                                         "regs"
                                                 WHERE "q17_19")
                                                  UNION ALL
                                                (SELECT True,
                                                        NULL :: numeric,
                                                        "ip_3" :: int4,
                                                        "regs"
                                                 WHERE NOT "q17_19")
                                               ) AS "ifresult13"
                                               ON True)
                                         WHERE ("ins_2".opc) = 'jeq')
                                          UNION ALL
                                        (SELECT "ifresult18".*
                                         FROM ((SELECT True,
                                                       NULL :: numeric,
                                                       "ins_2".reg1 :: int4 AS "ip_9",
                                                       "regs"
                                                 WHERE ("ins_2".opc) = 'jmp')
                                                  UNION ALL
                                                (SELECT "ifresult22".*
                                                 FROM ((SELECT True,
                                                               NULL :: numeric,
                                                               "ip_3" :: int4,
                                                               (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), ((("regs")[("ins_2".reg2)::int]) + (("regs")[("ins_2".reg3)::int])))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_10"
                                                         WHERE ("ins_2".opc) = 'add')
                                                          UNION ALL
                                                        (SELECT "ifresult26".*
                                                         FROM ((SELECT True,
                                                                       NULL :: numeric,
                                                                       "ip_3" :: int4,
                                                                       (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), ((("regs")[("ins_2".reg2)::int]) - (("regs")[("ins_2".reg3)::int])))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_13"
                                                                 WHERE ("ins_2".opc) = 'sub')
                                                                  UNION ALL
                                                                (SELECT "ifresult30".*
                                                                 FROM ((SELECT True,
                                                                                 NULL :: numeric,
                                                                                 "ip_3" :: int4,
                                                                                 (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), ((("regs")[("ins_2".reg2)::int]) * (("regs")[("ins_2".reg3)::int])))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_16"
                                                                         WHERE ("ins_2".opc) = 'mul')
                                                                          UNION ALL
                                                                        (SELECT "ifresult34".*
                                                                         FROM ((SELECT True,
                                                                                       NULL :: numeric,
                                                                                       "ip_3" :: int4,
                                                                                       (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), ((("regs")[("ins_2".reg2)::int]) / (("regs")[("ins_2".reg3)::int])))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_19"
                                                                                 WHERE ("ins_2".opc) = 'div')
                                                                                  UNION ALL
                                                                                (SELECT "ifresult38".*
                                                                                 FROM (SELECT True,
                                                                                               NULL :: numeric,
                                                                                               "ip_3" :: int4,
                                                                                               (array_append((("regs")[:(("ins_2".reg1) - (1))::int]), ((("regs")[("ins_2".reg2)::int]) % (("regs")[("ins_2".reg3)::int])))) || (("regs")[(("ins_2".reg1) + (1))::int:]) AS "regs_22"
                                                                                         WHERE ("ins_2".opc) = 'mod'
                                                                                          UNION ALL
                                                                                        (SELECT "ifresult42".*
                                                                                         FROM ((SELECT False,
                                                                                                        (SELECT ("regs")[("ins_2".reg1)::int] AS "regs") AS "result",
                                                                                                        "run"."ip" :: int4,
                                                                                                        "run"."regs"
                                                                                                 WHERE ("ins_2".opc) = 'hlt')
                                                                                                ) AS "ifresult42"
                                                                                       )) AS "ifresult38"
                                                                               )) AS "ifresult34"
                                                                       )) AS "ifresult30"
                                                               )) AS "ifresult26"
                                                       )) AS "ifresult22"
                                               )) AS "ifresult18"
                                       )) AS "ifresult11"
                               )) AS "ifresult7"
                         )) AS "ifresult3"
                      ON True)
                     ON True)
              ) AS "result"
         WHERE "run"."rec?" = True)
    )
    SELECT "run"."res" AS "res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
  ) AS padovan
FROM   (SELECT array[:iterations,0,0,0,0,0,i] :: numeric[] AS regs FROM generate_series(0,:N) AS _(i)) AS _;

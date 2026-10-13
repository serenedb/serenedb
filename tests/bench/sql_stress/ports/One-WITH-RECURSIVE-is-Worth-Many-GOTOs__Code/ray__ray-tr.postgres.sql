-- # of raytracer runs (~ invocations)
\set N :invocations
\set w sqrt(10 * :iterations) :: int
\set h sqrt(10 * :iterations) :: int

-- EXPLAIN
SELECT run.*
FROM  generate_series(1, :N) AS i, LATERAL (SELECT :w+i-i, :h+i-i) AS _(w, h),
LATERAL
(
    WITH :MODE run("rec?",
                       "label",
                       "res",
                       "aspect_ratio",
                       "c",
                       "cam",
                       "col",
                       "do_ray",
                       "epsilon",
                       "fov",
                       "ho",
                       "i",
                       "intersection",
                       "light",
                       "mat",
                       "material",
                       "max_rec_depth",
                       "mindist",
                       "no",
                       "nprimitives",
                       "ntriangles",
                       "prim_hit",
                       "pxx",
                       "pxy",
                       "r",
                       "rd",
                       "rec",
                       "ro",
                       "rotx1",
                       "rotx2",
                       "rotx3",
                       "roty1",
                       "roty2",
                       "roty3",
                       "rotz1",
                       "rotz2",
                       "rotz3",
                       "shadow_done",
                       "shadows",
                       "sp",
                       "tca",
                       "tdist",
                       "thc",
                       "v1") AS
    (
        (SELECT True,
               'fori1_head',
               NULL :: int4[],
               "aspect_ratio_1",
               NULL :: rgb,
               "cam_1",
               NULL :: rgb,
               NULL :: bool,
               "epsilon_1",
               "fov_2",
               NULL :: rgb,
               NULL :: int4,
               NULL :: bool,
               "light_1",
               NULL :: material,
               NULL :: material,
               "max_rec_depth_1",
               NULL :: float8,
               NULL :: vec3,
               "nprimitives_1",
               "ntriangles_1",
               NULL :: bool,
               NULL :: int4,
               (SELECT 0 AS "?column?"),
               "r_1",
               NULL :: vec3,
               NULL :: int4,
               NULL :: vec3,
               "rotx1_2",
               "rotx2_2",
               "rotx3_2",
               "roty1_1",
               "roty2_1",
               "roty3_1",
               "rotz1_1",
               "rotz2_1",
               "rotz3_1",
               NULL :: bool,
               "shadows_1",
               NULL :: vec3,
               NULL :: float8,
               NULL :: float8,
               NULL :: float8,
               NULL :: vec3
         FROM (LATERAL
               (SELECT (0.0, 0.0, -4.5) :: vec3 AS "cam_1",
                       (0.0, 0.0, 0.0) :: vec3 AS "cam_lookat_1",
                       (0.0, 1.0, 0.0) :: vec3 AS "cam_up_1",
                       50.0 AS "fov_1",
                       True AS "shadows_1",
                       10 AS "max_rec_depth_1",
                       ("w") :: float8 / ("h") AS "aspect_ratio_1",
                       1.0e-6 AS "epsilon_1",
                       ARRAY[] :: int4[] AS "r_1",
                       (SELECT count(*) AS "count" FROM triangles AS "RTE0") AS "ntriangles_1",
                       (SELECT count(*) AS "count" FROM spheres AS "RTE1") AS "nspheres_1"

               ) AS "let169"("cam_1",
                             "cam_lookat_1",
                             "cam_up_1",
                             "fov_1",
                             "shadows_1",
                             "max_rec_depth_1",
                             "aspect_ratio_1",
                             "epsilon_1",
                             "r_1",
                             "ntriangles_1",
                             "nspheres_1")

                         LEFT OUTER JOIN
                         (LATERAL
                          (SELECT "ntriangles_1" + "nspheres_1" AS "nprimitives_1",
                                  (SELECT "RTE2" AS "sph"
                                    FROM spheres AS "RTE2"("id", "center", "radius", "mat", "color")
                                    WHERE "RTE2"."mat" = 'l' :: material) AS "sphere_1"

                          ) AS "let180"("nprimitives_1", "sphere_1")

                           LEFT OUTER JOIN
                           (LATERAL
                            (SELECT ("sphere_1" :: spheres).center AS "light_1",
                                    ("sphere_1" :: spheres).radius AS "light_r_1",
                                    "fov_1" * ((pi()) / (180.0)) AS "fov_2",
                                    ((("cam_lookat_1" :: vec3).x) - (("cam_1" :: vec3).x),
                                        (("cam_lookat_1" :: vec3).y) - (("cam_1" :: vec3).y),
                                        (("cam_lookat_1" :: vec3).z)
                                        -
                                        (("cam_1" :: vec3).z)) :: vec3 AS "cd_1"

                            ) AS "let182"("light_1","light_r_1","fov_2","cd_1")

                               LEFT OUTER JOIN
                               (LATERAL
                                (SELECT sqrt((((("cd_1" :: vec3).x) ^ (2))
                                              +
                                              ((("cd_1" :: vec3).y) ^ (2)))
                                             +
                                             ((("cd_1" :: vec3).z) ^ (2))) AS "tlen_1"

                                ) AS "let186"("tlen_1")

                                LEFT OUTER JOIN
                                (LATERAL
                                 (SELECT  (("cd_1" :: vec3).x) / "tlen_1" AS "rotz1_1",
                                          (("cd_1" :: vec3).y) / "tlen_1" AS "rotz2_1",
                                          (("cd_1" :: vec3).z) / "tlen_1" AS "rotz3_1"
                                 ) AS "let187"("rotz1_1", "rotz2_1", "rotz3_1")

                                   LEFT OUTER JOIN
                                   (LATERAL
                                    (SELECT ((("cam_up_1" :: vec3).y) * "rotz3_1")
                                            -
                                            ((("cam_up_1" :: vec3).z) * "rotz2_1") AS "rotx1_1",
                                            ((("cam_up_1" :: vec3).z) * "rotz1_1")
                                             -
                                             ((("cam_up_1" :: vec3).x) * "rotz3_1") AS "rotx2_1",
                                             ((("cam_up_1" :: vec3).x) * "rotz2_1")
                                              -
                                              ((("cam_up_1" :: vec3).y) * "rotz1_1") AS "rotx3_1"

                                    ) AS "let190"("rotx1_1","rotx2_1","rotx3_1")

                                      LEFT OUTER JOIN
                                      (LATERAL
                                       (SELECT sqrt((("rotx1_1" ^ (2)) + ("rotx2_1" ^ (2)))
                                                    +
                                                    ("rotx3_1" ^ (2))) AS "tlen_2"

                                       ) AS "let193"("tlen_2")
                                       LEFT OUTER JOIN
                                       (LATERAL
                                        (SELECT "rotx1_1" / "tlen_2" AS "rotx1_2",
                                                "rotx2_1" / "tlen_2" AS "rotx2_2",
                                                "rotx3_1" / "tlen_2" AS "rotx3_2"

                                        ) AS "let194"("rotx1_2")

                                          LEFT OUTER JOIN
                                          LATERAL
                                           (SELECT ("rotz2_1" * "rotx3_2") - ("rotz3_1" * "rotx2_2") AS "roty1_1",
                                                   ("rotz3_1" * "rotx1_2") - ("rotz1_1" * "rotx3_2") AS "roty2_1",
                                                   ("rotz1_1" * "rotx2_2") - ("rotz2_1" * "rotx1_2") AS "roty3_1"
                                           ) AS "let197"("roty1_1")
                                            ON True)
                                           ON True)
                                          ON True)
                                         ON True)
                                        ON True)
                                       ON True)
                                      ON True)
                                     ON True)
         )
          UNION ALL
        (SELECT "result".*
         FROM run AS "run"("rec?",
                           "label",
                           "res",
                           "aspect_ratio",
                           "c",
                           "cam",
                           "col",
                           "do_ray",
                           "epsilon",
                           "fov",
                           "ho",
                           "i",
                           "intersection",
                           "light",
                           "mat",
                           "material",
                           "max_rec_depth",
                           "mindist",
                           "no",
                           "nprimitives",
                           "ntriangles",
                           "prim_hit",
                           "pxx",
                           "pxy",
                           "r",
                           "rd",
                           "rec",
                           "ro",
                           "rotx1",
                           "rotx2",
                           "rotx3",
                           "roty1",
                           "roty2",
                           "roty3",
                           "rotz1",
                           "rotz2",
                           "rotz3",
                           "shadow_done",
                           "shadows",
                           "sp",
                           "tca",
                           "tdist",
                           "thc",
                           "v1"),
              LATERAL
              ((SELECT "ifresult2".*
                FROM (LATERAL
                       (SELECT "i" <= "nprimitives" AS "pred_12") AS "let1"("pred_12")
                       LEFT OUTER JOIN
                       LATERAL
                       ((SELECT "ifresult5".*
                         FROM (LATERAL
                                (SELECT "i" <= "ntriangles" AS "q16_10") AS "let4"("q16_10")
                                LEFT OUTER JOIN
                                LATERAL
                                ((SELECT "ifresult17".*
                                  FROM (LATERAL
                                        (SELECT "triangle_22",
                                                "triangle_22".p1 AS "v1_22",
                                                "triangle_22".p2 AS "v2_22",
                                                "triangle_22".p3 AS "v3_22",
                                                "triangle_22".mat AS "mat_23",
                                                "triangle_22".color AS "col_23"
                                         FROM  (SELECT "RTE3" AS "tri", "RTE3".*
                                                 FROM triangles AS "RTE3"
                                                 WHERE "RTE3"."id" = "i") AS "triangle_22"
                                        ) AS "let6"("triangle_22")
                                             LEFT OUTER JOIN
                                             (LATERAL
                                              (SELECT ((("v2_22" :: vec3).x)
                                                       -
                                                       (("v1_22" :: vec3).x),
                                                       (("v2_22" :: vec3).y)
                                                       -
                                                       (("v1_22" :: vec3).y),
                                                       (("v2_22" :: vec3).z)
                                                       -
                                                       (("v1_22" :: vec3).z)) :: vec3 AS "e1_22",
                                                      ((("v3_22" :: vec3).x)
                                                        -
                                                        (("v1_22" :: vec3).x),
                                                        (("v3_22" :: vec3).y)
                                                        -
                                                        (("v1_22" :: vec3).y),
                                                        (("v3_22" :: vec3).z)
                                                        -
                                                        (("v1_22" :: vec3).z)) :: vec3 AS "e2_22"

                                              ) AS "let12"("e1_22")

                                               LEFT OUTER JOIN
                                               (LATERAL
                                                (SELECT (((("rd" :: vec3).y)
                                                          *
                                                          (("e2_22" :: vec3).z))
                                                         -
                                                         ((("rd" :: vec3).z)
                                                          *
                                                          (("e2_22" :: vec3).y)),
                                                         ((("rd" :: vec3).z)
                                                          *
                                                          (("e2_22" :: vec3).x))
                                                         -
                                                         ((("rd" :: vec3).x)
                                                          *
                                                          (("e2_22" :: vec3).z)),
                                                         ((("rd" :: vec3).x)
                                                          *
                                                          (("e2_22" :: vec3).y))
                                                         -
                                                         ((("rd" :: vec3).y)
                                                          *
                                                          (("e2_22" :: vec3).x))) :: vec3 AS "p_22"

                                                ) AS "let14"("p_22")
                                                LEFT OUTER JOIN
                                                (LATERAL
                                                 (SELECT (((("e1_22" :: vec3).x)
                                                           *
                                                           (("p_22" :: vec3).x))
                                                          +
                                                          ((("e1_22" :: vec3).y)
                                                           *
                                                           (("p_22" :: vec3).y)))
                                                         +
                                                         ((("e1_22" :: vec3).z)
                                                          *
                                                          (("p_22" :: vec3).z)) AS "det_22"

                                                 ) AS "let15"("det_22")
                                                 LEFT OUTER JOIN
                                                 (LATERAL
                                                  (SELECT (abs("det_22")) > "epsilon" AS "q20_22"

                                                  ) AS "let16"("q20_22")
                                                  LEFT OUTER JOIN
                                                  LATERAL
                                                  ((SELECT "ifresult22".*
                                                    FROM (LATERAL
                                                          (SELECT (1.0) / "det_22" AS "det_24"

                                                          ) AS "let18"("det_24")
                                                          LEFT OUTER JOIN
                                                          (LATERAL
                                                           (SELECT ((("ro" :: vec3).x)
                                                                    -
                                                                    (("v1_22" :: vec3).x),
                                                                    (("ro" :: vec3).y)
                                                                    -
                                                                    (("v1_22" :: vec3).y),
                                                                    (("ro" :: vec3).z)
                                                                    -
                                                                    (("v1_22" :: vec3).z)) :: vec3 AS "t1_23"

                                                           ) AS "let19"("t1_23")
                                                           LEFT OUTER JOIN
                                                           (LATERAL
                                                            (SELECT ((((("t1_23" :: vec3).x)
                                                                       *
                                                                       (("p_22" :: vec3).x))
                                                                      +
                                                                      ((("t1_23" :: vec3).y)
                                                                       *
                                                                       (("p_22" :: vec3).y)))
                                                                     +
                                                                     ((("t1_23" :: vec3).z)
                                                                      *
                                                                      (("p_22" :: vec3).z)))
                                                                    *
                                                                    "det_24" AS "u_23"

                                                            ) AS "let20"("u_23")
                                                            LEFT OUTER JOIN
                                                            (LATERAL
                                                             (SELECT ("u_23" >= (0.0)
                                                                      AND
                                                                      "u_23" <= (1.0)) AS "q24_23"

                                                             ) AS "let21"("q24_23")
                                                             LEFT OUTER JOIN
                                                             LATERAL
                                                             ((SELECT "ifresult26".*
                                                               FROM (LATERAL
                                                                     (SELECT (((("t1_23" :: vec3).y)
                                                                               *
                                                                               (("e1_22" :: vec3).z))
                                                                              -
                                                                              ((("t1_23" :: vec3).z)
                                                                               *
                                                                               (("e1_22" :: vec3).y)),
                                                                              ((("t1_23" :: vec3).z)
                                                                               *
                                                                               (("e1_22" :: vec3).x))
                                                                              -
                                                                              ((("t1_23" :: vec3).x)
                                                                               *
                                                                               (("e1_22" :: vec3).z)),
                                                                              ((("t1_23" :: vec3).x)
                                                                               *
                                                                               (("e1_22" :: vec3).y))
                                                                              -
                                                                              ((("t1_23" :: vec3).y)
                                                                               *
                                                                               (("e1_22" :: vec3).x))) :: vec3 AS "q_24"

                                                                     ) AS "let23"("q_24")
                                                                     LEFT OUTER JOIN
                                                                     (LATERAL
                                                                      (SELECT ((((("rd" :: vec3).x)
                                                                                 *
                                                                                 (("q_24" :: vec3).x))
                                                                                +
                                                                                ((("rd" :: vec3).y)
                                                                                 *
                                                                                 (("q_24" :: vec3).y)))
                                                                               +
                                                                               ((("rd" :: vec3).z)
                                                                                *
                                                                                (("q_24" :: vec3).z)))
                                                                              *
                                                                              "det_24" AS "v_24"

                                                                      ) AS "let24"("v_24")
                                                                      LEFT OUTER JOIN
                                                                      (LATERAL
                                                                       (SELECT ("v_24" >= (0.0)
                                                                                AND
                                                                                ("u_23" + "v_24")
                                                                                <=
                                                                                (1.0)) AS "q28_24"

                                                                       ) AS "let25"("q28_24")
                                                                       LEFT OUTER JOIN
                                                                       LATERAL
                                                                       ((SELECT "ifresult29".*
                                                                         FROM (LATERAL
                                                                               (SELECT ((((("e2_22" :: vec3).x)
                                                                                          *
                                                                                          (("q_24" :: vec3).x))
                                                                                         +
                                                                                         ((("e2_22" :: vec3).y)
                                                                                          *
                                                                                          (("q_24" :: vec3).y)))
                                                                                        +
                                                                                        ((("e2_22" :: vec3).z)
                                                                                         *
                                                                                         (("q_24" :: vec3).z)))
                                                                                       *
                                                                                       "det_24" AS "tdist_28"

                                                                               ) AS "let27"("tdist_28")
                                                                               LEFT OUTER JOIN
                                                                               (LATERAL
                                                                                (SELECT ("tdist_28"
                                                                                         >
                                                                                         "epsilon"
                                                                                         AND
                                                                                         "tdist_28"
                                                                                         <
                                                                                         "mindist") AS "q32_25"

                                                                                ) AS "let28"("q32_25")
                                                                                LEFT OUTER JOIN
                                                                                LATERAL
                                                                                ((SELECT "ifresult38".*
                                                                                  FROM (LATERAL
                                                                                        (SELECT True AS "prim_hit_28",
                                                                                                True AS "intersection_28",
                                                                                                "tdist_28" AS "mindist_28",
                                                                                                (((("e2_22" :: vec3).y)
                                                                                                   *
                                                                                                   (("e1_22" :: vec3).z))
                                                                                                  -
                                                                                                  ((("e2_22" :: vec3).z)
                                                                                                   *
                                                                                                   (("e1_22" :: vec3).y)),
                                                                                                  ((("e2_22" :: vec3).z)
                                                                                                   *
                                                                                                   (("e1_22" :: vec3).x))
                                                                                                  -
                                                                                                  ((("e2_22" :: vec3).x)
                                                                                                   *
                                                                                                   (("e1_22" :: vec3).z)),
                                                                                                  ((("e2_22" :: vec3).x)
                                                                                                   *
                                                                                                   (("e1_22" :: vec3).y))
                                                                                                  -
                                                                                                  ((("e2_22" :: vec3).y)
                                                                                                   *
                                                                                                   (("e1_22" :: vec3).x))) :: vec3 AS "no_29"

                                                                                        ) AS "let30"("prim_hit_28")

                                                                                           LEFT OUTER JOIN
                                                                                           (LATERAL
                                                                                            (SELECT sqrt((((("no_29" :: vec3).x)
                                                                                                           ^
                                                                                                           (2))
                                                                                                          +
                                                                                                          ((("no_29" :: vec3).y)
                                                                                                           ^
                                                                                                           (2)))
                                                                                                         +
                                                                                                         ((("no_29" :: vec3).z)
                                                                                                          ^
                                                                                                          (2))) AS "tlen_30"

                                                                                            ) AS "let34"("tlen_30")
                                                                                            LEFT OUTER JOIN
                                                                                            (LATERAL
                                                                                             (SELECT ((("no_29" :: vec3).x)
                                                                                                      /
                                                                                                      "tlen_30",
                                                                                                      (("no_29" :: vec3).y)
                                                                                                      /
                                                                                                      "tlen_30",
                                                                                                      (("no_29" :: vec3).z)
                                                                                                      /
                                                                                                      "tlen_30") :: vec3 AS "no_30"

                                                                                             ) AS "let35"("no_30")
                                                                                             LEFT OUTER JOIN
                                                                                             (LATERAL
                                                                                              (SELECT (((("no_30" :: vec3).x)
                                                                                                        *
                                                                                                        (("rd" :: vec3).x))
                                                                                                       +
                                                                                                       ((("no_30" :: vec3).y)
                                                                                                        *
                                                                                                        (("rd" :: vec3).y)))
                                                                                                      +
                                                                                                      ((("no_30" :: vec3).z)
                                                                                                       *
                                                                                                       (("rd" :: vec3).z)) AS "tdot_26"

                                                                                              ) AS "let36"("tdot_26")
                                                                                              LEFT OUTER JOIN
                                                                                              (LATERAL
                                                                                               (SELECT "tdot_26" > (0.0) AS "q36_26") AS "let37"("q36_26")
                                                                                               LEFT OUTER JOIN
                                                                                               LATERAL
                                                                                               ((SELECT True,
                                                                                                         'ifmerge15',
                                                                                                         NULL :: int4[],
                                                                                                         "aspect_ratio",
                                                                                                         "c",
                                                                                                         "cam",
                                                                                                         "col_23",
                                                                                                         "do_ray",
                                                                                                         "epsilon",
                                                                                                         "fov",
                                                                                                         "ho",
                                                                                                         "i",
                                                                                                         "intersection_28",
                                                                                                         "light",
                                                                                                         "mat_23",
                                                                                                         "material",
                                                                                                         "max_rec_depth",
                                                                                                         "mindist_28",
                                                                                                         (- ("no_30" :: vec3).x, - ("no_30" :: vec3).y, - ("no_30" :: vec3).z) :: vec3 AS "no_32",
                                                                                                         "nprimitives",
                                                                                                         "ntriangles",
                                                                                                         "prim_hit_28",
                                                                                                         "pxx",
                                                                                                         "pxy",
                                                                                                         "r",
                                                                                                         "rd",
                                                                                                         "rec",
                                                                                                         "ro",
                                                                                                         "rotx1",
                                                                                                         "rotx2",
                                                                                                         "rotx3",
                                                                                                         "roty1",
                                                                                                         "roty2",
                                                                                                         "roty3",
                                                                                                         "rotz1",
                                                                                                         "rotz2",
                                                                                                         "rotz3",
                                                                                                         "shadow_done",
                                                                                                         "shadows",
                                                                                                         "sp",
                                                                                                         "tca",
                                                                                                         "tdist_28",
                                                                                                         "thc",
                                                                                                         "v1_22"
                                                                                                 WHERE "q36_26"
                                                                                                 )
                                                                                                  UNION ALL
                                                                                                (SELECT True,
                                                                                                        'ifmerge15',
                                                                                                        NULL :: int4[],
                                                                                                        "aspect_ratio",
                                                                                                        "c",
                                                                                                        "cam",
                                                                                                        "col_23",
                                                                                                        "do_ray",
                                                                                                        "epsilon",
                                                                                                        "fov",
                                                                                                        "ho",
                                                                                                        "i",
                                                                                                        "intersection_28",
                                                                                                        "light",
                                                                                                        "mat_23",
                                                                                                        "material",
                                                                                                        "max_rec_depth",
                                                                                                        "mindist_28",
                                                                                                        "no_30",
                                                                                                        "nprimitives",
                                                                                                        "ntriangles",
                                                                                                        "prim_hit_28",
                                                                                                        "pxx",
                                                                                                        "pxy",
                                                                                                        "r",
                                                                                                        "rd",
                                                                                                        "rec",
                                                                                                        "ro",
                                                                                                        "rotx1",
                                                                                                        "rotx2",
                                                                                                        "rotx3",
                                                                                                        "roty1",
                                                                                                        "roty2",
                                                                                                        "roty3",
                                                                                                        "rotz1",
                                                                                                        "rotz2",
                                                                                                        "rotz3",
                                                                                                        "shadow_done",
                                                                                                        "shadows",
                                                                                                        "sp",
                                                                                                        "tca",
                                                                                                        "tdist_28",
                                                                                                        "thc",
                                                                                                        "v1_22"
                                                                                                 WHERE NOT "q36_26"
                                                                                                 )

                                                                                               ) AS "ifresult38"
                                                                                               ON True)
                                                                                              ON True)
                                                                                             ON True)
                                                                                            ON True)
                                                                                           ON True)
                                                                                  WHERE "q32_25"
                                                                                  )
                                                                                   UNION ALL
                                                                                 (SELECT True,
                                                                                         'ifmerge15',
                                                                                         NULL :: int4[],
                                                                                         "aspect_ratio",
                                                                                         "c",
                                                                                         "cam",
                                                                                         "col_23",
                                                                                         "do_ray",
                                                                                         "epsilon",
                                                                                         "fov",
                                                                                         "ho",
                                                                                         "i",
                                                                                         "intersection",
                                                                                         "light",
                                                                                         "mat_23",
                                                                                         "material",
                                                                                         "max_rec_depth",
                                                                                         "mindist",
                                                                                         "no",
                                                                                         "nprimitives",
                                                                                         "ntriangles",
                                                                                         False,
                                                                                         "pxx",
                                                                                         "pxy",
                                                                                         "r",
                                                                                         "rd",
                                                                                         "rec",
                                                                                         "ro",
                                                                                         "rotx1",
                                                                                         "rotx2",
                                                                                         "rotx3",
                                                                                         "roty1",
                                                                                         "roty2",
                                                                                         "roty3",
                                                                                         "rotz1",
                                                                                         "rotz2",
                                                                                         "rotz3",
                                                                                         "shadow_done",
                                                                                         "shadows",
                                                                                         "sp",
                                                                                         "tca",
                                                                                         "tdist_28",
                                                                                         "thc",
                                                                                         "v1_22"
                                                                                  WHERE NOT "q32_25"
                                                                                  )

                                                                                ) AS "ifresult29"
                                                                                ON True)
                                                                               ON True)
                                                                         WHERE "q28_24"
                                                                         )
                                                                          UNION ALL
                                                                        (SELECT True,
                                                                                'ifmerge15',
                                                                                NULL :: int4[],
                                                                                "aspect_ratio",
                                                                                "c",
                                                                                "cam",
                                                                                "col_23",
                                                                                "do_ray",
                                                                                "epsilon",
                                                                                "fov",
                                                                                "ho",
                                                                                "i",
                                                                                "intersection",
                                                                                "light",
                                                                                "mat_23",
                                                                                "material",
                                                                                "max_rec_depth",
                                                                                "mindist",
                                                                                "no",
                                                                                "nprimitives",
                                                                                "ntriangles",
                                                                                False,
                                                                                "pxx",
                                                                                "pxy",
                                                                                "r",
                                                                                "rd",
                                                                                "rec",
                                                                                "ro",
                                                                                "rotx1",
                                                                                "rotx2",
                                                                                "rotx3",
                                                                                "roty1",
                                                                                "roty2",
                                                                                "roty3",
                                                                                "rotz1",
                                                                                "rotz2",
                                                                                "rotz3",
                                                                                "shadow_done",
                                                                                "shadows",
                                                                                "sp",
                                                                                "tca",
                                                                                "tdist",
                                                                                "thc",
                                                                                "v1_22"
                                                                         WHERE NOT "q28_24"
                                                                         )

                                                                       ) AS "ifresult26"
                                                                       ON True)
                                                                      ON True)
                                                                     ON True)
                                                               WHERE "q24_23"
                                                               )
                                                                UNION ALL
                                                              (SELECT True,
                                                                      'ifmerge15',
                                                                      NULL :: int4[],
                                                                      "aspect_ratio",
                                                                      "c",
                                                                      "cam",
                                                                      "col_23",
                                                                      "do_ray",
                                                                      "epsilon",
                                                                      "fov",
                                                                      "ho",
                                                                      "i",
                                                                      "intersection",
                                                                      "light",
                                                                      "mat_23",
                                                                      "material",
                                                                      "max_rec_depth",
                                                                      "mindist",
                                                                      "no",
                                                                      "nprimitives",
                                                                      "ntriangles",
                                                                      False,
                                                                      "pxx",
                                                                      "pxy",
                                                                      "r",
                                                                      "rd",
                                                                      "rec",
                                                                      "ro",
                                                                      "rotx1",
                                                                      "rotx2",
                                                                      "rotx3",
                                                                      "roty1",
                                                                      "roty2",
                                                                      "roty3",
                                                                      "rotz1",
                                                                      "rotz2",
                                                                      "rotz3",
                                                                      "shadow_done",
                                                                      "shadows",
                                                                      "sp",
                                                                      "tca",
                                                                      "tdist",
                                                                      "thc",
                                                                      "v1_22"
                                                               WHERE NOT "q24_23"
                                                               )

                                                             ) AS "ifresult22"
                                                             ON True)
                                                            ON True)
                                                           ON True)
                                                          ON True)
                                                    WHERE "q20_22"
                                                    )
                                                     UNION ALL
                                                   (SELECT True,
                                                           'ifmerge15',
                                                           NULL :: int4[],
                                                           "aspect_ratio",
                                                           "c",
                                                           "cam",
                                                           "col_23",
                                                           "do_ray",
                                                           "epsilon",
                                                           "fov",
                                                           "ho",
                                                           "i",
                                                           "intersection",
                                                           "light",
                                                           "mat_23",
                                                           "material",
                                                           "max_rec_depth",
                                                           "mindist",
                                                           "no",
                                                           "nprimitives",
                                                           "ntriangles",
                                                           False,
                                                           "pxx",
                                                           "pxy",
                                                           "r",
                                                           "rd",
                                                           "rec",
                                                           "ro",
                                                           "rotx1",
                                                           "rotx2",
                                                           "rotx3",
                                                           "roty1",
                                                           "roty2",
                                                           "roty3",
                                                           "rotz1",
                                                           "rotz2",
                                                           "rotz3",
                                                           "shadow_done",
                                                           "shadows",
                                                           "sp",
                                                           "tca",
                                                           "tdist",
                                                           "thc",
                                                           "v1_22"
                                                    WHERE NOT "q20_22"
                                                    )

                                                  ) AS "ifresult17"
                                                  ON True)
                                                 ON True)
                                                ON True)
                                               ON True)
                                              ON True)
                                  WHERE "q16_10"
                                  )
                                   UNION ALL
                                 (SELECT "ifresult55".*
                                  FROM (LATERAL
                                        (SELECT "sphere_12",
                                                "sphere_12".center AS "sp_11",
                                                "sphere_12".radius AS "spr_11",
                                                "sphere_12".mat AS "mat_11",
                                                "sphere_12".color AS "col_11"
                                         FROM (SELECT "RTE4", "RTE4".*
                                                 FROM spheres AS "RTE4"("id",
                                                                        "center",
                                                                        "radius",
                                                                        "mat",
                                                                        "color")
                                                 WHERE "RTE4"."id"
                                                       =
                                                       ("i" - "ntriangles")) AS "sphere_12"

                                        ) AS "let46"("sphere_12")

                                            LEFT OUTER JOIN
                                            (LATERAL
                                             (SELECT ((("sp_11" :: vec3).x) - (("ro" :: vec3).x),
                                                      (("sp_11" :: vec3).y) - (("ro" :: vec3).y),
                                                      (("sp_11" :: vec3).z)
                                                      -
                                                      (("ro" :: vec3).z)) :: vec3 AS "l_11"

                                             ) AS "let51"("l_11")
                                             LEFT OUTER JOIN
                                             (LATERAL
                                              (SELECT (((("l_11" :: vec3).x) * (("rd" :: vec3).x))
                                                       +
                                                       ((("l_11" :: vec3).y) * (("rd" :: vec3).y)))
                                                      +
                                                      ((("l_11" :: vec3).z)
                                                       *
                                                       (("rd" :: vec3).z)) AS "tca_11"

                                              ) AS "let52"("tca_11")
                                              LEFT OUTER JOIN
                                              (LATERAL
                                               (SELECT ((((("l_11" :: vec3).x) ^ (2))
                                                         +
                                                         ((("l_11" :: vec3).y) ^ (2)))
                                                        +
                                                        ((("l_11" :: vec3).z) ^ (2)))
                                                       -
                                                       ("tca_11" ^ (2)) AS "d2_11"

                                               ) AS "let53"("d2_11")
                                               LEFT OUTER JOIN
                                               (LATERAL
                                                (SELECT "d2_11" <= ("spr_11" ^ (2)) AS "q40_11"

                                                ) AS "let54"("q40_11")
                                                LEFT OUTER JOIN
                                                LATERAL
                                                ((SELECT "ifresult59".*
                                                  FROM (LATERAL
                                                        (SELECT sqrt(("spr_11" ^ (2)) - "d2_11") AS "thc_12",
                                                                0.0 AS "tdist_12"

                                                        ) AS "let56"("thc_12")
                                                        LEFT OUTER JOIN
                                                         (LATERAL
                                                          (SELECT ("tca_11" - "thc_12") > (0.0) AS "q44_12"

                                                          ) AS "let58"("q44_12")
                                                          LEFT OUTER JOIN
                                                          LATERAL
                                                          ((SELECT "return61".*
                                                            FROM (LATERAL
                                                                  (SELECT "tca_11" - "thc_12" AS "tdist_14"

                                                                  ) AS "let60"("tdist_14")
                                                                  LEFT OUTER JOIN
                                                                  LATERAL
                                                                  (SELECT True,
                                                                          'ifmerge43',
                                                                          NULL :: int4[],
                                                                          "aspect_ratio",
                                                                          "c",
                                                                          "cam",
                                                                          "col_11",
                                                                          "do_ray",
                                                                          "epsilon",
                                                                          "fov",
                                                                          "ho",
                                                                          "i",
                                                                          "intersection",
                                                                          "light",
                                                                          "mat_11",
                                                                          "material",
                                                                          "max_rec_depth",
                                                                          "mindist",
                                                                          "no",
                                                                          "nprimitives",
                                                                          "ntriangles",
                                                                          False,
                                                                          "pxx",
                                                                          "pxy",
                                                                          "r",
                                                                          "rd",
                                                                          "rec",
                                                                          "ro",
                                                                          "rotx1",
                                                                          "rotx2",
                                                                          "rotx3",
                                                                          "roty1",
                                                                          "roty2",
                                                                          "roty3",
                                                                          "rotz1",
                                                                          "rotz2",
                                                                          "rotz3",
                                                                          "shadow_done",
                                                                          "shadows",
                                                                          "sp_11",
                                                                          "tca_11",
                                                                          "tdist_14",
                                                                          "thc_12",
                                                                          "v1"
                                                                  ) AS "return61"
                                                                  ON True)
                                                            WHERE "q44_12"
                                                            )
                                                             UNION ALL
                                                           (SELECT True,
                                                                   'ifmerge43',
                                                                   NULL :: int4[],
                                                                   "aspect_ratio",
                                                                   "c",
                                                                   "cam",
                                                                   "col_11",
                                                                   "do_ray",
                                                                   "epsilon",
                                                                   "fov",
                                                                   "ho",
                                                                   "i",
                                                                   "intersection",
                                                                   "light",
                                                                   "mat_11",
                                                                   "material",
                                                                   "max_rec_depth",
                                                                   "mindist",
                                                                   "no",
                                                                   "nprimitives",
                                                                   "ntriangles",
                                                                   False,
                                                                   "pxx",
                                                                   "pxy",
                                                                   "r",
                                                                   "rd",
                                                                   "rec",
                                                                   "ro",
                                                                   "rotx1",
                                                                   "rotx2",
                                                                   "rotx3",
                                                                   "roty1",
                                                                   "roty2",
                                                                   "roty3",
                                                                   "rotz1",
                                                                   "rotz2",
                                                                   "rotz3",
                                                                   "shadow_done",
                                                                   "shadows",
                                                                   "sp_11",
                                                                   "tca_11",
                                                                   "tdist_12",
                                                                   "thc_12",
                                                                   "v1"
                                                            WHERE NOT "q44_12"
                                                            )

                                                          ) AS "ifresult59"
                                                          ON True)
                                                         ON True)
                                                  WHERE "q40_11"
                                                  )
                                                   UNION ALL
                                                 (SELECT True,
                                                         'ifmerge15',
                                                         NULL :: int4[],
                                                         "aspect_ratio",
                                                         "c",
                                                         "cam",
                                                         "col_11",
                                                         "do_ray",
                                                         "epsilon",
                                                         "fov",
                                                         "ho",
                                                         "i",
                                                         "intersection",
                                                         "light",
                                                         "mat_11",
                                                         "material",
                                                         "max_rec_depth",
                                                         "mindist",
                                                         "no",
                                                         "nprimitives",
                                                         "ntriangles",
                                                         False,
                                                         "pxx",
                                                         "pxy",
                                                         "r",
                                                         "rd",
                                                         "rec",
                                                         "ro",
                                                         "rotx1",
                                                         "rotx2",
                                                         "rotx3",
                                                         "roty1",
                                                         "roty2",
                                                         "roty3",
                                                         "rotz1",
                                                         "rotz2",
                                                         "rotz3",
                                                         "shadow_done",
                                                         "shadows",
                                                         "sp_11",
                                                         "tca_11",
                                                         "tdist",
                                                         "thc",
                                                         "v1"
                                                  WHERE NOT "q40_11"
                                                  )

                                                ) AS "ifresult55"
                                                ON True)
                                               ON True)
                                              ON True)
                                             ON True)
                                            ON True)
                                  WHERE NOT "q16_10"
                                  )

                                ) AS "ifresult5"
                                ON True)
                         WHERE "pred_12"
                         )
                          UNION ALL
                        (SELECT "ifresult65".*
                         FROM (LATERAL
                               (SELECT "shadow_done" AS "q64_28") AS "let64"("q64_28")
                               LEFT OUTER JOIN
                               LATERAL
                               ((SELECT "ifresult67".*
                                 FROM ((SELECT True,
                                               'ifmerge9',
                                               NULL :: int4[],
                                               "aspect_ratio",
                                               (0.0, 0.0, 0.0) :: rgb AS "c_40",
                                               "cam",
                                               "col",
                                               "do_ray",
                                               "epsilon",
                                               "fov",
                                               "ho",
                                               "i",
                                               "intersection",
                                               "light",
                                               "mat",
                                               "material",
                                               "max_rec_depth",
                                               "mindist",
                                               "no",
                                               "nprimitives",
                                               "ntriangles",
                                               "prim_hit",
                                               "pxx",
                                               "pxy",
                                               "r",
                                               "rd",
                                               "rec",
                                               "ro",
                                               "rotx1",
                                               "rotx2",
                                               "rotx3",
                                               "roty1",
                                               "roty2",
                                               "roty3",
                                               "rotz1",
                                               "rotz2",
                                               "rotz3",
                                               "shadow_done",
                                               "shadows",
                                               "sp",
                                               "tca",
                                               "tdist",
                                               "thc",
                                               "v1"
                                         WHERE "material" <> 'l' :: material
                                         )
                                          UNION ALL
                                        (SELECT True,
                                                'ifmerge9',
                                                NULL :: int4[],
                                                "aspect_ratio",
                                                "c",
                                                "cam",
                                                "col",
                                                "do_ray",
                                                "epsilon",
                                                "fov",
                                                "ho",
                                                "i",
                                                "intersection",
                                                "light",
                                                "mat",
                                                "material",
                                                "max_rec_depth",
                                                "mindist",
                                                "no",
                                                "nprimitives",
                                                "ntriangles",
                                                "prim_hit",
                                                "pxx",
                                                "pxy",
                                                "r",
                                                "rd",
                                                "rec",
                                                "ro",
                                                "rotx1",
                                                "rotx2",
                                                "rotx3",
                                                "roty1",
                                                "roty2",
                                                "roty3",
                                                "rotz1",
                                                "rotz2",
                                                "rotz3",
                                                "shadow_done",
                                                "shadows",
                                                "sp",
                                                "tca",
                                                "tdist",
                                                "thc",
                                                "v1"
                                         WHERE NOT ("material" <> 'l' :: material)
                                         )

                                       ) AS "ifresult67"
                                 WHERE "q64_28"
                                 )
                                  UNION ALL
                                (SELECT "ifresult72".*
                                 FROM ((SELECT True,
                                               'ifmerge71',
                                               NULL :: int4[],
                                               "aspect_ratio",
                                               (1.0, 1.0, 1.0) :: rgb AS "c_31",
                                               "cam",
                                               "col",
                                               "do_ray",
                                               "epsilon",
                                               "fov",
                                               "ho",
                                               "i",
                                               "intersection",
                                               "light",
                                               "mat",
                                               "material",
                                               "max_rec_depth",
                                               "mindist",
                                               "no",
                                               "nprimitives",
                                               "ntriangles",
                                               "prim_hit",
                                               "pxx",
                                               "pxy",
                                               "r",
                                               "rd",
                                               "rec",
                                               "ro",
                                               "rotx1",
                                               "rotx2",
                                               "rotx3",
                                               "roty1",
                                               "roty2",
                                               "roty3",
                                               "rotz1",
                                               "rotz2",
                                               "rotz3",
                                               "shadow_done",
                                               "shadows",
                                               "sp",
                                               "tca",
                                               "tdist",
                                               "thc",
                                               "v1"
                                         WHERE "material" = 'l' :: material
                                         )
                                          UNION ALL
                                        (SELECT True,
                                                'ifmerge71',
                                                NULL :: int4[],
                                                "aspect_ratio",
                                                "c",
                                                "cam",
                                                "col",
                                                "do_ray",
                                                "epsilon",
                                                "fov",
                                                "ho",
                                                "i",
                                                "intersection",
                                                "light",
                                                "mat",
                                                "material",
                                                "max_rec_depth",
                                                "mindist",
                                                "no",
                                                "nprimitives",
                                                "ntriangles",
                                                "prim_hit",
                                                "pxx",
                                                "pxy",
                                                "r",
                                                "rd",
                                                "rec",
                                                "ro",
                                                "rotx1",
                                                "rotx2",
                                                "rotx3",
                                                "roty1",
                                                "roty2",
                                                "roty3",
                                                "rotz1",
                                                "rotz2",
                                                "rotz3",
                                                "shadow_done",
                                                "shadows",
                                                "sp",
                                                "tca",
                                                "tdist",
                                                "thc",
                                                "v1"
                                         WHERE NOT ("material" = 'l' :: material)
                                         )
                                       ) AS "ifresult72"
                                 WHERE NOT "q64_28"
                                 )
                               ) AS "ifresult65"
                               ON True)
                         WHERE NOT "pred_12"
                         )

                       ) AS "ifresult2"
                       ON True)
                WHERE "run"."label" = 'fori11_head'
                )
                 UNION ALL
               ((SELECT "ifresult78".*
                 FROM ((SELECT True,
                                 'fori3_head',
                                 NULL :: int4[],
                                 "aspect_ratio",
                                 "c",
                                 "cam",
                                 "col",
                                 "do_ray",
                                 "epsilon",
                                 "fov",
                                 "ho",
                                 "i",
                                 "intersection",
                                 "light",
                                 "mat",
                                 "material",
                                 "max_rec_depth",
                                 "mindist",
                                 "no",
                                 "nprimitives",
                                 "ntriangles",
                                 "prim_hit",
                                 (SELECT 0 AS "?column?"),
                                 "pxy",
                                 "r",
                                 "rd",
                                 "rec",
                                 "ro",
                                 "rotx1",
                                 "rotx2",
                                 "rotx3",
                                 "roty1",
                                 "roty2",
                                 "roty3",
                                 "rotz1",
                                 "rotz2",
                                 "rotz3",
                                 "shadow_done",
                                 "shadows",
                                 "sp",
                                 "tca",
                                 "tdist",
                                 "thc",
                                 "v1"
                          WHERE "pxy" <= "h" - 1
                          )
                           UNION ALL
                         (SELECT False,
                                 NULL :: text,
                                 "r" AS "result",
                                 "run"."aspect_ratio",
                                 "run"."c",
                                 "run"."cam",
                                 "run"."col",
                                 "run"."do_ray",
                                 "run"."epsilon",
                                 "run"."fov",
                                 "run"."ho",
                                 "run"."i",
                                 "run"."intersection",
                                 "run"."light",
                                 "run"."mat",
                                 "run"."material",
                                 "run"."max_rec_depth",
                                 "run"."mindist",
                                 "run"."no",
                                 "run"."nprimitives",
                                 "run"."ntriangles",
                                 "run"."prim_hit",
                                 "run"."pxx",
                                 "run"."pxy",
                                 "run"."r",
                                 "run"."rd",
                                 "run"."rec",
                                 "run"."ro",
                                 "run"."rotx1",
                                 "run"."rotx2",
                                 "run"."rotx3",
                                 "run"."roty1",
                                 "run"."roty2",
                                 "run"."roty3",
                                 "run"."rotz1",
                                 "run"."rotz2",
                                 "run"."rotz3",
                                 "run"."shadow_done",
                                 "run"."shadows",
                                 "run"."sp",
                                 "run"."tca",
                                 "run"."tdist",
                                 "run"."thc",
                                 "run"."v1"
                          WHERE NOT ("pxy" <= "h" - 1)
                          )

                        ) AS "ifresult78"
                 WHERE "run"."label" = 'fori1_head'
                 )
                 UNION ALL
               ((SELECT "ifresult83".*
                 FROM (LATERAL
                        (SELECT "pxx" <= "w" - 1 AS "pred_5") AS "let82"("pred_5")
                        LEFT OUTER JOIN
                        LATERAL
                        ((SELECT True,
                                 'fori5_head',
                                 NULL :: int4[],
                                 "aspect_ratio",
                                 "c_5",
                                 "cam",
                                 "col",
                                 "do_ray_5",
                                 "epsilon",
                                 "fov",
                                 "ho",
                                 "i",
                                 "intersection",
                                 "light",
                                 "mat",
                                 "material",
                                 "max_rec_depth",
                                 "mindist",
                                 "no",
                                 "nprimitives",
                                 "ntriangles",
                                 "prim_hit",
                                 "pxx",
                                 "pxy",
                                 "r",
                                 "rd_5",
                                 1,
                                 "ro_5",
                                 "rotx1",
                                 "rotx2",
                                 "rotx3",
                                 "roty1",
                                 "roty2",
                                 "roty3",
                                 "rotz1",
                                 "rotz2",
                                 "rotz3",
                                 "shadow_done_5",
                                 "shadows",
                                 "sp",
                                 "tca",
                                 "tdist",
                                 "thc",
                                 "v1"
                          FROM (LATERAL
                                (SELECT ((((("pxx") + 0.5) / ("w")) - 0.5) * "fov")
                                        *
                                        "aspect_ratio" AS "degx_5",
                                        (((("pxy") + 0.5) / ("h")) - 0.5) * "fov" AS "degy_5"

                                ) AS "let84"("degx_5")

                                 LEFT OUTER JOIN
                                 (LATERAL
                                  (SELECT (sin("degx_5"), sin("degy_5"), 1.0) :: vec3 AS "t_5"

                                  ) AS "let86"("t_5")
                                  LEFT OUTER JOIN
                                  LATERAL
                                   (SELECT ((((("t_5" :: vec3).x) * "rotx1")
                                             +
                                             ((("t_5" :: vec3).y) * "roty1"))
                                            +
                                            ((("t_5" :: vec3).z) * "rotz1"),
                                            (((("t_5" :: vec3).x) * "rotx2")
                                             +
                                             ((("t_5" :: vec3).y) * "roty2"))
                                            +
                                            ((("t_5" :: vec3).z) * "rotz2"),
                                            (((("t_5" :: vec3).x) * "rotx3")
                                             +
                                             ((("t_5" :: vec3).y) * "roty3"))
                                            +
                                            ((("t_5" :: vec3).z) * "rotz3")) :: vec3 AS "rd_5",
                                          "cam" AS "ro_5",
                                          True AS "do_ray_5",
                                          False AS "shadow_done_5",
                                          (0.0, 0.0, 0.0) :: rgb AS "c_5"

                                   ) AS "let87"("rd_5")
                                      ON True)
                                     ON True)
                          WHERE "pred_5"
                          )
                           UNION ALL
                         (SELECT True,
                                  'fori1_head',
                                  NULL :: int4[],
                                  "aspect_ratio",
                                  "c",
                                  "cam",
                                  "col",
                                  "do_ray",
                                  "epsilon",
                                  "fov",
                                  "ho",
                                  "i",
                                  "intersection",
                                  "light",
                                  "mat",
                                  "material",
                                  "max_rec_depth",
                                  "mindist",
                                  "no",
                                  "nprimitives",
                                  "ntriangles",
                                  "prim_hit",
                                  "pxx",
                                  "pxy" + 1 AS "pxy_44",
                                  "r",
                                  "rd",
                                  "rec",
                                  "ro",
                                  "rotx1",
                                  "rotx2",
                                  "rotx3",
                                  "roty1",
                                  "roty2",
                                  "roty3",
                                  "rotz1",
                                  "rotz2",
                                  "rotz3",
                                  "shadow_done",
                                  "shadows",
                                  "sp",
                                  "tca",
                                  "tdist",
                                  "thc",
                                  "v1"
                          WHERE NOT "pred_5"
                          )

                        ) AS "ifresult83"
                        ON True)
                 WHERE "run"."label" = 'fori3_head'
                 )
                 UNION ALL
               ((SELECT "ifresult97".*
                 FROM  (LATERAL
                        (SELECT "rec" <= (1 + "max_rec_depth") + ("shadows") :: int4 AS "pred_8") AS "let96"("pred_8")
                        LEFT OUTER JOIN
                        LATERAL
                        ((SELECT "ifresult99".*
                          FROM ((SELECT True,
                                          'fori11_head',
                                          NULL :: int4[],
                                          "aspect_ratio",
                                          "c",
                                          "cam",
                                          "col",
                                          False AS "do_ray_9",
                                          "epsilon",
                                          "fov",
                                          "ho",
                                          1,
                                          False AS "intersection_8",
                                          "light",
                                          "mat",
                                          'n' :: material AS "material_8",
                                          "max_rec_depth",
                                          999999 AS "mindist_8",
                                          (0.0, 0.0, 0.0) :: vec3 AS "no_8",
                                          "nprimitives",
                                          "ntriangles",
                                          "prim_hit",
                                          "pxx",
                                          "pxy",
                                          "r",
                                          ((("rd" :: vec3).x) / "tlen_10",
                                                   (("rd" :: vec3).y) / "tlen_10",
                                                   (("rd" :: vec3).z) / "tlen_10") :: vec3 AS "rd_9",
                                          "rec",
                                          "ro",
                                          "rotx1",
                                          "rotx2",
                                          "rotx3",
                                          "roty1",
                                          "roty2",
                                          "roty3",
                                          "rotz1",
                                          "rotz2",
                                          "rotz3",
                                          "shadow_done",
                                          "shadows",
                                          "sp",
                                          "tca",
                                          "tdist",
                                          "thc",
                                          "v1"
                                  FROM (SELECT sqrt((((("rd" :: vec3).x) ^ (2))
                                                       +
                                                       ((("rd" :: vec3).y) ^ (2)))
                                                      +
                                                      ((("rd" :: vec3).z) ^ (2))) AS "tlen_10"
                                        ) AS "let100"("tlen_10")
                                  WHERE "do_ray"
                                  )
                                   UNION ALL
                                 (SELECT True,
                                         'ifmerge9',
                                         NULL :: int4[],
                                         "aspect_ratio",
                                         "c",
                                         "cam",
                                         "col",
                                         "do_ray",
                                         "epsilon",
                                         "fov",
                                         "ho",
                                         "i",
                                         "intersection",
                                         "light",
                                         "mat",
                                         "material",
                                         "max_rec_depth",
                                         "mindist",
                                         "no",
                                         "nprimitives",
                                         "ntriangles",
                                         "prim_hit",
                                         "pxx",
                                         "pxy",
                                         "r",
                                         "rd",
                                         "rec",
                                         "ro",
                                         "rotx1",
                                         "rotx2",
                                         "rotx3",
                                         "roty1",
                                         "roty2",
                                         "roty3",
                                         "rotz1",
                                         "rotz2",
                                         "rotz3",
                                         "shadow_done",
                                         "shadows",
                                         "sp",
                                         "tca",
                                         "tdist",
                                         "thc",
                                         "v1"
                                  WHERE NOT "do_ray"
                                  )

                                ) AS "ifresult99"
                          WHERE "pred_8"
                          )
                           UNION ALL
                         (SELECT "ifresult110".*
                          FROM ((SELECT True,
                                        'ifmerge87',
                                        NULL :: int4[],
                                        "aspect_ratio",
                                        "c",
                                        "cam",
                                        "col",
                                        "do_ray",
                                        "epsilon",
                                        "fov",
                                        "ho",
                                        "i",
                                        "intersection",
                                        "light",
                                        "mat",
                                        "material",
                                        "max_rec_depth",
                                        "mindist",
                                        "no",
                                        "nprimitives",
                                        "ntriangles",
                                        "prim_hit",
                                        "pxx",
                                        "pxy",
                                        "r"
                                                ||
                                                (ARRAY[((("c" :: rgb).b) * (255)) :: int4,
                                                       ((("c" :: rgb).g) * (255)) :: int4,
                                                       ((("c" :: rgb).r)
                                                        *
                                                        (255)) :: int4] :: int4[]) AS "r_44",
                                        "rd",
                                        "rec",
                                        "ro",
                                        "rotx1",
                                        "rotx2",
                                        "rotx3",
                                        "roty1",
                                        "roty2",
                                        "roty3",
                                        "rotz1",
                                        "rotz2",
                                        "rotz3",
                                        "shadow_done",
                                        "shadows",
                                        "sp",
                                        "tca",
                                        "tdist",
                                        "thc",
                                        "v1"
                                  WHERE "intersection"
                                  )
                                   UNION ALL
                                 (SELECT True,
                                          'ifmerge87',
                                          NULL :: int4[],
                                          "aspect_ratio",
                                          "c",
                                          "cam",
                                          "col",
                                          "do_ray",
                                          "epsilon",
                                          "fov",
                                          "ho",
                                          "i",
                                          "intersection",
                                          "light",
                                          "mat",
                                          "material",
                                          "max_rec_depth",
                                          "mindist",
                                          "no",
                                          "nprimitives",
                                          "ntriangles",
                                          "prim_hit",
                                          "pxx",
                                          "pxy",
                                          "r" || (ARRAY[0, 0, 0] :: int4[]) AS "r_41",
                                          "rd",
                                          "rec",
                                          "ro",
                                          "rotx1",
                                          "rotx2",
                                          "rotx3",
                                          "roty1",
                                          "roty2",
                                          "roty3",
                                          "rotz1",
                                          "rotz2",
                                          "rotz3",
                                          "shadow_done",
                                          "shadows",
                                          "sp",
                                          "tca",
                                          "tdist",
                                          "thc",
                                          "v1"
                                  WHERE NOT "intersection"
                                  )

                                ) AS "ifresult110"
                          WHERE NOT "pred_8"
                          )

                        ) AS "ifresult97"
                        ON True)
                 WHERE "run"."label" = 'fori5_head'
                 )
                 UNION ALL
               ((SELECT "ifresult116".*
                 FROM ((SELECT True,
                                          'ifmerge55',
                                          NULL :: int4[],
                                          "aspect_ratio",
                                          "c",
                                          "cam",
                                          "col",
                                          "do_ray",
                                          "epsilon",
                                          "fov",
                                          "col" AS "ho_20",
                                          "i",
                                          "intersection",
                                          "light",
                                          "mat",
                                          "mat" AS "material_20",
                                          "max_rec_depth",
                                          "mindist",
                                          "no",
                                          "nprimitives",
                                          "ntriangles",
                                          "prim_hit",
                                          "pxx",
                                          "pxy",
                                          "r",
                                          "rd",
                                          "rec",
                                          "ro",
                                          "rotx1",
                                          "rotx2",
                                          "rotx3",
                                          "roty1",
                                          "roty2",
                                          "roty3",
                                          "rotz1",
                                          "rotz2",
                                          "rotz3",
                                          "shadow_done",
                                          "shadows",
                                          "sp",
                                          "tca",
                                          "tdist",
                                          "thc",
                                          "v1"
                                  WHERE "prim_hit" AND "mat" = 'm' :: material
                                  )
                                   UNION ALL
                                 (SELECT True,
                                         'ifmerge55',
                                         NULL :: int4[],
                                         "aspect_ratio",
                                         "c",
                                         "cam",
                                         "col",
                                         "do_ray",
                                         "epsilon",
                                         "fov",
                                         "ho",
                                         "i",
                                         "intersection",
                                         "light",
                                         "mat",
                                         "mat" AS "material_20",
                                         "max_rec_depth",
                                         "mindist",
                                         "no",
                                         "nprimitives",
                                         "ntriangles",
                                         "prim_hit",
                                         "pxx",
                                         "pxy",
                                         "r",
                                         "rd",
                                         "rec",
                                         "ro",
                                         "rotx1",
                                         "rotx2",
                                         "rotx3",
                                         "roty1",
                                         "roty2",
                                         "roty3",
                                         "rotz1",
                                         "rotz2",
                                         "rotz3",
                                         "shadow_done",
                                         "shadows",
                                         "sp",
                                         "tca",
                                         "tdist",
                                         "thc",
                                         "v1"
                                  WHERE "prim_hit" AND NOT ("mat" = 'm' :: material)
                                  )
                          UNION ALL
                        (SELECT True,
                                'ifmerge55',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                "do_ray",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                "intersection",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist",
                                "no",
                                "nprimitives",
                                "ntriangles",
                                "prim_hit",
                                "pxx",
                                "pxy",
                                "r",
                                "rd",
                                "rec",
                                "ro",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         WHERE NOT "prim_hit"
                         )

                       ) AS "ifresult116"
                 WHERE "run"."label" = 'ifmerge15'
                 )
                 UNION ALL
               ((SELECT "ifresult125".*
                 FROM ((SELECT True,
                               'ifmerge47',
                               NULL :: int4[],
                               "aspect_ratio",
                               "c",
                               "cam",
                               "col",
                               "do_ray",
                               "epsilon",
                               "fov",
                               "ho",
                               "i",
                               "intersection",
                               "light",
                               "mat",
                               "material",
                               "max_rec_depth",
                               "mindist",
                               "no",
                               "nprimitives",
                               "ntriangles",
                               "prim_hit",
                               "pxx",
                               "pxy",
                               "r",
                               "rd",
                               "rec",
                               "ro",
                               "rotx1",
                               "rotx2",
                               "rotx3",
                               "roty1",
                               "roty2",
                               "roty3",
                               "rotz1",
                               "rotz2",
                               "rotz3",
                               "shadow_done",
                               "shadows",
                               "sp",
                               "tca",
                               least("tca" + "thc", "tdist") AS "tdist_17",
                               "thc",
                               "v1"
                         WHERE ("tca" + "thc") > (0.0)
                         )
                          UNION ALL
                        (SELECT True,
                                'ifmerge47',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                "do_ray",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                "intersection",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist",
                                "no",
                                "nprimitives",
                                "ntriangles",
                                "prim_hit",
                                "pxx",
                                "pxy",
                                "r",
                                "rd",
                                "rec",
                                "ro",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         WHERE NOT (("tca" + "thc") > (0.0))
                         )

                       ) AS "ifresult125"
                 WHERE "run"."label" = 'ifmerge43'
                 )
                 UNION ALL
               ((SELECT "ifresult130".*
                 FROM ((SELECT True,
                                'ifmerge15',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                "do_ray",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                True AS "intersection_18",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist_18",
                                ((("no_18" :: vec3).x) / "tlen_20",
                                             (("no_18" :: vec3).y) / "tlen_20",
                                             (("no_18" :: vec3).z) / "tlen_20") :: vec3 AS "no_19",
                                "nprimitives",
                                "ntriangles",
                                True AS "prim_hit_18",
                                "pxx",
                                "pxy",
                                "r",
                                "rd",
                                "rec",
                                "ro",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         FROM (LATERAL
                                 (SELECT "tdist" AS "mindist_18") AS "let133"("mindist_18")
                                 LEFT OUTER JOIN
                                 (LATERAL
                                  (SELECT (((("ro" :: vec3).x) + ("tdist" * (("rd" :: vec3).x)))
                                           -
                                           (("sp" :: vec3).x),
                                           ((("ro" :: vec3).y) + ("tdist" * (("rd" :: vec3).y)))
                                           -
                                           (("sp" :: vec3).y),
                                           ((("ro" :: vec3).z) + ("tdist" * (("rd" :: vec3).z)))
                                           -
                                           (("sp" :: vec3).z)) :: vec3 AS "no_18"

                                  ) AS "let134"("no_18")
                                  LEFT OUTER JOIN
                                  LATERAL
                                   (SELECT sqrt((((("no_18" :: vec3).x) ^ (2))
                                                 +
                                                 ((("no_18" :: vec3).y) ^ (2)))
                                                +
                                                ((("no_18" :: vec3).z) ^ (2))) AS "tlen_20"

                                   ) AS "let135"("tlen_20")
                                  ON True)
                                 ON True)
                         WHERE ("tdist" > (0.0) AND "tdist" < "mindist")
                         )
                          UNION ALL
                        (SELECT True,
                                'ifmerge15',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                "do_ray",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                "intersection",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist",
                                "no",
                                "nprimitives",
                                "ntriangles",
                                "prim_hit",
                                "pxx",
                                "pxy",
                                "r",
                                "rd",
                                "rec",
                                "ro",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         WHERE NOT (("tdist" > (0.0) AND "tdist" < "mindist"))
                         )

                       ) AS "ifresult130"
                 WHERE "run"."label" = 'ifmerge47'
                 )
                 UNION ALL
               ((SELECT True,
                       'fori11_head',
                       NULL :: int4[],
                       "aspect_ratio",
                       "c",
                       "cam",
                       "col",
                       "do_ray",
                       "epsilon",
                       "fov",
                       "ho",
                       "i" + 1 AS "i_22",
                       "intersection",
                       "light",
                       "mat",
                       "material",
                       "max_rec_depth",
                       "mindist",
                       "no",
                       "nprimitives",
                       "ntriangles",
                       "prim_hit",
                       "pxx",
                       "pxy",
                       "r",
                       "rd",
                       "rec",
                       "ro",
                       "rotx1",
                       "rotx2",
                       "rotx3",
                       "roty1",
                       "roty2",
                       "roty3",
                       "rotz1",
                       "rotz2",
                       "rotz3",
                       "shadow_done",
                       "shadows",
                       "sp",
                       "tca",
                       "tdist",
                       "thc",
                       "v1"
                 WHERE "run"."label" = 'ifmerge55'
                 )
                 UNION ALL
               ((SELECT "ifresult142".*
                 FROM (LATERAL
                       (SELECT "material" = 'm' :: material AS "q76_31"

                       ) AS "let141"("q76_31")
                       LEFT OUTER JOIN
                       LATERAL
                       ((SELECT "ifresult149".*
                         FROM (LATERAL
                               (SELECT ((("light" :: vec3).x)
                                        -
                                        ((("ro" :: vec3).x) + ((("rd" :: vec3).x) * "mindist")),
                                        (("light" :: vec3).y)
                                        -
                                        ((("ro" :: vec3).y) + ((("rd" :: vec3).y) * "mindist")),
                                        (("light" :: vec3).z)
                                        -
                                        ((("ro" :: vec3).z)
                                         +
                                         ((("rd" :: vec3).z) * "mindist"))) :: vec3 AS "li_32"

                               ) AS "let143"("li_32")
                               LEFT OUTER JOIN
                               (LATERAL
                                (SELECT sqrt((((("li_32" :: vec3).x) ^ (2))
                                              +
                                              ((("li_32" :: vec3).y) ^ (2)))
                                             +
                                             ((("li_32" :: vec3).z) ^ (2))) AS "tlen_37"

                                ) AS "let144"("tlen_37")
                                LEFT OUTER JOIN
                                (LATERAL
                                 (SELECT ((("li_32" :: vec3).x) / "tlen_37",
                                          (("li_32" :: vec3).y) / "tlen_37",
                                          (("li_32" :: vec3).z) / "tlen_37") :: vec3 AS "li_33"

                                 ) AS "let145"("li_33")
                                 LEFT OUTER JOIN
                                 (LATERAL
                                  (SELECT greatest(0.0,
                                                   (((("li_33" :: vec3).x) * (("no" :: vec3).x))
                                                    +
                                                    ((("li_33" :: vec3).y) * (("no" :: vec3).y)))
                                                   +
                                                   ((("li_33" :: vec3).z)
                                                    *
                                                    (("no" :: vec3).z))) AS "tdot_33"

                                  ) AS "let146"("tdot_33")
                                  LEFT OUTER JOIN
                                  (LATERAL
                                   (SELECT ((("ho" :: rgb).r) * "tdot_33",
                                            (("ho" :: rgb).g) * "tdot_33",
                                            (("ho" :: rgb).b) * "tdot_33") :: rgb AS "c_34"

                                   ) AS "let147"("c_34")
                                   LEFT OUTER JOIN
                                   (LATERAL
                                    (SELECT ("shadows" AND NOT "shadow_done") AS "q80_32"

                                    ) AS "let148"("q80_32")
                                    LEFT OUTER JOIN
                                    LATERAL
                                    ((SELECT True,
                                             'ifmerge75',
                                             NULL :: int4[],
                                             "aspect_ratio",
                                             "c_34",
                                             "cam",
                                             "col",
                                             True AS "do_ray_35",
                                             "epsilon",
                                             "fov",
                                             "ho",
                                             "i",
                                             "intersection",
                                             "light",
                                             "mat",
                                             "material",
                                             "max_rec_depth",
                                             "mindist",
                                             "no",
                                             "nprimitives",
                                             "ntriangles",
                                             "prim_hit",
                                             "pxx",
                                             "pxy",
                                             "r",
                                             ((("light" :: vec3).x)
                                                       -
                                                       (("ro_34" :: vec3).x),
                                                       (("light" :: vec3).y)
                                                       -
                                                       (("ro_34" :: vec3).y),
                                                       (("light" :: vec3).z)
                                                       -
                                                       (("ro_34" :: vec3).z)) :: vec3 AS "rd_35",
                                             "rec",
                                             "ro_34",
                                             "rotx1",
                                             "rotx2",
                                             "rotx3",
                                             "roty1",
                                             "roty2",
                                             "roty3",
                                             "rotz1",
                                             "rotz2",
                                             "rotz3",
                                             True AS "shadow_done_34",
                                             "shadows",
                                             "sp",
                                             "tca",
                                             "tdist",
                                             "thc",
                                             "v1"
                                      FROM (SELECT (((("ro" :: vec3).x)
                                                       +
                                                       ((("rd" :: vec3).x) * "mindist"))
                                                      +
                                                      ((("no" :: vec3).x) * "epsilon"),
                                                      ((("ro" :: vec3).y)
                                                       +
                                                       ((("rd" :: vec3).y) * "mindist"))
                                                      +
                                                      ((("no" :: vec3).y) * "epsilon"),
                                                      ((("ro" :: vec3).z)
                                                       +
                                                       ((("rd" :: vec3).z) * "mindist"))
                                                      +
                                                      ((("no" :: vec3).z)
                                                       *
                                                       "epsilon")) :: vec3 AS "ro_34"
                                             ) AS "let151"("ro_34")
                                      WHERE "q80_32"
                                      )
                                       UNION ALL
                                     (SELECT True,
                                             'ifmerge75',
                                             NULL :: int4[],
                                             "aspect_ratio",
                                             "c_34",
                                             "cam",
                                             "col",
                                             "do_ray",
                                             "epsilon",
                                             "fov",
                                             "ho",
                                             "i",
                                             "intersection",
                                             "light",
                                             "mat",
                                             "material",
                                             "max_rec_depth",
                                             "mindist",
                                             "no",
                                             "nprimitives",
                                             "ntriangles",
                                             "prim_hit",
                                             "pxx",
                                             "pxy",
                                             "r",
                                             "rd",
                                             "rec",
                                             "ro",
                                             "rotx1",
                                             "rotx2",
                                             "rotx3",
                                             "roty1",
                                             "roty2",
                                             "roty3",
                                             "rotz1",
                                             "rotz2",
                                             "rotz3",
                                             "shadow_done",
                                             "shadows",
                                             "sp",
                                             "tca",
                                             "tdist",
                                             "thc",
                                             "v1"
                                      WHERE NOT "q80_32"
                                      )

                                    ) AS "ifresult149"
                                    ON True)
                                   ON True)
                                  ON True)
                                 ON True)
                                ON True)
                               ON True)
                         WHERE "q76_31"
                         )
                          UNION ALL
                        (SELECT True,
                                'ifmerge75',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                "do_ray",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                "intersection",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist",
                                "no",
                                "nprimitives",
                                "ntriangles",
                                "prim_hit",
                                "pxx",
                                "pxy",
                                "r",
                                "rd",
                                "rec",
                                "ro",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         WHERE NOT "q76_31"
                         )

                       ) AS "ifresult142"
                       ON True)
                 WHERE "run"."label" = 'ifmerge71'
                 )
                 UNION ALL
               ((SELECT "ifresult158".*
                 FROM ((SELECT True,
                                'ifmerge9',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                True AS "do_ray_38",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                "intersection",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist",
                                "no",
                                "nprimitives",
                                "ntriangles",
                                "prim_hit",
                                "pxx",
                                "pxy",
                                "r",
                                ((("rd" :: vec3).x)
                                          -
                                          (((2.0) * (("no" :: vec3).x)) * "tdot_37"),
                                          (("rd" :: vec3).y)
                                          -
                                          (((2.0) * (("no" :: vec3).y)) * "tdot_37"),
                                          (("rd" :: vec3).z)
                                          -
                                          (((2.0) * (("no" :: vec3).z))
                                           *
                                           "tdot_37")) :: vec3 AS "rd_38",
                                "rec",
                                (((("ro" :: vec3).x) + ((("rd" :: vec3).x) * "mindist"))
                                         +
                                         ((("no" :: vec3).x) * "epsilon"),
                                         ((("ro" :: vec3).y) + ((("rd" :: vec3).y) * "mindist"))
                                         +
                                         ((("no" :: vec3).y) * "epsilon"),
                                         ((("ro" :: vec3).z) + ((("rd" :: vec3).z) * "mindist"))
                                         +
                                         ((("no" :: vec3).z) * "epsilon")) :: vec3 AS "ro_37",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         FROM (SELECT (((("rd" :: vec3).x) * (("no" :: vec3).x))
                                        +
                                        ((("rd" :: vec3).y) * (("no" :: vec3).y)))
                                       +
                                       ((("rd" :: vec3).z) * (("no" :: vec3).z)) AS "tdot_37"

                               ) AS "let159"("tdot_37")
                         WHERE "material" = 'r' :: material
                         )
                          UNION ALL
                        (SELECT True,
                                'ifmerge9',
                                NULL :: int4[],
                                "aspect_ratio",
                                "c",
                                "cam",
                                "col",
                                "do_ray",
                                "epsilon",
                                "fov",
                                "ho",
                                "i",
                                "intersection",
                                "light",
                                "mat",
                                "material",
                                "max_rec_depth",
                                "mindist",
                                "no",
                                "nprimitives",
                                "ntriangles",
                                "prim_hit",
                                "pxx",
                                "pxy",
                                "r",
                                "rd",
                                "rec",
                                "ro",
                                "rotx1",
                                "rotx2",
                                "rotx3",
                                "roty1",
                                "roty2",
                                "roty3",
                                "rotz1",
                                "rotz2",
                                "rotz3",
                                "shadow_done",
                                "shadows",
                                "sp",
                                "tca",
                                "tdist",
                                "thc",
                                "v1"
                         WHERE NOT ("material" = 'r' :: material)
                         )

                       ) AS "ifresult158"
                 WHERE "run"."label" = 'ifmerge75'
                 )
                 UNION ALL
               ((SELECT True,
                         'fori3_head',
                         NULL :: int4[],
                         "aspect_ratio",
                         "c",
                         "cam",
                         "col",
                         "do_ray",
                         "epsilon",
                         "fov",
                         "ho",
                         "i",
                         "intersection",
                         "light",
                         "mat",
                         "material",
                         "max_rec_depth",
                         "mindist",
                         "no",
                         "nprimitives",
                         "ntriangles",
                         "prim_hit",
                         "pxx" + 1 AS "pxx_42",
                         "pxy",
                         "r",
                         "rd",
                         "rec",
                         "ro",
                         "rotx1",
                         "rotx2",
                         "rotx3",
                         "roty1",
                         "roty2",
                         "roty3",
                         "rotz1",
                         "rotz2",
                         "rotz3",
                         "shadow_done",
                         "shadows",
                         "sp",
                         "tca",
                         "tdist",
                         "thc",
                         "v1"
                 WHERE "run"."label" = 'ifmerge87'
                 )
                 UNION ALL
               (SELECT True,
                        'fori5_head',
                        NULL :: int4[],
                        "aspect_ratio",
                        "c",
                        "cam",
                        "col",
                        "do_ray",
                        "epsilon",
                        "fov",
                        "ho",
                        "i",
                        "intersection",
                        "light",
                        "mat",
                        "material",
                        "max_rec_depth",
                        "mindist",
                        "no",
                        "nprimitives",
                        "ntriangles",
                        "prim_hit",
                        "pxx",
                        "pxy",
                        "r",
                        "rd",
                        "rec" + 1 AS "rec_39",
                        "ro",
                        "rotx1",
                        "rotx2",
                        "rotx3",
                        "roty1",
                        "roty2",
                        "roty3",
                        "rotz1",
                        "rotz2",
                        "rotz3",
                        "shadow_done",
                        "shadows",
                        "sp",
                        "tca",
                        "tdist",
                        "thc",
                        "v1"
                WHERE "run"."label" = 'ifmerge9'
                )))))))))))

              ) AS "result"("rec?",
                            "label",
                            "res",
                            "aspect_ratio",
                            "c",
                            "cam",
                            "col",
                            "do_ray",
                            "epsilon",
                            "fov",
                            "ho",
                            "i",
                            "intersection",
                            "light",
                            "mat",
                            "material",
                            "max_rec_depth",
                            "mindist",
                            "no",
                            "nprimitives",
                            "ntriangles",
                            "prim_hit",
                            "pxx",
                            "pxy",
                            "r",
                            "rd",
                            "rec",
                            "ro",
                            "rotx1",
                            "rotx2",
                            "rotx3",
                            "roty1",
                            "roty2",
                            "roty3",
                            "rotz1",
                            "rotz2",
                            "rotz3",
                            "shadow_done",
                            "shadows",
                            "sp",
                            "tca",
                            "tdist",
                            "thc",
                            "v1")
         WHERE "run"."rec?" = True
         )
    )
    SELECT "run"."res"
    FROM run AS "run"
    WHERE "run"."rec?" = False
  ) AS run;



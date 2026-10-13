WITH
    RECURSIVE
    settings AS (
        SELECT 800   AS width,
               450   AS height,
               1     AS samples_per_pixel,
               50    AS max_depth,
               12.0  AS camera_look_from_x,
               2.0   AS camera_look_from_y,
               -3.0  AS camera_look_from_z,
               0.0   AS camera_look_at_x,
               0.0   AS camera_look_at_y,
               0.0   AS camera_look_at_z,
               25.0  AS camera_fov,
               0.001 AS sphere_collision_t_min
    ),
    spheres AS (
        WITH
            RECURSIVE
            static_spheres AS (
                -- ground sphere:
                SELECT -1          AS sphere_id,
                       0           AS sphere_center_x,
                       -1000       AS sphere_center_y,
                       0           AS sphere_center_z,
                       1000        AS sphere_radius,
                       'DIFFUSE'   AS sphere_material_type,
                       'CHECKERED' AS sphere_texture_type,
                       0.05        AS sphere_albedo_1_r,
                       0.05        AS sphere_albedo_1_g,
                       0.05        AS sphere_albedo_1_b,
                       0.95        AS sphere_albedo_2_r,
                       0.95        AS sphere_albedo_2_g,
                       0.95        AS sphere_albedo_2_b,
                       0           AS sphere_refraction_index
                UNION ALL
                -- left sphere:
                SELECT -2,
                       -4,
                       1,
                       0,
                       1,
                       'DIFFUSE',
                       'SOLID',
                       0.6,
                       0.3,
                       0.1,
                       0,
                       0,
                       0,
                       0
                UNION ALL
                -- center sphere:
                SELECT -3,
                       0,
                       1,
                       0,
                       1,
                       'DIELECTRIC',
                       'SOLID',
                       0,
                       0,
                       0,
                       0,
                       0,
                       0,
                       1.5
                UNION ALL
                -- right sphere:
                SELECT -4,
                       4,
                       1,
                       0,
                       1,
                       'METAL',
                       'SOLID',
                       0.7,
                       0.6,
                       0.5,
                       0,
                       0,
                       0,
                       0
            ),
            random_spheres AS (
                -- dummy sphere, so that there is an initial sphere_id for the recursion
                SELECT 0                AS sphere_id,
                       CAST(0 AS FLOAT) AS sphere_center_x,
                       CAST(0 AS FLOAT) AS sphere_center_y,
                       CAST(0 AS FLOAT) AS sphere_center_z,
                       CAST(0 AS FLOAT) AS sphere_radius,
                       'DIFFUSE'        AS sphere_material_type,
                       CAST(0 AS FLOAT) AS sphere_albedo_r,
                       CAST(0 AS FLOAT) AS sphere_albedo_g,
                       CAST(0 AS FLOAT) AS sphere_albedo_b,
                       CAST(0 AS FLOAT) AS sphere_refraction_index
                UNION ALL
                -- 22*22 random spheres:
                SELECT *
                FROM (
                    WITH
                        new_sphere AS (
                            SELECT sphere_id + 1 AS sphere_id
                            FROM random_spheres
                            WHERE sphere_id < 22 * 22
                        ),
                        sphere_location AS (
                            SELECT *,
                                   MOD(sphere_id, 22) - 11 + random() * 0.9    AS sphere_center_x,
                                   0.2                                         AS sphere_center_y,
                                   FLOOR(sphere_id / 22) - 11 + random() * 0.9 AS sphere_center_z,
                                   0.2                                         AS sphere_radius,
                                   random()                                    AS material_random
                            FROM new_sphere
                        ),
                        sphere_material AS (
                            SELECT *,
                                   CASE
                                       WHEN material_random < 0.8 THEN 'DIFFUSE'
                                       WHEN material_random >= 0.8 AND material_random < 0.95 THEN 'METAL'
                                       WHEN material_random >= 0.95 THEN 'DIELECTRIC'
                                       END AS sphere_material_type
                            FROM sphere_location
                        ),
                        sphere_color AS (
                            -- HSV to RGB conversion:
                            WITH
                                sphere_color_h AS (
                                    SELECT *,
                                           (random() * 360) / 60 AS color_h
                                    FROM sphere_material
                                ),
                                sphere_color_fraction AS (
                                    SELECT *,
                                           color_h - FLOOR(color_h) AS color_fraction
                                    FROM sphere_color_h
                                ),
                                sphere_color_calc AS (
                                    SELECT *,
                                           0.45 * (1 - 0.75)                        AS color_p,
                                           0.45 * (1 - 0.75 * color_fraction)       AS color_q,
                                           0.45 * (1 - 0.75 * (1 - color_fraction)) AS color_t
                                    fROM sphere_color_fraction
                                )
                            SELECT *,
                                   CASE
                                       WHEN 0 <= color_h AND color_h < 1 THEN 0.45
                                       WHEN 1 <= color_h AND color_h < 2 THEN color_q
                                       WHEN 2 <= color_h AND color_h < 3 THEN color_p
                                       WHEN 3 <= color_h AND color_h < 4 THEN color_p
                                       WHEN 4 <= color_h AND color_h < 5 THEN color_t
                                       WHEN 5 <= color_h AND color_h < 6 THEN 0.45
                                       ElSE 0
                                       END AS sphere_albedo_r,
                                   CASE
                                       WHEN 0 <= color_h AND color_h < 1 THEN color_t
                                       WHEN 1 <= color_h AND color_h < 2 THEN 0.45
                                       WHEN 2 <= color_h AND color_h < 3 THEN 0.45
                                       WHEN 3 <= color_h AND color_h < 4 THEN color_q
                                       WHEN 4 <= color_h AND color_h < 5 THEN color_p
                                       WHEN 5 <= color_h AND color_h < 6 THEN color_p
                                       ELSE 0
                                       END AS sphere_albedo_g,
                                   CASE
                                       WHEN 0 <= color_h AND color_h < 1 THEN color_p
                                       WHEN 1 <= color_h AND color_h < 2 THEN color_p
                                       WHEN 2 <= color_h AND color_h < 3 THEN color_t
                                       WHEN 3 <= color_h AND color_h < 4 THEN 0.45
                                       WHEN 4 <= color_h AND color_h < 5 THEN 0.45
                                       WHEN 5 <= color_h AND color_h < 6 THEN color_q
                                       ELSE 0
                                       END AS sphere_albedo_b
                            FROM sphere_color_calc
                        )
                    SELECT sphere_id,
                           sphere_center_x,
                           sphere_center_y,
                           sphere_center_z,
                           sphere_radius,
                           sphere_material_type,
                           sphere_albedo_r,
                           sphere_albedo_g,
                           sphere_albedo_b,
                           1.5 AS sphere_refraction_index
                    FROM sphere_color
                ) AS _
            )
        SELECT *
        FROM static_spheres
        UNION ALL
        SELECT sphere_id,
               sphere_center_x,
               sphere_center_y,
               sphere_center_z,
               sphere_radius,
               sphere_material_type,
               'SOLID',
               sphere_albedo_r,
               sphere_albedo_g,
               sphere_albedo_b,
               0,
               0,
               0,
               sphere_refraction_index
        FROM random_spheres
    ),
    camera AS (
        WITH
            camera_viewport_height AS (
                SELECT TAN(RADIANS(camera_fov) / 2.0) * 2.0 AS viewport_height
                FROM settings
            ),
            camera_viewport_width AS (
                SELECT CAST(width AS FLOAT) / height * viewport_height AS viewport_width
                FROM settings,
                     camera_viewport_height
            ),
            camera_forward AS (
                WITH
                    forward_non_normalized AS (
                        SELECT camera_look_at_x - camera_look_from_x AS forward_x,
                               camera_look_at_y - camera_look_from_y AS forward_y,
                               camera_look_at_z - camera_look_from_z AS forward_z
                        FROM settings
                    ),
                    forward_length_calc AS (
                        SELECT *,
                               SQRT(forward_x * forward_x + forward_y * forward_y + forward_z * forward_z) AS forward_length
                        fROM forward_non_normalized
                    )
                SELECT forward_x / forward_length AS forward_x,
                       forward_y / forward_length AS forward_y,
                       forward_z / forward_length AS forward_z
                FROM forward_length_calc
            ),
            camera_right AS (
                WITH
                    right_non_normalized AS (
                        SELECT forward_z   AS right_x,
                               0           AS right_y,
                               - forward_x AS right_z
                        FROM camera_forward
                    ),
                    right_length_calc AS (
                        SELECT *,
                               SQRT(right_x * right_x + right_y * right_y + right_z * right_z) AS right_length
                        FROM right_non_normalized
                    )
                SELECT right_x / right_length AS right_x,
                       right_y / right_length AS right_y,
                       right_z / right_length AS right_z
                FROM right_length_calc
            ),
            camera_up AS (
                WITH
                    up_non_normalized AS (
                        SELECT (forward_y * right_z) - (forward_z * right_y) AS up_x,
                               (forward_z * right_x) - (forward_x * right_z) AS up_y,
                               (forward_x * right_y) - (forward_y * right_x) AS up_z
                        FROM camera_forward,
                             camera_right
                    ),
                    up_length_calc AS (
                        SELECT *,
                               SQRT(up_x * up_x + up_y * up_y + up_z * up_z) AS up_length
                        FROM up_non_normalized
                    )
                SELECT up_x / up_length AS up_x,
                       up_y / up_length AS up_y,
                       up_z / up_length AS up_z
                FROM up_length_calc
            ),
            camera_directions AS (
                SELECT right_x * viewport_width AS horizontal_direction_x,
                       right_y * viewport_width AS horizontal_direction_y,
                       right_z * viewport_width AS horizontal_direction_z,
                       up_x * viewport_height   AS vertical_direction_x,
                       up_y * viewport_height   AS vertical_direction_y,
                       up_z * viewport_height   AS vertical_direction_z
                FROM camera_viewport_width,
                     camera_viewport_height,
                     camera_right,
                     camera_up
            ),
            camera_upper_left_corner AS (
                SELECT camera_look_from_x - horizontal_direction_x / 2.0 + vertical_direction_x / 2.0 +
                       forward_x AS upper_left_corner_x,
                       camera_look_from_y - horizontal_direction_y / 2.0 + vertical_direction_y / 2.0 +
                       forward_y AS upper_left_corner_y,
                       camera_look_from_z - horizontal_direction_z / 2.0 + vertical_direction_z / 2.0 +
                       forward_z AS upper_left_corner_z
                FROM settings,
                     camera_directions,
                     camera_forward
            )
        SELECT camera_directions.*, camera_upper_left_corner.*
        FROM camera_directions,
             camera_upper_left_corner
    ),
    pixels AS (
        WITH
            RECURSIVE
            pixel_ids AS (
                SELECT 0 AS pixel_id
                UNION ALL
                SELECT pixel_id + 1 AS pixel_id
                FROM pixel_ids
                WHERE pixel_id + 1 < (
                    SELECT width * height
                    FROM settings
                )
            ),
            pixel_samples AS (
                SELECT pixel_id, 0 AS sample_id
                FROM pixel_ids
                UNION ALL
                SElECT pixel_id, sample_id + 1 AS sample_id
                FROM pixel_samples
                WHERE sample_id + 1 < (
                    SELECT samples_per_pixel
                    FROM settings
                )
            ),
            pixel_xy AS (
                SELECT *,
                       MOD(pixel_id, width) + random()    AS x,
                       FLOOR(pixel_id / width) + random() AS y
                FROM pixel_samples,
                     settings
            ),
            pixel_uv AS (
                SELECT *,
                       CAST(x AS FLOAT) / width  AS u,
                       CAST(y AS FLOAT) / height AS v
                FROM pixel_xy
            )
        SELECT pixel_id, sample_id, u, v
        FROM pixel_uv
    ),
    -- unique identification of rays by (pixel_id, sample_id, ray_depth)
    rays AS (
        -- initial rays (origin at the camera):
        (
            WITH
                camera_rays AS (
                    WITH
                        ray_directions_non_normalized AS (
                            SELECT *,
                                   upper_left_corner_x + horizontal_direction_x * u - vertical_direction_x * v -
                                   camera_look_from_x AS direction_x,
                                   upper_left_corner_y + horizontal_direction_y * u - vertical_direction_y * v -
                                   camera_look_from_y AS direction_y,
                                   upper_left_corner_z + horizontal_direction_z * u - vertical_direction_z * v -
                                   camera_look_from_z AS direction_z
                            FROM pixels,
                                 camera,
                                 settings
                        ),
                        ray_directions_length AS (
                            SELECT *,
                                   SQRT(direction_x * direction_x + direction_y * direction_y +
                                        direction_z * direction_z) AS direction_length
                            FROM ray_directions_non_normalized
                        ),
                        ray_directions AS (
                            SELECT *,
                                   direction_x / direction_length AS ray_direction_x,
                                   direction_y / direction_length AS ray_direction_y,
                                   direction_z / direction_length AS ray_direction_z
                            FROM ray_directions_length
                        )
                    SELECT pixel_id,
                           sample_id,
                           0                                 AS ray_depth,
                           TRUE                              AS should_trace,
                           CAST(camera_look_from_x AS FLOAT) AS ray_origin_x,
                           CAST(camera_look_from_y AS FLOAT) AS ray_origin_y,
                           CAST(camera_look_from_z AS FLOAT) AS ray_origin_z,
                           ray_direction_x,
                           ray_direction_y,
                           ray_direction_z
                    FROM ray_directions
                )
            -- background color interpolation:
            SELECT *,
                   1.0 * (1.0 - ((ray_direction_y + 1.0) * 0.5)) + 0.3 * ((ray_direction_y + 1.0) * 0.5) AS ray_color_r,
                   1.0 * (1.0 - ((ray_direction_y + 1.0) * 0.5)) + 0.5 * ((ray_direction_y + 1.0) * 0.5) AS ray_color_g,
                   1.0 * (1.0 - ((ray_direction_y + 1.0) * 0.5)) + 0.8 * ((ray_direction_y + 1.0) * 0.5) AS ray_color_b
            FROM camera_rays
        )

        UNION ALL

        -- recursive ray intersection and color calculation:
        SELECT *
        FROM (
            WITH
                last_ray_per_pixel AS (
                    WITH
                        all_rays AS (
                            SELECT *
                            FROM rays
                        )
                    SELECT all_rays.*
                    FROM (
                        SELECT pixel_id, MAX(ray_depth) AS depth
                        FROM all_rays
                        GROUP BY pixel_id
                    ) AS last_ray_per_pixel
                    JOIN all_rays
                         ON last_ray_per_pixel.pixel_id = all_rays.pixel_id AND
                            last_ray_per_pixel.depth = all_rays.ray_depth
                    WHERE should_trace = TRUE
                ),
                rays_to_trace AS (
                    SELECT last_ray_per_pixel.*
                    FROM last_ray_per_pixel,
                         settings
                    WHERE ray_depth < max_depth
                ),
                rays_exceeding_depth_limit AS (
                    SELECT pixel_id, sample_id, ray_depth
                    FROM last_ray_per_pixel,
                         settings
                    WHERE ray_depth >= max_depth
                ),
                closest_ray_intersections AS (
                    WITH
                        sphere_intersection_calc AS (
                            SELECT rays_to_trace.*,
                                   spheres.*,
                                   ray_direction_x * ray_direction_x + ray_direction_y * ray_direction_y +
                                   ray_direction_z * ray_direction_z                  AS a,
                                   (ray_origin_x - sphere_center_x) * ray_direction_x +
                                   (ray_origin_y - sphere_center_y) * ray_direction_y +
                                   (ray_origin_z - sphere_center_z) * ray_direction_z AS half_b,
                                   ((ray_origin_x - sphere_center_x) * (ray_origin_x - sphere_center_x) +
                                    (ray_origin_y - sphere_center_y) * (ray_origin_y - sphere_center_y) +
                                    (ray_origin_z - sphere_center_z) * (ray_origin_z - sphere_center_z)) -
                                   sphere_radius * sphere_radius                      AS c
                            FROM rays_to_trace,
                                 spheres
                        ),
                        sphere_intersection_discriminants AS (
                            SELECT *,
                                   half_b * half_b - a * c AS discriminant
                            FROM sphere_intersection_calc
                        ),
                        sphere_intersection_roots AS (
                            SELECT *,
                                   (-half_b - SQRT(discriminant)) / a AS root_1,
                                   (-half_b + SQRT(discriminant)) / a AS root_2
                            FROM sphere_intersection_discriminants
                            WHERE discriminant >= 0
                        ),
                        sphere_intersection_t AS (
                            SELECT *,
                                   CASE
                                       WHEN root_1 > sphere_collision_t_min AND root_1 <= root_2 THEN root_1
                                       WHEN root_2 > sphere_collision_t_min AND root_2 < root_1 THEN root_2
                                       ELSE -1.0
                                       END AS t
                            FROM sphere_intersection_roots,
                                 settings
                        ),
                        closest_sphere_intersection AS (
                            -- Postgres only:
                            SELECT DISTINCT ON (pixel_id, sample_id, ray_depth) * -- identifies a single ray
                            FROM sphere_intersection_t
                            WHERE t >= 0.0
                            ORDER BY pixel_id, sample_id, ray_depth, t
                        )
                    SELECT *
                    FROM closest_sphere_intersection
                ),
                hit_records AS (
                    WITH
                        intersection_point AS (
                            SELECT *,
                                   ray_origin_x + ray_direction_x * t AS point_x,
                                   ray_origin_y + ray_direction_y * t AS point_y,
                                   ray_origin_z + ray_direction_z * t AS point_z
                            FROM closest_ray_intersections
                        ),
                        intersection_outward_normal AS (
                            SELECT intersection_point.*,
                                   (point_x - sphere_center_x) / sphere_radius AS outward_normal_x,
                                   (point_y - sphere_center_y) / sphere_radius AS outward_normal_y,
                                   (point_z - sphere_center_z) / sphere_radius AS outward_normal_z
                            FROM intersection_point
                        ),
                        intersection_is_front_face AS (
                            SELECT *,
                                   ray_direction_x * outward_normal_x + ray_direction_y * outward_normal_y +
                                   ray_direction_z * outward_normal_z <
                                   0.0 AS is_front_face
                            FROM intersection_outward_normal
                        ),
                        intersections AS (
                            SELECT *,
                                   CASE WHEN is_front_face THEN outward_normal_x ELSE -outward_normal_x END AS normal_x,
                                   CASE WHEN is_front_face THEN outward_normal_y ELSE -outward_normal_y END AS normal_y,
                                   CASE WHEN is_front_face THEN outward_normal_z ELSE -outward_normal_z END AS normal_z
                            FROM intersection_is_front_face
                        )
                    SELECT *
                    FROM intersections
                ),
                scattered_diffuse_rays AS (
                    WITH
                        RECURSIVE
                        incomplete_scattered_rays AS (
                            WITH
                                diffuse_hits AS (
                                    SELECT *
                                    FROM hit_records
                                    WHERE sphere_material_type = 'DIFFUSE'
                                ),
                                scatter_rays_with_color AS (
                                    SELECT *,
                                           CASE WHEN is_color_1 THEN sphere_albedo_1_r ELSE sphere_albedo_2_r END AS texture_color_r,
                                           CASE WHEN is_color_1 THEN sphere_albedo_1_g ELSE sphere_albedo_2_g END AS texture_color_g,
                                           CASE WHEN is_color_1 THEN sphere_albedo_1_b ELSE sphere_albedo_2_b END AS texture_color_b
                                    FROM (
                                        SELECT *,
                                               CASE
                                                   WHEN sphere_texture_type = 'SOLID' THEN TRUE
                                                   WHEN sphere_texture_type = 'CHECKERED' THEN
                                                       SIN(6 * point_x) * SIN(6 * point_y) * SIN(6 * point_z) > 0
                                                   END
                                                   AS is_color_1
                                        FROM diffuse_hits
                                    ) AS _
                                )
                            SELECT pixel_id,
                                   sample_id,
                                   ray_depth + 1                 AS ray_depth,
                                   point_x                       AS ray_origin_x,
                                   point_y                       AS ray_origin_y,
                                   point_z                       AS ray_origin_z,
                                   ray_color_r * texture_color_r AS ray_color_r,
                                   ray_color_g * texture_color_g AS ray_color_g,
                                   ray_color_b * texture_color_b AS ray_color_b,
                                   normal_x                      AS hit_normal_x,
                                   normal_y                      AS hit_normal_y,
                                   normal_z                      AS hit_normal_z
                            FROM scatter_rays_with_color
                        ),
                        random_unit_vectors AS (
                            (
                                SELECT *,
                                       random() * 2.0 - 1.0 AS x,
                                       random() * 2.0 - 1.0 AS y,
                                       random() * 2.0 - 1.0 AS z
                                FROM incomplete_scattered_rays
                            )
                            UNION ALL
                            (
                                SELECT pixel_id,
                                       sample_id,
                                       ray_depth,
                                       ray_origin_x,
                                       ray_origin_y,
                                       ray_origin_z,
                                       ray_color_r,
                                       ray_color_g,
                                       ray_color_b,
                                       hit_normal_x,
                                       hit_normal_y,
                                       hit_normal_z,
                                       random() * 2.0 - 1.0 AS x,
                                       random() * 2.0 - 1.0 AS y,
                                       random() * 2.0 - 1.0 AS z
                                FROM random_unit_vectors
                                WHERE x * x + y * y + z * z > 1
                                   OR x + y + z = 0
                                   OR (ABS(x) < 1e-8 AND ABS(z) < 1e-8 AND ABS(z) < 1e-8)
                            )
                        ),
                        normalized_random_unit_vectors AS (
                            -- Postgres only:
                            SELECT DISTINCT ON (pixel_id, sample_id) *, -- identifies a single ray
                                                                     x / SQRT(x * x + y * y + z * z) AS random_unit_vector_x,
                                                                     y / SQRT(x * x + y * y + z * z) AS random_unit_vector_y,
                                                                     z / SQRT(x * x + y * y + z * z) AS random_unit_vector_z
                            FROM random_unit_vectors
                            WHERE x * x + y * y + z * z <= 1
                              AND x + y + z <> 0
                              AND NOT (ABS(x) < 1e-8 AND ABS(z) < 1e-8 AND ABS(z) < 1e-8)
                        ),
                        scatter_records_with_non_normalized_scatter_ray_direction AS (
                            SELECT *,
                                   hit_normal_x + random_unit_vector_x AS direction_x,
                                   hit_normal_y + random_unit_vector_y AS direction_y,
                                   hit_normal_z + random_unit_vector_z AS direction_z
                            FROM normalized_random_unit_vectors
                        ),
                        scatter_records_with_scatter_ray_direction AS (
                            SELECT *,
                                   direction_x / SQRT(direction_x * direction_x + direction_y * direction_y +
                                                      direction_z * direction_z) AS ray_direction_x,
                                   direction_y / SQRT(direction_x * direction_x + direction_y * direction_y +
                                                      direction_z * direction_z) AS ray_direction_y,
                                   direction_z / SQRT(direction_x * direction_x + direction_y * direction_y +
                                                      direction_z * direction_z) AS ray_direction_z
                            FROM scatter_records_with_non_normalized_scatter_ray_direction
                        )
                    SELECT pixel_id,
                           sample_id,
                           ray_depth,
                           TRUE AS should_trace,
                           ray_origin_x,
                           ray_origin_y,
                           ray_origin_z,
                           ray_direction_x,
                           ray_direction_y,
                           ray_direction_z,
                           ray_color_r,
                           ray_color_g,
                           ray_color_b
                    FROM scatter_records_with_scatter_ray_direction
                ),
                scattered_metal_rays AS (
                    WITH
                        metal_hits AS (
                            SELECT *
                            FROM hit_records
                            WHERE sphere_material_type = 'METAL'
                        ),
                        reflection_vectors AS (
                            WITH
                                reflection_calc_dot AS (
                                    SELECT *,
                                           (normal_x * ray_direction_x + normal_y * ray_direction_y +
                                            normal_z * ray_direction_z) AS dot_n_i
                                    FROM metal_hits
                                )
                            SELECT *,
                                   ray_direction_x - 2.0 * normal_x * dot_n_i AS reflection_vector_x,
                                   ray_direction_y - 2.0 * normal_y * dot_n_i AS reflection_vector_y,
                                   ray_direction_z - 2.0 * normal_z * dot_n_i AS reflection_vector_z
                            FROM reflection_calc_dot
                        ),
                        with_does_scatter AS (
                            SELECT *,
                                   reflection_vector_x * normal_x + reflection_vector_y * normal_y +
                                   reflection_vector_z * normal_z > 0 AS does_scatter
                            FROM reflection_vectors
                        )
                    SELECT pixel_id,
                           sample_id,
                           ray_depth + 1       AS ray_depth,
                           does_scatter        AS should_trace,
                           point_x             AS ray_origin_x,
                           point_y             AS ray_origin_y,
                           point_z             AS ray_origin_z,
                           reflection_vector_x AS ray_direction_x,
                           reflection_vector_y AS ray_direction_y,
                           reflection_vector_z AS ray_direction_z,
                           CASE
                               WHEN does_scatter THEN ray_color_r * sphere_albedo_1_r
                               ELSE 0 END      AS ray_color_r,
                           CASE
                               WHEN does_scatter THEN ray_color_g * sphere_albedo_1_g
                               ELSE 0 END      AS ray_color_g,
                           CASE
                               WHEN does_scatter THEN ray_color_b * sphere_albedo_1_b
                               ELSE 0 END      AS ray_color_b
                    FROM with_does_scatter
                ),
                scattered_dielectric_rays AS (
                    WITH
                        dielectric_hits AS (
                            SELECT *
                            FROM hit_records
                            WHERE sphere_material_type = 'DIELECTRIC'
                        ),
                        refraction_vectors AS (
                            WITH
                                refraction_ratios AS (
                                    SELECT *,
                                           CASE
                                               WHEN is_front_face THEN 1.0 / sphere_refraction_index
                                               ELSE sphere_refraction_index END AS refraction_ratio
                                    FROM dielectric_hits
                                ),
                                refraction_calc_dot AS (
                                    SELECT *,
                                           (normal_x * ray_direction_x + normal_y * ray_direction_y +
                                            normal_z * ray_direction_z) AS dot_n_i
                                    FROM refraction_ratios
                                ),
                                refraction_calc_k AS (
                                    SELECT *,
                                           1.0 - refraction_ratio * refraction_ratio * (1.0 - dot_n_i * dot_n_i) AS refraction_k
                                    FROM refraction_calc_dot
                                ),
                                refraction_calc_refraction_vector AS (
                                    SELECT *,
                                           refraction_ratio * ray_direction_x -
                                           (refraction_ratio * dot_n_i + SQRT(refraction_k)) *
                                           normal_x AS refraction_vector_x,
                                           refraction_ratio * ray_direction_y -
                                           (refraction_ratio * dot_n_i + SQRT(refraction_k)) *
                                           normal_y AS refraction_vector_y,
                                           refraction_ratio * ray_direction_z -
                                           (refraction_ratio * dot_n_i + SQRT(refraction_k)) *
                                           normal_z AS refraction_vector_z
                                    FROM refraction_calc_k
                                )
                            SELECT *,
                                   CASE
                                       WHEN refraction_k < 0.0 THEN 0
                                       ELSE refraction_vector_x END AS ray_direction_x,
                                   CASE
                                       WHEN refraction_k < 0.0 THEN 0
                                       ELSE refraction_vector_y END AS ray_direction_y,
                                   CASE
                                       WHEN refraction_k < 0.0 THEN 0
                                       ELSE refraction_vector_z END AS ray_direction_z
                            FROM refraction_calc_refraction_vector
                        )
                    SELECT pixel_id,
                           sample_id,
                           ray_depth + 1       AS ray_depth,
                           TRUE                AS should_trace,
                           point_x             AS ray_origin_x,
                           point_y             AS ray_origin_y,
                           point_z             AS ray_origin_z,
                           refraction_vector_x AS ray_direction_x,
                           refraction_vector_y AS ray_direction_y,
                           refraction_vector_z AS ray_direction_z,
                           ray_color_r,
                           ray_color_g,
                           ray_color_b
                    FROM refraction_vectors
                ),
                new_rays AS (
                    SELECT *
                    FROM scattered_diffuse_rays
                    UNION ALL
                    SELECT *
                    FROM scattered_metal_rays
                    UNION ALL
                    SELECT *
                    FROM scattered_dielectric_rays
                    UNION ALL
                    (
                        SELECT pixel_id,
                               sample_id,
                               ray_depth + 1,
                               FALSE,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0
                        FROM (
                            SELECT pixel_id, sample_id, ray_depth
                            FROM rays_exceeding_depth_limit
                        ) AS _
                    )
                )
            SELECT *
            FROM new_rays
        ) AS _
    ),
    pixel_colors AS (
        WITH
            ray_colors AS (
                -- Postgres only:
                SELECT DISTINCT ON (pixel_id, sample_id) pixel_id,
                                                         sample_id,
                                                         ray_color_r AS r,
                                                         ray_color_g AS g,
                                                         ray_color_b AS b
                FROM rays
                ORDER BY pixel_id, sample_id, ray_depth DESC
            ),
            raw_pixel_colors AS (
                SELECT pixel_id, AVG(r) AS r, AVG(g) AS g, AVG(b) AS b
                FROM ray_colors
                GROUP BY pixel_id
            ),
            processed_pixel_colors AS (
                SELECT pixel_id,
                       FLOOR(SQRT(r) * 255) AS r,
                       FLOOR(SQRT(g) * 255) AS g,
                       FLOOR(SQRT(b) * 255) AS b
                FROM raw_pixel_colors
            )
        SELECT pixel_id, CONCAT(r, ' ', g, ' ', b) AS rgb
        FROM processed_pixel_colors
    ),
    image AS (
        WITH
            pixels_concatenated AS (
                -- SELECT STRING_AGG(rgb, E'\n' ORDER BY pixel_id) AS pixels -- Postgres
                 SELECT string_agg(rgb, ',') AS pixels -- ClickHouse
                --  SELECT GROUP_CONCAT(rgb SEPARATOR '\n') AS pixels -- MySQL
                FROM pixel_colors
            )
        -- SELECT CONCAT('P3', E'\n', width, ' ', height, E'\n', '255', E'\n', pixels, E'\n') AS image_ppm -- Postgres
        SELECT CONCAT('P3', E'\n', width, ' ', height, E'\n', '255', E'\n', pixels, E'\n') AS image_ppm -- ClickHouse, MySQL
        FROM pixels_concatenated,
             settings
    )
SELECT image_ppm
FROM image

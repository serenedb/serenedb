DROP TABLE IF EXISTS sphere_sample CASCADE;
CREATE TABLE IF NOT EXISTS sphere_sample (x DOUBLE PRECISION NOT NULL, y DOUBLE PRECISION NOT NULL, z DOUBLE PRECISION NOT NULL,
	a DOUBLE PRECISION NOT NULL, b DOUBLE PRECISION NOT NULL, c DOUBLE PRECISION NOT NULL, sampleno INTEGER NOT NULL, n_samples INTEGER NOT NULL);
INSERT INTO sphere_sample
     WITH square_sample AS (SELECT 2.0*(RANDOM() - 0.5) AS a1, 2.0*(RANDOM() - 0.5) AS b1, 2.0*(RANDOM() - 0.5) AS c1
          FROM generate_series(1, 5000)),
     ball_sample AS (SELECT a1 AS a, b1 AS b, c1 AS c, SQRT(a1*a1+b1*b1+c1*c1) AS radius FROM square_sample WHERE 1>=(a1*a1+b1*b1+c1*c1)),
     sphere_sample AS (SELECT a/radius AS x, b/radius AS y, c/radius AS z, a, b, c, ROW_NUMBER() OVER () AS sampleno, COUNT(*) OVER () AS n_samples FROM ball_sample)
     SELECT x,y,z,a,b,c,sampleno,n_samples FROM sphere_sample;
DROP INDEX IF EXISTS idx_ss;
CREATE INDEX IF NOT EXISTS idx_ss ON sphere_sample(sampleno);

DROP VIEW IF EXISTS rays CASCADE;
CREATE VIEW rays AS
    WITH RECURSIVE
     xs AS (SELECT 0 AS u, 0.0 AS img_frac_x UNION ALL SELECT u+1, (u+1.0)/img.res_x FROM xs, img WHERE xs.u<img.res_x-1),
     ys AS (SELECT 0 AS v, 0.0 AS img_frac_y UNION ALL SELECT v+1, (v+1.0)/img.res_y FROM ys, img WHERE ys.v<img.res_y-1),
     px_sample_n(px_sample_n, px_jitter_u, px_jitter_v) AS (SELECT 1, (RANDOM()-0.5) * (fov_rad_x/res_x), (RANDOM()-0.5) * (fov_rad_y/res_y)
                FROM camera, img
        UNION ALL SELECT px_sample_n+1, (RANDOM()-0.5) * (fov_rad_x/res_x), (RANDOM()-0.5) * (fov_rad_y/res_y)
                FROM px_sample_n, camera, img WHERE px_sample_n<camera.samples_per_px),
     rs(img_x, img_y, sceneid, depth, max_ray_depth, samples_per_px, px_sample_n, color_mult,
          ray_col_r, ray_col_g, ray_col_b,
          x1, y1, z1,
          dir_x, dir_y, dir_z,
          dir_lensquared,
          n_x, n_y, n_z, n_len,
          stop_tracing, ray_len_idx, hit_sphereid, n_sphere_samples, inside_dielectric) AS
        -- Send out initial set of rays from camera
         (SELECT xs.u, ys.v, c.sceneid, -1, max_ray_depth, samples_per_px, px_sample_n, CAST(2.0 AS DOUBLE PRECISION),
                CAST(NULL AS DOUBLE PRECISION), CAST(NULL AS DOUBLE PRECISION), CAST(NULL AS DOUBLE PRECISION),
                 c.x, c.y, c.z,
                 (SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u) /
                    SQRT(((SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u)*(SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u) +
                      (SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v)*(SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v) + 1.0)),
                 (SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v) /
                    SQRT(((SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u)*(SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u) +
                      (SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v)*(SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v) + 1.0)),
                 CAST(1.0 AS DOUBLE PRECISION) /
                    SQRT(((SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u)*(SIN(c.rot_y-(fov_rad_x/2.0)+img_frac_x*fov_rad_x) + px_jitter_u) +
                      (SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v)*(SIN(c.rot_x-(fov_rad_y/2.0)+img_frac_y*fov_rad_y) + px_jitter_v) + 1.0)),
                 CAST(1.0 AS DOUBLE PRECISION),
                 CAST(NULL AS DOUBLE PRECISION), CAST(NULL AS DOUBLE PRECISION), CAST(NULL AS DOUBLE PRECISION), CAST(NULL AS DOUBLE PRECISION),
                 CAST(0 AS BOOLEAN), CAST(1 AS BIGINT), CAST(NULL AS INTEGER),
                 (SELECT COUNT(*) FROM sphere_sample), FALSE
              FROM camera c, img, xs, ys, px_sample_n
        UNION ALL
         -- Collide all rays with spheres
          SELECT img_x, img_y, rs.sceneid, depth+1, max_ray_depth, samples_per_px, px_sample_n,
                 (CASE WHEN norm_x IS NULL THEN 0.5 ELSE mirror_frac END)*color_mult,
                 CASE WHEN discrim>0 THEN (CASE
                                                WHEN shade_normal THEN mat_col_r*(1+norm_x)/2
                                                ELSE mat_col_r
                                           END)
                     ELSE 1.0-(0.5*((dir_y/SQRT(dir_lensquared)+1.0)))+0.2*(0.5*((dir_y/SQRT(dir_lensquared)+1.0))) END,
                 CASE WHEN discrim>0 THEN (CASE
                                                WHEN shade_normal THEN mat_col_g*(1+norm_y)/2
                                                ELSE mat_col_g
                                           END)
                     ELSE 1.0-(0.5*((dir_y/SQRT(dir_lensquared)+1.0)))+0.3*(0.5*((dir_y/SQRT(dir_lensquared)+1.0))) END,
                 CASE WHEN discrim>0 THEN (CASE
                                                WHEN shade_normal THEN mat_col_b*(1+norm_z)/2
                                                ELSE mat_col_b
                                           END)
                     ELSE 1.0-(0.5*((dir_y/SQRT(dir_lensquared)+1.0)))+1.0*(0.5*((dir_y/SQRT(dir_lensquared)+1.0))) END,
                 -- x1, y1, z1
                 hit_x, hit_y, hit_z,
                 -- dir_x, dir_y, dir_z
CASE WHEN is_metal 
     THEN (dir_x - 2 * norm_x * dot_ray_norm) / reflection_len
     WHEN is_dielectric AND must_reflect
     THEN reflec_dir_x / final_dir_len
     WHEN is_dielectric 
     THEN refrac_dir_x / final_dir_len
     ELSE diffuse_dir_x/diffuse_dir_len
END,
CASE WHEN is_metal 
     THEN (dir_y - 2 * norm_y * dot_ray_norm) / reflection_len
     WHEN is_dielectric AND must_reflect
     THEN reflec_dir_y / final_dir_len
     WHEN is_dielectric 
     THEN refrac_dir_y / final_dir_len
     ELSE diffuse_dir_y/diffuse_dir_len
END,
CASE WHEN is_metal 
     THEN (dir_z - 2 * norm_z * dot_ray_norm) / reflection_len
     WHEN is_dielectric AND must_reflect
     THEN reflec_dir_z / final_dir_len
     WHEN is_dielectric 
     THEN refrac_dir_z / final_dir_len
     ELSE diffuse_dir_z/diffuse_dir_len
END,
                 1.0,
                 norm_x, norm_y, norm_z, 1.0,
                 discrim IS NULL, ROW_NUMBER() OVER (PARTITION BY img_x, img_y, depth+1, px_sample_n
                                                          ORDER BY t),
                 sphereid, n_sphere_samples, (inside_dielectric AND NOT must_reflect) OR (NOT inside_dielectric AND NOT is_dielectric)
           FROM rs
           LEFT JOIN LATERAL
               (SELECT s.*, discrim,
                       CASE WHEN t_near > 0.001 THEN t_near ELSE t_far END AS t
                         FROM sphere s,
                              LATERAL (SELECT (x1-s.cx)*dir_x+(y1-s.cy)*dir_y+(z1-s.cz)*dir_z AS hb) hbv,
                              LATERAL (SELECT hb*hb - ((x1-s.cx)*(x1-s.cx)+(y1-s.cy)*(y1-s.cy)+(z1-s.cz)*(z1-s.cz)-s.radius2)*dir_lensquared AS discrim) dv,
                              LATERAL (SELECT (-hb - SQRT(discrim))/dir_lensquared AS t_near,
                                              (-hb + SQRT(discrim))/dir_lensquared AS t_far) tv
                         WHERE s.sceneid=rs.sceneid AND discrim > 0
                       ) hit_sphere ON t>0.001
           LEFT JOIN LATERAL
               (SELECT x1+dir_x*t AS hit_x, y1+dir_y*t AS hit_y, z1+dir_z*t AS hit_z,
                       x1+dir_x*t-cx AS norm_x_nonunit, y1+dir_y*t-cy AS norm_y_nonunit, z1+dir_z*t-cz AS norm_z_nonunit,
                       SQRT((x1+dir_x*t-cx)*(x1+dir_x*t-cx)+(y1+dir_y*t-cy)*(y1+dir_y*t-cy)+(z1+dir_z*t-cz)*(z1+dir_z*t-cz)) AS norm_len,
                       ROW_NUMBER() OVER (PARTITION BY img_x, img_y, depth, px_sample_n ORDER BY t ASC) AS t_idx
                       WHERE t>0
               ) sphere_normal ON t_idx=1
           LEFT JOIN LATERAL
               (SELECT CASE WHEN (norm_x_nonunit * dir_x + norm_y_nonunit * dir_y + norm_z_nonunit * dir_z) > 0
                         THEN -norm_x_nonunit/norm_len 
                         ELSE norm_x_nonunit/norm_len END AS norm_x,
                    CASE WHEN (norm_x_nonunit * dir_x + norm_y_nonunit * dir_y + norm_z_nonunit * dir_z) > 0
                         THEN -norm_y_nonunit/norm_len 
                         ELSE norm_y_nonunit/norm_len END AS norm_y,
                    CASE WHEN (norm_x_nonunit * dir_x + norm_y_nonunit * dir_y + norm_z_nonunit * dir_z) > 0
                         THEN -norm_z_nonunit/norm_len 
                         ELSE norm_z_nonunit/norm_len END AS norm_z
               ) sphere_unit_normal ON norm_x IS NOT NULL
           LEFT JOIN LATERAL
               (SELECT dir_x*norm_x + dir_y*norm_y + dir_z*norm_z AS dot_ray_norm,
                       SQRT((dir_x - 2 * norm_x * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z)) * (dir_x - 2 * norm_x * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z)) +
                       (dir_y - 2 * norm_y * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z)) * (dir_y - 2 * norm_y * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z)) +
                       (dir_z - 2 * norm_z * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z)) * (dir_z - 2 * norm_z * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z))) AS reflection_len
               ) dot_ray_norm ON norm_x IS NOT NULL
           LEFT JOIN material ON material.materialid=hit_sphere.materialid
           LEFT JOIN LATERAL
               (SELECT x, y, z,
                       x+norm_x AS diffuse_dir_x, y+norm_y AS diffuse_dir_y, z+norm_z AS diffuse_dir_z,
                       SQRT((x+norm_x)*(x+norm_x)+(y+norm_y)*(y+norm_y)+(z+norm_z)*(z+norm_z)) AS diffuse_dir_len
                FROM sphere_sample ss WHERE ss.sampleno=1+CAST(FLOOR(ABS((100000*dir_x)-FLOOR(100000*dir_x))*n_sphere_samples) AS INTEGER)
               ) diffuse_scatter ON norm_x IS NOT NULL
           LEFT JOIN LATERAL
               (SELECT (CASE WHEN is_dielectric AND (norm_x_nonunit * dir_x + norm_y_nonunit * dir_y + norm_z_nonunit * dir_z) > 0
                             THEN eta
                             WHEN is_dielectric
                             THEN 1.0/eta
                             ELSE eta END) AS ir) index_of_refraction ON norm_x IS NOT NULL
LEFT JOIN LATERAL
    (SELECT ABS(dir_x*norm_x + dir_y*norm_y + dir_z*norm_z) AS cos_theta,
            ((1.0-ir)/(1.0+ir))*((1.0-ir)/(1.0+ir)) AS r0
    ) refract_cos_theta ON norm_x IS NOT NULL
LEFT JOIN LATERAL
    (SELECT 
        -- Discriminant for total internal reflection check
        1.0 - ir*ir*(1.0-cos_theta*cos_theta) AS refrac_discriminant,
        -- Correct refraction direction
        ir * dir_x + (ir * cos_theta - SQRT(GREATEST(0.0, 1.0 - ir*ir*(1.0-cos_theta*cos_theta)))) * norm_x AS refrac_dir_x,
        ir * dir_y + (ir * cos_theta - SQRT(GREATEST(0.0, 1.0 - ir*ir*(1.0-cos_theta*cos_theta)))) * norm_y AS refrac_dir_y,
        ir * dir_z + (ir * cos_theta - SQRT(GREATEST(0.0, 1.0 - ir*ir*(1.0-cos_theta*cos_theta)))) * norm_z AS refrac_dir_z,
        -- Fresnel reflectance
        r0 + (1.0 - r0)*pow(1.0-cos_theta, 5) AS reflectance
    ) refrac_vec ON norm_x IS NOT NULL
LEFT JOIN LATERAL
    (SELECT 
        dir_x - 2.0 * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z) * norm_x AS reflec_dir_x,
        dir_y - 2.0 * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z) * norm_y AS reflec_dir_y,
        dir_z - 2.0 * (dir_x*norm_x + dir_y*norm_y + dir_z*norm_z) * norm_z AS reflec_dir_z,
        -- Total internal reflection check
        (refrac_discriminant < 0) OR (reflectance > RANDOM()) AS must_reflect
    ) reflec_vec ON norm_x IS NOT NULL
    LEFT JOIN LATERAL
    (SELECT 
        CASE WHEN must_reflect 
             THEN SQRT(reflec_dir_x*reflec_dir_x + reflec_dir_y*reflec_dir_y + reflec_dir_z*reflec_dir_z)
             ELSE SQRT(refrac_dir_x*refrac_dir_x + refrac_dir_y*refrac_dir_y + refrac_dir_z*refrac_dir_z)
        END AS final_dir_len
    ) final_dir_len ON norm_x IS NOT NULL

          LEFT JOIN LATERAL
               (SELECT SQRT((reflec_dir_x+refrac_dir_x)*(reflec_dir_x+refrac_dir_x)+
                       (reflec_dir_y+refrac_dir_y)*(reflec_dir_y+refrac_dir_y)+
                       (reflec_dir_z+refrac_dir_z)*(reflec_dir_z+refrac_dir_z)) refrac_len
               ) refrac_len ON norm_x IS NOT NULL
              WHERE depth<max_ray_depth AND NOT stop_tracing AND ray_len_idx=1
             )
   SELECT * FROM rs WHERE ray_len_idx=1;
-- select * from rays;

DROP VIEW IF EXISTS do_render;
CREATE VIEW do_render AS
 SELECT A.img_x, -A.img_y,
         GREATEST(0.0, LEAST(1.0, SUM(POW(A.color_mult * COALESCE(A.ray_col_r, 0.0)/A.samples_per_px, gamma)))) col_r,
         GREATEST(0.0, LEAST(1.0, SUM(POW(A.color_mult * COALESCE(A.ray_col_g, 0.0)/A.samples_per_px, gamma)))) col_g,
         GREATEST(0.0, LEAST(1.0, SUM(POW(A.color_mult * COALESCE(A.ray_col_b, 0.0)/A.samples_per_px, gamma)))) col_b
    FROM rays A, img
     WHERE A.depth>=0
    GROUP BY -A.img_y, A.img_x
    ORDER BY -A.img_y, A.img_x;

DROP VIEW IF EXISTS ppm;
CREATE VIEW ppm AS
 WITH maxcol(mc) AS (SELECT 255)
    SELECT 'P3'
  UNION ALL
    SELECT res_x || ' ' || res_y || ' ' || mc FROM img, maxcol
  UNION ALL
    SELECT CAST(col_r*mc AS INTEGER) || ' ' || CAST(col_g*mc AS INTEGER) || ' ' || CAST(col_b*mc AS INTEGER)
      FROM do_render, maxcol;
  ;

-- SELECT * FROM rays WHERE img_x=2 AND img_y=2;

SELECT * FROM ppm;

-- DuckDB Raymarching Sphere + Box with Metaball Blending
-- Returns RGB color values for each pixel
-- @run: resolution=80x60, zoom=4

WITH
uniforms AS (
  SELECT '{"iResolution.x": 160, "iResolution.y": 120, "iTime": 1.0, "iMouse.x": 80, "iMouse.y": 60}'::JSON AS iUniforms
),
parsed AS (
  SELECT
    CAST(json_extract(iUniforms, 'iResolution.x') AS BIGINT) AS width,
    CAST(json_extract(iUniforms, 'iResolution.y') AS BIGINT) AS height,
    CAST(json_extract(iUniforms, 'iTime') AS DOUBLE) AS iTime,
    CAST(json_extract(iUniforms, 'iMouse.x') AS DOUBLE) AS mx,
    CAST(json_extract(iUniforms, 'iMouse.y') AS DOUBLE) AS my
  FROM uniforms
),

-- Generate all pixels and calculate everything in one go
pixels AS (
    SELECT 
        x, y,
        u.width, u.height, u.iTime,
        
        -- Convert to normalized coordinates (-1 to 1)
        (x::DOUBLE / u.width) * 2.0 - 1.0 as screen_x,
        (y::DOUBLE / u.height) * 2.0 - 1.0 as screen_y,
        
        -- Simple camera setup
        0.0 as cam_x,
        0.0 as cam_y, 
        -3.0 as cam_z,
        
        -- ANIMATED sphere
        sin(u.iTime * 1.2) * 0.8 as sphere_x,              
        cos(u.iTime * 0.8) * 0.4 as sphere_y,        
        sin(u.iTime * 0.5) * 0.3 as sphere_z,        
        0.6 + sin(u.iTime * 2.0) * 0.2 as sphere_radius,
        
        -- ANIMATED spinning box (counter-orbiting with rotation)
        -sin(u.iTime * 0.9) * 0.9 as box_center_x,          
        sin(u.iTime * 1.1) * 0.5 as box_center_y,           
        cos(u.iTime * 0.7) * 0.4 as box_center_z,           
        0.4 + sin(u.iTime * 1.8) * 0.15 as box_size,
        
        -- Box rotation angles
        u.iTime * 1.3 as box_rot_x,  -- Spin around X axis
        u.iTime * 0.8 as box_rot_y,  -- Spin around Y axis  
        u.iTime * 1.1 as box_rot_z,  -- Spin around Z axis
        
        -- Metaball blending strength
        0.3 as blend_strength
        
    FROM parsed u
    CROSS JOIN generate_series(0, u.width - 1) as t(x)
    CROSS JOIN generate_series(0, u.height - 1) as s(y)
),

-- Raymarching with metaball SDF blending
hits AS (
    SELECT 
        x, y, iTime,
        
        -- Find the hit point using metaball blending
        COALESCE(
            (SELECT step * 0.025
             FROM (
                SELECT 
                    step,
                    -- Current ray position
                    cam_x + screen_x + screen_x * (step * 0.025) as ray_x,
                    cam_y + screen_y + screen_y * (step * 0.025) as ray_y,
                    cam_z + 1.0 * (step * 0.025) as ray_z,
                    
                    -- Distance to sphere (SDF)
                    sqrt(
                        power(cam_x + screen_x + screen_x * (step * 0.025) - sphere_x, 2) +
                        power(cam_y + screen_y + screen_y * (step * 0.025) - sphere_y, 2) + 
                        power(cam_z + 1.0 * (step * 0.025) - sphere_z, 2)
                    ) - sphere_radius as sphere_dist,
                    
                    -- Distance to spinning box (SDF with rotation)
                    -- First translate to box center
                    cam_x + screen_x + screen_x * (step * 0.025) - box_center_x as box_local_x,
                    cam_y + screen_y + screen_y * (step * 0.025) - box_center_y as box_local_y,
                    cam_z + 1.0 * (step * 0.025) - box_center_z as box_local_z,
                    
                    -- Apply rotation (simplified - just Y rotation for performance)
                    (cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * cos(box_rot_y) - 
                    (cam_z + 1.0 * (step * 0.025) - box_center_z) * sin(box_rot_y) as rotated_box_x,
                    
                    cam_y + screen_y + screen_y * (step * 0.025) - box_center_y as rotated_box_y,
                    
                    (cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * sin(box_rot_y) + 
                    (cam_z + 1.0 * (step * 0.025) - box_center_z) * cos(box_rot_y) as rotated_box_z,
                    
                    -- Box SDF on rotated coordinates
                    GREATEST(
                        abs((cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * cos(box_rot_y) - 
                            (cam_z + 1.0 * (step * 0.025) - box_center_z) * sin(box_rot_y)) - box_size,
                        abs(cam_y + screen_y + screen_y * (step * 0.025) - box_center_y) - box_size,
                        abs((cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * sin(box_rot_y) + 
                            (cam_z + 1.0 * (step * 0.025) - box_center_z) * cos(box_rot_y)) - box_size
                    ) as box_dist,
                    
                    -- METABALL BLENDING: Smooth minimum function
                    -- smin(a,b,k) = -log(exp(-k*a) + exp(-k*b))/k
                    -- Approximation: min(a,b) - smoothstep blend
                    LEAST(
                        sqrt(
                            power(cam_x + screen_x + screen_x * (step * 0.025) - sphere_x, 2) +
                            power(cam_y + screen_y + screen_y * (step * 0.025) - sphere_y, 2) + 
                            power(cam_z + 1.0 * (step * 0.025) - sphere_z, 2)
                        ) - sphere_radius,
                        GREATEST(
                            abs((cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * cos(box_rot_y) - 
                                (cam_z + 1.0 * (step * 0.025) - box_center_z) * sin(box_rot_y)) - box_size,
                            abs(cam_y + screen_y + screen_y * (step * 0.025) - box_center_y) - box_size,
                            abs((cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * sin(box_rot_y) + 
                                (cam_z + 1.0 * (step * 0.025) - box_center_z) * cos(box_rot_y)) - box_size
                        )
                    ) - 
                    -- Smooth blending term
                    blend_strength * exp(-
                        power(
                            sqrt(
                                power(cam_x + screen_x + screen_x * (step * 0.025) - sphere_x, 2) +
                                power(cam_y + screen_y + screen_y * (step * 0.025) - sphere_y, 2) + 
                                power(cam_z + 1.0 * (step * 0.025) - sphere_z, 2)
                            ) - sphere_radius -
                            GREATEST(
                                abs((cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * cos(box_rot_y) - 
                                    (cam_z + 1.0 * (step * 0.025) - box_center_z) * sin(box_rot_y)) - box_size,
                                abs(cam_y + screen_y + screen_y * (step * 0.025) - box_center_y) - box_size,
                                abs((cam_x + screen_x + screen_x * (step * 0.025) - box_center_x) * sin(box_rot_y) + 
                                    (cam_z + 1.0 * (step * 0.025) - box_center_z) * cos(box_rot_y)) - box_size
                            ), 2
                        ) / 0.5
                    ) as metaball_dist
                    
                FROM generate_series(0, 100) as t(step)
             ) distances
             WHERE metaball_dist <= 0.05
             ORDER BY step
             LIMIT 1),
            -1.0
        ) as hit_distance,
        
        cam_x + screen_x as ray_start_x,
        cam_y + screen_y as ray_start_y,
        cam_z as ray_start_z,
        
        sphere_x, sphere_y, sphere_z, sphere_radius,
        box_center_x, box_center_y, box_center_z, box_size, box_rot_x, box_rot_y, box_rot_z,
        screen_x, screen_y
        
    FROM pixels
),

-- Calculate lighting for the blended surface
lighting AS (
    SELECT 
        x, y, iTime,
        CASE WHEN hit_distance > 0 THEN 1 ELSE 0 END as hit,
        hit_distance,
        
        -- Hit position in world space
        ray_start_x + screen_x * hit_distance as hit_x,
        ray_start_y + screen_y * hit_distance as hit_y,
        ray_start_z + 1.0 * hit_distance as hit_z,
        
        -- Approximate surface normal (gradient of the metaball field)
        CASE WHEN hit_distance > 0 THEN
            -- Blend between sphere and box normals based on proximity
            CASE 
                WHEN sqrt(power(ray_start_x + screen_x * hit_distance - sphere_x, 2) + 
                         power(ray_start_y + screen_y * hit_distance - sphere_y, 2) + 
                         power(ray_start_z + 1.0 * hit_distance - sphere_z, 2)) - sphere_radius < 
                     GREATEST(abs((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)) - box_size,
                             abs(ray_start_y + screen_y * hit_distance - box_center_y) - box_size,
                             abs((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)) - box_size)
                THEN
                    -- Closer to sphere - use sphere normal
                    (ray_start_x + screen_x * hit_distance - sphere_x) / sphere_radius * 0.7
                ELSE
                    -- Closer to spinning box - use rotated box normal  
                    CASE 
                        WHEN abs((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)) >= 
                             GREATEST(abs(ray_start_y + screen_y * hit_distance - box_center_y), 
                                     abs((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                         (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)))
                        THEN SIGN((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                  (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)) * 0.5
                        ELSE 0.0
                    END
            END
        ELSE 0.0 END as normal_x,
        
        CASE WHEN hit_distance > 0 THEN
            CASE 
                WHEN sqrt(power(ray_start_x + screen_x * hit_distance - sphere_x, 2) + 
                         power(ray_start_y + screen_y * hit_distance - sphere_y, 2) + 
                         power(ray_start_z + 1.0 * hit_distance - sphere_z, 2)) - sphere_radius < 
                     GREATEST(abs((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)) - box_size,
                             abs(ray_start_y + screen_y * hit_distance - box_center_y) - box_size,
                             abs((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)) - box_size)
                THEN
                    (ray_start_y + screen_y * hit_distance - sphere_y) / sphere_radius * 0.7
                ELSE
                    CASE 
                        WHEN abs(ray_start_y + screen_y * hit_distance - box_center_y) >= 
                             GREATEST(abs((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                         (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)), 
                                     abs((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                         (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)))
                        THEN SIGN(ray_start_y + screen_y * hit_distance - box_center_y) * 0.5
                        ELSE 0.0
                    END
            END
        ELSE 0.0 END as normal_y,
        
        CASE WHEN hit_distance > 0 THEN
            CASE 
                WHEN sqrt(power(ray_start_x + screen_x * hit_distance - sphere_x, 2) + 
                         power(ray_start_y + screen_y * hit_distance - sphere_y, 2) + 
                         power(ray_start_z + 1.0 * hit_distance - sphere_z, 2)) - sphere_radius < 
                     GREATEST(abs((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)) - box_size,
                             abs(ray_start_y + screen_y * hit_distance - box_center_y) - box_size,
                             abs((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)) - box_size)
                THEN
                    (ray_start_z + 1.0 * hit_distance - sphere_z) / sphere_radius * 0.7
                ELSE
                    CASE 
                        WHEN abs((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                 (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)) >= 
                             GREATEST(abs((ray_start_x + screen_x * hit_distance - box_center_x) * cos(box_rot_y) - 
                                         (ray_start_z + 1.0 * hit_distance - box_center_z) * sin(box_rot_y)), 
                                     abs(ray_start_y + screen_y * hit_distance - box_center_y))
                        THEN SIGN((ray_start_x + screen_x * hit_distance - box_center_x) * sin(box_rot_y) + 
                                  (ray_start_z + 1.0 * hit_distance - box_center_z) * cos(box_rot_y)) * 0.5
                        ELSE 0.0
                    END
            END
        ELSE 0.0 END as normal_z,
        
        -- Animated light direction 
        -0.6 + sin(iTime * 0.8) * 0.3 as light_x,
        0.8 + cos(iTime * 0.6) * 0.2 as light_y, 
        -0.5 + sin(iTime * 1.1) * 0.2 as light_z
        
    FROM hits
)

-- Generate beautiful RGB colors
SELECT 
    CASE 
        WHEN hit = 1 THEN
            -- Metaball surface with beautiful colors
            GREATEST(0.0, LEAST(1.0, 
                0.15 + GREATEST(0.0, normal_x * light_x + normal_y * light_y + normal_z * light_z) * 
                (0.6 + sin(lighting.iTime * 1.2 + hit_x * 3.0 + hit_y * 2.0) * 0.3)
            ))
        ELSE 
            -- Animated background
            0.05 + (x::DOUBLE / width) * 0.1 + sin(lighting.iTime * 0.4) * 0.03
    END as r,
    
    CASE 
        WHEN hit = 1 THEN
            GREATEST(0.0, LEAST(1.0,
                0.1 + GREATEST(0.0, normal_x * light_x + normal_y * light_y + normal_z * light_z) * 
                (0.5 + cos(lighting.iTime * 1.5 + hit_x * 2.5 + hit_z * 3.0) * 0.3)
            ))
        ELSE 
            0.08 + (y::DOUBLE / height) * 0.15 + cos(lighting.iTime * 0.3) * 0.04
    END as g,
    
    CASE 
        WHEN hit = 1 THEN
            GREATEST(0.0, LEAST(1.0,
                0.2 + GREATEST(0.0, normal_x * light_x + normal_y * light_y + normal_z * light_z) * 
                (0.4 + sin(lighting.iTime * 1.8 + hit_y * 4.0 + hit_z * 2.0) * 0.4)
            ))
        ELSE 
            0.1 + ((x + y)::DOUBLE / (width + height)) * 0.2 + sin(lighting.iTime * 0.6) * 0.02
    END as b

FROM lighting
CROSS JOIN parsed
ORDER BY y, x;
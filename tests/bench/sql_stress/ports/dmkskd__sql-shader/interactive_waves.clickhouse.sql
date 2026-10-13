CREATE OR REPLACE MACRO ch_numbers() AS TABLE SELECT range AS number FROM range(9223372036854775807);
CREATE OR REPLACE MACRO width() AS CAST(160 AS UINTEGER);
CREATE OR REPLACE MACRO height() AS CAST(120 AS UINTEGER);
CREATE OR REPLACE MACRO iTime() AS CAST(1.0 AS DOUBLE);
CREATE OR REPLACE MACRO mx() AS CAST(80 AS DOUBLE);
CREATE OR REPLACE MACRO my() AS CAST(60 AS DOUBLE);
-- "Interactive Waves" - A pattern that reacts to mouse movement and time.
-- INTERACTIVE WAVES - SPICY EDITION" 
-- Multiple rotations, color explosions, and mind-bending effects!
-- @run: resolution=320x240, zoom=2

SELECT
    -- Normalize coordinates
    (x - (width() / 2)) / width() AS norm_x,
    (y - (height() / 2)) / height() AS norm_y,
    
    -- Multiple rotation layers with different speeds
    iTime() * 0.8 AS fast_rotation,
    iTime() * -0.3 AS slow_counter_rotation,
    iTime() * 1.2 AS hyper_rotation,
    
    -- Dynamic multi-layer zoom with phase shifts
    (1.5 + sin(iTime() * 0.4) * 0.8 + cos(iTime() * 0.7) * 0.3) AS primary_zoom,
    (1.0 + cos(iTime() * 0.9 + 1.57) * 0.6) AS secondary_zoom,
    
    -- First rotation layer
    norm_x * cos(fast_rotation) - norm_y * sin(fast_rotation) AS rot1_x,
    norm_x * sin(fast_rotation) + norm_y * cos(fast_rotation) AS rot1_y,
    
    -- Apply primary zoom and second rotation
    (rot1_x * primary_zoom) * cos(slow_counter_rotation) - (rot1_y * primary_zoom) * sin(slow_counter_rotation) AS rot2_x,
    (rot1_x * primary_zoom) * sin(slow_counter_rotation) + (rot1_y * primary_zoom) * cos(slow_counter_rotation) AS rot2_y,
    
    -- Final coordinates with secondary zoom
    rot2_x * secondary_zoom AS final_x,
    rot2_y * secondary_zoom AS final_y,
    
    -- Distance for radial effects
    sqrt(final_x * final_x + final_y * final_y) AS radial_dist,
    atan2(final_y, final_x) AS polar_angle,
    
    -- Multiple wave systems with different frequencies and phases
    sin((final_y * 25.0) + iTime() * 3.0 + cos(radial_dist * 10.0)) AS wave1,
    cos((final_x * 35.0) + iTime() * 2.2 + sin(polar_angle * 4.0)) AS wave2,
    sin(radial_dist * 15.0 - iTime() * 4.0 + polar_angle * 3.0) AS radial_wave,
    cos(polar_angle * 8.0 + iTime() * 1.8) AS spiral_wave,
    sin((final_x + final_y) * 20.0 + iTime() * 2.5) AS diagonal_wave,
    
    -- Mouse interaction with rotation and pulsing
    sqrt(pow(norm_x - ((mx() / width()) - 0.5), 2) + pow(norm_y - ((my() / height()) - 0.5), 2)) AS mouse_dist,
    sin(mouse_dist * 30.0 - iTime() * 5.0) * (1.0 / (mouse_dist + 0.1)) AS mouse_pulse,
    
    -- Advanced color mixing with HSV-like effects
    -- RED: Complex wave interactions with mouse pulse
    abs((wave1 * wave2 + radial_wave * spiral_wave) * 0.3 + mouse_pulse * 0.4 + sin(iTime() * 0.6) * 0.2) AS r_intensity,
    0.3 + r_intensity * 0.7 AS r,
    
    -- GREEN: Diagonal and radial patterns with time shift
    abs((diagonal_wave + radial_wave * cos(iTime() * 0.4)) * 0.4 + wave2 * 0.3 + cos(iTime() * 0.8 + 2.09) * 0.2) AS g_intensity,
    0.2 + g_intensity * 0.8 AS g,
    
    -- BLUE: Spiral and mouse interaction with hyper rotation influence
    abs((spiral_wave * wave1 + mouse_pulse * cos(hyper_rotation)) * 0.35 + sin(radial_dist * 8.0 + iTime() * 3.5) * 0.4) AS b_intensity,
    0.1 + b_intensity * 0.9 AS b,
    
    -- Bonus: Add some chromatic aberration effect (slight coordinate offsets)
    final_x + sin(iTime() * 2.0) * 0.01 AS aberration_x,
    final_y + cos(iTime() * 2.3) * 0.01 AS aberration_y

FROM
(
    SELECT number AS y
    FROM ch_numbers() LIMIT height()
) AS t_y
CROSS JOIN (SELECT number AS x FROM ch_numbers() LIMIT width()) AS t_x
ORDER BY
    y ASC,
    x ASC
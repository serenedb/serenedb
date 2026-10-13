CREATE OR REPLACE MACRO toUInt8(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS UTINYINT);
CREATE OR REPLACE MACRO toUInt32(x) AS CAST(trunc(CAST(x AS DOUBLE)) AS UINTEGER);
CREATE OR REPLACE MACRO arrayProduct(a) AS list_product(a);
CREATE OR REPLACE MACRO arrayFlatten(a) AS flatten(a);
CREATE OR REPLACE MACRO arrayEnumerate(a) AS range(1, len(a) + 1);
CREATE OR REPLACE MACRO arraySum(a) AS list_sum(a);
CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO splitByString(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO replaceAll(s, a, b) AS replace(s, a, b);
CREATE OR REPLACE MACRO replaceRegexpAll(s, re, r) AS regexp_replace(s, re, r, 'g');
CREATE OR REPLACE MACRO arraySlice(a, o) AS a[o:], (a, o, l) AS a[o:o + l - 1];
CREATE OR REPLACE MACRO ngrams(s, n) AS string_split(s, NULL);
CREATE OR REPLACE MACRO countIf(c) AS count_if(c);
CREATE OR REPLACE MACRO input_raw() AS '0:
###
##.
##.

1:
###
##.
.##

2:
.##
###
##.

3:
##.
###
##.

4:
###
#..
###

5:
###
.#.
###

4x4: 0 0 0 0 2 0
12x5: 1 0 1 0 2 2
12x5: 1 0 1 0 3 2';
/*
NOTE: Similar to past years, Advent of Code Day 12 has a deceptively complex puzzle description and sample input.
The actual puzzle inputs provided are more straightforward - all we need to do is determine whether or not:
(squares used by shape) * (requested volume for shape), summed across all shapes, fits within the area of the region.
This can be solved with basic array and string manipulation without complex geometry or packing algorithms.
*/

-- Define puzzle input 
WITH 
    -- Split input into sections
input_sections AS (
    SELECT list_transform(splitByString(E'\n\n', input_raw()), section -> splitByChar(E'\n', section)) AS sections
),
    -- Extract regions section (last section)
regions_section AS (
    SELECT sections[-1] AS region_lines
    FROM input_sections
),
    -- Extract shape sections (all except last)
shapes_sections AS (
    SELECT unnest(
        arraySlice(sections, 1, length(sections) - 1)
    ) AS shape_lines
    FROM input_sections
),
    -- Parse shape data
parsed_shapes AS (
    SELECT
        shape_lines,
        
        -- Transform shape lines: first line is name, rest is pattern
        list_transform(arrayEnumerate(shape_lines), line_index -> if(
                line_index = 1,
                -- First line: remove ':' from name
                replaceAll(shape_lines[line_index], ':', ''),
                -- Other lines: convert '#' to 1, '.' to 0
                replaceRegexpAll(
                    replaceRegexpAll(shape_lines[line_index], '#', '1'),
                    '\.',
                    '0'
                )
            )) AS transformed_lines
    FROM shapes_sections
),
    -- Convert shape patterns to binary arrays
shape_patterns AS (
    SELECT
        transformed_lines,
        list_transform(arraySlice(transformed_lines, 2), line -> list_transform(ngrams(line, 1), "char" -> toUInt8("char"))) AS shape_grid
    FROM parsed_shapes
),
    -- Calculate area needed for each shape
shape_areas AS (
    SELECT list(
        len(list_filter(
            arrayFlatten(shape_grid),
            cell -> cell = 1
        ))
    ) AS areas_per_shape
    FROM shape_patterns
),
    -- Parse region specifications
parsed_regions AS (
    SELECT
        unnest(
            list_transform(region_lines, line -> splitByString(': ', line))
        ) AS region_parts
    FROM regions_section
),
    -- Calculate region dimensions and requested volumes
region_specifications AS (
    SELECT
        region_parts,
        
        -- Calculate total region area (product of dimensions)
        arrayProduct(
            list_transform(splitByChar('x', region_parts[1]), dim -> toUInt32(dim))
        ) AS total_region_area,
        
        -- Extract requested volumes for each shape
        list_transform(splitByChar(' ', region_parts[2]), vol -> toUInt8(vol)) AS requested_shape_volumes
    FROM parsed_regions
),
    -- Check if each region can fit the requested shapes
region_fit_analysis AS (
    SELECT
        total_region_area,
        requested_shape_volumes,
        areas_per_shape,
        
        -- Calculate total area needed: sum of (volume * area) for each shape
        arraySum(
            list_transform(list_zip(
            requested_shape_volumes,
            areas_per_shape
            ), t -> t[1] * t[2])
        ) AS total_area_needed,
        
        -- Check if shapes fit in region
        total_area_needed <= total_region_area AS shapes_fit
    FROM region_specifications
    CROSS JOIN shape_areas
),
    -- Count regions where shapes fit
solution AS (
    SELECT countIf(shapes_fit) AS solution
    FROM region_fit_analysis
)

-- Return final answer
SELECT solution -- 463 with my input
FROM solution
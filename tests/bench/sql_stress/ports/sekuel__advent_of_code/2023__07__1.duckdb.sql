WITH input AS (
    SELECT
        row_number() OVER () AS id,
        split_part(column0, ' ', 2)::int AS bid,
        trim(split_part(column0, ' ', 1)) AS cards,
        split(trim(split_part(column0, ' ', 1)), NULL) AS cards_1,
        list_transform(
            split(trim(split_part(column0, ' ', 1)), NULL), 
            c -> 
                CASE c
                    WHEN 'T' THEN 10
                    WHEN 'J' THEN 11
                    WHEN 'Q' THEN 12
                    WHEN 'K' THEN 13
                    WHEN 'A' THEN 14
                ELSE c::INT END) AS card_value,
        list_transform(
            split(trim(split_part(column0, ' ', 1)), NULL), 
            c -> 
                CASE c
                    WHEN 'T' THEN 10
                    WHEN 'J' THEN 1
                    WHEN 'Q' THEN 12
                    WHEN 'K' THEN 13
                    WHEN 'A' THEN 14
                ELSE c::INT END) AS card_value_2
    FROM read_csv_auto('./2023/07/input.csv', header=false)
), with_jokers AS (
    SELECT 
        *, 
        row_number() OVER (PARTITION by id) AS _rn,
        len(regexp_extract_all(cards, 'J')) AS jokers,
        list_transform(
                split(trim(split_part(cards, ' ', 1)), NULL),
                c -> 
                    CASE c
                        WHEN 'J' THEN j.replacement
                    ELSE c END) AS cards_2
    FROM input,
    (
        SELECT 
            unnest(list_distinct(split(replace(replace(string_agg(cards), ',', ''), 'J', ''), NULL)))
        FROM input) j(replacement)
), unnested_cards AS (
    SELECT
        id,
        _rn,
        unnest(cards_1) AS card_1,
        unnest(cards_2) AS card_2,
    FROM with_jokers
), counting_cards AS (
    SELECT 'Part I' AS part, id, _rn, card_1, count(*) AS counts
    FROM unnested_cards
    GROUP BY 1,2,3,4
    UNION ALL 
    SELECT 'Part II' AS part, id, _rn, card_2, count(*) AS counts
    FROM unnested_cards
    GROUP BY 1,2,3,4
), hand_types AS (
    SELECT 
        part,
        id, 
        _rn,
        string_agg(counts ORDER BY counts DESC) AS hand_type
    FROM counting_cards
    GROUP BY 1,2,3
), good_hands AS (
    SELECT 
        part,
        id, 
        max(hand_type) AS hand_type
    FROM hand_types
    GROUP BY 1,2
), scores AS (
    SELECT 
        *, 
        row_number() OVER (
            PARTITION BY part 
            ORDER BY hand_type, if(part = 'Part I', card_value, card_value_2)
            ) * bid AS score,
    FROM good_hands 
    JOIN input USING (id)
)
SELECT part, sum(score) AS answer FROM scores GROUP BY 1;
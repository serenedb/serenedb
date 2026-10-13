-- v_musical_notes contains the frequency in hertz for each note

CREATE OR REPLACE VIEW v_musical_notes AS
SELECT      note_name,
            octave,
            -- Frequency calculation, based on the total offset from A4
            440 * pow(2, ((octave-4) * 12.0 + halftone_offset) / 12) AS freq
FROM        (VALUES ('A', 0), ('A#', 1), ('B', 2), ('C', 3),
            ('C#', 4), ('D', 5), ('D#', 6), ('E', 7),
            ('F', 8), ('F#', 9), ('G', 10), ('G#', 11)) AS note_offset(note_name, halftone_offset) -- Offset of a note, measured in half-tones, from A

            CROSS JOIN  generate_series(1, 6) octave; -- Octaves (1, 2, 3, 4, 5, 6)


-- music_sheet is used to feed a melody to the synthesiser

DROP TABLE IF EXISTS music_sheet;

CREATE TABLE music_sheet AS
SELECT      pos, -- Position, used to order notes from the first to the last
            note_name, -- e.g. A
            octave, -- e.g. 4
            relative_value -- Duration, with 0.25 being 1 beat (4/4 signature)
FROM        (VALUES 

            (1, 'C', 3, 0.25), (2, 'C', 3, 0.25), (3, 'G', 3, 0.25), (4, 'G', 3, 0.25), (5, 'A', 3, 0.25), (6, 'A', 3, 0.25), (7, 'G', 3, 0.5),
            (8, 'F', 3, 0.25), (9, 'F', 3, 0.25), (10, 'E', 3, 0.25), (11, 'E', 3, 0.25), (12, 'D', 3, 0.25), (13, 'D', 3, 0.25), (14, 'C', 3, 0.5),
            (15, 'G', 3, 0.25), (16, 'G', 3, 0.25), (17, 'F', 3, 0.25), (18, 'F', 3, 0.25), (19, 'E', 3, 0.25), (20, 'E', 3, 0.25), (21, 'D', 3, 0.5),
            (22, 'G', 3, 0.25), (23, 'G', 3, 0.25), (24, 'F', 3, 0.25), (25, 'F', 3, 0.25), (26, 'E', 3, 0.25), (27, 'E', 3, 0.25), (28, 'D', 3, 0.5),
            (29, 'C', 3, 0.25), (30, 'C', 3, 0.25), (31, 'G', 3, 0.25), (32, 'G', 3, 0.25), (33, 'A', 3, 0.25), (34, 'A', 3, 0.25), (35, 'G', 3, 0.5),
            (36, 'F', 3, 0.25), (37, 'F', 3, 0.25), (38, 'E', 3, 0.25), (39, 'E', 3, 0.25), (40, 'D', 3, 0.25), (41, 'D', 3, 0.25), (42, 'C', 3, 0.5)
            ) AS sheet(pos, note_name, octave, relative_value);


-- v_technical_sheet translates music_sheet to easily synthesisable values

CREATE OR REPLACE VIEW v_technical_sheet AS
SELECT      ms.pos,
            mn.freq,
            ms.relative_value * 4 * 44100 / (60 / 60) AS n_samples
FROM        music_sheet ms
            JOIN v_musical_notes mn
                ON ms.note_name = mn.note_name AND ms.octave = mn.octave;


-- v_synthesised synthesises the sound wave

CREATE OR REPLACE VIEW v_synthesised AS
SELECT      set_byte('\x00', 0,cast(
                least(127, note_sample_id/10000.0*127)*sin(freq*note_sample_id*2*pi()/44100)+128
            AS integer)) AS sample_value
FROM        v_technical_sheet
            INNER JOIN generate_series(1, 44100*4) note_sample_id
                ON note_sample_id <= n_samples -- Provides as many samples as needed depending on the note length (n_samples)
ORDER BY    pos, note_sample_id;


-- v_wav generates a binary WAV file containing the previously synthesised sound 

CREATE OR REPLACE VIEW v_wav AS
WITH precalc AS

(
SELECT
    -- Data size in bytes (excluding header)
    (SELECT COUNT(*) FROM v_synthesised)::INTEGER AS data_size, -- 1 sample per row, 1 byte per sample
    -- Overall size in bytes (including 45-byte header)
    ((SELECT COUNT(*) FROM v_synthesised)+45)::INTEGER AS overall_size
)

SELECT 

                                                                  -- Byte position (starting from 1)
'RIFF'::bytea ||                                                  -- 1-4 Literal
set_byte('\x00', 0, precalc.overall_size >> 0 & 255)::bytea ||    -- 5 File total size. Header (44 bytes) + data.
set_byte('\x00', 0, precalc.overall_size >> 8 & 255)::bytea ||    -- 6
set_byte('\x00', 0, precalc.overall_size >> 16 & 255)::bytea ||   -- 7
set_byte('\x00', 0, precalc.overall_size >> 24 & 255)::bytea ||   -- 8
'WAVE'::bytea ||                                                  -- 9-12 Literal
'fmt'::bytea || '\x20'::bytea ||                                  -- 13-16 Literal, format chunk marker
set_byte('\x00', 0, 16) ::bytea ||'\x000000'::bytea ||            -- 17-20 Constant, length of format data.
set_byte('\x00', 0, 1) ::bytea ||'\x00'::bytea ||                 -- 21-22 Constant, 1 = PCM
set_byte('\x00', 0, 1) ::bytea || '\x00'::bytea ||                -- 23-24 Number of channels
set_byte('\x00', 0, 44100 >> 0 & 255) ::bytea ||                  -- 25 Sample rate
set_byte('\x00', 0, 44100 >> 8 & 255) ::bytea ||                  -- 26
set_byte('\x00', 0, 44100 >> 16 & 255) ::bytea ||                 -- 27
set_byte('\x00', 0, 44100 >> 24 & 255) ::bytea ||                 -- 28
set_byte('\x00', 0, 44100 >> 0 & 255) ::bytea ||                  -- 29 Sample rate*bits per sample*channels/8 = 44100
set_byte('\x00', 0, 44100 >> 8 & 255) ::bytea ||                  -- 30
set_byte('\x00', 0, 44100 >> 16 & 255) ::bytea ||                 -- 31
set_byte('\x00', 0, 44100 >> 24 & 255) ::bytea ||                 -- 32
set_byte('\x00', 0, 1) ::bytea || '\x00'::bytea ||                -- 33-34 Bits per sample*Channels/8 = 1
set_byte('\x00', 0, 8) ::bytea || '\x00'::bytea ||                -- 35-36 Bits per sample
'data'::bytea ||                                                  -- 37-40 Data chunk header
set_byte('\x00', 0, precalc.data_size >> 0 & 255) ::bytea ||      -- 41 Data size
set_byte('\x00', 0, precalc.data_size >> 8 & 255) ::bytea ||      -- 42
set_byte('\x00', 0, precalc.data_size >> 16 & 255) ::bytea ||     -- 43
set_byte('\x00', 0, precalc.data_size >> 24 & 255) ::bytea ||     -- 44
(SELECT STRING_AGG(sample_value, null) FROM v_synthesised)        -- 45 Data. All samples concatenated.
AS file 

FROM precalc;

-- The following bash line exports v_wav to a file in /tmp
-- psql -U postgres -qAt -c "select encode(file,'base64') from v_wav limit 1" | base64 -d > /tmp/twinkletwinkle.wav
select encode(file,'base64') from v_wav limit 1;

CREATE OR REPLACE MACRO exp2(x) AS pow(2, x);
CREATE OR REPLACE MACRO ch_splitByChar(sep, s) AS string_split(s, sep);
CREATE OR REPLACE MACRO ch_transform(x, a, b, d) AS coalesce(b[list_position(a, x)], d);
CREATE OR REPLACE MACRO sample_frequency() AS 44100;
CREATE OR REPLACE MACRO master_volume() AS 1;
CREATE OR REPLACE MACRO clamp(level) AS least(1.0::DOUBLE, greatest(-1.0::DOUBLE, level));
CREATE OR REPLACE MACRO output(level) AS CAST(trunc((clamp(level) * 0x7FFF * master_volume())) AS SMALLINT);
CREATE OR REPLACE MACRO mono(x) AS (x, x);
CREATE OR REPLACE MACRO sine_wave("time") AS sin("time" * 2 * pi());
CREATE OR REPLACE MACRO square_wave("time") AS CAST(trunc("time") AS UBIGINT) % 2 * 2 - 1;
CREATE OR REPLACE MACRO sawtooth_wave("time") AS ("time" - floor("time")) * 2 - 1;
CREATE OR REPLACE MACRO triangle_wave("time") AS abs(sawtooth_wave("time")) * 2 - 1;
CREATE OR REPLACE MACRO step_lfo("from", "to", steps, "time") AS "from" + floor(("time" - floor("time")) * steps) / steps * ("to" - "from");
CREATE OR REPLACE MACRO exp_step_lfo("from", "to", steps, "time") AS exp(step_lfo(ln("from"), ln("to"), steps, "time"));
CREATE OR REPLACE MACRO linear_transition(a, b, t) AS (1 - t) * a + t * b;
CREATE OR REPLACE MACRO smooth_transition(a, b, t) AS linear_transition(a, b, (1 - sin((t + 1/2) * pi())) / 2);
CREATE OR REPLACE MACRO clipping(x, amount) AS clamp(x * amount);
CREATE OR REPLACE MACRO power_distortion(x, amount) AS clamp(CASE WHEN x > 0 THEN pow(x, amount) ELSE -pow(-x, amount) END);
CREATE OR REPLACE MACRO bitcrush(x, amount) AS round_even(x * exp2(amount), 0) / exp2(amount);
CREATE OR REPLACE MACRO desample("time", sample_frequency) AS round_even("time" * sample_frequency, 0) / sample_frequency;
CREATE OR REPLACE MACRO combine(a, b, weight) AS a * (1 - weight) + b * weight;
CREATE OR REPLACE MACRO envelope("time", "offset", attack, hold, release) AS CASE WHEN "time" < "offset" THEN 0 ELSE (CASE WHEN "time" < "offset" + attack THEN (("time" - "offset") / attack) ELSE (CASE WHEN "time" < "offset" + attack + hold THEN 1 ELSE (CASE WHEN "time" < "offset" + attack + hold + release THEN ("offset" + attack + hold + release - "time") / release ELSE 0 END) END) END) END;
CREATE OR REPLACE MACRO running_envelope(bpm, "time", "offset", attack, hold, release) AS envelope(
        "time" * (bpm / 60) - floor("time" * (bpm / 60)),
        "offset",
        attack,
        hold,
        release);
CREATE OR REPLACE MACRO running_envelope_swing(bpm, "time", "offset", swing, attack, hold, release) AS envelope(
        "time" * (bpm / 60) - floor("time" * (bpm / 60)),CASE WHEN CAST(trunc(("time" * (bpm / 60))) AS UBIGINT) % 2 THEN "offset" - swing ELSE "offset" + swing END,
        attack,
        hold,
        release);
CREATE OR REPLACE MACRO sequencer(sequence, "time") AS sequence[1 + CAST(trunc("time") AS UBIGINT) % length(sequence)];
CREATE OR REPLACE MACRO sierpinski("time") AS (CAST(trunc("time") AS UTINYINT) & CAST(trunc("time") AS UTINYINT) * 8);
CREATE OR REPLACE MACRO trailing_zero_bits("time") AS log2(CASE WHEN CAST(trunc("time") AS UBIGINT) > 0 THEN xor(CAST(trunc("time") AS UBIGINT), CAST(trunc("time") AS UBIGINT) - 1) ELSE 1 END);
CREATE OR REPLACE MACRO note(n) AS ch_transform(n, ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B'], (list_transform(range(12), x__note -> 220 * exp2((x__note + 3) / 12))), 0);
CREATE OR REPLACE MACRO notes(ns) AS list_transform(ch_splitByChar(' ', ns), n__notes -> note(n__notes));
CREATE OR REPLACE MACRO notes_in_octave(ns, octave) AS list_transform(notes(ns), x__notes_in_octave -> x__notes_in_octave * exp2(octave - 4));
CREATE OR REPLACE MACRO chatgpt_melody1() AS 'G G A B A G E G G A B A G E G G A B A G E G A A B C B A G';
CREATE OR REPLACE MACRO chatgpt_melody2() AS 'E E G A G E E G A G E E E G A G E E D D F# G F# E D D F# G F# E';
CREATE OR REPLACE MACRO chatgpt_melody3() AS 'C C E G G E C C E G G E C C E G G E G G F F G A G F F G A G';
SELECT

mono(output(
    list_sum(list_transform(range(10), x -> 0.25::DOUBLE * sine_wave(((number) / sample_frequency()) * notes_in_octave('C D E F G A B', 4)[x + 1]
        + sine_wave(((number) / sample_frequency()) * notes_in_octave('C D E F G A B', 2)[x + 1])
        ) * running_envelope(120 / exp2(x), ((number) / sample_frequency()), 0.1::DOUBLE * x, 0.01::DOUBLE / (x + 1), 0.01::DOUBLE / (x + 1), 0.25::DOUBLE / (x + 1))))
))

FROM "table";

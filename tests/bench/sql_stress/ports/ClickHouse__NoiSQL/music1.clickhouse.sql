CREATE OR REPLACE MACRO exp2(x) AS pow(2, x);
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
CREATE OR REPLACE MACRO sequencer(sequence, "time") AS sequence[1 + CAST(trunc("time") AS UBIGINT) % length(sequence)];
SELECT

mono(output(
    list_sum(list_transform(range(72), x -> 1 / (x + 1) * running_envelope(120 / (x + 1), ((number) / sample_frequency()) + 100, 0.01::DOUBLE * x, 0.005::DOUBLE, 0.1::DOUBLE, 0.1::DOUBLE) * sine_wave(((number) / sample_frequency()) * 50 * exp2(x / 12))))
))

FROM "table";

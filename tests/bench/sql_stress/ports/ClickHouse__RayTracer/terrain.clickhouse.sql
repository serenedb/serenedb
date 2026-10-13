CREATE OR REPLACE MACRO tuplePlus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] + ch_ly[1], ch_lx[2] + ch_ly[2], ch_lx[3] + ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMinus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] - ch_ly[1], ch_lx[2] - ch_ly[2], ch_lx[3] - ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMultiply(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] * ch_ly[1], ch_lx[2] * ch_ly[2], ch_lx[3] * ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMultiplyByNumber(a, s) AS list_transform([a], ch_lx -> list_transform([s], ch_lk -> (ch_lx[1] * ch_lk, ch_lx[2] * ch_lk, ch_lx[3] * ch_lk))[1])[1];
CREATE OR REPLACE MACRO dotProduct(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> ch_lx[1] * ch_ly[1] + ch_lx[2] * ch_ly[2] + ch_lx[3] * ch_ly[3])[1])[1];
CREATE OR REPLACE MACRO L2Normalize(a) AS list_transform([a], ch_lx -> list_transform([sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3])], ch_ln -> (ch_lx[1] / ch_ln, ch_lx[2] / ch_ln, ch_lx[3] / ch_ln))[1])[1];
CREATE OR REPLACE MACRO clamp(x, lo, hi) AS greatest(lo, least(hi, x));
CREATE OR REPLACE MACRO ch_numbers_n(n) AS TABLE SELECT range AS number FROM range(CAST(n AS BIGINT));
CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(x) AS BIGINT);
CREATE OR REPLACE MACRO ch_intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO W() AS 112::UBIGINT;
CREATE OR REPLACE MACRO H() AS 63::UBIGINT;
CREATE OR REPLACE MACRO imgW() AS W()::DOUBLE;
CREATE OR REPLACE MACRO imgH() AS H()::DOUBLE;
CREATE OR REPLACE MACRO eye() AS (0.0::DOUBLE, -9.0::DOUBLE, 4.2::DOUBLE);
CREATE OR REPLACE MACRO target() AS (0.0::DOUBLE,  9.0::DOUBLE, 1.3::DOUBLE);
CREATE OR REPLACE MACRO fovDeg() AS 52.0::DOUBLE;
CREATE OR REPLACE MACRO sun() AS L2Normalize((0.55::DOUBLE, 0.45::DOUBLE, 0.70::DOUBLE));
CREATE OR REPLACE MACRO sunCol() AS (1.05::DOUBLE, 0.96::DOUBLE, 0.80::DOUBLE);
CREATE OR REPLACE MACRO amp() AS 7.0::DOUBLE;
CREATE OR REPLACE MACRO nscale() AS 0.085::DOUBLE;
CREATE OR REPLACE MACRO waterLevel() AS 0.6::DOUBLE;
CREATE OR REPLACE MACRO fogDist() AS 90.0::DOUBLE;
CREATE OR REPLACE MACRO skyHorizon() AS (0.62::DOUBLE, 0.74::DOUBLE, 0.92::DOUBLE);
CREATE OR REPLACE MACRO skyTop() AS (0.30::DOUBLE, 0.50::DOUBLE, 0.85::DOUBLE);
CREATE OR REPLACE MACRO va(a, b) AS tuplePlus(a, b);
CREATE OR REPLACE MACRO vs(a, b) AS tupleMinus(a, b);
CREATE OR REPLACE MACRO vm(a, s) AS tupleMultiplyByNumber(a, s);
CREATE OR REPLACE MACRO vd(a, b) AS dotProduct(a, b);
CREATE OR REPLACE MACRO vn(a) AS L2Normalize(a);
CREATE OR REPLACE MACRO vc(a, b) AS (a[2]*b[3] - a[3]*b[2], a[3]*b[1] - a[1]*b[3], a[1]*b[2] - a[2]*b[1]);
CREATE OR REPLACE MACRO vmul3(a, b) AS tupleMultiply(a, b);
CREATE OR REPLACE MACRO mix(a, b, t) AS tuplePlus(a, tupleMultiplyByNumber(tupleMinus(b, a), t));
CREATE OR REPLACE MACRO clamp3(lo, hi, t) AS greatest(lo, least(hi, t));
CREATE OR REPLACE MACRO camFwd() AS L2Normalize(tupleMinus(target(), eye()));
CREATE OR REPLACE MACRO camRight() AS L2Normalize(vc(L2Normalize(tupleMinus(target(), eye())), (0.0::DOUBLE,0.0::DOUBLE,1.0::DOUBLE)));
CREATE OR REPLACE MACRO camUp() AS vc(camRight(), camFwd());
CREATE OR REPLACE MACRO tanHalf() AS tan(fovDeg()*0.5::DOUBLE*pi()/180.0::DOUBLE);
CREATE OR REPLACE MACRO aspect() AS imgW()/imgH();
CREATE OR REPLACE MACRO vnoise(x, y) AS list_transform([floor(x)], ix__vnoise -> list_transform([floor(y)], iy__vnoise -> list_transform([x - ix__vnoise], fx__vnoise -> list_transform([y - iy__vnoise], fy__vnoise -> list_transform([fx__vnoise*fx__vnoise*fx__vnoise*(fx__vnoise*(fx__vnoise*6.0::DOUBLE-15.0::DOUBLE)+10.0::DOUBLE)], u__vnoise -> list_transform([fy__vnoise*fy__vnoise*fy__vnoise*(fy__vnoise*(fy__vnoise*6.0::DOUBLE-15.0::DOUBLE)+10.0::DOUBLE)], v__vnoise -> list_transform([(hash_city64(toInt64(ix__vnoise),   toInt64(iy__vnoise))   % 4194304) / 4194304.0::DOUBLE], a__vnoise -> list_transform([(hash_city64(toInt64(ix__vnoise)+1, toInt64(iy__vnoise))   % 4194304) / 4194304.0::DOUBLE], b__vnoise -> list_transform([(hash_city64(toInt64(ix__vnoise),   toInt64(iy__vnoise)+1) % 4194304) / 4194304.0::DOUBLE], c__vnoise -> list_transform([(hash_city64(toInt64(ix__vnoise)+1, toInt64(iy__vnoise)+1) % 4194304) / 4194304.0::DOUBLE], d__vnoise -> (a__vnoise + (b__vnoise-a__vnoise)*u__vnoise) + ((c__vnoise + (d__vnoise-c__vnoise)*u__vnoise) - (a__vnoise + (b__vnoise-a__vnoise)*u__vnoise)) * v__vnoise)[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
CREATE OR REPLACE MACRO fbm(x, y) AS (vnoise(x, y)*0.5::DOUBLE + vnoise(x*2.0::DOUBLE, y*2.0::DOUBLE)*0.25::DOUBLE  + vnoise(x*4.0::DOUBLE, y*4.0::DOUBLE)*0.125::DOUBLE + vnoise(x*8.0::DOUBLE, y*8.0::DOUBLE)*0.0625::DOUBLE) / 0.9375::DOUBLE;
CREATE OR REPLACE MACRO height(x, y) AS amp() * fbm(x*nscale(), y*nscale());
CREATE OR REPLACE MACRO terrainN(x, y) AS vn(((height(x-0.15::DOUBLE, y) - height(x+0.15::DOUBLE, y)) / 0.3::DOUBLE,
                   (height(x, y-0.15::DOUBLE) - height(x, y+0.15::DOUBLE)) / 0.3::DOUBLE,
                   1.0::DOUBLE));
CREATE OR REPLACE MACRO terrainCol(h) AS mix(mix(mix(mix((0.14::DOUBLE,0.28::DOUBLE,0.50::DOUBLE), (0.78::DOUBLE,0.72::DOUBLE,0.46::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h-waterLevel())/0.4::DOUBLE)),
                        (0.24::DOUBLE,0.44::DOUBLE,0.17::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h-1.3::DOUBLE)/1.1::DOUBLE)),
                    (0.40::DOUBLE,0.35::DOUBLE,0.31::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h-3.4::DOUBLE)/1.4::DOUBLE)),
                (0.93::DOUBLE,0.94::DOUBLE,0.98::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h-5.0::DOUBLE)/1.0::DOUBLE));
CREATE OR REPLACE MACRO sky(d) AS mix(skyHorizon(), skyTop(), clamp3(0.0::DOUBLE,1.0::DOUBLE,d[3]*1.6::DOUBLE));
CREATE OR REPLACE MACRO march(O, D) AS list_transform([list_reduce(range(170), (s__march, i__march) -> if(s__march[2] < 1e8, s__march,
             if(O[3] + s__march[1]*D[3] < height(O[1] + s__march[1]*D[1], O[2] + s__march[1]*D[2]),
                (s__march[1], s__march[1]),
                (s__march[1] + greatest(0.04::DOUBLE, s__march[1]*0.035::DOUBLE), 1e9))), (0.05::DOUBLE, 1e9))], s__march -> s__march[2])[1];
CREATE OR REPLACE MACRO shadowed(P) AS list_reduce(range(48), (blk__shadowed, i__shadowed) -> blk__shadowed OR
              (P[3] + (0.3::DOUBLE + i__shadowed*0.45::DOUBLE)*sun()[3] < height(P[1] + (0.3::DOUBLE + i__shadowed*0.45::DOUBLE)*sun()[1], P[2] + (0.3::DOUBLE + i__shadowed*0.45::DOUBLE)*sun()[2])), 0) > 0;
-- ============================================================================
--  A Perlin/value-noise terrain in one ClickHouse SQL query.
--
--  The ground is a height field  z = amp * fBm(x, y), where fBm sums 4 octaves
--  of lattice value-noise (each octave: hash the 4 cell corners -> smootherstep
--  interpolation). Every pixel's camera ray is RAY-MARCHED against the field
--  (advance with a growing step until the ray dips below the surface); the
--  surface normal is the height gradient. Shading = a height color ramp
--  (water -> sand -> grass -> rock -> snow) lit by a cool sky-ambient term plus
--  a warm directional sun, with a marched shadow ray and distance fog into the
--  sky. Pixels stream to ClickHouse's PNG output format.
--
--  Run it (image size and samples are runtime parameters):
--      clickhouse local --output_format_image_width 896 --output_format_image_height 504 \
--                       --param_SAMPLES 4 --queries-file terrain.sql > terrain.png
--
--  Knobs: image size via --output_format_image_width/height (read in SQL with `getSetting`),
--  samples per pixel via --param_SAMPLES; amp / nscale (terrain height & feature size), the fBm
--  octaves in the `fbm` lambda, the 170-step `march` and 48-step `shadowed`, camera eye/target.
-- ============================================================================
SELECT
    clamp(pow(greatest(avg(col[1]), 0.0::DOUBLE), 1.0::DOUBLE/2.2::DOUBLE), 0.0::DOUBLE, 1.0::DOUBLE) AS r,
    clamp(pow(greatest(avg(col[2]), 0.0::DOUBLE), 1.0::DOUBLE/2.2::DOUBLE), 0.0::DOUBLE, 1.0::DOUBLE) AS g,
    clamp(pow(greatest(avg(col[3]), 0.0::DOUBLE), 1.0::DOUBLE/2.2::DOUBLE), 0.0::DOUBLE, 1.0::DOUBLE) AS b,
    pixel % W()      AS x,                            /* explicit PNG coordinates: no ORDER BY needed */
    ch_intDiv(pixel, W()) AS y
FROM (
  SELECT pixel,
    list_transform([march(O, D)], t -> list_transform([va(O, vm(D, t))], hp -> list_transform([height(hp[1], hp[2])], hgt -> list_transform([terrainN(hp[1], hp[2])], nrm -> list_transform([greatest(vd(nrm, sun()), 0.0::DOUBLE) * if(shadowed(va(hp, vm(nrm, 0.08::DOUBLE))), 0.0::DOUBLE, 1.0::DOUBLE)], lit -> list_transform([va(vm((0.42::DOUBLE,0.54::DOUBLE,0.80::DOUBLE), 0.30::DOUBLE + 0.25::DOUBLE*greatest(nrm[3],0.0::DOUBLE)), vm(sunCol(), 1.5::DOUBLE*lit))], light -> list_transform([vmul3(terrainCol(hgt), light)], ground -> list_transform([greatest(0.0::DOUBLE, least(1.0::DOUBLE, t/fogDist()))], fog -> if(t >= 1e8, sky(D), mix(ground, skyHorizon(), fog)))[1])[1])[1])[1])[1])[1])[1])[1] AS col
  FROM (
    SELECT
      ch_intDiv(number, CAST(2 AS UINTEGER)) AS pixel,
      number % CAST(2 AS UINTEGER) AS sample,
      eye() AS O,
      vn(va(camFwd(), va(vm(camRight(), (((pixel % W()) + (hash_city64(pixel,sample,1)%1024)/1024.0::DOUBLE) / imgW() * 2.0::DOUBLE - 1.0::DOUBLE) * aspect() * tanHalf()),
                       vm(camUp(),    (1.0::DOUBLE - (ch_intDiv(pixel, W()) + (hash_city64(pixel,sample,2)%1024)/1024.0::DOUBLE) / imgH() * 2.0::DOUBLE) * tanHalf())))) AS D
    FROM ch_numbers_n(W() * H() * CAST(2 AS UINTEGER))
  )
)
GROUP BY pixel

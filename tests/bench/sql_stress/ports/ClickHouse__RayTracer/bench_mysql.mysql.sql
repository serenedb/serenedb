DROP FUNCTION IF EXISTS va;
DROP FUNCTION IF EXISTS vs;
DROP FUNCTION IF EXISTS vm;
DROP FUNCTION IF EXISTS vd;
DROP FUNCTION IF EXISTS vmul;
DROP FUNCTION IF EXISTS vn;
DROP FUNCTION IF EXISTS vref;
DROP FUNCTION IF EXISTS mix;
DROP FUNCTION IF EXISTS lerpS;
DROP FUNCTION IF EXISTS clampS;
DROP FUNCTION IF EXISTS satb;
DROP FUNCTION IF EXISTS hashf;
DROP FUNCTION IF EXISTS fade;
DROP FUNCTION IF EXISTS vnoise;
DROP FUNCTION IF EXISTS fbm;
DROP FUNCTION IF EXISTS terrH;
DROP FUNCTION IF EXISTS terrN;
DROP FUNCTION IF EXISTS terrCol;
DROP FUNCTION IF EXISTS sky;
DROP FUNCTION IF EXISTS tsdf3;
DROP FUNCTION IF EXISTS bsdf3;
DROP FUNCTION IF EXISTS toriSDF3;

-- ====================================================================
-- ClickHouse-over-terrain benchmark, CedarDB port  (16x8, depth 2)
--   terrain + analytic letters + tori march + planet + shadows + bounces
-- ====================================================================

-- ---- vector helpers (double CAST(JSON_VALUE(precision,'$[2]') AS DOUBLE), 1-indexed) ----
CREATE FUNCTION va(a JSON, b JSON) RETURNS JSON RETURN JSON_ARRAY(CAST(JSON_VALUE(a,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(b,'$[0]') AS DOUBLE),CAST(JSON_VALUE(a,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(b,'$[1]') AS DOUBLE),CAST(JSON_VALUE(a,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(b,'$[2]') AS DOUBLE));
CREATE FUNCTION vs(a JSON, b JSON) RETURNS JSON RETURN JSON_ARRAY(CAST(JSON_VALUE(a,'$[0]') AS DOUBLE)-CAST(JSON_VALUE(b,'$[0]') AS DOUBLE),CAST(JSON_VALUE(a,'$[1]') AS DOUBLE)-CAST(JSON_VALUE(b,'$[1]') AS DOUBLE),CAST(JSON_VALUE(a,'$[2]') AS DOUBLE)-CAST(JSON_VALUE(b,'$[2]') AS DOUBLE));
CREATE FUNCTION vm(a JSON, s DOUBLE) RETURNS JSON RETURN JSON_ARRAY(CAST(JSON_VALUE(a,'$[0]') AS DOUBLE)*s,CAST(JSON_VALUE(a,'$[1]') AS DOUBLE)*s,CAST(JSON_VALUE(a,'$[2]') AS DOUBLE)*s);
CREATE FUNCTION vd(a JSON, b JSON) RETURNS DOUBLE RETURN CAST(JSON_VALUE(a,'$[0]') AS DOUBLE)*CAST(JSON_VALUE(b,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(a,'$[1]') AS DOUBLE)*CAST(JSON_VALUE(b,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(a,'$[2]') AS DOUBLE)*CAST(JSON_VALUE(b,'$[2]') AS DOUBLE);
CREATE FUNCTION vmul(a JSON, b JSON) RETURNS JSON RETURN JSON_ARRAY(CAST(JSON_VALUE(a,'$[0]') AS DOUBLE)*CAST(JSON_VALUE(b,'$[0]') AS DOUBLE),CAST(JSON_VALUE(a,'$[1]') AS DOUBLE)*CAST(JSON_VALUE(b,'$[1]') AS DOUBLE),CAST(JSON_VALUE(a,'$[2]') AS DOUBLE)*CAST(JSON_VALUE(b,'$[2]') AS DOUBLE));
CREATE FUNCTION vn(a JSON) RETURNS JSON RETURN vm(a, 1.0/sqrt(greatest(vd(a,a), 1e-18)));
CREATE FUNCTION vref(d JSON, n JSON) RETURNS JSON RETURN vs(d, vm(n, 2.0*vd(d,n)));
CREATE FUNCTION mix(a JSON, b JSON, t DOUBLE) RETURNS JSON RETURN va(a, vm(vs(b,a), t));
CREATE FUNCTION lerpS(a DOUBLE, b DOUBLE, t DOUBLE) RETURNS DOUBLE RETURN a*(1.0-t)+b*t;
CREATE FUNCTION clampS(lo DOUBLE, hi DOUBLE, x DOUBLE) RETURNS DOUBLE RETURN greatest(lo, least(hi, x));
CREATE FUNCTION satb(c JSON) RETURNS JSON RETURN mix(JSON_ARRAY(CAST(JSON_VALUE(c,'$[0]') AS DOUBLE)*0.30+CAST(JSON_VALUE(c,'$[1]') AS DOUBLE)*0.59+CAST(JSON_VALUE(c,'$[2]') AS DOUBLE)*0.11, CAST(JSON_VALUE(c,'$[0]') AS DOUBLE)*0.30+CAST(JSON_VALUE(c,'$[1]') AS DOUBLE)*0.59+CAST(JSON_VALUE(c,'$[2]') AS DOUBLE)*0.11, CAST(JSON_VALUE(c,'$[0]') AS DOUBLE)*0.30+CAST(JSON_VALUE(c,'$[1]') AS DOUBLE)*0.59+CAST(JSON_VALUE(c,'$[2]') AS DOUBLE)*0.11), c, 1.45);

-- ---- portable terrain noise (identical to ClickHouse/DuckDB; never overflows int64) ----
CREATE FUNCTION hashf(ix bigint, iy bigint) RETURNS DOUBLE RETURN ((((((ix+4096)+(iy+4096)*8192)*1103515245+12345)%2147483648)*1103515245+12345)%2147483648%65536)/65536.0;
CREATE FUNCTION fade(t DOUBLE) RETURNS DOUBLE RETURN t*t*t*(t*(t*6.0-15.0)+10.0);
CREATE FUNCTION vnoise(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN lerpS(lerpS(hashf(CAST(floor(x) AS BIGINT),CAST(floor(y) AS BIGINT)),   hashf(CAST(floor(x)+1 AS BIGINT),CAST(floor(y) AS BIGINT)),   fade(x-floor(x))),
         lerpS(hashf(CAST(floor(x) AS BIGINT),CAST(floor(y)+1 AS BIGINT)), hashf(CAST(floor(x)+1 AS BIGINT),CAST(floor(y)+1 AS BIGINT)), fade(x-floor(x))),
         fade(y-floor(y)));
CREATE FUNCTION fbm(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN (vnoise(x,y)*0.5 + vnoise(x*2.0,y*2.0)*0.25 + vnoise(x*4.0,y*4.0)*0.125 + vnoise(x*8.0,y*8.0)*0.0625)/0.9375;
CREATE FUNCTION terrH(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN 2.4*(fbm(x*0.05, y*0.05) - 0.5);
CREATE FUNCTION terrN(x DOUBLE, y DOUBLE) RETURNS JSON RETURN vn(JSON_ARRAY((terrH(x-0.25,y)-terrH(x+0.25,y))/0.5, (terrH(x,y-0.25)-terrH(x,y+0.25))/0.5, 1.0));
CREATE FUNCTION terrCol(h DOUBLE) RETURNS JSON RETURN mix(mix(mix(mix(JSON_ARRAY(0.06,0.24,0.55), JSON_ARRAY(0.84,0.68,0.32), clampS(0.0,1.0,(h+0.6)/0.4)),
                  JSON_ARRAY(0.16,0.52,0.09), clampS(0.0,1.0,(h+0.1)/0.5)),
              JSON_ARRAY(0.46,0.32,0.22), clampS(0.0,1.0,(h-0.6)/0.4)),
          JSON_ARRAY(0.97,0.98,1.0), clampS(0.0,1.0,(h-1.0)/0.3));
CREATE FUNCTION sky(d JSON) RETURNS JSON RETURN mix(JSON_ARRAY(0.66,0.78,0.96), JSON_ARRAY(0.30,0.50,0.85), greatest(0.0, least(1.0, CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*1.5)));

-- ---- torus / box signed-distance fields (scalar args; no array subscripts) ----
CREATE FUNCTION tsdf3(px DOUBLE, py DOUBLE, pz DOUBLE,
                                 cx DOUBLE, cz DOUBLE, R DOUBLE, rr DOUBLE)
  RETURNS DOUBLE RETURN sqrt((sqrt((px-cx)*(px-cx)+(pz-cz)*(pz-cz))-R)*(sqrt((px-cx)*(px-cx)+(pz-cz)*(pz-cz))-R) + py*py) - rr;
CREATE FUNCTION bsdf3(px DOUBLE, py DOUBLE, pz DOUBLE,
                                 cx DOUBLE, cy DOUBLE, cz DOUBLE,
                                 hx DOUBLE, hy DOUBLE, hz DOUBLE)
  RETURNS DOUBLE RETURN sqrt(greatest(abs(px-cx)-hx,0.0)*greatest(abs(px-cx)-hx,0.0)
       + greatest(abs(py-cy)-hy,0.0)*greatest(abs(py-cy)-hy,0.0)
       + greatest(abs(pz-cz)-hz,0.0)*greatest(abs(pz-cz)-hz,0.0))
   + least(greatest(abs(px-cx)-hx, greatest(abs(py-cy)-hy, abs(pz-cz)-hz)), 0.0);
CREATE FUNCTION toriSDF3(px DOUBLE, py DOUBLE, pz DOUBLE)
  RETURNS DOUBLE RETURN least(greatest(tsdf3(px,py,pz,1.3,5.4,2.28,0.62), -bsdf3(px,py,pz,4.425,0.0,5.4,3.025,100.0,1.5)), least(greatest(tsdf3(px,py,pz,12.95,4.5,1.425,0.575), -bsdf3(px,py,pz,16.45,0.0,4.5,3.4,100.0,0.94)), least(greatest(tsdf3(px,py,pz,30.4,4.5,1.475,0.525), -bsdf3(px,py,pz,1000000.0,0.0,1000000.0,0.1,0.1,0.1)), least(greatest(tsdf3(px,py,pz,36.27,4.0,1.45,0.62), -bsdf3(px,py,pz,33.95,0.0,7.75,9.0,100.0,3.75)), least(greatest(tsdf3(px,py,pz,41.9,6.0,1.025,0.425), -bsdf3(px,py,pz,45.375,0.0,-0.25,3.475,100.0,6.25)), least(greatest(tsdf3(px,py,pz,41.9,3.9,1.025,0.425), -bsdf3(px,py,pz,36.375,0.0,7.7,5.525,100.0,3.8)), greatest(tsdf3(px,py,pz,47.25,4.55,1.585,0.465), -bsdf3(px,py,pz,50.8,0.0,3.87,3.25,100.0,0.32))))))));

-- ====================================================================
--  render
-- ====================================================================
WITH RECURSIVE
seq(g) AS (SELECT 0 UNION ALL SELECT g+1 FROM seq WHERE g < 127),
gs24(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM gs24 WHERE i < 23),
px AS (
  SELECT g AS idx, JSON_ARRAY(24.0,-45.0,12.0) AS o,
         vn(va(JSON_ARRAY(0.0,0.9912279,-0.13216372),
                va(vm(JSON_ARRAY(1.0,-0.0,0.0), (((g % 16) + 0.5) / 16.0 * 2.0 - 1.0) * 2.0 * 0.3249197),
                   vm(JSON_ARRAY(0.0,0.13216372,0.9912279),    (1.0 - (FLOOR(g / 16) + 0.5) / 8.0 * 2.0) * 0.3249197)))) AS d,
         CAST(1.0 AS DOUBLE) AS thr, JSON_ARRAY(0.0,0.0,0.0) AS acc, CAST(1.0 AS DOUBLE) AS alive
  FROM seq
),
cyls(a, b, r) AS (
     SELECT JSON_ARRAY(5.75,0.0,2.5) AS a, JSON_ARRAY(5.75,0.0,9.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(8.65,0.0,2.5) AS a, JSON_ARRAY(8.65,0.0,7.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(16.8,0.0,2.5) AS a, JSON_ARRAY(16.8,0.0,9.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(17.15,0.0,5.0) AS a, JSON_ARRAY(20.05,0.0,7.6) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(17.35,0.0,5.1) AS a, JSON_ARRAY(20.05,0.0,2.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(22.4,0.0,2.5) AS a, JSON_ARRAY(22.4,0.0,9.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(26.15,0.0,2.5) AS a, JSON_ARRAY(26.15,0.0,9.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(22.35,0.0,6.0) AS a, JSON_ARRAY(26.15,0.0,6.0) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(34.82,0.0,3.5) AS a, JSON_ARRAY(34.82,0.0,7.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(37.72,0.0,3.5) AS a, JSON_ARRAY(37.72,0.0,7.5) AS b, CAST(0.62 AS DOUBLE) AS r
     UNION ALL SELECT JSON_ARRAY(45.35,0.0,4.6) AS a, JSON_ARRAY(48.85,0.0,4.6) AS b, CAST(0.41333333 AS DOUBLE) AS r
),
-- ============================ bounce b1 ============================
in_b1 AS (SELECT idx, o, d, thr, acc FROM px WHERE alive > 0.5),

-- terrain height-field march (lockstep; per-step expressions inlined)
m_b1 AS (
  SELECT idx, o, d,
         CASE WHEN CAST(JSON_VALUE(d,'$[2]') AS DOUBLE) < -0.001 THEN greatest(0.05, (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)-1.55)/(-CAST(JSON_VALUE(d,'$[2]') AS DOUBLE))) ELSE 0.05 END AS t,
         CAST(0.0 AS DOUBLE) AS prevT, CAST(1e9 AS DOUBLE) AS prevDiff,
         CAST(1e9 AS DOUBLE) AS result, 0 AS done, 0 AS iter
  FROM in_b1
  UNION ALL
  SELECT idx, o, d,
    CASE WHEN prevDiff > 0.0 AND (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))) <= 0.0 THEN t ELSE t + greatest(0.06, t*0.035) END,
    t,
    (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))),
    CASE WHEN prevDiff > 0.0 AND (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))) <= 0.0
              THEN prevT + (t - prevT)*prevDiff/(prevDiff - (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)))) ELSE 1e9 END,
    CASE WHEN prevDiff > 0.0 AND (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))) <= 0.0 THEN 1
         WHEN CAST(JSON_VALUE(o,'$[2]') AS DOUBLE) + t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE) > 1.55 AND CAST(JSON_VALUE(d,'$[2]') AS DOUBLE) >= 0.0 THEN 2 ELSE 0 END,
    iter + 1
  FROM m_b1 WHERE done = 0 AND iter < 110
),
terr_b1 AS (SELECT idx, min(CASE WHEN done = 1 THEN result ELSE 1e9 END) AS tTerr FROM m_b1 GROUP BY idx),

-- tori sphere-trace march (48 accumulate steps, like DuckDB)
tmar_b1 AS (
  SELECT idx, o, d, CAST(0.02 AS DOUBLE) AS t, 0 AS iter FROM in_b1
  UNION ALL
  SELECT idx, o, d, t + toriSDF3(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[0]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*t), iter + 1
  FROM tmar_b1 WHERE iter < 48
),
tori_hit_b1 AS (
  SELECT idx, tfin, CASE WHEN sdf < 0.01 AND tfin > 0.002 AND tfin < 260.0 THEN tfin ELSE 1e9 END AS tTorus
  FROM (SELECT t1.idx, t1.t AS tfin,
               toriSDF3(CAST(JSON_VALUE(t1.o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(t1.d,'$[0]') AS DOUBLE)*t1.t, CAST(JSON_VALUE(t1.o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(t1.d,'$[1]') AS DOUBLE)*t1.t, CAST(JSON_VALUE(t1.o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(t1.d,'$[2]') AS DOUBLE)*t1.t) AS sdf
        FROM tmar_b1 t1 WHERE t1.iter = 48) z
),
tori_nrm_b1 AS (
  SELECT h.idx, h.tTorus,
         vn(JSON_ARRAY(toriSDF3(hx+0.0009,hy,hz)-toriSDF3(hx-0.0009,hy,hz),
                  toriSDF3(hx,hy+0.0009,hz)-toriSDF3(hx,hy-0.0009,hz),
                  toriSDF3(hx,hy,hz+0.0009)-toriSDF3(hx,hy,hz-0.0009))) AS nTorus
  FROM tori_hit_b1 h JOIN in_b1 p USING (idx),
       LATERAL (SELECT CAST(JSON_VALUE(p.o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(p.d,'$[0]') AS DOUBLE)*least(h.tTorus,5000.0) AS hx,
                       CAST(JSON_VALUE(p.o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(p.d,'$[1]') AS DOUBLE)*least(h.tTorus,5000.0) AS hy,
                       CAST(JSON_VALUE(p.o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(p.d,'$[2]') AS DOUBLE)*least(h.tTorus,5000.0) AS hz) g
),

-- analytic cylinders (rod = body + 2 disk caps), nearest per pixel
cyl_raw_b1 AS (
  SELECT p.idx, least(tBody, least(tCapA, tCapB)) AS t,
         mix(mix(vn(l1.ba), vn(vm(l1.ba,-1.0)),
                 CASE WHEN least(tBody,least(tCapA,tCapB)) = tCapA THEN 1.0 ELSE 0.0 END),
             vn(vs(va(p.o, vm(p.d, least(tBody,least(tCapA,tCapB)))),
                   va(c.a, vm(l1.ba, vd(vs(va(p.o, vm(p.d, least(tBody,least(tCapA,tCapB)))), c.a), l1.ba)/l1.baba)))),
             CASE WHEN least(tBody,least(tCapA,tCapB)) = tBody THEN 1.0 ELSE 0.0 END) AS n
  FROM in_b1 p CROSS JOIN cyls c
  , LATERAL (SELECT vs(c.b,c.a) AS ba, vs(p.o,c.a) AS oa) l0
  , LATERAL (SELECT l0.ba AS ba, l0.oa AS oa, vd(l0.ba,l0.ba) AS baba, vd(l0.ba,p.d) AS bard,
                    vd(l0.ba,l0.oa) AS baoa, vd(p.d,l0.oa) AS doa, vd(l0.oa,l0.oa) AS oaoa) l1
  , LATERAL (SELECT l1.baba - l1.bard*l1.bard AS aa, l1.baba*l1.doa - l1.baoa*l1.bard AS bb,
                    l1.baba*l1.oaoa - l1.baoa*l1.baoa - c.r*c.r*l1.baba AS cc) l2
  , LATERAL (SELECT l2.bb*l2.bb - l2.aa*l2.cc AS hh) l3
  , LATERAL (SELECT (-l2.bb - sqrt(greatest(l3.hh,0.0)))/l2.aa AS tb) l4
  , LATERAL (SELECT CASE WHEN l3.hh >= 0.0 AND l2.aa > 1e-9 AND l4.tb > 0.002
                              AND (l1.baoa + l4.tb*l1.bard) > 0.0 AND (l1.baoa + l4.tb*l1.bard) < l1.baba
                         THEN l4.tb ELSE 1e9 END AS tBody) l5
  , LATERAL (SELECT vd(l1.ba, p.d) AS den) l6
  , LATERAL (SELECT vd(l1.ba, vs(c.a, p.o))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taA,
                    vd(l1.ba, vs(c.b, p.o))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taB) l7
  , LATERAL (SELECT vs(va(p.o, vm(p.d, l7.taA)), c.a) AS dA, vs(va(p.o, vm(p.d, l7.taB)), c.b) AS dB) l8
  , LATERAL (SELECT CASE WHEN abs(l6.den)>1e-9 AND l7.taA>0.002 AND vd(l8.dA,l8.dA)<=c.r*c.r THEN l7.taA ELSE 1e9 END AS tCapA,
                    CASE WHEN abs(l6.den)>1e-9 AND l7.taB>0.002 AND vd(l8.dB,l8.dB)<=c.r*c.r THEN l7.taB ELSE 1e9 END AS tCapB) l9
),
cyl_hit_b1 AS (
  SELECT idx, t AS tCyl, n AS nCyl FROM (
    SELECT idx, t, n, row_number() OVER (PARTITION BY idx ORDER BY t) AS rn FROM cyl_raw_b1
  ) z WHERE rn = 1
),

-- i-dot sphere
sph_hit_b1 AS (
  SELECT p.idx,
         CASE WHEN disc > 0.0 AND tnear > 0.002 THEN tnear ELSE 1e9 END AS tSph,
         vn(vs(va(p.o, vm(p.d, CASE WHEN disc > 0.0 AND tnear > 0.002 THEN tnear ELSE 1e9 END)), JSON_ARRAY(8.65,0.0,8.8))) AS nSph
  FROM in_b1 p
  , LATERAL (SELECT vs(p.o, JSON_ARRAY(8.65,0.0,8.8)) AS oc) s0
  , LATERAL (SELECT vd(s0.oc, p.d) AS bb, vd(s0.oc, s0.oc) - 0.5184 AS cc) s1
  , LATERAL (SELECT s1.bb*s1.bb - s1.cc AS disc) s2
  , LATERAL (SELECT -s1.bb - sqrt(greatest(s2.disc,0.0)) AS tnear) s3
),

-- planet: sphere csgA with a spherical cavity csgB
planet_b1 AS (
  SELECT p.idx, least(cEnter, cCav) AS tC,
         mix(vn(vs(JSON_ARRAY(24.0,6.6,17.0), va(p.o, vm(p.d, least(cEnter,cCav))))),
             vn(vs(va(p.o, vm(p.d, least(cEnter,cCav))), JSON_ARRAY(24.0,12.0,17.5))),
             CASE WHEN cEnter <= cCav THEN 1.0 ELSE 0.0 END) AS nC
  FROM in_b1 p
  , LATERAL (SELECT vs(p.o, JSON_ARRAY(24.0,12.0,17.5)) AS ocA, vs(p.o, JSON_ARRAY(24.0,6.6,17.0)) AS ocB) q0
  , LATERAL (SELECT vd(q0.ocA,p.d) AS bA, vd(q0.ocA,q0.ocA)-36.0 AS cA,
                    vd(q0.ocB,p.d) AS bB, vd(q0.ocB,q0.ocB)-19.36 AS cB) q1
  , LATERAL (SELECT q1.bA*q1.bA-q1.cA AS dA, q1.bB*q1.bB-q1.cB AS dB) q2
  , LATERAL (SELECT CASE WHEN q2.dA>0.0 THEN -q1.bA-sqrt(q2.dA) ELSE 1e9 END AS aNear,
                    CASE WHEN q2.dA>0.0 THEN -q1.bA+sqrt(q2.dA) ELSE -1e9 END AS aFar,
                    CASE WHEN q2.dB>0.0 THEN -q1.bB-sqrt(q2.dB) ELSE 1e9 END AS bNear,
                    CASE WHEN q2.dB>0.0 THEN -q1.bB+sqrt(q2.dB) ELSE -1e9 END AS bFar) q3
  , LATERAL (SELECT CASE WHEN q3.aNear>0.002 AND q3.aNear<1e8 AND NOT (q3.bNear<=q3.aNear AND q3.aNear<=q3.bFar)
                         THEN q3.aNear ELSE 1e9 END AS cEnter,
                    CASE WHEN q3.bNear<1e8 AND q3.bFar>0.002 AND q3.aNear<=q3.bFar AND q3.bFar<=q3.aFar
                         THEN q3.bFar ELSE 1e9 END AS cCav) q4
),

-- combine nearest hit; derive normals, shadow origin, light + reflect dirs
comb_b1 AS (
  SELECT p.idx, p.o, p.d, p.thr, p.acc,
         a2.tHit AS tHit, a1.tM AS tM, a0.tLet AS tLet, a2.tTerr AS tTerr,
         a3.isMirror AS isMirror, a3.isTerr AS isTerr, a3.isMiss AS isMiss,
         a3.hp AS hp, a3.sn AS sn, a4.tN AS tN, a4.rdir AS rdir,
         a5.shOrig AS shOrig, a4.ldir AS ldir, a4.distL AS distL
  FROM in_b1 p
  JOIN tori_hit_b1 th USING (idx) JOIN tori_nrm_b1 tn USING (idx)
  JOIN cyl_hit_b1 ch USING (idx) JOIN sph_hit_b1 sh USING (idx)
  JOIN planet_b1 pl USING (idx) JOIN terr_b1 tr USING (idx)
  , LATERAL (SELECT least(ch.tCyl, least(sh.tSph, th.tTorus)) AS tLet) a0
  , LATERAL (SELECT least(a0.tLet, pl.tC) AS tM) a1
  , LATERAL (SELECT least(a1.tM, tr.tTerr) AS tHit, tr.tTerr AS tTerr) a2
  , LATERAL (SELECT va(p.o, vm(p.d, least(a2.tHit,5000.0))) AS hp,
                    CASE WHEN a1.tM < tr.tTerr AND a1.tM < 1e8 THEN 1.0 ELSE 0.0 END AS isMirror,
                    CASE WHEN tr.tTerr <= a1.tM AND tr.tTerr < 1e8 THEN 1.0 ELSE 0.0 END AS isTerr,
                    CASE WHEN a1.tM >= 1e8 AND tr.tTerr >= 1e8 THEN 1.0 ELSE 0.0 END AS isMiss,
                    mix(mix(mix(sh.nSph, ch.nCyl, CASE WHEN ch.tCyl<=sh.tSph THEN 1.0 ELSE 0.0 END),
                            tn.nTorus, CASE WHEN th.tTorus<=ch.tCyl AND th.tTorus<=sh.tSph THEN 1.0 ELSE 0.0 END),
                        pl.nC, CASE WHEN pl.tC<a0.tLet THEN 1.0 ELSE 0.0 END) AS sn) a3
  , LATERAL (SELECT terrN(CAST(JSON_VALUE(a3.hp,'$[0]') AS DOUBLE), CAST(JSON_VALUE(a3.hp,'$[1]') AS DOUBLE)) AS tN, vref(p.d, a3.sn) AS rdir,
                    vn(vs(JSON_ARRAY(9.0,-30.0,52.0), a3.hp)) AS ldir,
                    sqrt(vd(vs(JSON_ARRAY(9.0,-30.0,52.0), a3.hp), vs(JSON_ARRAY(9.0,-30.0,52.0), a3.hp))) AS distL) a4
  , LATERAL (SELECT va(a3.hp, vm(mix(a3.sn, a4.tN, a3.isTerr), 0.02)) AS shOrig) a5
),

-- shadow: terrain (fixed 24-step set-based), analytic cyl/sph, tori march
sterr_b1 AS (
  SELECT cb.idx, MAX(
           (0.3 + i*0.6) < cb.distL-0.05 AND
           CAST(JSON_VALUE(cb.shOrig,'$[2]') AS DOUBLE) + (0.3+i*0.6)*CAST(JSON_VALUE(cb.ldir,'$[2]') AS DOUBLE) <
             terrH(CAST(JSON_VALUE(cb.shOrig,'$[0]') AS DOUBLE) + (0.3+i*0.6)*CAST(JSON_VALUE(cb.ldir,'$[0]') AS DOUBLE), CAST(JSON_VALUE(cb.shOrig,'$[1]') AS DOUBLE) + (0.3+i*0.6)*CAST(JSON_VALUE(cb.ldir,'$[1]') AS DOUBLE))
         ) AS occ
  FROM comb_b1 cb CROSS JOIN gs24
  GROUP BY cb.idx
),
socc_b1 AS (
  SELECT cb.idx, MAX(least(tBody, least(tCapA, tCapB)) < cb.distL-0.05) AS occ
  FROM comb_b1 cb CROSS JOIN cyls c
  , LATERAL (SELECT vs(c.b,c.a) AS ba, vs(cb.shOrig,c.a) AS oa) l0
  , LATERAL (SELECT l0.ba AS ba, vd(l0.ba,l0.ba) AS baba, vd(l0.ba,cb.ldir) AS bard,
                    vd(l0.ba,l0.oa) AS baoa, vd(cb.ldir,l0.oa) AS doa, vd(l0.oa,l0.oa) AS oaoa) l1
  , LATERAL (SELECT l1.baba-l1.bard*l1.bard AS aa, l1.baba*l1.doa-l1.baoa*l1.bard AS bb,
                    l1.baba*l1.oaoa-l1.baoa*l1.baoa-c.r*c.r*l1.baba AS cc) l2
  , LATERAL (SELECT l2.bb*l2.bb-l2.aa*l2.cc AS hh) l3
  , LATERAL (SELECT (-l2.bb-sqrt(greatest(l3.hh,0.0)))/l2.aa AS tb) l4
  , LATERAL (SELECT CASE WHEN l3.hh>=0.0 AND l2.aa>1e-9 AND l4.tb>0.002
                              AND (l1.baoa+l4.tb*l1.bard)>0.0 AND (l1.baoa+l4.tb*l1.bard)<l1.baba
                         THEN l4.tb ELSE 1e9 END AS tBody) l5
  , LATERAL (SELECT vd(l1.ba,cb.ldir) AS den) l6
  , LATERAL (SELECT vd(l1.ba,vs(c.a,cb.shOrig))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taA,
                    vd(l1.ba,vs(c.b,cb.shOrig))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taB) l7
  , LATERAL (SELECT vs(va(cb.shOrig,vm(cb.ldir,l7.taA)),c.a) AS dA, vs(va(cb.shOrig,vm(cb.ldir,l7.taB)),c.b) AS dB) l8
  , LATERAL (SELECT CASE WHEN abs(l6.den)>1e-9 AND l7.taA>0.002 AND vd(l8.dA,l8.dA)<=c.r*c.r THEN l7.taA ELSE 1e9 END AS tCapA,
                    CASE WHEN abs(l6.den)>1e-9 AND l7.taB>0.002 AND vd(l8.dB,l8.dB)<=c.r*c.r THEN l7.taB ELSE 1e9 END AS tCapB) l9
  GROUP BY cb.idx
),
soccs_b1 AS (
  SELECT cb.idx, CASE WHEN disc>0.0 AND tnear>0.002 AND tnear < cb.distL-0.05 THEN true ELSE false END AS occ
  FROM comb_b1 cb
  , LATERAL (SELECT vs(cb.shOrig, JSON_ARRAY(8.65,0.0,8.8)) AS oc) s0
  , LATERAL (SELECT vd(s0.oc,cb.ldir) AS bb, vd(s0.oc,s0.oc)-0.5184 AS cc) s1
  , LATERAL (SELECT s1.bb*s1.bb-s1.cc AS disc) s2
  , LATERAL (SELECT -s1.bb-sqrt(greatest(s2.disc,0.0)) AS tnear) s3
),
stmar_b1 AS (
  SELECT idx, shOrig AS o, ldir AS d, distL, CAST(0.02 AS DOUBLE) AS t, 0 AS iter FROM comb_b1
  UNION ALL
  SELECT idx, o, d, distL, t + toriSDF3(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[0]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*t), iter + 1
  FROM stmar_b1 WHERE iter < 48
),
stori_b1 AS (
  SELECT idx, CASE WHEN sdf < 0.01 AND tfin > 0.002 AND tfin < 260.0 AND tfin < distL-0.05 THEN true ELSE false END AS occ
  FROM (SELECT idx, t AS tfin, distL, toriSDF3(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[0]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*t) AS sdf
        FROM stmar_b1 WHERE iter = 48) z
),

-- per-bounce shading + accumulation + next ray
shade_b1 AS (
  SELECT cb.idx,
         -- new accumulated colour
         va(cb.acc, va(vm(JSON_ARRAY(1.0,1.0,1.0), cb.thr*spec*cb.isMirror),
                       va(vm(terrShade, cb.thr*cb.isTerr),
                          vm(sky(cb.d), cb.thr*cb.isMiss)))) AS acc,
         mix(cb.o, cb.shOrig, cb.isMirror) AS o,
         mix(cb.d, cb.rdir,   cb.isMirror) AS d,
         cb.thr * (1.0 - 0.28*cb.isMirror) AS thr,
         cb.isMirror AS alive
  FROM comb_b1 cb
  JOIN sterr_b1 se USING (idx) JOIN socc_b1 sc USING (idx)
  JOIN soccs_b1 ss USING (idx) JOIN stori_b1 st USING (idx)
  , LATERAL (SELECT (se.occ OR sc.occ OR ss.occ OR st.occ) AS shadowed) z0
  , LATERAL (SELECT CASE WHEN z0.shadowed THEN 0.0 ELSE 1.0 END AS lit) z1
  , LATERAL (SELECT power(greatest(vd(cb.ldir, cb.rdir),0.0), 60.0) * z1.lit AS spec,
                    greatest(vd(cb.ldir, cb.tN), 0.0) * z1.lit AS lambert,
                    least(1.0, greatest(0.0, (cb.tHit-70.0)/160.0)) AS fog) z2
  , LATERAL (SELECT va(vm(satb(vmul(terrCol(CAST(JSON_VALUE(cb.hp,'$[2]') AS DOUBLE)),
                          va(vm(JSON_ARRAY(0.45,0.56,0.8), 0.26 + 0.18*greatest(CAST(JSON_VALUE(cb.tN,'$[2]') AS DOUBLE),0.0)),
                             vm(JSON_ARRAY(1.06,0.97,0.82), 1.45*z2.lambert)))), 1.0 - z2.fog),
                      vm(JSON_ARRAY(0.66,0.78,0.96), z2.fog)) AS terrShade) z3
),
rays_b1 AS (
  SELECT idx, o, d, thr, acc, alive FROM shade_b1
  UNION ALL
  SELECT idx, o, d, thr, acc, 0.0 AS alive FROM px WHERE alive <= 0.5
),
-- ============================ bounce b2 ============================
in_b2 AS (SELECT idx, o, d, thr, acc FROM rays_b1 WHERE alive > 0.5),

-- terrain height-field march (lockstep; per-step expressions inlined)
m_b2 AS (
  SELECT idx, o, d,
         CASE WHEN CAST(JSON_VALUE(d,'$[2]') AS DOUBLE) < -0.001 THEN greatest(0.05, (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)-1.55)/(-CAST(JSON_VALUE(d,'$[2]') AS DOUBLE))) ELSE 0.05 END AS t,
         CAST(0.0 AS DOUBLE) AS prevT, CAST(1e9 AS DOUBLE) AS prevDiff,
         CAST(1e9 AS DOUBLE) AS result, 0 AS done, 0 AS iter
  FROM in_b2
  UNION ALL
  SELECT idx, o, d,
    CASE WHEN prevDiff > 0.0 AND (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))) <= 0.0 THEN t ELSE t + greatest(0.06, t*0.035) END,
    t,
    (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))),
    CASE WHEN prevDiff > 0.0 AND (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))) <= 0.0
              THEN prevT + (t - prevT)*prevDiff/(prevDiff - (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)))) ELSE 1e9 END,
    CASE WHEN prevDiff > 0.0 AND (CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)-terrH(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[0]') AS DOUBLE),CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+t*CAST(JSON_VALUE(d,'$[1]') AS DOUBLE))) <= 0.0 THEN 1
         WHEN CAST(JSON_VALUE(o,'$[2]') AS DOUBLE) + t*CAST(JSON_VALUE(d,'$[2]') AS DOUBLE) > 1.55 AND CAST(JSON_VALUE(d,'$[2]') AS DOUBLE) >= 0.0 THEN 2 ELSE 0 END,
    iter + 1
  FROM m_b2 WHERE done = 0 AND iter < 110
),
terr_b2 AS (SELECT idx, min(CASE WHEN done = 1 THEN result ELSE 1e9 END) AS tTerr FROM m_b2 GROUP BY idx),

-- tori sphere-trace march (48 accumulate steps, like DuckDB)
tmar_b2 AS (
  SELECT idx, o, d, CAST(0.02 AS DOUBLE) AS t, 0 AS iter FROM in_b2
  UNION ALL
  SELECT idx, o, d, t + toriSDF3(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[0]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*t), iter + 1
  FROM tmar_b2 WHERE iter < 48
),
tori_hit_b2 AS (
  SELECT idx, tfin, CASE WHEN sdf < 0.01 AND tfin > 0.002 AND tfin < 260.0 THEN tfin ELSE 1e9 END AS tTorus
  FROM (SELECT t1.idx, t1.t AS tfin,
               toriSDF3(CAST(JSON_VALUE(t1.o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(t1.d,'$[0]') AS DOUBLE)*t1.t, CAST(JSON_VALUE(t1.o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(t1.d,'$[1]') AS DOUBLE)*t1.t, CAST(JSON_VALUE(t1.o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(t1.d,'$[2]') AS DOUBLE)*t1.t) AS sdf
        FROM tmar_b2 t1 WHERE t1.iter = 48) z
),
tori_nrm_b2 AS (
  SELECT h.idx, h.tTorus,
         vn(JSON_ARRAY(toriSDF3(hx+0.0009,hy,hz)-toriSDF3(hx-0.0009,hy,hz),
                  toriSDF3(hx,hy+0.0009,hz)-toriSDF3(hx,hy-0.0009,hz),
                  toriSDF3(hx,hy,hz+0.0009)-toriSDF3(hx,hy,hz-0.0009))) AS nTorus
  FROM tori_hit_b2 h JOIN in_b2 p USING (idx),
       LATERAL (SELECT CAST(JSON_VALUE(p.o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(p.d,'$[0]') AS DOUBLE)*least(h.tTorus,5000.0) AS hx,
                       CAST(JSON_VALUE(p.o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(p.d,'$[1]') AS DOUBLE)*least(h.tTorus,5000.0) AS hy,
                       CAST(JSON_VALUE(p.o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(p.d,'$[2]') AS DOUBLE)*least(h.tTorus,5000.0) AS hz) g
),

-- analytic cylinders (rod = body + 2 disk caps), nearest per pixel
cyl_raw_b2 AS (
  SELECT p.idx, least(tBody, least(tCapA, tCapB)) AS t,
         mix(mix(vn(l1.ba), vn(vm(l1.ba,-1.0)),
                 CASE WHEN least(tBody,least(tCapA,tCapB)) = tCapA THEN 1.0 ELSE 0.0 END),
             vn(vs(va(p.o, vm(p.d, least(tBody,least(tCapA,tCapB)))),
                   va(c.a, vm(l1.ba, vd(vs(va(p.o, vm(p.d, least(tBody,least(tCapA,tCapB)))), c.a), l1.ba)/l1.baba)))),
             CASE WHEN least(tBody,least(tCapA,tCapB)) = tBody THEN 1.0 ELSE 0.0 END) AS n
  FROM in_b2 p CROSS JOIN cyls c
  , LATERAL (SELECT vs(c.b,c.a) AS ba, vs(p.o,c.a) AS oa) l0
  , LATERAL (SELECT l0.ba AS ba, l0.oa AS oa, vd(l0.ba,l0.ba) AS baba, vd(l0.ba,p.d) AS bard,
                    vd(l0.ba,l0.oa) AS baoa, vd(p.d,l0.oa) AS doa, vd(l0.oa,l0.oa) AS oaoa) l1
  , LATERAL (SELECT l1.baba - l1.bard*l1.bard AS aa, l1.baba*l1.doa - l1.baoa*l1.bard AS bb,
                    l1.baba*l1.oaoa - l1.baoa*l1.baoa - c.r*c.r*l1.baba AS cc) l2
  , LATERAL (SELECT l2.bb*l2.bb - l2.aa*l2.cc AS hh) l3
  , LATERAL (SELECT (-l2.bb - sqrt(greatest(l3.hh,0.0)))/l2.aa AS tb) l4
  , LATERAL (SELECT CASE WHEN l3.hh >= 0.0 AND l2.aa > 1e-9 AND l4.tb > 0.002
                              AND (l1.baoa + l4.tb*l1.bard) > 0.0 AND (l1.baoa + l4.tb*l1.bard) < l1.baba
                         THEN l4.tb ELSE 1e9 END AS tBody) l5
  , LATERAL (SELECT vd(l1.ba, p.d) AS den) l6
  , LATERAL (SELECT vd(l1.ba, vs(c.a, p.o))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taA,
                    vd(l1.ba, vs(c.b, p.o))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taB) l7
  , LATERAL (SELECT vs(va(p.o, vm(p.d, l7.taA)), c.a) AS dA, vs(va(p.o, vm(p.d, l7.taB)), c.b) AS dB) l8
  , LATERAL (SELECT CASE WHEN abs(l6.den)>1e-9 AND l7.taA>0.002 AND vd(l8.dA,l8.dA)<=c.r*c.r THEN l7.taA ELSE 1e9 END AS tCapA,
                    CASE WHEN abs(l6.den)>1e-9 AND l7.taB>0.002 AND vd(l8.dB,l8.dB)<=c.r*c.r THEN l7.taB ELSE 1e9 END AS tCapB) l9
),
cyl_hit_b2 AS (
  SELECT idx, t AS tCyl, n AS nCyl FROM (
    SELECT idx, t, n, row_number() OVER (PARTITION BY idx ORDER BY t) AS rn FROM cyl_raw_b2
  ) z WHERE rn = 1
),

-- i-dot sphere
sph_hit_b2 AS (
  SELECT p.idx,
         CASE WHEN disc > 0.0 AND tnear > 0.002 THEN tnear ELSE 1e9 END AS tSph,
         vn(vs(va(p.o, vm(p.d, CASE WHEN disc > 0.0 AND tnear > 0.002 THEN tnear ELSE 1e9 END)), JSON_ARRAY(8.65,0.0,8.8))) AS nSph
  FROM in_b2 p
  , LATERAL (SELECT vs(p.o, JSON_ARRAY(8.65,0.0,8.8)) AS oc) s0
  , LATERAL (SELECT vd(s0.oc, p.d) AS bb, vd(s0.oc, s0.oc) - 0.5184 AS cc) s1
  , LATERAL (SELECT s1.bb*s1.bb - s1.cc AS disc) s2
  , LATERAL (SELECT -s1.bb - sqrt(greatest(s2.disc,0.0)) AS tnear) s3
),

-- planet: sphere csgA with a spherical cavity csgB
planet_b2 AS (
  SELECT p.idx, least(cEnter, cCav) AS tC,
         mix(vn(vs(JSON_ARRAY(24.0,6.6,17.0), va(p.o, vm(p.d, least(cEnter,cCav))))),
             vn(vs(va(p.o, vm(p.d, least(cEnter,cCav))), JSON_ARRAY(24.0,12.0,17.5))),
             CASE WHEN cEnter <= cCav THEN 1.0 ELSE 0.0 END) AS nC
  FROM in_b2 p
  , LATERAL (SELECT vs(p.o, JSON_ARRAY(24.0,12.0,17.5)) AS ocA, vs(p.o, JSON_ARRAY(24.0,6.6,17.0)) AS ocB) q0
  , LATERAL (SELECT vd(q0.ocA,p.d) AS bA, vd(q0.ocA,q0.ocA)-36.0 AS cA,
                    vd(q0.ocB,p.d) AS bB, vd(q0.ocB,q0.ocB)-19.36 AS cB) q1
  , LATERAL (SELECT q1.bA*q1.bA-q1.cA AS dA, q1.bB*q1.bB-q1.cB AS dB) q2
  , LATERAL (SELECT CASE WHEN q2.dA>0.0 THEN -q1.bA-sqrt(q2.dA) ELSE 1e9 END AS aNear,
                    CASE WHEN q2.dA>0.0 THEN -q1.bA+sqrt(q2.dA) ELSE -1e9 END AS aFar,
                    CASE WHEN q2.dB>0.0 THEN -q1.bB-sqrt(q2.dB) ELSE 1e9 END AS bNear,
                    CASE WHEN q2.dB>0.0 THEN -q1.bB+sqrt(q2.dB) ELSE -1e9 END AS bFar) q3
  , LATERAL (SELECT CASE WHEN q3.aNear>0.002 AND q3.aNear<1e8 AND NOT (q3.bNear<=q3.aNear AND q3.aNear<=q3.bFar)
                         THEN q3.aNear ELSE 1e9 END AS cEnter,
                    CASE WHEN q3.bNear<1e8 AND q3.bFar>0.002 AND q3.aNear<=q3.bFar AND q3.bFar<=q3.aFar
                         THEN q3.bFar ELSE 1e9 END AS cCav) q4
),

-- combine nearest hit; derive normals, shadow origin, light + reflect dirs
comb_b2 AS (
  SELECT p.idx, p.o, p.d, p.thr, p.acc,
         a2.tHit AS tHit, a1.tM AS tM, a0.tLet AS tLet, a2.tTerr AS tTerr,
         a3.isMirror AS isMirror, a3.isTerr AS isTerr, a3.isMiss AS isMiss,
         a3.hp AS hp, a3.sn AS sn, a4.tN AS tN, a4.rdir AS rdir,
         a5.shOrig AS shOrig, a4.ldir AS ldir, a4.distL AS distL
  FROM in_b2 p
  JOIN tori_hit_b2 th USING (idx) JOIN tori_nrm_b2 tn USING (idx)
  JOIN cyl_hit_b2 ch USING (idx) JOIN sph_hit_b2 sh USING (idx)
  JOIN planet_b2 pl USING (idx) JOIN terr_b2 tr USING (idx)
  , LATERAL (SELECT least(ch.tCyl, least(sh.tSph, th.tTorus)) AS tLet) a0
  , LATERAL (SELECT least(a0.tLet, pl.tC) AS tM) a1
  , LATERAL (SELECT least(a1.tM, tr.tTerr) AS tHit, tr.tTerr AS tTerr) a2
  , LATERAL (SELECT va(p.o, vm(p.d, least(a2.tHit,5000.0))) AS hp,
                    CASE WHEN a1.tM < tr.tTerr AND a1.tM < 1e8 THEN 1.0 ELSE 0.0 END AS isMirror,
                    CASE WHEN tr.tTerr <= a1.tM AND tr.tTerr < 1e8 THEN 1.0 ELSE 0.0 END AS isTerr,
                    CASE WHEN a1.tM >= 1e8 AND tr.tTerr >= 1e8 THEN 1.0 ELSE 0.0 END AS isMiss,
                    mix(mix(mix(sh.nSph, ch.nCyl, CASE WHEN ch.tCyl<=sh.tSph THEN 1.0 ELSE 0.0 END),
                            tn.nTorus, CASE WHEN th.tTorus<=ch.tCyl AND th.tTorus<=sh.tSph THEN 1.0 ELSE 0.0 END),
                        pl.nC, CASE WHEN pl.tC<a0.tLet THEN 1.0 ELSE 0.0 END) AS sn) a3
  , LATERAL (SELECT terrN(CAST(JSON_VALUE(a3.hp,'$[0]') AS DOUBLE), CAST(JSON_VALUE(a3.hp,'$[1]') AS DOUBLE)) AS tN, vref(p.d, a3.sn) AS rdir,
                    vn(vs(JSON_ARRAY(9.0,-30.0,52.0), a3.hp)) AS ldir,
                    sqrt(vd(vs(JSON_ARRAY(9.0,-30.0,52.0), a3.hp), vs(JSON_ARRAY(9.0,-30.0,52.0), a3.hp))) AS distL) a4
  , LATERAL (SELECT va(a3.hp, vm(mix(a3.sn, a4.tN, a3.isTerr), 0.02)) AS shOrig) a5
),

-- shadow: terrain (fixed 24-step set-based), analytic cyl/sph, tori march
sterr_b2 AS (
  SELECT cb.idx, MAX(
           (0.3 + i*0.6) < cb.distL-0.05 AND
           CAST(JSON_VALUE(cb.shOrig,'$[2]') AS DOUBLE) + (0.3+i*0.6)*CAST(JSON_VALUE(cb.ldir,'$[2]') AS DOUBLE) <
             terrH(CAST(JSON_VALUE(cb.shOrig,'$[0]') AS DOUBLE) + (0.3+i*0.6)*CAST(JSON_VALUE(cb.ldir,'$[0]') AS DOUBLE), CAST(JSON_VALUE(cb.shOrig,'$[1]') AS DOUBLE) + (0.3+i*0.6)*CAST(JSON_VALUE(cb.ldir,'$[1]') AS DOUBLE))
         ) AS occ
  FROM comb_b2 cb CROSS JOIN gs24
  GROUP BY cb.idx
),
socc_b2 AS (
  SELECT cb.idx, MAX(least(tBody, least(tCapA, tCapB)) < cb.distL-0.05) AS occ
  FROM comb_b2 cb CROSS JOIN cyls c
  , LATERAL (SELECT vs(c.b,c.a) AS ba, vs(cb.shOrig,c.a) AS oa) l0
  , LATERAL (SELECT l0.ba AS ba, vd(l0.ba,l0.ba) AS baba, vd(l0.ba,cb.ldir) AS bard,
                    vd(l0.ba,l0.oa) AS baoa, vd(cb.ldir,l0.oa) AS doa, vd(l0.oa,l0.oa) AS oaoa) l1
  , LATERAL (SELECT l1.baba-l1.bard*l1.bard AS aa, l1.baba*l1.doa-l1.baoa*l1.bard AS bb,
                    l1.baba*l1.oaoa-l1.baoa*l1.baoa-c.r*c.r*l1.baba AS cc) l2
  , LATERAL (SELECT l2.bb*l2.bb-l2.aa*l2.cc AS hh) l3
  , LATERAL (SELECT (-l2.bb-sqrt(greatest(l3.hh,0.0)))/l2.aa AS tb) l4
  , LATERAL (SELECT CASE WHEN l3.hh>=0.0 AND l2.aa>1e-9 AND l4.tb>0.002
                              AND (l1.baoa+l4.tb*l1.bard)>0.0 AND (l1.baoa+l4.tb*l1.bard)<l1.baba
                         THEN l4.tb ELSE 1e9 END AS tBody) l5
  , LATERAL (SELECT vd(l1.ba,cb.ldir) AS den) l6
  , LATERAL (SELECT vd(l1.ba,vs(c.a,cb.shOrig))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taA,
                    vd(l1.ba,vs(c.b,cb.shOrig))/CASE WHEN abs(l6.den)<1e-12 THEN 1e-12 ELSE l6.den END AS taB) l7
  , LATERAL (SELECT vs(va(cb.shOrig,vm(cb.ldir,l7.taA)),c.a) AS dA, vs(va(cb.shOrig,vm(cb.ldir,l7.taB)),c.b) AS dB) l8
  , LATERAL (SELECT CASE WHEN abs(l6.den)>1e-9 AND l7.taA>0.002 AND vd(l8.dA,l8.dA)<=c.r*c.r THEN l7.taA ELSE 1e9 END AS tCapA,
                    CASE WHEN abs(l6.den)>1e-9 AND l7.taB>0.002 AND vd(l8.dB,l8.dB)<=c.r*c.r THEN l7.taB ELSE 1e9 END AS tCapB) l9
  GROUP BY cb.idx
),
soccs_b2 AS (
  SELECT cb.idx, CASE WHEN disc>0.0 AND tnear>0.002 AND tnear < cb.distL-0.05 THEN true ELSE false END AS occ
  FROM comb_b2 cb
  , LATERAL (SELECT vs(cb.shOrig, JSON_ARRAY(8.65,0.0,8.8)) AS oc) s0
  , LATERAL (SELECT vd(s0.oc,cb.ldir) AS bb, vd(s0.oc,s0.oc)-0.5184 AS cc) s1
  , LATERAL (SELECT s1.bb*s1.bb-s1.cc AS disc) s2
  , LATERAL (SELECT -s1.bb-sqrt(greatest(s2.disc,0.0)) AS tnear) s3
),
stmar_b2 AS (
  SELECT idx, shOrig AS o, ldir AS d, distL, CAST(0.02 AS DOUBLE) AS t, 0 AS iter FROM comb_b2
  UNION ALL
  SELECT idx, o, d, distL, t + toriSDF3(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[0]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*t), iter + 1
  FROM stmar_b2 WHERE iter < 48
),
stori_b2 AS (
  SELECT idx, CASE WHEN sdf < 0.01 AND tfin > 0.002 AND tfin < 260.0 AND tfin < distL-0.05 THEN true ELSE false END AS occ
  FROM (SELECT idx, t AS tfin, distL, toriSDF3(CAST(JSON_VALUE(o,'$[0]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[0]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[1]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[1]') AS DOUBLE)*t, CAST(JSON_VALUE(o,'$[2]') AS DOUBLE)+CAST(JSON_VALUE(d,'$[2]') AS DOUBLE)*t) AS sdf
        FROM stmar_b2 WHERE iter = 48) z
),

-- per-bounce shading + accumulation + next ray
shade_b2 AS (
  SELECT cb.idx,
         -- new accumulated colour
         va(cb.acc, va(vm(JSON_ARRAY(1.0,1.0,1.0), cb.thr*spec*cb.isMirror),
                       va(vm(terrShade, cb.thr*cb.isTerr),
                          vm(sky(cb.d), cb.thr*cb.isMiss)))) AS acc,
         mix(cb.o, cb.shOrig, cb.isMirror) AS o,
         mix(cb.d, cb.rdir,   cb.isMirror) AS d,
         cb.thr * (1.0 - 0.28*cb.isMirror) AS thr,
         cb.isMirror AS alive
  FROM comb_b2 cb
  JOIN sterr_b2 se USING (idx) JOIN socc_b2 sc USING (idx)
  JOIN soccs_b2 ss USING (idx) JOIN stori_b2 st USING (idx)
  , LATERAL (SELECT (se.occ OR sc.occ OR ss.occ OR st.occ) AS shadowed) z0
  , LATERAL (SELECT CASE WHEN z0.shadowed THEN 0.0 ELSE 1.0 END AS lit) z1
  , LATERAL (SELECT power(greatest(vd(cb.ldir, cb.rdir),0.0), 60.0) * z1.lit AS spec,
                    greatest(vd(cb.ldir, cb.tN), 0.0) * z1.lit AS lambert,
                    least(1.0, greatest(0.0, (cb.tHit-70.0)/160.0)) AS fog) z2
  , LATERAL (SELECT va(vm(satb(vmul(terrCol(CAST(JSON_VALUE(cb.hp,'$[2]') AS DOUBLE)),
                          va(vm(JSON_ARRAY(0.45,0.56,0.8), 0.26 + 0.18*greatest(CAST(JSON_VALUE(cb.tN,'$[2]') AS DOUBLE),0.0)),
                             vm(JSON_ARRAY(1.06,0.97,0.82), 1.45*z2.lambert)))), 1.0 - z2.fog),
                      vm(JSON_ARRAY(0.66,0.78,0.96), z2.fog)) AS terrShade) z3
),
rays_b2 AS (
  SELECT idx, o, d, thr, acc, alive FROM shade_b2
  UNION ALL
  SELECT idx, o, d, thr, acc, 0.0 AS alive FROM rays_b1 WHERE alive <= 0.5
)
SELECT idx,
  CAST(ROUND(clampS(0.0,1.0, POWER(GREATEST(CAST(JSON_VALUE(col,'$[0]') AS DOUBLE),0.0)*1.15, 1.0/2.2))*255) AS SIGNED) AS r,
  CAST(ROUND(clampS(0.0,1.0, POWER(GREATEST(CAST(JSON_VALUE(col,'$[1]') AS DOUBLE),0.0)*1.15, 1.0/2.2))*255) AS SIGNED) AS g,
  CAST(ROUND(clampS(0.0,1.0, POWER(GREATEST(CAST(JSON_VALUE(col,'$[2]') AS DOUBLE),0.0)*1.15, 1.0/2.2))*255) AS SIGNED) AS b
FROM (
  SELECT idx, va(acc, vm(sky(d), thr*alive)) AS col FROM rays_b2
) f
ORDER BY idx;

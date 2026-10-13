CREATE OR REPLACE MACRO tuplePlus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] + ch_ly[1], ch_lx[2] + ch_ly[2], ch_lx[3] + ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMinus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] - ch_ly[1], ch_lx[2] - ch_ly[2], ch_lx[3] - ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMultiply(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] * ch_ly[1], ch_lx[2] * ch_ly[2], ch_lx[3] * ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMultiplyByNumber(a, s) AS list_transform([a], ch_lx -> list_transform([s], ch_lk -> (ch_lx[1] * ch_lk, ch_lx[2] * ch_lk, ch_lx[3] * ch_lk))[1])[1];
CREATE OR REPLACE MACRO dotProduct(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> ch_lx[1] * ch_ly[1] + ch_lx[2] * ch_ly[2] + ch_lx[3] * ch_ly[3])[1])[1];
CREATE OR REPLACE MACRO L2Norm(a) AS list_transform([a], ch_lx -> sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3]))[1];
CREATE OR REPLACE MACRO L2Normalize(a) AS list_transform([a], ch_lx -> list_transform([sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3])], ch_ln -> (ch_lx[1] / ch_ln, ch_lx[2] / ch_ln, ch_lx[3] / ch_ln))[1])[1];
CREATE OR REPLACE MACRO clamp(x, lo, hi) AS greatest(lo, least(hi, x));
CREATE OR REPLACE MACRO indexOf(a, x) AS coalesce(list_position(a, x), 0);
CREATE OR REPLACE MACRO ch_numbers_n(n) AS TABLE SELECT range AS number FROM range(CAST(n AS BIGINT));
CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(x) AS BIGINT);
CREATE OR REPLACE MACRO toUInt8(x) AS CAST(trunc(x) AS UTINYINT);
CREATE OR REPLACE MACRO ch_u64(x) AS list_transform([CAST(x AS HUGEINT)], ch_lv -> CAST(CASE WHEN ch_lv < 0 THEN ch_lv + CAST('18446744073709551616' AS HUGEINT) ELSE ch_lv END AS UBIGINT))[1];
CREATE OR REPLACE MACRO ch_mul64(a, c) AS CAST((CAST(a AS UHUGEINT) * CAST(c AS UHUGEINT)) % CAST('18446744073709551616' AS UHUGEINT) AS UBIGINT);
CREATE OR REPLACE MACRO ch_shr_xor(x, s) AS list_transform([x], ch_lv -> xor(ch_lv, ch_lv >> s))[1];
CREATE OR REPLACE MACRO ch_intHash64(x) AS ch_shr_xor(ch_mul64(ch_shr_xor(ch_mul64(ch_shr_xor(xor(ch_u64(x), CAST(5544725790478674055 AS UBIGINT)), 33), 18397679294719823053), 33), 14181476777654086739), 33);
CREATE OR REPLACE MACRO ch_hash128to64(lo, hi) AS list_transform([hi], ch_lh -> list_transform([ch_shr_xor(ch_mul64(xor(lo, ch_lh), 11376068507788127593), 47)], ch_la -> ch_mul64(ch_shr_xor(ch_mul64(xor(ch_lh, ch_la), 11376068507788127593), 47), 11376068507788127593))[1])[1];
CREATE OR REPLACE MACRO cityHash64(a, b) AS ch_hash128to64(ch_intHash64(a), ch_intHash64(b)), (a, b, c) AS ch_hash128to64(ch_hash128to64(ch_intHash64(a), ch_intHash64(b)), ch_intHash64(c));
CREATE OR REPLACE MACRO ch_intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO z0() AS 2.5::DOUBLE;
CREATE OR REPLACE MACRO ty() AS 0.6::DOUBLE;
CREATE OR REPLACE MACRO target() AS (24.0::DOUBLE, 0.0::DOUBLE, 6.0::DOUBLE);
CREATE OR REPLACE MACRO eye() AS (24.0::DOUBLE, -45.0::DOUBLE, 12.0::DOUBLE);
CREATE OR REPLACE MACRO fovDeg() AS 36.0::DOUBLE;
CREATE OR REPLACE MACRO imgW() AS 48.0::DOUBLE;
CREATE OR REPLACE MACRO imgH() AS 24.0::DOUBLE;
CREATE OR REPLACE MACRO maxDepth() AS 1;
CREATE OR REPLACE MACRO lightPos() AS (9.0::DOUBLE, -30.0::DOUBLE, 52.0::DOUBLE);
CREATE OR REPLACE MACRO lightR() AS 5.0::DOUBLE;
CREATE OR REPLACE MACRO skyColor() AS (0.42::DOUBLE, 0.60::DOUBLE, 0.95::DOUBLE);
CREATE OR REPLACE MACRO hazeColor() AS (0.66::DOUBLE, 0.78::DOUBLE, 0.96::DOUBLE);
CREATE OR REPLACE MACRO fogStart() AS 70.0::DOUBLE;
CREATE OR REPLACE MACRO fogRange() AS 160.0::DOUBLE;
CREATE OR REPLACE MACRO floorA() AS (0.02::DOUBLE, 0.02::DOUBLE, 0.02::DOUBLE);
CREATE OR REPLACE MACRO floorB() AS (1.00::DOUBLE, 0.80::DOUBLE, 0.00::DOUBLE);
CREATE OR REPLACE MACRO tile() AS 5.0::DOUBLE;
CREATE OR REPLACE MACRO floorAmb() AS 0.30::DOUBLE;
CREATE OR REPLACE MACRO floorDiff() AS 0.85::DOUBLE;
CREATE OR REPLACE MACRO specColor() AS (1.0::DOUBLE, 1.0::DOUBLE, 1.0::DOUBLE);
CREATE OR REPLACE MACRO shininess() AS 60.0::DOUBLE;
CREATE OR REPLACE MACRO reflect_k() AS 0.72::DOUBLE;
CREATE OR REPLACE MACRO csgA() AS (24.0::DOUBLE, 12.0::DOUBLE, 17.5::DOUBLE);
CREATE OR REPLACE MACRO csgAr() AS 6.0::DOUBLE;
CREATE OR REPLACE MACRO csgB() AS (24.0::DOUBLE, 6.6::DOUBLE, 17.0::DOUBLE);
CREATE OR REPLACE MACRO csgBr() AS 4.4::DOUBLE;
CREATE OR REPLACE MACRO exposure() AS 1.15::DOUBLE;
CREATE OR REPLACE MACRO gamma() AS 2.2::DOUBLE;
CREATE OR REPLACE MACRO eps() AS 0.002::DOUBLE;
CREATE OR REPLACE MACRO marchSteps() AS 48;
CREATE OR REPLACE MACRO marchEps() AS 0.01::DOUBLE;
CREATE OR REPLACE MACRO marchMax() AS 260.0::DOUBLE;
CREATE OR REPLACE MACRO tAmp() AS 2.4::DOUBLE;
CREATE OR REPLACE MACRO tscale() AS 0.05::DOUBLE;
CREATE OR REPLACE MACRO tBias() AS 0.5::DOUBLE;
CREATE OR REPLACE MACRO maxTerrH() AS 1.55::DOUBLE;
CREATE OR REPLACE MACRO terrSteps() AS 110;
CREATE OR REPLACE MACRO terrShadowSteps() AS 24;
CREATE OR REPLACE MACRO skyAmb() AS (0.45::DOUBLE, 0.56::DOUBLE, 0.80::DOUBLE);
CREATE OR REPLACE MACRO sunCol() AS (1.06::DOUBLE, 0.97::DOUBLE, 0.82::DOUBLE);
CREATE OR REPLACE MACRO skyTop() AS (0.30::DOUBLE, 0.50::DOUBLE, 0.85::DOUBLE);
CREATE OR REPLACE MACRO rodR() AS 0.62::DOUBLE;
CREATE OR REPLACE MACRO oboxes() AS [((100000.0::DOUBLE,100000.0::DOUBLE,100000.0::DOUBLE),(1.0::DOUBLE,0.0::DOUBLE,0.0::DOUBLE),(0.0::DOUBLE,1.0::DOUBLE,0.0::DOUBLE),0.1::DOUBLE,0.1::DOUBLE)];
CREATE OR REPLACE MACRO cyls() AS [((5.75::DOUBLE,0.0::DOUBLE,2.5::DOUBLE),(5.75::DOUBLE,0.0::DOUBLE,9.5::DOUBLE),0.62::DOUBLE), ((8.65::DOUBLE,0.0::DOUBLE,2.5::DOUBLE),(8.65::DOUBLE,0.0::DOUBLE,7.5::DOUBLE),0.62::DOUBLE), ((16.8::DOUBLE,0.0::DOUBLE,2.5::DOUBLE),(16.8::DOUBLE,0.0::DOUBLE,9.5::DOUBLE),0.62::DOUBLE), ((17.15::DOUBLE,0.0::DOUBLE,5.0::DOUBLE),(20.05::DOUBLE,0.0::DOUBLE,7.6::DOUBLE),0.62::DOUBLE), ((17.35::DOUBLE,0.0::DOUBLE,5.1::DOUBLE),(20.05::DOUBLE,0.0::DOUBLE,2.5::DOUBLE),0.62::DOUBLE), ((22.4::DOUBLE,0.0::DOUBLE,2.5::DOUBLE),(22.4::DOUBLE,0.0::DOUBLE,9.5::DOUBLE),0.62::DOUBLE), ((26.15::DOUBLE,0.0::DOUBLE,2.5::DOUBLE),(26.15::DOUBLE,0.0::DOUBLE,9.5::DOUBLE),0.62::DOUBLE), ((22.35::DOUBLE,0.0::DOUBLE,6.0::DOUBLE),(26.15::DOUBLE,0.0::DOUBLE,6.0::DOUBLE),0.62::DOUBLE), ((34.82::DOUBLE,0.0::DOUBLE,3.5::DOUBLE),(34.82::DOUBLE,0.0::DOUBLE,7.5::DOUBLE),0.62::DOUBLE), ((37.72::DOUBLE,0.0::DOUBLE,3.5::DOUBLE),(37.72::DOUBLE,0.0::DOUBLE,7.5::DOUBLE),0.62::DOUBLE), ((45.35::DOUBLE,0.0::DOUBLE,4.6::DOUBLE),(48.85::DOUBLE,0.0::DOUBLE,4.6::DOUBLE),0.41333::DOUBLE)];
CREATE OR REPLACE MACRO rings() AS [(100000.0::DOUBLE,100000.0::DOUBLE,0.1::DOUBLE,0.05::DOUBLE,100000000.0::DOUBLE,100000000.0::DOUBLE,100000000.0::DOUBLE,100000000.0::DOUBLE)];
CREATE OR REPLACE MACRO spheres() AS [((8.65::DOUBLE,0.0::DOUBLE,8.8::DOUBLE),0.72::DOUBLE)];
CREATE OR REPLACE MACRO tori() AS [(1.3::DOUBLE,5.4::DOUBLE,2.28::DOUBLE,0.62::DOUBLE,(4.425::DOUBLE,0.0::DOUBLE,5.4::DOUBLE),(3.025::DOUBLE,100.0::DOUBLE,1.5::DOUBLE)), (12.95::DOUBLE,4.5::DOUBLE,1.425::DOUBLE,0.575::DOUBLE,(16.45::DOUBLE,0.0::DOUBLE,4.5::DOUBLE),(3.4::DOUBLE,100.0::DOUBLE,0.94::DOUBLE)), (30.4::DOUBLE,4.5::DOUBLE,1.475::DOUBLE,0.525::DOUBLE,(1000000.0::DOUBLE,0.0::DOUBLE,1000000.0::DOUBLE),(0.1::DOUBLE,0.1::DOUBLE,0.1::DOUBLE)), (36.27::DOUBLE,4.0::DOUBLE,1.45::DOUBLE,0.62::DOUBLE,(33.95::DOUBLE,0.0::DOUBLE,7.75::DOUBLE),(9.0::DOUBLE,100.0::DOUBLE,3.75::DOUBLE)), (41.9::DOUBLE,6.0::DOUBLE,1.025::DOUBLE,0.425::DOUBLE,(45.375::DOUBLE,0.0::DOUBLE,-0.25::DOUBLE),(3.475::DOUBLE,100.0::DOUBLE,6.25::DOUBLE)), (41.9::DOUBLE,3.9::DOUBLE,1.025::DOUBLE,0.425::DOUBLE,(36.375::DOUBLE,0.0::DOUBLE,7.7::DOUBLE),(5.525::DOUBLE,100.0::DOUBLE,3.8::DOUBLE)), (47.25::DOUBLE,4.55::DOUBLE,1.585::DOUBLE,0.465::DOUBLE,(50.8::DOUBLE,0.0::DOUBLE,3.87::DOUBLE),(3.25::DOUBLE,100.0::DOUBLE,0.32::DOUBLE))];
CREATE OR REPLACE MACRO va(a, b) AS tuplePlus(a, b);
CREATE OR REPLACE MACRO vs(a, b) AS tupleMinus(a, b);
CREATE OR REPLACE MACRO vm(a, s) AS tupleMultiplyByNumber(a, s);
CREATE OR REPLACE MACRO vd(a, b) AS dotProduct(a, b);
CREATE OR REPLACE MACRO vn(a) AS L2Normalize(a);
CREATE OR REPLACE MACRO vc(a, b) AS (a[2]*b[3] - a[3]*b[2], a[3]*b[1] - a[1]*b[3], a[1]*b[2] - a[2]*b[1]);
CREATE OR REPLACE MACRO vref(d, n) AS tupleMinus(d, tupleMultiplyByNumber(n, 2.0::DOUBLE * dotProduct(d, n)));
CREATE OR REPLACE MACRO camFwd() AS L2Normalize(tupleMinus(target(), eye()));
CREATE OR REPLACE MACRO camRight() AS L2Normalize(vc(L2Normalize(tupleMinus(target(), eye())), (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE)));
CREATE OR REPLACE MACRO camUp() AS vc(camRight(), camFwd());
CREATE OR REPLACE MACRO tanHalf() AS tan(fovDeg() * 0.5::DOUBLE * pi() / 180.0::DOUBLE);
CREATE OR REPLACE MACRO aspect() AS imgW() / imgH();
CREATE OR REPLACE MACRO oboxT(O, D, b) AS list_transform([vd(b[2], vs(b[1],O))], eu__oboxT -> list_transform([vd(b[2],D)], fu__oboxT -> list_transform([vd(b[3], vs(b[1],O))], ev__oboxT -> list_transform([vd(b[3],D)], fv__oboxT -> list_transform([b[1][2] - O[2]], ew__oboxT -> list_transform([D[2]], fw__oboxT -> list_transform([least((eu__oboxT-b[4])/fu__oboxT,(eu__oboxT+b[4])/fu__oboxT)], lou__oboxT -> list_transform([greatest((eu__oboxT-b[4])/fu__oboxT,(eu__oboxT+b[4])/fu__oboxT)], hiu__oboxT -> list_transform([least((ev__oboxT-b[5])/fv__oboxT,(ev__oboxT+b[5])/fv__oboxT)], lov__oboxT -> list_transform([greatest((ev__oboxT-b[5])/fv__oboxT,(ev__oboxT+b[5])/fv__oboxT)], hiv__oboxT -> list_transform([least((ew__oboxT-ty())/fw__oboxT,(ew__oboxT+ty())/fw__oboxT)], low__oboxT -> list_transform([greatest((ew__oboxT-ty())/fw__oboxT,(ew__oboxT+ty())/fw__oboxT)], hiw__oboxT -> list_transform([greatest(lou__oboxT, greatest(lov__oboxT, low__oboxT))], tn__oboxT -> list_transform([least(hiu__oboxT, least(hiv__oboxT, hiw__oboxT))], tf__oboxT -> if(tf__oboxT >= tn__oboxT AND tf__oboxT > eps(), if(tn__oboxT > eps(), tn__oboxT, tf__oboxT), 1e9))[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
CREATE OR REPLACE MACRO oboxN(hp, b) AS list_transform([vd(b[2], vs(hp,b[1]))/b[4]], qu__oboxN -> list_transform([vd(b[3], vs(hp,b[1]))/b[5]], qv__oboxN -> list_transform([(hp[2] - b[1][2])/ty()], qw__oboxN -> if(abs(qu__oboxN) >= abs(qv__oboxN) AND abs(qu__oboxN) >= abs(qw__oboxN), vm(b[2], if(qu__oboxN>0.0::DOUBLE,1.0::DOUBLE,-1.0::DOUBLE)), if(abs(qv__oboxN) >= abs(qw__oboxN), vm(b[3], if(qv__oboxN>0.0::DOUBLE,1.0::DOUBLE,-1.0::DOUBLE)), (0.0::DOUBLE, if(qw__oboxN>0.0::DOUBLE,1.0::DOUBLE,-1.0::DOUBLE), 0.0::DOUBLE))))[1])[1])[1];
CREATE OR REPLACE MACRO cylBodyT(O, D, A, B, r) AS list_transform([vs(B,A)], ba__cylBodyT -> list_transform([vs(O,A)], oa__cylBodyT -> list_transform([vd(ba__cylBodyT,ba__cylBodyT)], baba__cylBodyT -> list_transform([vd(ba__cylBodyT,D)], bard__cylBodyT -> list_transform([vd(ba__cylBodyT,oa__cylBodyT)], baoa__cylBodyT -> list_transform([baba__cylBodyT - bard__cylBodyT*bard__cylBodyT], aa__cylBodyT -> list_transform([baba__cylBodyT*vd(D,oa__cylBodyT) - baoa__cylBodyT*bard__cylBodyT], bb__cylBodyT -> list_transform([baba__cylBodyT*vd(oa__cylBodyT,oa__cylBodyT) - baoa__cylBodyT*baoa__cylBodyT - r*r*baba__cylBodyT], cc__cylBodyT -> list_transform([bb__cylBodyT*bb__cylBodyT - aa__cylBodyT*cc__cylBodyT], hh__cylBodyT -> list_transform([(-bb__cylBodyT - sqrt(greatest(hh__cylBodyT,0.0::DOUBLE)))/aa__cylBodyT], t__cylBodyT -> list_transform([baoa__cylBodyT + t__cylBodyT*bard__cylBodyT], y__cylBodyT -> if(hh__cylBodyT >= 0.0::DOUBLE AND aa__cylBodyT > 1e-9 AND t__cylBodyT > eps() AND y__cylBodyT > 0.0::DOUBLE AND y__cylBodyT < baba__cylBodyT, t__cylBodyT, 1e9))[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
CREATE OR REPLACE MACRO diskT(O, D, P, ax, r) AS list_transform([vd(ax, D)], den__diskT -> list_transform([vd(ax, vs(P,O))/den__diskT], t__diskT -> list_transform([va(O, vm(D, t__diskT))], hp__diskT -> if(abs(den__diskT) > 1e-9 AND t__diskT > eps() AND vd(vs(hp__diskT,P), vs(hp__diskT,P)) <= r*r, t__diskT, 1e9))[1])[1])[1];
CREATE OR REPLACE MACRO rodT(O, D, A, B, r) AS least(cylBodyT(O,D,A,B,r), least(diskT(O,D,A,vs(B,A),r), diskT(O,D,B,vs(B,A),r)));
CREATE OR REPLACE MACRO rodN(O, D, A, B, r) AS list_transform([vs(B,A)], ba__rodN -> list_transform([cylBodyT(O,D,A,B,r)], tL__rodN -> list_transform([diskT(O,D,A,ba__rodN,r)], tA__rodN -> list_transform([diskT(O,D,B,ba__rodN,r)], tB__rodN -> list_transform([least(tL__rodN, least(tA__rodN,tB__rodN))], tm__rodN -> list_transform([va(O, vm(D,tm__rodN))], hp__rodN -> if(tm__rodN = tL__rodN, vn(vs(hp__rodN, va(A, vm(ba__rodN, vd(vs(hp__rodN,A),ba__rodN)/vd(ba__rodN,ba__rodN))))), if(tm__rodN = tA__rodN, vn(vm(ba__rodN,-1.0::DOUBLE)), vn(ba__rodN))))[1])[1])[1])[1])[1])[1];
CREATE OR REPLACE MACRO ringT(O, D, rg) AS list_transform([greatest(D[1]*D[1] + D[3]*D[3], 1e-12)], a2__ringT -> list_transform([(O[1]-rg[1])*D[1] + (O[3]-rg[2])*D[3]], hb__ringT -> list_transform([(O[1]-rg[1])*(O[1]-rg[1]) + (O[3]-rg[2])*(O[3]-rg[2])], rr__ringT -> list_transform([hb__ringT*hb__ringT - a2__ringT*(rr__ringT - rg[3]*rg[3])], dO__ringT -> list_transform([hb__ringT*hb__ringT - a2__ringT*(rr__ringT - rg[4]*rg[4])], dI__ringT -> list_transform([if(dO__ringT > 0.0::DOUBLE, (-hb__ringT - sqrt(dO__ringT))/a2__ringT, 1e9)], toN__ringT -> list_transform([if(dO__ringT > 0.0::DOUBLE, (-hb__ringT + sqrt(dO__ringT))/a2__ringT, -1e9)], toF__ringT -> list_transform([if(dI__ringT > 0.0::DOUBLE, (-hb__ringT - sqrt(dI__ringT))/a2__ringT, 1e9)], tiN__ringT -> list_transform([if(dI__ringT > 0.0::DOUBLE, (-hb__ringT + sqrt(dI__ringT))/a2__ringT, -1e9)], tiF__ringT -> list_transform([least((-ty()-O[2])/D[2], (ty()-O[2])/D[2])], ty0__ringT -> list_transform([greatest((-ty()-O[2])/D[2], (ty()-O[2])/D[2])], ty1__ringT -> list_transform([greatest(least((rg[5]-O[1])/D[1],(rg[7]-O[1])/D[1]), least((rg[6]-O[3])/D[3],(rg[8]-O[3])/D[3]))], tcN__ringT -> list_transform([least(greatest((rg[5]-O[1])/D[1],(rg[7]-O[1])/D[1]), greatest((rg[6]-O[3])/D[3],(rg[8]-O[3])/D[3]))], tcF__ringT -> list_transform([greatest(ty0__ringT, toN__ringT)], aN__ringT -> list_transform([least(ty1__ringT, toF__ringT)], aF__ringT -> list_transform([if(aN__ringT < aF__ringT AND aN__ringT > eps() AND NOT (tiN__ringT < aN__ringT AND aN__ringT < tiF__ringT) AND NOT (tcN__ringT < aN__ringT AND aN__ringT < tcF__ringT), aN__ringT, 1e9)], e1__ringT -> list_transform([if(aN__ringT < tiF__ringT AND tiF__ringT < aF__ringT AND tiF__ringT > eps() AND NOT (tcN__ringT < tiF__ringT AND tiF__ringT < tcF__ringT), tiF__ringT, 1e9)], e2__ringT -> list_transform([if(aN__ringT < tcF__ringT AND tcF__ringT < aF__ringT AND tcF__ringT > eps() AND NOT (tiN__ringT < tcF__ringT AND tcF__ringT < tiF__ringT), tcF__ringT, 1e9)], e3__ringT -> list_transform([least(e1__ringT, least(e2__ringT, e3__ringT))], tm__ringT -> tm__ringT)[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
CREATE OR REPLACE MACRO ringN(O, D, rg) AS list_transform([greatest(D[1]*D[1] + D[3]*D[3], 1e-12)], a2__ringN -> list_transform([(O[1]-rg[1])*D[1] + (O[3]-rg[2])*D[3]], hb__ringN -> list_transform([(O[1]-rg[1])*(O[1]-rg[1]) + (O[3]-rg[2])*(O[3]-rg[2])], rr__ringN -> list_transform([hb__ringN*hb__ringN - a2__ringN*(rr__ringN - rg[3]*rg[3])], dO__ringN -> list_transform([hb__ringN*hb__ringN - a2__ringN*(rr__ringN - rg[4]*rg[4])], dI__ringN -> list_transform([if(dO__ringN > 0.0::DOUBLE, (-hb__ringN - sqrt(dO__ringN))/a2__ringN, 1e9)], toN__ringN -> list_transform([if(dO__ringN > 0.0::DOUBLE, (-hb__ringN + sqrt(dO__ringN))/a2__ringN, -1e9)], toF__ringN -> list_transform([if(dI__ringN > 0.0::DOUBLE, (-hb__ringN - sqrt(dI__ringN))/a2__ringN, 1e9)], tiN__ringN -> list_transform([if(dI__ringN > 0.0::DOUBLE, (-hb__ringN + sqrt(dI__ringN))/a2__ringN, -1e9)], tiF__ringN -> list_transform([least((-ty()-O[2])/D[2], (ty()-O[2])/D[2])], ty0__ringN -> list_transform([greatest((-ty()-O[2])/D[2], (ty()-O[2])/D[2])], ty1__ringN -> list_transform([greatest(least((rg[5]-O[1])/D[1],(rg[7]-O[1])/D[1]), least((rg[6]-O[3])/D[3],(rg[8]-O[3])/D[3]))], tcN__ringN -> list_transform([least(greatest((rg[5]-O[1])/D[1],(rg[7]-O[1])/D[1]), greatest((rg[6]-O[3])/D[3],(rg[8]-O[3])/D[3]))], tcF__ringN -> list_transform([greatest(ty0__ringN, toN__ringN)], aN__ringN -> list_transform([least(ty1__ringN, toF__ringN)], aF__ringN -> list_transform([if(aN__ringN < aF__ringN AND aN__ringN > eps() AND NOT (tiN__ringN < aN__ringN AND aN__ringN < tiF__ringN) AND NOT (tcN__ringN < aN__ringN AND aN__ringN < tcF__ringN), aN__ringN, 1e9)], e1__ringN -> list_transform([if(aN__ringN < tiF__ringN AND tiF__ringN < aF__ringN AND tiF__ringN > eps() AND NOT (tcN__ringN < tiF__ringN AND tiF__ringN < tcF__ringN), tiF__ringN, 1e9)], e2__ringN -> list_transform([if(aN__ringN < tcF__ringN AND tcF__ringN < aF__ringN AND tcF__ringN > eps() AND NOT (tiN__ringN < tcF__ringN AND tcF__ringN < tiF__ringN), tcF__ringN, 1e9)], e3__ringN -> list_transform([least(e1__ringN, least(e2__ringN, e3__ringN))], tm__ringN -> list_transform([va(O, vm(D, tm__ringN))], hp__ringN -> if(tm__ringN = e1__ringN, if(toN__ringN >= ty0__ringN, vn((hp__ringN[1]-rg[1], 0.0::DOUBLE, hp__ringN[3]-rg[2])), (0.0::DOUBLE, if(D[2]>0.0::DOUBLE,-1.0::DOUBLE,1.0::DOUBLE), 0.0::DOUBLE)), if(tm__ringN = e2__ringN, vn((rg[1]-hp__ringN[1], 0.0::DOUBLE, rg[2]-hp__ringN[3])),    if(greatest((rg[5]-O[1])/D[1],(rg[7]-O[1])/D[1]) <= greatest((rg[6]-O[3])/D[3],(rg[8]-O[3])/D[3]),       (if(D[1]>0.0::DOUBLE,-1.0::DOUBLE,1.0::DOUBLE),0.0::DOUBLE,0.0::DOUBLE), (0.0::DOUBLE,0.0::DOUBLE,if(D[3]>0.0::DOUBLE,-1.0::DOUBLE,1.0::DOUBLE))))))[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
CREATE OR REPLACE MACRO sphT(O, D, C, r) AS if(dotProduct(vs(O,C),D)*dotProduct(vs(O,C),D) - (dotProduct(vs(O,C),vs(O,C))-r*r) > 0.0::DOUBLE
                     AND -dotProduct(vs(O,C),D) - sqrt(greatest(dotProduct(vs(O,C),D)*dotProduct(vs(O,C),D)-(dotProduct(vs(O,C),vs(O,C))-r*r),0.0::DOUBLE)) > eps(),
                     -dotProduct(vs(O,C),D) - sqrt(greatest(dotProduct(vs(O,C),D)*dotProduct(vs(O,C),D)-(dotProduct(vs(O,C),vs(O,C))-r*r),0.0::DOUBLE)), 1e9);
CREATE OR REPLACE MACRO sphPair(o, d, c, rad) AS if(
        dotProduct(vs(o,c), d) * dotProduct(vs(o,c), d) - (dotProduct(vs(o,c), vs(o,c)) - rad*rad) > 0.0::DOUBLE,
        (-dotProduct(vs(o,c), d) - sqrt(greatest(dotProduct(vs(o,c), d) * dotProduct(vs(o,c), d) - (dotProduct(vs(o,c), vs(o,c)) - rad*rad), 0.0::DOUBLE)),
         -dotProduct(vs(o,c), d) + sqrt(greatest(dotProduct(vs(o,c), d) * dotProduct(vs(o,c), d) - (dotProduct(vs(o,c), vs(o,c)) - rad*rad), 0.0::DOUBLE))),
        (1e9, -1e9));
CREATE OR REPLACE MACRO tsdf(p, tl) AS sqrt(pow(sqrt((p[1]-tl[1])*(p[1]-tl[1])+(p[3]-tl[2])*(p[3]-tl[2])) - tl[3], 2) + p[2]*p[2]) - tl[4];
CREATE OR REPLACE MACRO bsdf(p, C, H) AS sqrt(pow(greatest(abs(p[1]-C[1])-H[1],0.0::DOUBLE),2) + pow(greatest(abs(p[2]-C[2])-H[2],0.0::DOUBLE),2) + pow(greatest(abs(p[3]-C[3])-H[3],0.0::DOUBLE),2))
                + least(greatest(abs(p[1]-C[1])-H[1], greatest(abs(p[2]-C[2])-H[2], abs(p[3]-C[3])-H[3])), 0.0::DOUBLE);
CREATE OR REPLACE MACRO toriSDF(p) AS list_min(list_transform(tori(), tl__toriSDF -> greatest(tsdf(p, tl__toriSDF), -bsdf(p, tl__toriSDF[5], tl__toriSDF[6]))));
CREATE OR REPLACE MACRO marchTorus(O, D) AS list_transform([list_reduce(range(marchSteps()), (t__marchTorus, i__marchTorus) -> t__marchTorus + toriSDF(va(O, vm(D, t__marchTorus))), 0.02::DOUBLE)], tfin__marchTorus -> if(toriSDF(va(O, vm(D, tfin__marchTorus))) < marchEps() AND tfin__marchTorus > eps() AND tfin__marchTorus < marchMax(), tfin__marchTorus, 1e9))[1];
CREATE OR REPLACE MACRO torusN(O, D, t) AS list_transform([va(O, vm(D, t))], p__torusN -> vn(va(va(vm((1.0::DOUBLE,-1.0::DOUBLE,-1.0::DOUBLE), toriSDF(va(p__torusN, (0.0009::DOUBLE,-0.0009::DOUBLE,-0.0009::DOUBLE)))),
                                       vm((-1.0::DOUBLE,-1.0::DOUBLE,1.0::DOUBLE), toriSDF(va(p__torusN, (-0.0009::DOUBLE,-0.0009::DOUBLE,0.0009::DOUBLE))))),
                                    va(vm((-1.0::DOUBLE,1.0::DOUBLE,-1.0::DOUBLE), toriSDF(va(p__torusN, (-0.0009::DOUBLE,0.0009::DOUBLE,-0.0009::DOUBLE)))),
                                       vm((1.0::DOUBLE,1.0::DOUBLE,1.0::DOUBLE), toriSDF(va(p__torusN, (0.0009::DOUBLE,0.0009::DOUBLE,0.0009::DOUBLE))))))))[1];
CREATE OR REPLACE MACRO planeT(o, d) AS if(d[3] < -eps() AND o[3] > 0.0::DOUBLE, -o[3] / d[3], 1e9);
CREATE OR REPLACE MACRO occluded(o, d, maxd) AS list_bool_or(list_transform(oboxes(), b__occluded -> oboxT(o, d, b__occluded) < maxd))
        OR list_bool_or(list_transform(cyls(), c__occluded -> rodT(o, d, c__occluded[1], c__occluded[2], c__occluded[3]) < maxd))
        OR list_bool_or(list_transform(rings(), rg__occluded -> ringT(o, d, rg__occluded) < maxd))
        OR list_bool_or(list_transform(spheres(), sp__occluded -> sphT(o, d, sp__occluded[1], sp__occluded[2]) < maxd))
        OR (marchTorus(o, d) < maxd);
CREATE OR REPLACE MACRO mix(a, b, t) AS tuplePlus(a, tupleMultiplyByNumber(tupleMinus(b, a), t));
CREATE OR REPLACE MACRO sky(d) AS mix(hazeColor(), skyTop(), greatest(0.0::DOUBLE, least(1.0::DOUBLE, d[3]*1.5::DOUBLE)));
CREATE OR REPLACE MACRO rnd(a, b, c) AS (cityHash64(a, b, c) % 1048576) / 1048576.0::DOUBLE;
CREATE OR REPLACE MACRO vmul(a, b) AS tupleMultiply(a, b);
CREATE OR REPLACE MACRO lerpS(a, b, t) AS a*(1.0::DOUBLE-t) + b*t;
CREATE OR REPLACE MACRO clamp3(lo, hi, x) AS greatest(lo, least(hi, x));
CREATE OR REPLACE MACRO ch_wrap64(x) AS CAST(((x + 9223372036854775808) % 18446744073709551616 + 18446744073709551616) % 18446744073709551616 - 9223372036854775808 AS BIGINT);
CREATE OR REPLACE MACRO hashf(ix, iy) AS ((((ch_wrap64(((CAST(ix AS HUGEINT) + 4096) + (CAST(iy AS HUGEINT) + 4096) * 8192) * 1103515245 + 12345)) % 2147483648) * 1103515245 + 12345) % 2147483648 % 65536) / 65536.0::DOUBLE;
CREATE OR REPLACE MACRO fade(t) AS t*t*t*(t*(t*6.0::DOUBLE-15.0::DOUBLE)+10.0::DOUBLE);
CREATE OR REPLACE MACRO vnoise(x, y) AS lerpS(lerpS(hashf(toInt64(floor(x)),   toInt64(floor(y))),   hashf(toInt64(floor(x))+1, toInt64(floor(y))),   fade(x-floor(x))),
                     lerpS(hashf(toInt64(floor(x)),   toInt64(floor(y))+1), hashf(toInt64(floor(x))+1, toInt64(floor(y))+1), fade(x-floor(x))),
                     fade(y-floor(y)));
CREATE OR REPLACE MACRO fbm(x, y) AS (vnoise(x,y)*0.5::DOUBLE + vnoise(x*2.0::DOUBLE,y*2.0::DOUBLE)*0.25::DOUBLE + vnoise(x*4.0::DOUBLE,y*4.0::DOUBLE)*0.125::DOUBLE + vnoise(x*8.0::DOUBLE,y*8.0::DOUBLE)*0.0625::DOUBLE)/0.9375::DOUBLE;
CREATE OR REPLACE MACRO terrH(x, y) AS tAmp()*(fbm(x*tscale(), y*tscale()) - tBias());
CREATE OR REPLACE MACRO terrN(x, y) AS vn(((terrH(x-0.25::DOUBLE,y)-terrH(x+0.25::DOUBLE,y))/0.5::DOUBLE, (terrH(x,y-0.25::DOUBLE)-terrH(x,y+0.25::DOUBLE))/0.5::DOUBLE, 1.0::DOUBLE));
CREATE OR REPLACE MACRO satb(c) AS mix((c[1]*0.30::DOUBLE+c[2]*0.59::DOUBLE+c[3]*0.11::DOUBLE, c[1]*0.30::DOUBLE+c[2]*0.59::DOUBLE+c[3]*0.11::DOUBLE, c[1]*0.30::DOUBLE+c[2]*0.59::DOUBLE+c[3]*0.11::DOUBLE), c, 1.45::DOUBLE);
CREATE OR REPLACE MACRO terrCol(h) AS mix(mix(mix(mix((0.06::DOUBLE,0.24::DOUBLE,0.55::DOUBLE), (0.84::DOUBLE,0.68::DOUBLE,0.32::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h+0.6::DOUBLE)/0.4::DOUBLE)),
                        (0.16::DOUBLE,0.52::DOUBLE,0.09::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h+0.1::DOUBLE)/0.5::DOUBLE)),
                    (0.46::DOUBLE,0.32::DOUBLE,0.22::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h-0.6::DOUBLE)/0.4::DOUBLE)),
                (0.97::DOUBLE,0.98::DOUBLE,1.0::DOUBLE), clamp3(0.0::DOUBLE,1.0::DOUBLE,(h-1.0::DOUBLE)/0.3::DOUBLE));
CREATE OR REPLACE MACRO marchTerr(O, D) AS list_transform([list_reduce(range(terrSteps()), (st__marchTerr, i__marchTerr) -> if(st__marchTerr[4] < 1e8 OR st__marchTerr[4] > 1.5e9::DOUBLE, st__marchTerr,
             list_transform([O[3] + st__marchTerr[1]*D[3] - terrH(O[1] + st__marchTerr[1]*D[1], O[2] + st__marchTerr[1]*D[2])], dc__marchTerr ->
                if(st__marchTerr[3] > 0.0::DOUBLE AND dc__marchTerr <= 0.0::DOUBLE, (st__marchTerr[1], st__marchTerr[1], dc__marchTerr, st__marchTerr[2] + (st__marchTerr[1]-st__marchTerr[2])*st__marchTerr[3]/(st__marchTerr[3]-dc__marchTerr)),
                   if(O[3] + st__marchTerr[1]*D[3] > maxTerrH() AND D[3] >= 0.0::DOUBLE, (st__marchTerr[1], st__marchTerr[1], dc__marchTerr, 2e9),
                      (st__marchTerr[1] + greatest(0.45::DOUBLE, st__marchTerr[1]*0.008::DOUBLE), st__marchTerr[1], dc__marchTerr, 1e9))))[1]), (if(D[3] < -0.001::DOUBLE, greatest(0.05::DOUBLE, (O[3] - maxTerrH())/(-D[3])), 0.05::DOUBLE), 0.0::DOUBLE, 1e9, 1e9))], rr__marchTerr -> if(rr__marchTerr[4] < 1e8, rr__marchTerr[4], 1e9))[1];
CREATE OR REPLACE MACRO terrShadow(P, dir, maxd) AS list_reduce(range(terrShadowSteps()), (blk__terrShadow, i__terrShadow) -> blk__terrShadow OR
            (((0.3::DOUBLE + i__terrShadow*0.6::DOUBLE) < maxd) AND (P[3] + (0.3::DOUBLE+i__terrShadow*0.6::DOUBLE)*dir[3] < terrH(P[1] + (0.3::DOUBLE+i__terrShadow*0.6::DOUBLE)*dir[1], P[2] + (0.3::DOUBLE+i__terrShadow*0.6::DOUBLE)*dir[2]))), 0) > 0;
CREATE OR REPLACE MACRO oneBounce(s, lpos) AS list_transform([s[1]], o__oneBounce -> list_transform([s[2]], d__oneBounce -> list_transform([s[3]], thr__oneBounce -> list_transform([s[4]], acc__oneBounce -> list_transform([list_transform(oboxes(), b__oneBounce -> oboxT(o__oneBounce, d__oneBounce, b__oneBounce))], oboxHits__oneBounce -> list_transform([list_transform(cyls(), c__oneBounce -> rodT(o__oneBounce, d__oneBounce, c__oneBounce[1], c__oneBounce[2], c__oneBounce[3]))], cylHits__oneBounce -> list_transform([list_transform(rings(), rg__oneBounce -> ringT(o__oneBounce, d__oneBounce, rg__oneBounce))], ringHits__oneBounce -> list_transform([list_transform(spheres(), sp__oneBounce -> sphT(o__oneBounce, d__oneBounce, sp__oneBounce[1], sp__oneBounce[2]))], sphHits__oneBounce -> list_transform([marchTorus(o__oneBounce, d__oneBounce)], tTorus__oneBounce -> list_transform([list_min(oboxHits__oneBounce)], tBox__oneBounce -> list_transform([list_min(cylHits__oneBounce)], tCyl__oneBounce -> list_transform([list_min(ringHits__oneBounce)], tRing__oneBounce -> list_transform([list_min(sphHits__oneBounce)], tSph__oneBounce -> list_transform([least(least(tBox__oneBounce, tCyl__oneBounce), least(tRing__oneBounce, least(tSph__oneBounce, tTorus__oneBounce)))], tLet__oneBounce -> list_transform([marchTerr(o__oneBounce, d__oneBounce)], tTerr__oneBounce -> list_transform([sphPair(o__oneBounce, d__oneBounce, csgA(), csgAr())], pairA__oneBounce -> list_transform([sphPair(o__oneBounce, d__oneBounce, csgB(), csgBr())], pairB__oneBounce -> list_transform([if(pairA__oneBounce[1] > eps() AND pairA__oneBounce[1] < 1e8 AND NOT (pairB__oneBounce[1] <= pairA__oneBounce[1] AND pairA__oneBounce[1] <= pairB__oneBounce[2]), pairA__oneBounce[1], 1e9)], cEnter__oneBounce -> list_transform([if(pairB__oneBounce[1] < 1e8 AND pairB__oneBounce[2] > eps() AND pairA__oneBounce[1] <= pairB__oneBounce[2] AND pairB__oneBounce[2] <= pairA__oneBounce[2], pairB__oneBounce[2], 1e9)], cCav__oneBounce -> list_transform([least(cEnter__oneBounce, cCav__oneBounce)], tC__oneBounce -> list_transform([least(tLet__oneBounce, tC__oneBounce)], tM__oneBounce -> list_transform([least(tM__oneBounce, tTerr__oneBounce)], tHit__oneBounce -> list_transform([va(o__oneBounce, vm(d__oneBounce, tHit__oneBounce))], hp__oneBounce -> list_transform([if(tTorus__oneBounce <= tBox__oneBounce AND tTorus__oneBounce <= tCyl__oneBounce AND tTorus__oneBounce <= tRing__oneBounce AND tTorus__oneBounce <= tSph__oneBounce, torusN(o__oneBounce, d__oneBounce, tTorus__oneBounce), if(tCyl__oneBounce <= tBox__oneBounce AND tCyl__oneBounce <= tRing__oneBounce AND tCyl__oneBounce <= tSph__oneBounce,    rodN(o__oneBounce, d__oneBounce, cyls()[indexOf(cylHits__oneBounce, tCyl__oneBounce)][1], cyls()[indexOf(cylHits__oneBounce, tCyl__oneBounce)][2], cyls()[indexOf(cylHits__oneBounce, tCyl__oneBounce)][3]), if(tBox__oneBounce <= tRing__oneBounce AND tBox__oneBounce <= tSph__oneBounce, oboxN(hp__oneBounce, oboxes()[indexOf(oboxHits__oneBounce, tBox__oneBounce)]), if(tRing__oneBounce <= tSph__oneBounce, ringN(o__oneBounce, d__oneBounce, rings()[indexOf(ringHits__oneBounce, tRing__oneBounce)]),    vn(vs(hp__oneBounce, spheres()[indexOf(sphHits__oneBounce, tSph__oneBounce)][1]))))))], letN__oneBounce -> list_transform([if(tC__oneBounce < tLet__oneBounce, if(cEnter__oneBounce <= cCav__oneBounce, vn(vs(hp__oneBounce, csgA())), vn(vs(csgB(), hp__oneBounce))), letN__oneBounce)], sn__oneBounce -> list_transform([(tM__oneBounce < tTerr__oneBounce) AND (tM__oneBounce < 1e8)], isMirror__oneBounce -> list_transform([(tTerr__oneBounce <= tM__oneBounce) AND (tTerr__oneBounce < 1e8)], isTerr__oneBounce -> list_transform([(tM__oneBounce >= 1e8) AND (tTerr__oneBounce >= 1e8)], isMiss__oneBounce -> list_transform([terrN(hp__oneBounce[1], hp__oneBounce[2])], tN__oneBounce -> list_transform([if(isTerr__oneBounce, tN__oneBounce, sn__oneBounce)], nrm__oneBounce -> list_transform([va(hp__oneBounce, vm(nrm__oneBounce, 0.02::DOUBLE))], shOrig__oneBounce -> list_transform([vref(d__oneBounce, sn__oneBounce)], rdir__oneBounce -> list_transform([vn(vs(lpos, hp__oneBounce))], ldir__oneBounce -> list_transform([L2Norm(vs(lpos, hp__oneBounce))], distL__oneBounce -> list_transform([occluded(shOrig__oneBounce, ldir__oneBounce, distL__oneBounce - 0.05::DOUBLE) OR terrShadow(shOrig__oneBounce, ldir__oneBounce, distL__oneBounce - 0.05::DOUBLE)], shadowed__oneBounce -> list_transform([pow(greatest(vd(ldir__oneBounce, rdir__oneBounce), 0.0::DOUBLE), shininess()) * if(shadowed__oneBounce, 0.0::DOUBLE, 1.0::DOUBLE)], spec__oneBounce -> list_transform([greatest(vd(ldir__oneBounce, tN__oneBounce), 0.0::DOUBLE) * if(shadowed__oneBounce, 0.0::DOUBLE, 1.0::DOUBLE)], lambert__oneBounce -> list_transform([least(1.0::DOUBLE, greatest(0.0::DOUBLE, (tHit__oneBounce - fogStart()) / fogRange()))], fog__oneBounce -> list_transform([va(vm(satb(vmul(terrCol(hp__oneBounce[3]), va(vm(skyAmb(), 0.26::DOUBLE + 0.18::DOUBLE*greatest(tN__oneBounce[3],0.0::DOUBLE)), vm(sunCol(), 1.45::DOUBLE*lambert__oneBounce)))), 1.0::DOUBLE - fog__oneBounce), vm(hazeColor(), fog__oneBounce))], terrShade__oneBounce -> (
        if(isMirror__oneBounce, shOrig__oneBounce, o__oneBounce),
        if(isMirror__oneBounce, rdir__oneBounce, d__oneBounce),
        if(isMirror__oneBounce, thr__oneBounce * reflect_k(), thr__oneBounce),
        va(acc__oneBounce, va(if(isMirror__oneBounce, vm(specColor(), thr__oneBounce * spec__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
                   va(if(isTerr__oneBounce, vm(terrShade__oneBounce, thr__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
                      if(isMiss__oneBounce, vm(sky(d__oneBounce), thr__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE))))),
        if(isMirror__oneBounce, 1.0::DOUBLE, 0.0::DOUBLE)))[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
SELECT pixel AS idx, toUInt8(round_even(clamp(pow(greatest(col[1], 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) * 255, 0)) AS r, toUInt8(round_even(clamp(pow(greatest(col[2], 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) * 255, 0)) AS g, toUInt8(round_even(clamp(pow(greatest(col[3], 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) * 255, 0)) AS b
FROM (
  SELECT pixel, va(st[4], if(st[5] > 0.5::DOUBLE, vm(sky(st[2]), st[3]), (0.0::DOUBLE,0.0::DOUBLE,0.0::DOUBLE))) AS col
  FROM (
    SELECT pixel,
      list_reduce(range(maxDepth()), (s, i) -> if(s[5] > 0.5::DOUBLE, oneBounce(s, lpos), s), st0) AS st
    FROM (
      SELECT
        number AS pixel,
        lightPos() AS lpos,                                   /* deterministic: no light jitter */
        CAST((eye(),
              vn(va(camFwd(), va(vm(camRight(), (((number % 48) + 0.5::DOUBLE) / imgW() * 2.0::DOUBLE - 1.0::DOUBLE) * aspect() * tanHalf()),
                                vm(camUp(),    (1.0::DOUBLE - (ch_intDiv(number, 48) + 0.5::DOUBLE) / imgH() * 2.0::DOUBLE) * tanHalf())))),
              1.0::DOUBLE, (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE), 1.0::DOUBLE) AS TUPLE(TUPLE(DOUBLE,DOUBLE,DOUBLE), TUPLE(DOUBLE,DOUBLE,DOUBLE), DOUBLE, TUPLE(DOUBLE,DOUBLE,DOUBLE), DOUBLE)) AS st0   /* centered ray */
      FROM ch_numbers_n(48 * 24)
    )
  )
)

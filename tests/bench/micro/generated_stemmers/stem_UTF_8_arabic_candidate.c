/* Generated from arabic.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_arabic_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

#ifdef SNOWBALL_BIGENDIAN
#define S(W) ((0x##W & 0xff) << 8 | 0x##W >> 8)
#else
#define S(W) (0x##W)
#endif

#ifdef __cplusplus
extern "C" {
#endif
extern int candidate_arabic_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_Suffix_Verb_Step2a(struct SN_env * z);
static int r_Suffix_Noun_Step2c1(struct SN_env * z);
static int r_Suffix_Noun_Step2b(struct SN_env * z);
static int r_Suffix_Noun_Step2a(struct SN_env * z);

#define s_1 (s_0 + 2)
#define s_13 (s_46 + 2)
#define s_14 (s_47 + 2)
#define s_16 (s_48 + 2)
#define s_28 (s_49 + 2)
#define s_0 (s_39 + 2)
static const symbol s_49[] = { 0xD8, 0xA7, 0xD8, 0xB3, 0xD8, 0xAA };
static const symbol s_2[] = { '0' };
static const symbol s_3[] = { '1' };
static const symbol s_4[] = { '2' };
static const symbol s_5[] = { '3' };
static const symbol s_6[] = { '4' };
static const symbol s_7[] = { '5' };
static const symbol s_8[] = { '6' };
static const symbol s_9[] = { '7' };
static const symbol s_10[] = { '8' };
static const symbol s_11[] = { '9' };
static const symbol s_12[] = { 0xD8, 0xA1 };
static const symbol s_15[] = { 0xD8, 0xA6 };
static const symbol s_17[] = { 0xD8, 0xA4 };
static const symbol s_18[] = { 0xD8, 0xA8 };
static const symbol s_19[] = { 0xD8, 0xA9 };
static const symbol s_20[] = { 0xD8, 0xAB };
static const symbol s_21[] = { 0xD8, 0xAC };
static const symbol s_22[] = { 0xD8, 0xAD };
static const symbol s_23[] = { 0xD8, 0xAE };
static const symbol s_24[] = { 0xD8, 0xAF };
static const symbol s_25[] = { 0xD8, 0xB0 };
static const symbol s_26[] = { 0xD8, 0xB1 };
static const symbol s_27[] = { 0xD8, 0xB2 };
static const symbol s_29[] = { 0xD8, 0xB4 };
static const symbol s_30[] = { 0xD8, 0xB5 };
static const symbol s_31[] = { 0xD8, 0xB6 };
static const symbol s_32[] = { 0xD8, 0xB7 };
static const symbol s_33[] = { 0xD8, 0xB8 };
static const symbol s_34[] = { 0xD8, 0xB9 };
static const symbol s_35[] = { 0xD8, 0xBA };
static const symbol s_36[] = { 0xD9, 0x81 };
static const symbol s_37[] = { 0xD9, 0x82 };
static const symbol s_38[] = { 0xD9, 0x83 };
static const symbol s_39[] = { 0xD9, 0x84, 0xD8, 0xA7, 0xD8, 0xAA };
static const symbol s_40[] = { 0xD9, 0x85 };
static const symbol s_41[] = { 0xD9, 0x86 };
static const symbol s_42[] = { 0xD9, 0x87 };
static const symbol s_43[] = { 0xD9, 0x88 };
static const symbol s_44[] = { 0xD9, 0x89 };
static const symbol s_45[] = { 0xD9, 0x8A };
static const symbol s_46[] = { 0xD9, 0x84, 0xD8, 0xA3 };
static const symbol s_47[] = { 0xD9, 0x84, 0xD8, 0xA5 };
static const symbol s_48[] = { 0xD9, 0x84, 0xD8, 0xA2 };

static const unsigned short a_0[] = {
    0x0000 , 0xD9EF , 0x0004 , 0x0030 , 0x0000 , 0xA980 , 0xFFFF , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0xFFFF , 0xFFFF , 0xFFFF , 0xFFFF , 0xFFFF , 0xFFFF , 0xFFFF ,
    0xFFFF , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0xFFFE , 0xFFFD ,
    0xFFFC , 0xFFFB , 0xFFFA , 0xFFF9 , 0xFFF8 , 0xFFF7 , 0xFFF6 , 0xFFF5 ,
    0x0000 , 0xBABB , 0x0034 , 0x0076 , 0x0000 , 0xBF80 , 0xFFF4 , 0xFFF0 ,
    0xFFF0 , 0xFFF3 , 0xFFF3 , 0xFFEF , 0xFFEF , 0xFFF2 , 0xFFF2 , 0xFFF1 ,
    0xFFF1 , 0xFFF1 , 0xFFF1 , 0xFFEE , 0xFFEE , 0xFFED , 0xFFED , 0xFFED ,
    0xFFED , 0xFFEC , 0xFFEC , 0xFFEB , 0xFFEB , 0xFFEB , 0xFFEB , 0xFFEA ,
    0xFFEA , 0xFFEA , 0xFFEA , 0xFFE9 , 0xFFE9 , 0xFFE9 , 0xFFE9 , 0xFFE8 ,
    0xFFE8 , 0xFFE8 , 0xFFE8 , 0xFFE7 , 0xFFE7 , 0xFFE7 , 0xFFE7 , 0xFFE6 ,
    0xFFE6 , 0xFFE5 , 0xFFE5 , 0xFFE4 , 0xFFE4 , 0xFFE3 , 0xFFE3 , 0xFFE2 ,
    0xFFE2 , 0xFFE2 , 0xFFE2 , 0xFFE1 , 0xFFE1 , 0xFFE1 , 0xFFE1 , 0xFFE0 ,
    0xFFE0 , 0xFFE0 , 0xFFE0 , 0xFFDF , 0xFFDF , 0xFFDF , 0x0000 , 0xBC80 ,
    0xFFDF , 0xFFDE , 0xFFDE , 0xFFDE , 0xFFDE , 0xFFDD , 0xFFDD , 0xFFDD ,
    0xFFDD , 0xFFDC , 0xFFDC , 0xFFDC , 0xFFDC , 0xFFDB , 0xFFDB , 0xFFDB ,
    0xFFDB , 0xFFDA , 0xFFDA , 0xFFDA , 0xFFDA , 0xFFD9 , 0xFFD9 , 0xFFD9 ,
    0xFFD9 , 0xFFD8 , 0xFFD8 , 0xFFD8 , 0xFFD8 , 0xFFD7 , 0xFFD7 , 0xFFD7 ,
    0xFFD7 , 0xFFD6 , 0xFFD6 , 0xFFD6 , 0xFFD6 , 0xFFD5 , 0xFFD5 , 0xFFD5 ,
    0xFFD5 , 0xFFD4 , 0xFFD4 , 0xFFD4 , 0xFFD4 , 0xFFD3 , 0xFFD3 , 0xFFD2 ,
    0xFFD2 , 0xFFD1 , 0xFFD1 , 0xFFD1 , 0xFFD1 , 0xFFCD , 0xFFCD , 0xFFCF ,
    0xFFCF , 0xFFCE , 0xFFCE , 0xFFD0 , 0xFFD0
};

static const unsigned short a_1[] = {
    0x0000 , 0xA6A2 , 0x0007 , 0x0007 , 0x0007 , 0x0007 , 0x0007 , 0x0000 ,
    0xD8D8 , 0xFFFF
};

static const unsigned short a_2[] = {
    0x0000 , 0xD8D8 , 0x0003 , 0x0000 , 0xA6A2 , 0xFFFF , 0xFFFF , 0xFFFE ,
    0xFFFF , 0xFFFD
};

static const unsigned short a_3[] = {
    0x0000 , 0xD8D9 , 0x0004 , 0x0011 , 0x0000 , 0xA7A8 , 0x0008 , 0x000C ,
    0x0000 , 0x0002 , 0xFFFE , S(84D9), 0x0000 , 0x0004 , 0xFFFF , S(A7D8),
    S(84D9), 0x0000 , 0x8384 , 0x000C , 0x0008
};

static const unsigned short a_4[] = {
    0x0000 , 0x0003 , 0x0005 , S(A3D8), S(00D8), 0x0000 , 0xA7A2 , 0xFFFE ,
    0xFFFF , 0xFFFF , 0xFFFC , 0x0000 , 0xFFFD
};

static const unsigned short a_5[] = {
    0x0000 , 0xD9D9 , 0x0003 , 0x0000 , 0x8188 , 0xFFFF , 0xFFFF
};

static const unsigned short a_6[] = {
    0x0000 , 0xD8D9 , 0x0004 , 0x0011 , 0x0000 , 0xA7A8 , 0x0008 , 0x000C ,
    0x0000 , 0x0002 , 0xFFFE , S(84D9), 0x0000 , 0x0004 , 0xFFFF , S(A7D8),
    S(84D9), 0x0000 , 0x8384 , 0x000C , 0x0008
};

static const unsigned short a_7[] = {
    0x0000 , 0xD8D9 , 0x0004 , 0x000E , 0x0000 , 0xA8A8 , 0x0007 , 0x0001 ,
    0xD8D8 , 0x000A , 0x0000 , 0xA7A8 , 0xC001 , 0xFFFE , 0x0000 , 0x0003 ,
    0xFFFD , S(D983), S(0083)
};

static const unsigned short a_8[] = {
    0x0000 , 0x0002 , 0x0004 , S(B3D8), 0x0000 , 0xD8D9 , 0x0008 , 0x000C ,
    0x0000 , 0xA3AA , 0xFFFC , 0xFFFE , 0x0000 , 0x868A , 0xFFFD , 0xFFFF
};

static const unsigned short a_9[] = {
    0x0000 , 0xD8D9 , 0x0004 , 0x000A , 0x0000 , 0x0005 , 0xFFFF , S(D8AA),
    S(D8B3), S(00AA), 0x0000 , 0x868A , 0x000E , 0x000E , 0x0000 , 0x0004 ,
    0xFFFF , S(B3D8), S(AAD8)
};

static const unsigned short a_10[] = {
    0x0000 , 0xA783 , 0x0027 , 0x0000 , 0x002A , 0x0034 , 0x0027 , 0x0000 ,
    0x0000 , 0x0027 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0039 , 0x0000 ,
    0xD9D9 , 0xFFFF , 0x0000 , 0xD9D9 , 0x002D , 0x0000 , 0x8387 , 0x0031 ,
    0x0031 , 0x0000 , 0xD9D9 , 0xFFFE , 0x0000 , 0x0003 , 0xFFFE , S(87D9),
    S(00D9), 0x0000 , 0xD8D8 , 0x003C , 0x0000 , 0x8785 , 0x0041 , 0x0031 ,
    0x0031 , 0x0000 , 0xD9D9 , 0x0044 , 0x0000 , 0x8387 , 0x0048 , 0x0048 ,
    0x0000 , 0xD9D9 , 0xFFFD
};

static const unsigned short a_11[] = {
    0x0000 , 0xA788 , 0x0022 , 0x0000 , 0x0022 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0025 , 0x0000 , 0xD9D9 , 0xFFFF , 0x0000 , 0xD8D8 , 0xFFFF
};

static const unsigned short a_12[] = {
    0x0000 , 0xA783 , 0x0027 , 0x0000 , 0x002A , 0x0034 , 0x0027 , 0x003B ,
    0x0000 , 0x0041 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0046 , 0x0000 ,
    0xD9D9 , 0xFFFF , 0x0000 , 0xD9D9 , 0x002D , 0x0000 , 0x8387 , 0x0031 ,
    0x0031 , 0x0000 , 0xD9D9 , 0xFFFE , 0x0000 , 0xD9D9 , 0x0037 , 0x0000 ,
    0x8387 , 0x0031 , 0x0031 , 0x0000 , 0x0005 , 0xFFFD , S(83D9), S(85D9),
    S(00D9), 0x0000 , 0x0003 , 0xFFFE , S(86D9), S(00D9), 0x0000 , 0xD8D8 ,
    0x0049 , 0x0000 , 0x8785 , 0x004E , 0x0031 , 0x0031 , 0x0000 , 0xD9D9 ,
    0x0051 , 0x0000 , 0x8387 , 0x0055 , 0x0055 , 0x0000 , 0xD9D9 , 0xFFFD
};

static const unsigned short a_13[] = {
    0x0000 , 0xAA86 , 0x0027 , 0x0000 , 0x0000 , 0x0000 , 0x0058 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x005B , 0x0000 , 0x0000 , 0x008E , 0x0000 ,
    0xD9D9 , 0x002A , 0x0001 , 0xAA88 , 0x004F , 0x0000 , 0x004F , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0052 , 0x0000 , 0x0000 , 0x0055 , 0x0000 ,
    0xD9D9 , 0xFFFD , 0x0000 , 0xD8D8 , 0xFFFD , 0x0000 , 0xD8D8 , 0xFFFE ,
    0x0000 , 0xD9D9 , 0xFFFF , 0x0000 , 0xD8D8 , 0x005E , 0x0001 , 0xAA85 ,
    0x0086 , 0x008B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0055 , 0x0000 , 0x0003 ,
    0xFFFD , S(AAD8), S(00D9), 0x0000 , 0xD9D9 , 0xFFFE , 0x0000 , 0xD8D8 ,
    0xFFFF
};

static const unsigned short a_14[] = {
    0x0000 , 0x85A7 , 0x0004 , 0x0009 , 0x0000 , 0x0003 , 0xFFFF , S(AAD8),
    S(00D9), 0x0000 , 0x0003 , 0xFFFF , S(88D9), S(00D8)
};

static const unsigned short a_15[] = {
    0x0000 , 0x0002 , 0x0004 , S(88D9), 0x0001 , 0x0004 , 0xFFFE , S(AAD8),
    S(85D9)
};

static int r_Suffix_Noun_Step2a(struct SN_env * z) {
    z->ket = z->c;
    if (!find_among_b(z, a_11)) return 0;
    z->bra = z->c;
    if (len_utf8(z->p) < 5) return 0;
    {
        int ret = slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_Suffix_Noun_Step2b(struct SN_env * z) {
    z->ket = z->c;
    if (!(eq_s_b(z, 4, s_0))) return 0;
    z->bra = z->c;
    if (len_utf8(z->p) < 5) return 0;
    {
        int ret = slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_Suffix_Noun_Step2c1(struct SN_env * z) {
    z->ket = z->c;
    if (!(eq_s_b(z, 2, s_1))) return 0;
    z->bra = z->c;
    if (len_utf8(z->p) < 4) return 0;
    {
        int ret = slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_Suffix_Verb_Step2a(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    among_var = find_among_b(z, a_13);
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            if (len_utf8(z->p) < 4) return 0;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            if (len_utf8(z->p) < 5) return 0;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        case 3:
            if (len_utf8(z->p) < 6) return 0;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

extern int candidate_arabic_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int b_is_defined;
    int b_is_verb;
    int b_is_noun;
    b_is_noun = 1;
    b_is_verb = 1;
    b_is_defined = 0;
    {
        int v_1 = z->c;
        z->bra = z->c;
        if (z->c + 3 >= z->l || (z->p[z->c + 3] != 132 && z->p[z->c + 3] != 167)) goto lab0;
        among_var = find_among(z, a_3);
        if (!among_var) goto lab0;
        z->ket = z->c;
        switch (among_var) {
            case 1:
                if (len_utf8(z->p) < 5) goto lab0;
                b_is_noun = 1;
                b_is_verb = 0;
                b_is_defined = 1;
                break;
            case 2:
                if (len_utf8(z->p) < 4) goto lab0;
                b_is_noun = 1;
                b_is_verb = 0;
                b_is_defined = 1;
                break;
        }
    lab0:
        z->c = v_1;
    }
    {
        int v_2 = z->c;
        while (1) {
            int v_3 = z->c;
            do {
                int v_4 = z->c;
                z->bra = z->c;
                among_var = find_among(z, a_0);
                if (!among_var) goto lab4;
                z->ket = z->c;
                switch (among_var) {
                    case 1:
                        {
                            int ret = slice_del(z);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 2:
                        {
                            int ret = slice_from_s(z, 1, s_2);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 3:
                        {
                            int ret = slice_from_s(z, 1, s_3);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 4:
                        {
                            int ret = slice_from_s(z, 1, s_4);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 5:
                        {
                            int ret = slice_from_s(z, 1, s_5);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 6:
                        {
                            int ret = slice_from_s(z, 1, s_6);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 7:
                        {
                            int ret = slice_from_s(z, 1, s_7);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 8:
                        {
                            int ret = slice_from_s(z, 1, s_8);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 9:
                        {
                            int ret = slice_from_s(z, 1, s_9);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 10:
                        {
                            int ret = slice_from_s(z, 1, s_10);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 11:
                        {
                            int ret = slice_from_s(z, 1, s_11);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 12:
                        {
                            int ret = slice_from_s(z, 2, s_12);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 13:
                        {
                            int ret = slice_from_s(z, 2, s_13);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 14:
                        {
                            int ret = slice_from_s(z, 2, s_14);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 15:
                        {
                            int ret = slice_from_s(z, 2, s_15);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 16:
                        {
                            int ret = slice_from_s(z, 2, s_16);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 17:
                        {
                            int ret = slice_from_s(z, 2, s_17);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 18:
                        {
                            int ret = slice_from_s(z, 2, s_0);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 19:
                        {
                            int ret = slice_from_s(z, 2, s_18);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 20:
                        {
                            int ret = slice_from_s(z, 2, s_19);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 21:
                        {
                            int ret = slice_from_s(z, 2, s_1);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 22:
                        {
                            int ret = slice_from_s(z, 2, s_20);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 23:
                        {
                            int ret = slice_from_s(z, 2, s_21);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 24:
                        {
                            int ret = slice_from_s(z, 2, s_22);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 25:
                        {
                            int ret = slice_from_s(z, 2, s_23);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 26:
                        {
                            int ret = slice_from_s(z, 2, s_24);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 27:
                        {
                            int ret = slice_from_s(z, 2, s_25);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 28:
                        {
                            int ret = slice_from_s(z, 2, s_26);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 29:
                        {
                            int ret = slice_from_s(z, 2, s_27);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 30:
                        {
                            int ret = slice_from_s(z, 2, s_28);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 31:
                        {
                            int ret = slice_from_s(z, 2, s_29);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 32:
                        {
                            int ret = slice_from_s(z, 2, s_30);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 33:
                        {
                            int ret = slice_from_s(z, 2, s_31);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 34:
                        {
                            int ret = slice_from_s(z, 2, s_32);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 35:
                        {
                            int ret = slice_from_s(z, 2, s_33);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 36:
                        {
                            int ret = slice_from_s(z, 2, s_34);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 37:
                        {
                            int ret = slice_from_s(z, 2, s_35);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 38:
                        {
                            int ret = slice_from_s(z, 2, s_36);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 39:
                        {
                            int ret = slice_from_s(z, 2, s_37);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 40:
                        {
                            int ret = slice_from_s(z, 2, s_38);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 41:
                        {
                            int ret = slice_from_s(z, 2, s_39);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 42:
                        {
                            int ret = slice_from_s(z, 2, s_40);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 43:
                        {
                            int ret = slice_from_s(z, 2, s_41);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 44:
                        {
                            int ret = slice_from_s(z, 2, s_42);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 45:
                        {
                            int ret = slice_from_s(z, 2, s_43);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 46:
                        {
                            int ret = slice_from_s(z, 2, s_44);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 47:
                        {
                            int ret = slice_from_s(z, 2, s_45);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 48:
                        {
                            int ret = slice_from_s(z, 4, s_39);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 49:
                        {
                            int ret = slice_from_s(z, 4, s_46);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 50:
                        {
                            int ret = slice_from_s(z, 4, s_47);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 51:
                        {
                            int ret = slice_from_s(z, 4, s_48);
                            if (ret < 0) return ret;
                        }
                        break;
                }
                break;
            lab4:
                z->c = v_4;
                {
                    int ret = skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab3;
                    z->c = ret;
                }
            } while (0);
            continue;
        lab3:
            z->c = v_3;
            break;
        }
        z->c = v_2;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_5 = z->l - z->c;
        do {
            int v_6 = z->l - z->c;
            if (!b_is_verb) goto lab6;
            do {
                int v_7 = z->l - z->c;
                {
                    int v_8 = 1;
                    while (1) {
                        int v_9 = z->l - z->c;
                        z->ket = z->c;
                        among_var = find_among_b(z, a_12);
                        if (!among_var) goto lab8;
                        z->bra = z->c;
                        switch (among_var) {
                            case 1:
                                if (len_utf8(z->p) < 4) goto lab8;
                                {
                                    int ret = slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                            case 2:
                                if (len_utf8(z->p) < 5) goto lab8;
                                {
                                    int ret = slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                            case 3:
                                if (len_utf8(z->p) < 6) goto lab8;
                                {
                                    int ret = slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                        }
                        v_8--;
                        continue;
                    lab8:
                        z->c = z->l - v_9;
                        break;
                    }
                    if (v_8 > 0) goto lab7;
                }
                do {
                    int v_10 = z->l - z->c;
                    {
                        int ret = r_Suffix_Verb_Step2a(z);
                        if (ret == 0) goto lab9;
                        if (ret < 0) return ret;
                    }
                    break;
                lab9:
                    z->c = z->l - v_10;
                    z->ket = z->c;
                    if (z->c - 1 <= z->lb || z->p[z->c - 1] != 136) goto lab10;
                    among_var = find_among_b(z, a_15);
                    if (!among_var) goto lab10;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            if (len_utf8(z->p) < 4) goto lab10;
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            if (len_utf8(z->p) < 6) goto lab10;
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                    }
                    break;
                lab10:
                    z->c = z->l - v_10;
                    {
                        int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab7;
                        z->c = ret;
                    }
                } while (0);
                break;
            lab7:
                z->c = z->l - v_7;
                z->ket = z->c;
                if (z->c - 3 <= z->lb || (z->p[z->c - 1] != 133 && z->p[z->c - 1] != 167)) goto lab11;
                if (!find_among_b(z, a_14)) goto lab11;
                z->bra = z->c;
                if (len_utf8(z->p) < 5) goto lab11;
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            lab11:
                z->c = z->l - v_7;
                {
                    int ret = r_Suffix_Verb_Step2a(z);
                    if (ret == 0) goto lab6;
                    if (ret < 0) return ret;
                }
            } while (0);
            break;
        lab6:
            z->c = z->l - v_6;
            if (!b_is_noun) goto lab12;
            {
                int v_11 = z->l - z->c;
                do {
                    int v_12 = z->l - z->c;
                    z->ket = z->c;
                    if (!(eq_s_b(z, 2, s_19))) goto lab14;
                    z->bra = z->c;
                    if (len_utf8(z->p) < 4) goto lab14;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                lab14:
                    z->c = z->l - v_12;
                    if (b_is_defined) goto lab15;
                    z->ket = z->c;
                    among_var = find_among_b(z, a_10);
                    if (!among_var) goto lab15;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            if (len_utf8(z->p) < 4) goto lab15;
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            if (len_utf8(z->p) < 5) goto lab15;
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 3:
                            if (len_utf8(z->p) < 6) goto lab15;
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                    }
                    do {
                        int v_13 = z->l - z->c;
                        {
                            int ret = r_Suffix_Noun_Step2a(z);
                            if (ret == 0) goto lab16;
                            if (ret < 0) return ret;
                        }
                        break;
                    lab16:
                        z->c = z->l - v_13;
                        {
                            int ret = r_Suffix_Noun_Step2b(z);
                            if (ret == 0) goto lab17;
                            if (ret < 0) return ret;
                        }
                        break;
                    lab17:
                        z->c = z->l - v_13;
                        {
                            int ret = r_Suffix_Noun_Step2c1(z);
                            if (ret == 0) goto lab18;
                            if (ret < 0) return ret;
                        }
                        break;
                    lab18:
                        z->c = z->l - v_13;
                        {
                            int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
                            if (ret < 0) goto lab15;
                            z->c = ret;
                        }
                    } while (0);
                    break;
                lab15:
                    z->c = z->l - v_12;
                    z->ket = z->c;
                    if (!(eq_s_b(z, 2, s_41))) goto lab19;
                    z->bra = z->c;
                    if (len_utf8(z->p) < 6) goto lab19;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    do {
                        int v_14 = z->l - z->c;
                        {
                            int ret = r_Suffix_Noun_Step2a(z);
                            if (ret == 0) goto lab20;
                            if (ret < 0) return ret;
                        }
                        break;
                    lab20:
                        z->c = z->l - v_14;
                        {
                            int ret = r_Suffix_Noun_Step2b(z);
                            if (ret == 0) goto lab21;
                            if (ret < 0) return ret;
                        }
                        break;
                    lab21:
                        z->c = z->l - v_14;
                        {
                            int ret = r_Suffix_Noun_Step2c1(z);
                            if (ret == 0) goto lab19;
                            if (ret < 0) return ret;
                        }
                    } while (0);
                    break;
                lab19:
                    z->c = z->l - v_12;
                    if (b_is_defined) goto lab22;
                    {
                        int ret = r_Suffix_Noun_Step2a(z);
                        if (ret == 0) goto lab22;
                        if (ret < 0) return ret;
                    }
                    break;
                lab22:
                    z->c = z->l - v_12;
                    {
                        int ret = r_Suffix_Noun_Step2b(z);
                        if (ret == 0) { z->c = z->l - v_11; goto lab13; }
                        if (ret < 0) return ret;
                    }
                } while (0);
            lab13:
                ;
            }
            z->ket = z->c;
            if (!(eq_s_b(z, 2, s_45))) goto lab12;
            z->bra = z->c;
            if (len_utf8(z->p) < 3) goto lab12;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab12:
            z->c = z->l - v_6;
            z->ket = z->c;
            if (!(eq_s_b(z, 2, s_44))) goto lab5;
            z->bra = z->c;
            {
                int ret = slice_from_s(z, 2, s_45);
                if (ret < 0) return ret;
            }
        } while (0);
    lab5:
        z->c = z->l - v_5;
    }
    z->c = z->lb;
    {
        int v_15 = z->c;
        {
            int v_16 = z->c;
            z->bra = z->c;
            if (z->c + 3 >= z->l || z->p[z->c + 3] >> 5 != 5 || !((188 >> (z->p[z->c + 3] & 0x1f)) & 1)) { z->c = v_16; goto lab24; }
            among_var = find_among(z, a_4);
            if (!among_var) { z->c = v_16; goto lab24; }
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    if (len_utf8(z->p) < 4) { z->c = v_16; goto lab24; }
                    {
                        int ret = slice_from_s(z, 2, s_13);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (len_utf8(z->p) < 4) { z->c = v_16; goto lab24; }
                    {
                        int ret = slice_from_s(z, 2, s_16);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    if (len_utf8(z->p) < 4) { z->c = v_16; goto lab24; }
                    {
                        int ret = slice_from_s(z, 2, s_0);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    if (len_utf8(z->p) < 4) { z->c = v_16; goto lab24; }
                    {
                        int ret = slice_from_s(z, 2, s_14);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        lab24:
            ;
        }
        {
            int v_17 = z->c;
            z->bra = z->c;
            if (z->c + 1 >= z->l || (z->p[z->c + 1] != 129 && z->p[z->c + 1] != 136)) { z->c = v_17; goto lab25; }
            if (!find_among(z, a_5)) { z->c = v_17; goto lab25; }
            z->ket = z->c;
            if (len_utf8(z->p) < 4) { z->c = v_17; goto lab25; }
            if (!(eq_s(z, 2, s_0))) goto lab26;
            { z->c = v_17; goto lab25; }
        lab26:
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
        lab25:
            ;
        }
        do {
            int v_18 = z->c;
            z->bra = z->c;
            if (z->c + 3 >= z->l || (z->p[z->c + 3] != 132 && z->p[z->c + 3] != 167)) goto lab27;
            among_var = find_among(z, a_6);
            if (!among_var) goto lab27;
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    if (len_utf8(z->p) < 6) goto lab27;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (len_utf8(z->p) < 5) goto lab27;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            break;
        lab27:
            z->c = v_18;
            if (!b_is_noun) goto lab28;
            z->bra = z->c;
            if (z->c + 1 >= z->l || (z->p[z->c + 1] != 168 && z->p[z->c + 1] != 131)) goto lab28;
            among_var = find_among(z, a_7);
            if (!among_var) goto lab28;
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    if (len_utf8(z->p) < 4) goto lab28;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (len_utf8(z->p) < 4) goto lab28;
                    {
                        int ret = slice_from_s(z, 2, s_18);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    if (len_utf8(z->p) < 4) goto lab28;
                    {
                        int ret = slice_from_s(z, 2, s_38);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            break;
        lab28:
            z->c = v_18;
            if (!b_is_verb) goto lab23;
            {
                int v_19 = z->c;
                z->bra = z->c;
                among_var = find_among(z, a_8);
                if (!among_var) { z->c = v_19; goto lab29; }
                z->ket = z->c;
                switch (among_var) {
                    case 1:
                        if (len_utf8(z->p) < 5) { z->c = v_19; goto lab29; }
                        {
                            int ret = slice_from_s(z, 2, s_45);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 2:
                        if (len_utf8(z->p) < 5) { z->c = v_19; goto lab29; }
                        {
                            int ret = slice_from_s(z, 2, s_1);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 3:
                        if (len_utf8(z->p) < 5) { z->c = v_19; goto lab29; }
                        {
                            int ret = slice_from_s(z, 2, s_41);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 4:
                        if (len_utf8(z->p) < 5) { z->c = v_19; goto lab29; }
                        {
                            int ret = slice_from_s(z, 2, s_13);
                            if (ret < 0) return ret;
                        }
                        break;
                }
            lab29:
                ;
            }
            z->bra = z->c;
            if (z->c + 5 >= z->l || z->p[z->c + 5] != 170) goto lab23;
            if (!find_among(z, a_9)) goto lab23;
            z->ket = z->c;
            if (len_utf8(z->p) < 5) goto lab23;
            b_is_verb = 1;
            b_is_noun = 0;
            {
                int ret = slice_from_s(z, 6, s_49);
                if (ret < 0) return ret;
            }
        } while (0);
    lab23:
        z->c = v_15;
    }
    {
        int v_20 = z->c;
        z->lb = z->c; z->c = z->l;
        z->ket = z->c;
        if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 5 || !((124 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab31;
        if (!find_among_b(z, a_1)) goto lab31;
        z->bra = z->c;
        {
            int ret = slice_from_s(z, 2, s_12);
            if (ret < 0) return ret;
        }
        z->c = z->lb;
    lab31:
        z->c = v_20;
    }
    {
        int v_21 = z->c;
        while (1) {
            int v_22 = z->c;
            do {
                int v_23 = z->c;
                z->bra = z->c;
                if (z->c + 1 >= z->l || z->p[z->c + 1] >> 5 != 5 || !((124 >> (z->p[z->c + 1] & 0x1f)) & 1)) goto lab34;
                among_var = find_among(z, a_2);
                if (!among_var) goto lab34;
                z->ket = z->c;
                switch (among_var) {
                    case 1:
                        {
                            int ret = slice_from_s(z, 2, s_0);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 2:
                        {
                            int ret = slice_from_s(z, 2, s_43);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 3:
                        {
                            int ret = slice_from_s(z, 2, s_45);
                            if (ret < 0) return ret;
                        }
                        break;
                }
                break;
            lab34:
                z->c = v_23;
                {
                    int ret = skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab33;
                    z->c = ret;
                }
            } while (0);
            continue;
        lab33:
            z->c = v_22;
            break;
        }
        z->c = v_21;
    }
    return 1;
}

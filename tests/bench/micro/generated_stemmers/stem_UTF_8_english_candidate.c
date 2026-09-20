/* Generated from english.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_english_candidate.h"

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
extern int candidate_english_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_shortv(struct SN_env * z);

#define s_28 (s_2 + 2)
#define s_17 (s_4 + 1)
#define s_24 (s_16 + 1)
#define s_10 (s_26 + 2)
#define s_7 (s_13 + 2)
#define s_26 (s_16 + 2)
#define s_12 (s_11 + 1)
#define s_27 (s_1 + 2)
#define s_2 (s_22 + 2)
#define s_3 (s_5 + 3)
#define s_4 (s_25 + 1)
#define s_14 (s_23 + 2)
#define s_11 (s_6 + 4)
#define s_18 (s_13 + 5)
#define s_1 (s_16 + 5)
#define s_6 (s_19 + 2)
#define s_13 (s_0 + 3)
static const symbol s_0[] = {
    'p', 'a', 's', 't', 'i', 'o', 'n', 'l',
    'i', 'z', 'e'
};
static const symbol s_15[] = { 'a', 'n', 'c', 'e' };
static const symbol s_22[] = { 'o', 'u', 's', 'k', 'y' };
static const symbol s_21[] = { 'f', 'u', 'l' };
static const symbol s_20[] = { 'a', 'l' };
static const symbol s_5[] = { 'u', 'g', 'l', 'i', 'd', 'l' };
static const symbol s_19[] = {
    'a', 't', 'e', 'a', 'r', 'l', 'i', 'e',
    'e'
};
static const symbol s_23[] = { 'i', 'v', 'e', 'n', 'c', 'e' };
static const symbol s_8[] = { 's', 'i', 'n', 'g', 'l' };
static const symbol s_9[] = { 'Y' };
static const symbol s_16[] = {
    'a', 'b', 'l', 'e', 's', 's', 'k', 'i',
    'c'
};
static const symbol s_25[] = { 'o', 'g', 'e', 'n', 't', 'l' };

static const unsigned short a_0[] = {
    0x0000 , 0x7561 , 0x0017 , 0x0000 , 0x001C , 0x0000 , 0x0022 , 0x0000 ,
    0x0027 , 0x0000 , 0x002C , 0x0000 , 0x0000 , 0x0031 , 0x0000 , 0x0000 ,
    0x0036 , 0x003B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0040 , 0x0000 ,
    0x0004 , 0xC001 , S(7372), S(6E65), 0x0000 , 0x0005 , 0xC001 , S(6D6F),
    S(756D), S(006E), 0x0000 , 0x0004 , 0xC001 , S(656D), S(6772), 0x0000 ,
    0x0004 , 0xC001 , S(6E65), S(7265), 0x0000 , 0x0004 , 0xC001 , S(746E),
    S(7265), 0x0000 , 0x0004 , 0xC001 , S(7461), S(7265), 0x0000 , 0x0004 ,
    0xC001 , S(6772), S(6E61), 0x0000 , 0x0003 , 0xC001 , S(7361), S(0074),
    0x0000 , 0x0006 , 0xC001 , S(696E), S(6576), S(7372)
};

static const unsigned short a_1[] = {
    0x0000 , 0x2773 , 0x0004 , 0x0008 , 0x0001 , 0x0002 , 0xFFFF , S(7327),
    0x0000 , 0x2727 , 0xFFFF
};

static const unsigned short a_2[] = {
    0x0000 , 0x6473 , 0x0004 , 0x0008 , 0x0000 , 0x0002 , 0xFFFE , S(6569),
    0x0003 , 0x7565 , 0x001B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0xC001 , 0x0000 , 0xC001 , 0x0000 , 0x6973 , 0xFFFE , 0x001F , 0x0000 ,
    0x7373 , 0xFFFF
};

static const unsigned short a_3[] = {
    0x0000 , 0x6363 , 0x0003 , 0x0000 , 0x7863 , 0x001B , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x001F , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0023 , 0x0000 , 0x0002 , 0xFFFF , S(7573), 0x0000 ,
    0x0002 , 0xFFFF , S(7270), 0x0000 , 0x6565 , 0xFFFF
};

static const unsigned short a_4[] = {
    0x0000 , 0x796E , 0x000E , 0x0000 , 0x0000 , 0x0000 , 0x001D , 0x0000 ,
    0x002A , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0xFFFF , 0x0000 , 0x656E ,
    0x0012 , 0x0016 , 0x0000 , 0x0002 , 0xFFFE , S(7665), 0x0000 , 0x6169 ,
    0x001A , 0xFFFE , 0x0000 , 0x6363 , 0xFFFE , 0x0000 , 0x7272 , 0x0020 ,
    0x0000 , 0x6165 , 0x0024 , 0x0027 , 0x0000 , 0x6565 , 0xFFFE , 0x0000 ,
    0x6868 , 0xFFFE , 0x0000 , 0x0002 , 0xFFFE , S(756F)
};

static const unsigned short a_5[] = {
    0x3FFF , 0x7964 , 0x0018 , 0x0000 , 0x0000 , 0x001E , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0022 ,
    0x0000 , 0x6565 , 0x001B , 0x0002 , 0x6565 , 0xFFFF , 0x0000 , 0x0002 ,
    0xFFFD , S(6E69), 0x0000 , 0x6C6C , 0x0025 , 0x0000 , 0x6467 , 0x0029 ,
    0x002C , 0x0000 , 0x6565 , 0x001B , 0x0000 , 0x0002 , 0xFFFE , S(6E69)
};

static const unsigned short a_6[] = {
    0x0003 , 0x7A62 , 0x001B , 0x0000 , 0x001E , 0x0000 , 0x0021 , 0x0024 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0027 , 0x002A , 0x002D , 0x0000 ,
    0x0030 , 0x0000 , 0x0033 , 0x0000 , 0x0036 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x003A , 0x0000 , 0x6262 , 0xFFFE , 0x0000 , 0x6464 ,
    0xFFFE , 0x0000 , 0x6666 , 0xFFFE , 0x0000 , 0x6767 , 0xFFFE , 0x0000 ,
    0x6262 , 0xFFFF , 0x0000 , 0x6D6D , 0xFFFE , 0x0000 , 0x6E6E , 0xFFFE ,
    0x0000 , 0x7070 , 0xFFFE , 0x0000 , 0x7272 , 0xFFFE , 0x0000 , 0x6174 ,
    0xFFFF , 0xFFFE , 0x0000 , 0x6969 , 0xFFFF
};

static const unsigned short a_7[] = {
    0x0000 , 0x7469 , 0x000E , 0x0000 , 0x0000 , 0x006B , 0x0074 , 0x0079 ,
    0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x008E , 0x00B0 , 0x0000 , 0x7463 ,
    0x0022 , 0x0000 , 0x0000 , 0x0000 , 0x0029 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x002C , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x005A , 0x0000 , 0x6E6E , 0x0025 , 0x0000 , 0x6165 , 0xFFFD ,
    0xFFFE , 0x0000 , 0x6F6F , 0xFFF2 , 0x0010 , 0x7462 , 0x0041 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0044 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x004B ,
    0x0056 , 0x000C , 0x6161 , 0xFFFC , 0x0000 , 0x6175 , 0xFFF8 , 0x0048 ,
    0x0000 , 0x6666 , 0xFFF7 , 0x0000 , 0x7375 , 0x004F , 0x0053 , 0x0000 ,
    0x0002 , 0xFFF1 , S(656C), 0x0000 , 0x6F6F , 0xFFF6 , 0x0000 , 0x0002 ,
    0xFFFB , S(6E65), 0x0000 , 0x6969 , 0x005D , 0x0000 , 0x6C76 , 0x0061 ,
    0x0068 , 0x0000 , 0x6169 , 0xFFF8 , 0x0065 , 0x0000 , 0x6262 , 0xFFF4 ,
    0x0000 , 0x6969 , 0xFFF5 , 0x0000 , 0x0005 , 0x0071 , S(6974), S(6E6F),
    S(0061), 0x0001 , 0x6161 , 0xFFF9 , 0x0000 , 0x0004 , 0xFFF8 , S(6C61),
    S(7369), 0x0000 , 0x0004 , 0x007E , S(7461), S(6F69), 0x0007 , 0x0002 ,
    0xFFFA , S(7A69), 0x0000 , 0x656F , 0x0086 , 0x008A , 0x0000 , 0x0002 ,
    0xFFFA , S(7A69), 0x0000 , 0x0002 , 0xFFF9 , S(7461), 0x0000 , 0x0003 ,
    0x0093 , S(656E), S(0073), 0x0000 , 0x7365 , 0x00A4 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00A8 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x00AC , 0x0000 , 0x0002 , 0xFFF5 , S(7669),
    0x0000 , 0x0002 , 0xFFF7 , S(7566), 0x0000 , 0x0002 , 0xFFF6 , S(756F),
    0x0000 , 0x0004 , 0xFFF3 , S(676F), S(7369)
};

static const unsigned short a_8[] = {
    0x0000 , 0x7365 , 0x0011 , 0x0000 , 0x0000 , 0x0000 , 0x0029 , 0x0000 ,
    0x0000 , 0x002E , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0044 , 0x0000 , 0x7A74 , 0x001A , 0x0000 , 0x001F , 0x0000 , 0x0000 ,
    0x0000 , 0x0024 , 0x0000 , 0x0003 , 0xFFFC , S(6369), S(0061), 0x0000 ,
    0x0003 , 0xFFFA , S(7461), S(0069), 0x0000 , 0x0003 , 0xFFFD , S(6C61),
    S(0069), 0x0000 , 0x0004 , 0xFFFC , S(6369), S(7469), 0x0000 , 0x6175 ,
    0x0032 , 0x0041 , 0x0000 , 0x636E , 0x0036 , 0x0039 , 0x0000 , 0x6969 ,
    0xFFFC , 0x0000 , 0x0003 , 0x003E , S(6974), S(006F), 0x0001 , 0x6161 ,
    0xFFFE , 0x0000 , 0x6666 , 0xFFFB , 0x0000 , 0x0003 , 0xFFFB , S(656E),
    S(0073)
};

static const unsigned short a_9[] = {
    0x0000 , 0x7463 , 0x0014 , 0x0000 , 0x0017 , 0x0000 , 0x0000 , 0x0000 ,
    0x0042 , 0x0000 , 0x0000 , 0x003F , 0x0046 , 0x004A , 0x0000 , 0x0000 ,
    0x0000 , 0x004E , 0x0051 , 0x0055 , 0x0000 , 0x6969 , 0xFFFF , 0x0000 ,
    0x7A63 , 0x0031 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0038 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x003F , 0x0000 , 0x0014 , 0x0000 , 0x0000 , 0x0000 ,
    0x0014 , 0x0000 , 0x6E6E , 0x0034 , 0x0000 , 0x6165 , 0xFFFF , 0xFFFF ,
    0x0000 , 0x6262 , 0x003B , 0x0000 , 0x6169 , 0xFFFF , 0xFFFF , 0x0000 ,
    0x6161 , 0xFFFF , 0x0000 , 0x0002 , 0xFFFF , S(7469), 0x0000 , 0x0002 ,
    0xFFFF , S(7369), 0x0000 , 0x0002 , 0xFFFE , S(6F69), 0x0000 , 0x6565 ,
    0xFFFF , 0x0000 , 0x0002 , 0xFFFF , S(756F), 0x0000 , 0x6E6E , 0x0058 ,
    0x0000 , 0x6165 , 0xFFFF , 0x005C , 0x0001 , 0x6D6D , 0x005F , 0x0001 ,
    0x6565 , 0xFFFF
};

static const unsigned short a_10[] = {
    0x0000 , 0x656C , 0xFFFF , 0xFFFE
};

static const unsigned short a_11[] = {
    0x0000 , 0x7561 , 0x0017 , 0x0025 , 0x002A , 0x0000 , 0x0030 , 0x0000 ,
    0x0035 , 0x003B , 0x0040 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0045 ,
    0x004A , 0x0000 , 0x0000 , 0x0000 , 0x004F , 0x0000 , 0x0063 , 0x0000 ,
    0x6E74 , 0x001B , 0x0020 , 0x0000 , 0x0003 , 0xC001 , S(6564), S(0073),
    0x0000 , 0x0003 , 0xC001 , S(616C), S(0073), 0x0000 , 0x0003 , 0xC001 ,
    S(6169), S(0073), 0x0000 , 0x0005 , 0xC001 , S(736F), S(6F6D), S(0073),
    0x0000 , 0x0004 , 0xFFFA , S(7261), S(796C), 0x0000 , 0x0005 , 0xFFFC ,
    S(6E65), S(6C74), S(0079), 0x0000 , 0x0003 , 0xC001 , S(776F), S(0065),
    0x0000 , 0x0003 , 0xFFFD , S(6C64), S(0079), 0x0000 , 0x0003 , 0xC001 ,
    S(7765), S(0073), 0x0000 , 0x0003 , 0xFFF9 , S(6C6E), S(0079), 0x0000 ,
    0x696B , 0x0053 , 0x0058 , 0x0000 , 0x0004 , 0xFFF8 , S(676E), S(796C),
    0x0000 , 0x6979 , 0x005C , 0xC001 , 0x0000 , 0x6573 , 0x0060 , 0xFFFF ,
    0x0000 , 0x7373 , 0xFFFE , 0x0000 , 0x0003 , 0xFFFB , S(6C67), S(0079)
};

static const unsigned char g_aeo[] = { 17, 64 };

static const unsigned char g_v[] = { 17, 65, 16, 1 };

static const unsigned char g_v_WXY[] = { 1, 17, 65, 208, 1 };

static const unsigned char g_valid_LI[] = { 55, 141, 2 };

static int r_shortv(struct SN_env * z) {
    do {
        int v_1 = z->l - z->c;
        if (out_grouping_b_U(z, g_v_WXY, 89, 121, 0)) goto lab0;
        if (in_grouping_b_U(z, g_v, 97, 121, 0)) goto lab0;
        if (out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab0;
        break;
    lab0:
        z->c = z->l - v_1;
        if (out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab1;
        if (in_grouping_b_U(z, g_v, 97, 121, 0)) goto lab1;
        if (z->c > z->lb) goto lab1;
        break;
    lab1:
        z->c = z->l - v_1;
        if (!(eq_s_b(z, 4, s_0))) return 0;
    } while (0);
    return 1;
}

extern int candidate_english_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int b_Y_found;
    int i_p2;
    int i_p1;
    do {
        int v_1 = z->c;
        z->bra = z->c;
        if (z->c + 2 >= z->l || z->p[z->c + 2] >> 5 != 3 || !((42750482 >> (z->p[z->c + 2] & 0x1f)) & 1)) goto lab0;
        among_var = find_among(z, a_11);
        if (!among_var) goto lab0;
        z->ket = z->c;
        if (z->c < z->l) goto lab0;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 3, s_1);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 3, s_2);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 3, s_3);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 5, s_4);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int ret = slice_from_s(z, 4, s_5);
                    if (ret < 0) return ret;
                }
                break;
            case 6:
                {
                    int ret = slice_from_s(z, 5, s_6);
                    if (ret < 0) return ret;
                }
                break;
            case 7:
                {
                    int ret = slice_from_s(z, 4, s_7);
                    if (ret < 0) return ret;
                }
                break;
            case 8:
                {
                    int ret = slice_from_s(z, 5, s_8);
                    if (ret < 0) return ret;
                }
                break;
        }
        break;
    lab0:
        z->c = v_1;
        {
            int ret = skip_utf8(z->p, z->c, z->l, 3);
            if (ret < 0) goto lab2;
            z->c = ret;
        }
        goto lab1;
    lab2:
        break;
    lab1:
        z->c = v_1;
        b_Y_found = 0;
        {
            int v_2 = z->c;
            z->bra = z->c;
            if (z->c == z->l || z->p[z->c] != '\'') goto lab4;
            z->c++;
            z->ket = z->c;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
        lab4:
            z->c = v_2;
        }
        {
            int v_3 = z->c;
            z->bra = z->c;
            if (z->c == z->l || z->p[z->c] != 'y') goto lab5;
            z->c++;
            z->ket = z->c;
            {
                int ret = slice_from_s(z, 1, s_9);
                if (ret < 0) return ret;
            }
            b_Y_found = 1;
        lab5:
            z->c = v_3;
        }
        {
            int v_4 = z->c;
            while (1) {
                int v_5 = z->c;
                while (1) {
                    int v_6 = z->c;
                    if (in_grouping_U(z, g_v, 97, 121, 0)) goto lab8;
                    z->bra = z->c;
                    if (z->c == z->l || z->p[z->c] != 'y') goto lab8;
                    z->c++;
                    z->ket = z->c;
                    z->c = v_6;
                    break;
                lab8:
                    z->c = v_6;
                    {
                        int ret = skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab7;
                        z->c = ret;
                    }
                }
                {
                    int ret = slice_from_s(z, 1, s_9);
                    if (ret < 0) return ret;
                }
                b_Y_found = 1;
                continue;
            lab7:
                z->c = v_5;
                break;
            }
            z->c = v_4;
        }
        i_p1 = z->l;
        i_p2 = z->l;
        {
            int v_7 = z->c;
            do {
                int v_8 = z->c;
                if (z->c + 3 >= z->l || z->p[z->c + 3] >> 5 != 3 || !((5513250 >> (z->p[z->c + 3] & 0x1f)) & 1)) goto lab11;
                if (!find_among(z, a_0)) goto lab11;
                break;
            lab11:
                z->c = v_8;
                {
                    int ret = out_grouping_U(z, g_v, 97, 121, 1);
                    if (ret < 0) goto lab10;
                    z->c += ret;
                }
                {
                    int ret = in_grouping_U(z, g_v, 97, 121, 1);
                    if (ret < 0) goto lab10;
                    z->c += ret;
                }
            } while (0);
            i_p1 = z->c;
            {
                int ret = out_grouping_U(z, g_v, 97, 121, 1);
                if (ret < 0) goto lab10;
                z->c += ret;
            }
            {
                int ret = in_grouping_U(z, g_v, 97, 121, 1);
                if (ret < 0) goto lab10;
                z->c += ret;
            }
            i_p2 = z->c;
        lab10:
            z->c = v_7;
        }
        z->lb = z->c; z->c = z->l;
        {
            int v_9 = z->l - z->c;
            {
                int v_10 = z->l - z->c;
                z->ket = z->c;
                if (z->c <= z->lb || (z->p[z->c - 1] != 39 && z->p[z->c - 1] != 115)) { z->c = z->l - v_10; goto lab13; }
                if (!find_among_b(z, a_1)) { z->c = z->l - v_10; goto lab13; }
                z->bra = z->c;
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
            lab13:
                ;
            }
            z->ket = z->c;
            if (z->c <= z->lb || (z->p[z->c - 1] != 100 && z->p[z->c - 1] != 115)) goto lab12;
            among_var = find_among_b(z, a_2);
            if (!among_var) goto lab12;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 2, s_10);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    do {
                        int v_11 = z->l - z->c;
                        {
                            int ret = skip_b_utf8(z->p, z->c, z->lb, 2);
                            if (ret < 0) goto lab14;
                            z->c = ret;
                        }
                        {
                            int ret = slice_from_s(z, 1, s_3);
                            if (ret < 0) return ret;
                        }
                        break;
                    lab14:
                        z->c = z->l - v_11;
                        {
                            int ret = slice_from_s(z, 2, s_11);
                            if (ret < 0) return ret;
                        }
                    } while (0);
                    break;
                case 3:
                    {
                        int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab12;
                        z->c = ret;
                    }
                    {
                        int ret = out_grouping_b_U(z, g_v, 97, 121, 1);
                        if (ret < 0) goto lab12;
                        z->c -= ret;
                    }
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        lab12:
            z->c = z->l - v_9;
        }
        {
            int v_12 = z->l - z->c;
            z->ket = z->c;
            if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((33554576 >> (z->p[z->c - 1] & 0x1f)) & 1)) among_var = -1; else
            among_var = find_among_b(z, a_5);
            z->bra = z->c;
            do {
                int v_13 = z->l - z->c;
                switch (among_var) {
                    case 1:
                        {
                            int v_14 = z->l - z->c;
                            if (i_p1 > z->c) goto lab17;
                            do {
                                int v_15 = z->l - z->c;
                                if (z->c - 2 <= z->lb || z->p[z->c - 1] != 99) goto lab18;
                                if (!find_among_b(z, a_3)) goto lab18;
                                if (z->c > z->lb) goto lab18;
                                break;
                            lab18:
                                z->c = z->l - v_15;
                                {
                                    int ret = slice_from_s(z, 2, s_12);
                                    if (ret < 0) return ret;
                                }
                            } while (0);
                        lab17:
                            z->c = z->l - v_14;
                        }
                        break;
                    case 2:
                        goto lab16;
                        break;
                    case 3:
                        if (z->c <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((34881536 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab16;
                        among_var = find_among_b(z, a_4);
                        if (!among_var) goto lab16;
                        switch (among_var) {
                            case 1:
                                {
                                    int v_16 = z->l - z->c;
                                    if (out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab16;
                                    if (z->c > z->lb) goto lab16;
                                    z->c = z->l - v_16;
                                }
                                z->bra = z->c;
                                {
                                    int ret = slice_from_s(z, 2, s_11);
                                    if (ret < 0) return ret;
                                }
                                break;
                            case 2:
                                if (z->c > z->lb) goto lab16;
                                break;
                        }
                        break;
                }
                break;
            lab16:
                z->c = z->l - v_13;
                {
                    int v_17 = z->l - z->c;
                    {
                        int ret = out_grouping_b_U(z, g_v, 97, 121, 1);
                        if (ret < 0) goto lab15;
                        z->c -= ret;
                    }
                    z->c = z->l - v_17;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                z->ket = z->c;
                z->bra = z->c;
                {
                    int v_18 = z->l - z->c;
                    if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((68514004 >> (z->p[z->c - 1] & 0x1f)) & 1)) among_var = 3; else
                    among_var = find_among_b(z, a_6);
                    switch (among_var) {
                        case 1:
                            {
                                int ret = slice_from_s(z, 1, s_6);
                                if (ret < 0) return ret;
                            }
                            goto lab15;
                            break;
                        case 2:
                            {
                                int v_19 = z->l - z->c;
                                if (in_grouping_b_U(z, g_aeo, 97, 111, 0)) goto lab19;
                                if (z->c > z->lb) goto lab19;
                                goto lab15;
                            lab19:
                                z->c = z->l - v_19;
                            }
                            break;
                        case 3:
                            if (z->c != i_p1) goto lab15;
                            {
                                int v_20 = z->l - z->c;
                                if (!r_shortv(z)) goto lab15;
                                z->c = z->l - v_20;
                            }
                            {
                                int ret = slice_from_s(z, 1, s_6);
                                if (ret < 0) return ret;
                            }
                            goto lab15;
                            break;
                    }
                    z->c = z->l - v_18;
                }
                z->ket = z->c;
                {
                    int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
                    if (ret < 0) goto lab15;
                    z->c = ret;
                }
                z->bra = z->c;
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
            } while (0);
        lab15:
            z->c = z->l - v_12;
        }
        {
            int v_21 = z->l - z->c;
            z->ket = z->c;
            do {
                if (z->c <= z->lb || z->p[z->c - 1] != 'y') goto lab21;
                z->c--;
                break;
            lab21:
                if (z->c <= z->lb || z->p[z->c - 1] != 'Y') goto lab20;
                z->c--;
            } while (0);
            z->bra = z->c;
            if (out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab20;
            if (z->c <= z->lb) goto lab20;
            {
                int ret = slice_from_s(z, 1, s_3);
                if (ret < 0) return ret;
            }
        lab20:
            z->c = z->l - v_21;
        }
        {
            int v_22 = z->l - z->c;
            z->ket = z->c;
            if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((1864192 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab22;
            among_var = find_among_b(z, a_7);
            if (!among_var) goto lab22;
            z->bra = z->c;
            if (i_p1 > z->c) goto lab22;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 4, s_13);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 4, s_14);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 4, s_15);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 4, s_16);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = slice_from_s(z, 3, s_17);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    {
                        int ret = slice_from_s(z, 3, s_18);
                        if (ret < 0) return ret;
                    }
                    break;
                case 7:
                    {
                        int ret = slice_from_s(z, 3, s_19);
                        if (ret < 0) return ret;
                    }
                    break;
                case 8:
                    {
                        int ret = slice_from_s(z, 2, s_20);
                        if (ret < 0) return ret;
                    }
                    break;
                case 9:
                    {
                        int ret = slice_from_s(z, 3, s_21);
                        if (ret < 0) return ret;
                    }
                    break;
                case 10:
                    {
                        int ret = slice_from_s(z, 3, s_22);
                        if (ret < 0) return ret;
                    }
                    break;
                case 11:
                    {
                        int ret = slice_from_s(z, 3, s_23);
                        if (ret < 0) return ret;
                    }
                    break;
                case 12:
                    {
                        int ret = slice_from_s(z, 3, s_24);
                        if (ret < 0) return ret;
                    }
                    break;
                case 13:
                    {
                        int ret = slice_from_s(z, 2, s_25);
                        if (ret < 0) return ret;
                    }
                    break;
                case 14:
                    if (z->c <= z->lb || z->p[z->c - 1] != 'l') goto lab22;
                    z->c--;
                    {
                        int ret = slice_from_s(z, 2, s_25);
                        if (ret < 0) return ret;
                    }
                    break;
                case 15:
                    {
                        int ret = slice_from_s(z, 4, s_26);
                        if (ret < 0) return ret;
                    }
                    break;
                case 16:
                    if (in_grouping_b_U(z, g_valid_LI, 99, 116, 0)) goto lab22;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        lab22:
            z->c = z->l - v_22;
        }
        {
            int v_23 = z->l - z->c;
            z->ket = z->c;
            if (z->c - 2 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((528928 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab23;
            among_var = find_among_b(z, a_8);
            if (!among_var) goto lab23;
            z->bra = z->c;
            if (i_p1 > z->c) goto lab23;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 4, s_13);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 3, s_19);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 2, s_20);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 2, s_27);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    if (i_p2 > z->c) goto lab23;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        lab23:
            z->c = z->l - v_23;
        }
        {
            int v_24 = z->l - z->c;
            z->ket = z->c;
            if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((1864232 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab24;
            among_var = find_among_b(z, a_9);
            if (!among_var) goto lab24;
            z->bra = z->c;
            if (i_p2 > z->c) goto lab24;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    do {
                        if (z->c <= z->lb || z->p[z->c - 1] != 's') goto lab25;
                        z->c--;
                        break;
                    lab25:
                        if (z->c <= z->lb || z->p[z->c - 1] != 't') goto lab24;
                        z->c--;
                    } while (0);
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        lab24:
            z->c = z->l - v_24;
        }
        {
            int v_25 = z->l - z->c;
            z->ket = z->c;
            if (z->c <= z->lb || (z->p[z->c - 1] != 101 && z->p[z->c - 1] != 108)) goto lab26;
            among_var = find_among_b(z, a_10);
            if (!among_var) goto lab26;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    do {
                        if (i_p2 > z->c) goto lab27;
                        break;
                    lab27:
                        if (i_p1 > z->c) goto lab26;
                        {
                            int v_26 = z->l - z->c;
                            if (!r_shortv(z)) goto lab28;
                            goto lab26;
                        lab28:
                            z->c = z->l - v_26;
                        }
                    } while (0);
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (i_p2 > z->c) goto lab26;
                    if (z->c <= z->lb || z->p[z->c - 1] != 'l') goto lab26;
                    z->c--;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        lab26:
            z->c = z->l - v_25;
        }
        z->c = z->lb;
        {
            int v_27 = z->c;
            if (!b_Y_found) goto lab29;
            while (1) {
                int v_28 = z->c;
                while (1) {
                    int v_29 = z->c;
                    z->bra = z->c;
                    if (z->c == z->l || z->p[z->c] != 'Y') goto lab31;
                    z->c++;
                    z->ket = z->c;
                    z->c = v_29;
                    break;
                lab31:
                    z->c = v_29;
                    {
                        int ret = skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab30;
                        z->c = ret;
                    }
                }
                {
                    int ret = slice_from_s(z, 1, s_28);
                    if (ret < 0) return ret;
                }
                continue;
            lab30:
                z->c = v_28;
                break;
            }
        lab29:
            z->c = v_27;
        }
    } while (0);
    return 1;
}

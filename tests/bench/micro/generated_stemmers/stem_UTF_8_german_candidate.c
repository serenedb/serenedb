/* Generated from german.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_german_candidate.h"

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
extern int candidate_german_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

#define s_12 (s_6 + 1)
#define s_2 (s_7 + 2)
#define s_7 (s_11 + 1)
#define s_6 (s_11 + 4)
static const symbol s_0[] = { 'U' };
static const symbol s_1[] = { 'Y' };
static const symbol s_15[] = { 'o' };
static const symbol s_3[] = { 0xC3, 0xA4 };
static const symbol s_4[] = { 0xC3, 0xB6 };
static const symbol s_5[] = { 0xC3, 0xBC };
static const symbol s_13[] = { 'u' };
static const symbol s_14[] = { 'a' };
static const symbol s_8[] = { 'l' };
static const symbol s_9[] = { 'i', 'g' };
static const symbol s_10[] = { 'e', 'r' };
static const symbol s_11[] = { 'e', 'n', 'i', 's', 's', 'y', 's', 't' };

static const unsigned short a_0[] = {
    0x0005 , 0xC361 , 0x0065 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0068 , 0x0000 , 0x006B , 0x0000 , 0x0000 , 0x0000 , 0x006E , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0071 , 0x0000 , 0x6565 , 0xFFFE ,
    0x0000 , 0x6565 , 0xFFFD , 0x0000 , 0x7575 , 0xC001 , 0x0000 , 0x6565 ,
    0xFFFC , 0x0000 , 0x9F9F , 0xFFFF
};

static const unsigned short a_1[] = {
    0x0005 , 0xC355 , 0xFFFE , 0x0000 , 0x0000 , 0x0000 , 0xFFFF , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0071 , 0x0000 , 0xBCA4 , 0xFFFD , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0xFFFC , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0xFFFE
};

static const unsigned short a_2[] = {
    0x0000 , 0x7365 , 0xFFFD , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0011 , 0x0014 , 0x0000 , 0x0000 , 0x0000 , 0x002E ,
    0x0031 , 0x0000 , 0x6565 , 0xFFFF , 0x0000 , 0x7265 , 0x0024 , 0x0000 ,
    0x0000 , 0x0000 , 0x002A , 0x0000 , 0x0000 , 0xFFFB , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x002E , 0x0003 , 0x0005 , 0xFFFE , S(7265),
    S(6E69), S(006E), 0x0000 , 0x0002 , 0xFFFE , S(7265), 0x0000 , 0x6565 ,
    0xFFFE , 0x0004 , 0x656E , 0xFFFD , 0x0035 , 0x0000 , 0x6C6C , 0xFFFB
};

static const unsigned short a_3[] = {
    0x0000 , 0x726B , 0x000A , 0x0000 , 0x0000 , 0x000F , 0x0000 , 0x0000 ,
    0x0000 , 0x0031 , 0x0000 , 0x0003 , 0xC001 , S(6974), S(0063), 0x0000 ,
    0x7261 , 0x0023 , 0x0000 , 0x0000 , 0x0027 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x002C , 0x0000 , 0x0002 , 0xC001 , S(6C70), 0x0000 ,
    0x0004 , 0xC001 , S(6567), S(726F), 0x0000 , 0x0004 , 0xC001 , S(6E69),
    S(6574), 0x0000 , 0x7474 , 0xC001
};

static const unsigned short a_4[] = {
    0x0000 , 0x746E , 0x0009 , 0x0000 , 0x0000 , 0x0000 , 0x0009 , 0x0000 ,
    0x000C , 0x0000 , 0x6565 , 0xFFFF , 0x0000 , 0x6573 , 0xFFFD , 0x0010 ,
    0x0002 , 0x6565 , 0xFFFF
};

static const unsigned short a_5[] = {
    0x0000 , 0x6768 , 0x0004 , 0x0007 , 0x0000 , 0x6969 , 0xFFFF , 0x0000 ,
    0x0003 , 0xFFFF , S(696C), S(0063)
};

static const unsigned short a_6[] = {
    0x0000 , 0x7464 , 0x0013 , 0x0000 , 0x0000 , 0x0017 , 0x001E , 0x0000 ,
    0x0000 , 0x0028 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x002B , 0x0000 , 0x0002 , 0xFFFF , S(6E65), 0x0000 ,
    0x696E , 0xFFFE , 0x001B , 0x0000 , 0x7575 , 0xFFFF , 0x0000 , 0x6363 ,
    0x0021 , 0x0000 , 0x6973 , 0x0025 , 0x0028 , 0x0000 , 0x6C6C , 0xFFFD ,
    0x0000 , 0x6969 , 0xFFFE , 0x0000 , 0x0002 , 0x002F , S(6965), 0x0000 ,
    0x686B , 0xFFFD , 0xFFFC
};

static const unsigned short a_7[] = {
    0x0000 , 0x7327 , 0xFFFF , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x004F , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0054 , 0x0000 ,
    0x0003 , 0xFFFF , S(7327), S(0063), 0x0000 , 0x2727 , 0xFFFF
};

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32, 8 };

static const unsigned char g_et_ending[] = { 1, 128, 198, 227, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128 };

static const unsigned char g_s_ending[] = { 117, 30, 5 };

static const unsigned char g_st_ending[] = { 117, 30, 4 };

extern int candidate_german_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_x;
    int i_p2;
    int i_p1;
    {
        int v_1 = z->c;
        {
            int v_2 = z->c;
            while (1) {
                int v_3 = z->c;
                while (1) {
                    int v_4 = z->c;
                    if (in_grouping_U(z, g_v, 97, 252, 0)) goto lab2;
                    z->bra = z->c;
                    do {
                        int v_5 = z->c;
                        if (z->c == z->l || z->p[z->c] != 'u') goto lab3;
                        z->c++;
                        z->ket = z->c;
                        if (in_grouping_U(z, g_v, 97, 252, 0)) goto lab3;
                        {
                            int ret = slice_from_s(z, 1, s_0);
                            if (ret < 0) return ret;
                        }
                        break;
                    lab3:
                        z->c = v_5;
                        if (z->c == z->l || z->p[z->c] != 'y') goto lab2;
                        z->c++;
                        z->ket = z->c;
                        if (in_grouping_U(z, g_v, 97, 252, 0)) goto lab2;
                        {
                            int ret = slice_from_s(z, 1, s_1);
                            if (ret < 0) return ret;
                        }
                    } while (0);
                    z->c = v_4;
                    break;
                lab2:
                    z->c = v_4;
                    {
                        int ret = skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab1;
                        z->c = ret;
                    }
                }
                continue;
            lab1:
                z->c = v_3;
                break;
            }
            z->c = v_2;
        }
        while (1) {
            int v_6 = z->c;
            z->bra = z->c;
            among_var = find_among(z, a_0);
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 2, s_2);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 2, s_3);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 2, s_4);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 2, s_5);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab4;
                        z->c = ret;
                    }
                    break;
            }
            continue;
        lab4:
            z->c = v_6;
            break;
        }
        z->c = v_1;
    }
    {
        int v_7 = z->c;
        i_p1 = z->l;
        i_p2 = z->l;
        {
            int v_8 = z->c;
            {
                int ret = skip_utf8(z->p, z->c, z->l, 3);
                if (ret < 0) goto lab5;
                z->c = ret;
            }
            i_x = z->c;
            z->c = v_8;
        }
        {
            int ret = out_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        {
            int ret = in_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        i_p1 = z->c;
        if (i_p1 >= i_x) goto lab6;
        i_p1 = i_x;
    lab6:
        {
            int ret = out_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        {
            int ret = in_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        i_p2 = z->c;
    lab5:
        z->c = v_7;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_9 = z->l - z->c;
        z->ket = z->c;
        if (z->c <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((811040 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab8;
        among_var = find_among_b(z, a_2);
        if (!among_var) goto lab8;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab8;
        switch (among_var) {
            case 1:
                if (!(eq_s_b(z, 4, s_6))) goto lab9;
                goto lab8;
            lab9:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_10 = z->l - z->c;
                    z->ket = z->c;
                    if (z->c <= z->lb || z->p[z->c - 1] != 's') { z->c = z->l - v_10; goto lab10; }
                    z->c--;
                    z->bra = z->c;
                    if (!(eq_s_b(z, 3, s_7))) { z->c = z->l - v_10; goto lab10; }
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab10:
                    ;
                }
                break;
            case 4:
                if (in_grouping_b_U(z, g_s_ending, 98, 116, 0)) goto lab8;
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int ret = slice_from_s(z, 1, s_8);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab8:
        z->c = z->l - v_9;
    }
    {
        int v_11 = z->l - z->c;
        z->ket = z->c;
        if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((1327104 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab11;
        among_var = find_among_b(z, a_4);
        if (!among_var) goto lab11;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab11;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                if (in_grouping_b_U(z, g_st_ending, 98, 116, 0)) goto lab11;
                {
                    int ret = skip_b_utf8(z->p, z->c, z->lb, 3);
                    if (ret < 0) goto lab11;
                    z->c = ret;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int v_12 = z->l - z->c;
                    if (in_grouping_b_U(z, g_et_ending, 85, 228, 0)) goto lab11;
                    z->c = z->l - v_12;
                }
                {
                    int v_13 = z->l - z->c;
                    if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((280576 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab12;
                    if (!find_among_b(z, a_3)) goto lab12;
                    goto lab11;
                lab12:
                    z->c = z->l - v_13;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab11:
        z->c = z->l - v_11;
    }
    {
        int v_14 = z->l - z->c;
        z->ket = z->c;
        if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((1051024 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab13;
        among_var = find_among_b(z, a_6);
        if (!among_var) goto lab13;
        z->bra = z->c;
        if (i_p2 > z->c) goto lab13;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_15 = z->l - z->c;
                    z->ket = z->c;
                    if (!(eq_s_b(z, 2, s_9))) { z->c = z->l - v_15; goto lab14; }
                    z->bra = z->c;
                    if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab15;
                    z->c--;
                    { z->c = z->l - v_15; goto lab14; }
                lab15:
                    if (i_p2 > z->c) { z->c = z->l - v_15; goto lab14; }
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab14:
                    ;
                }
                break;
            case 2:
                if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab16;
                z->c--;
                goto lab13;
            lab16:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_16 = z->l - z->c;
                    z->ket = z->c;
                    do {
                        if (!(eq_s_b(z, 2, s_10))) goto lab18;
                        break;
                    lab18:
                        if (!(eq_s_b(z, 2, s_11))) { z->c = z->l - v_16; goto lab17; }
                    } while (0);
                    z->bra = z->c;
                    if (i_p1 > z->c) { z->c = z->l - v_16; goto lab17; }
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab17:
                    ;
                }
                break;
            case 4:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_17 = z->l - z->c;
                    z->ket = z->c;
                    if (z->c - 1 <= z->lb || (z->p[z->c - 1] != 103 && z->p[z->c - 1] != 104)) { z->c = z->l - v_17; goto lab19; }
                    if (!find_among_b(z, a_5)) { z->c = z->l - v_17; goto lab19; }
                    z->bra = z->c;
                    if (i_p2 > z->c) { z->c = z->l - v_17; goto lab19; }
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab19:
                    ;
                }
                break;
        }
    lab13:
        z->c = z->l - v_14;
    }
    {
        int v_18 = z->l - z->c;
        z->ket = z->c;
        if (!find_among_b(z, a_7)) goto lab20;
        z->bra = z->c;
        {
            int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
            if (ret < 0) goto lab20;
            z->c = ret;
        }
        if (z->c <= z->lb) goto lab20;
        {
            int ret = slice_del(z);
            if (ret < 0) return ret;
        }
    lab20:
        z->c = z->l - v_18;
    }
    z->c = z->lb;
    {
        int v_19 = z->c;
        while (1) {
            int v_20 = z->c;
            z->bra = z->c;
            among_var = find_among(z, a_1);
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 1, s_12);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 1, s_13);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 1, s_14);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 1, s_15);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab22;
                        z->c = ret;
                    }
                    break;
            }
            continue;
        lab22:
            z->c = v_20;
            break;
        }
        z->c = v_19;
    }
    return 1;
}

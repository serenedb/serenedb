/* Generated from english.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_english_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

typedef struct SN_env SN_env;

#if defined(__GNUC__) || defined(__clang__)
#define SNOWBALL_UNUSED __attribute__((unused))
#else
#define SNOWBALL_UNUSED
#endif

static inline SNOWBALL_UNUSED int snowball_decode_two_byte_utf8(const symbol * p, int c, int limit, int * ch) {
    if (c + 1 >= limit) return 0;
    int lead = p[c];
    int tail = p[c + 1];
    if (lead < 0xC2 || lead > 0xDF || (tail & 0xC0) != 0x80) return 0;
    *ch = ((lead & 0x1F) << 6) | (tail & 0x3F);
    return 2;
}

static inline SNOWBALL_UNUSED int snowball_decode_two_byte_b_utf8(const symbol * p, int c, int limit, int * ch) {
    if (c - limit < 2) return 0;
    int lead = p[c - 2];
    int tail = p[c - 1];
    if (lead < 0xC2 || lead > 0xDF || (tail & 0xC0) != 0x80) return 0;
    *ch = ((lead & 0x1F) << 6) | (tail & 0x3F);
    return 2;
}

static inline SNOWBALL_UNUSED int snowball_grouping_contains(const unsigned char * s, int min, int max, int ch) {
    return ch >= min && ch <= max &&
           (s[(ch - min) >> 3] & (1u << ((ch - min) & 7))) != 0;
}

static inline SNOWBALL_UNUSED int snowball_in_grouping_U(SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        if (z->c >= z->l) return -1;
        int ch = z->p[z->c];
        int width = 1;
        if (ch >= 0x80) {
            width = snowball_decode_two_byte_utf8(z->p, z->c, z->l, &ch);
            if (!width) return in_grouping_U(z, s, min, max, repeat);
        }
        if (!snowball_grouping_contains(s, min, max, ch)) return width;
        z->c += width;
    } while (repeat);
    return 0;
}

static inline SNOWBALL_UNUSED int snowball_in_grouping_b_U(SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        if (z->c <= z->lb) return -1;
        int ch = z->p[z->c - 1];
        int width = 1;
        if (ch >= 0x80) {
            width = snowball_decode_two_byte_b_utf8(z->p, z->c, z->lb, &ch);
            if (!width) return in_grouping_b_U(z, s, min, max, repeat);
        }
        if (!snowball_grouping_contains(s, min, max, ch)) return width;
        z->c -= width;
    } while (repeat);
    return 0;
}

static inline SNOWBALL_UNUSED int snowball_out_grouping_U(SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        if (z->c >= z->l) return -1;
        int ch = z->p[z->c];
        int width = 1;
        if (ch >= 0x80) {
            width = snowball_decode_two_byte_utf8(z->p, z->c, z->l, &ch);
            if (!width) return out_grouping_U(z, s, min, max, repeat);
        }
        if (snowball_grouping_contains(s, min, max, ch)) return width;
        z->c += width;
    } while (repeat);
    return 0;
}

static inline SNOWBALL_UNUSED int snowball_out_grouping_b_U(SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        if (z->c <= z->lb) return -1;
        int ch = z->p[z->c - 1];
        int width = 1;
        if (ch >= 0x80) {
            width = snowball_decode_two_byte_b_utf8(z->p, z->c, z->lb, &ch);
            if (!width) return out_grouping_b_U(z, s, min, max, repeat);
        }
        if (snowball_grouping_contains(s, min, max, ch)) return width;
        z->c -= width;
    } while (repeat);
    return 0;
}

static inline SNOWBALL_UNUSED int snowball_skip_utf8(const symbol * p, int c, int limit, int n) {
    if (n == 1) {
        if (c >= limit) return -1;
        int lead = p[c];
        if (lead < 0x80) return c + 1;
        if (lead >= 0xC2 && lead <= 0xDF && c + 1 < limit && (p[c + 1] & 0xC0) == 0x80) return c + 2;
        if (lead >= 0xE0 && lead <= 0xEF && c + 2 < limit && (p[c + 1] & 0xC0) == 0x80 && (p[c + 2] & 0xC0) == 0x80 && (lead != 0xE0 || p[c + 1] >= 0xA0) && (lead != 0xED || p[c + 1] < 0xA0)) return c + 3;
        if (lead >= 0xF0 && lead <= 0xF4 && c + 3 < limit && (p[c + 1] & 0xC0) == 0x80 && (p[c + 2] & 0xC0) == 0x80 && (p[c + 3] & 0xC0) == 0x80 && (lead != 0xF0 || p[c + 1] >= 0x90) && (lead != 0xF4 || p[c + 1] < 0x90)) return c + 4;
        return skip_utf8(p, c, limit, 1);
    }
    for (; n > 0; --n) {
        if (c >= limit) return -1;
        int b = p[c++];
        if (b >= 0xC0) {
            while (c < limit && p[c] >= 0x80 && p[c] < 0xC0) ++c;
        }
    }
    return c;
}

static inline SNOWBALL_UNUSED int snowball_skip_b_utf8(const symbol * p, int c, int limit, int n) {
    if (n == 1) {
        if (c <= limit) return -1;
        int tail = p[c - 1];
        if (tail < 0x80) return c - 1;
        if ((tail & 0xC0) == 0x80 && c - limit >= 4) {
            int lead = p[c - 4];
            if (lead >= 0xF0 && lead <= 0xF4 && (p[c - 3] & 0xC0) == 0x80 && (p[c - 2] & 0xC0) == 0x80 && (lead != 0xF0 || p[c - 3] >= 0x90) && (lead != 0xF4 || p[c - 3] < 0x90)) return c - 4;
        }
        if ((tail & 0xC0) == 0x80 && c - limit >= 3) {
            int lead = p[c - 3];
            if (lead >= 0xE0 && lead <= 0xEF && (p[c - 2] & 0xC0) == 0x80 && (lead != 0xE0 || p[c - 2] >= 0xA0) && (lead != 0xED || p[c - 2] < 0xA0)) return c - 3;
        }
        if ((tail & 0xC0) == 0x80 && c - limit >= 2) {
            int lead = p[c - 2];
            if (lead >= 0xC2 && lead <= 0xDF) return c - 2;
        }
        return skip_b_utf8(p, c, limit, 1);
    }
    for (; n > 0; --n) {
        if (c <= limit) return -1;
        int b = p[--c];
        if (b >= 0x80) {
            while (c > limit && p[c] < 0xC0) --c;
        }
    }
    return c;
}

static inline SNOWBALL_UNUSED int snowball_slice_del(SN_env * z) {
    if (z->bra >= 0 && z->bra <= z->ket && z->ket == z->l && z->l <= SIZE(z->p)) {
        SET_SIZE(z->p, z->bra);
        z->l = z->bra;
        if (z->c > z->bra) z->c = z->bra;
        z->ket = z->bra;
        return 0;
    }
    return slice_del(z);
}

#undef SNOWBALL_UNUSED

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

static const unsigned char g_aeo[] = { 17, 64 };

static const unsigned char g_v[] = { 17, 65, 16, 1 };

static const unsigned char g_v_WXY[] = { 1, 17, 65, 208, 1 };

static const unsigned char g_valid_LI[] = { 55, 141, 2 };

static int r_shortv(struct SN_env * z) {
    do {
        int v_1 = z->l - z->c;
        if (snowball_out_grouping_b_U(z, g_v_WXY, 89, 121, 0)) goto lab0;
        if (snowball_in_grouping_b_U(z, g_v, 97, 121, 0)) goto lab0;
        if (snowball_out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab0;
        break;
    lab0:
        z->c = z->l - v_1;
        if (snowball_out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab1;
        if (snowball_in_grouping_b_U(z, g_v, 97, 121, 0)) goto lab1;
        if (z->c > z->lb) goto lab1;
        break;
    lab1:
        z->c = z->l - v_1;
        if (z->c - z->lb < 4 || __builtin_memcmp(z->p + z->c - 4, s_0, 4) != 0) return 0;
        z->c -= 4;
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
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among < z->l) {
                switch (z->p[c_among]) {
                    case 'c':
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "osmos", 5) == 0) { among_var = -1; z->c = c_among + 6; break; }
                        break;
                    case 'g':
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ently", 5) == 0) { among_var = 4; z->c = c_among + 6; break; }
                        break;
                    case 's':
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ingly", 5) == 0) { among_var = 8; z->c = c_among + 6; break; }
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "kies", 4) == 0) { among_var = 2; z->c = c_among + 5; break; }
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "kis", 3) == 0) { among_var = 1; z->c = c_among + 4; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ky", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        break;
                    case 'a':
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ndes", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "tlas", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        break;
                    case 'e':
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "arly", 4) == 0) { among_var = 6; z->c = c_among + 5; break; }
                        break;
                    case 'b':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ias", 3) == 0) { among_var = -1; z->c = c_among + 4; break; }
                        break;
                    case 'h':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "owe", 3) == 0) { among_var = -1; z->c = c_among + 4; break; }
                        break;
                    case 'i':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "dly", 3) == 0) { among_var = 3; z->c = c_among + 4; break; }
                        break;
                    case 'n':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ews", 3) == 0) { among_var = -1; z->c = c_among + 4; break; }
                        break;
                    case 'o':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "nly", 3) == 0) { among_var = 7; z->c = c_among + 4; break; }
                        break;
                    case 'u':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "gly", 3) == 0) { among_var = 5; z->c = c_among + 4; break; }
                        break;
                }
            }
        }
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
            int ret = snowball_skip_utf8(z->p, z->c, z->l, 3);
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
                int ret = snowball_slice_del(z);
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
                    if (snowball_in_grouping_U(z, g_v, 97, 121, 0)) goto lab8;
                    z->bra = z->c;
                    if (z->c == z->l || z->p[z->c] != 'y') goto lab8;
                    z->c++;
                    z->ket = z->c;
                    z->c = v_6;
                    break;
                lab8:
                    z->c = v_6;
                    {
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among < z->l) {
                        switch (z->p[c_among]) {
                            case 'u':
                                if (c_among + 7 <= z->l && __builtin_memcmp(z->p + c_among + 1, "nivers", 6) == 0) { among_var = -1; z->c = c_among + 7; break; }
                                break;
                            case 'c':
                                if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ommun", 5) == 0) { among_var = -1; z->c = c_among + 6; break; }
                                break;
                            case 'a':
                                if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "rsen", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                                break;
                            case 'e':
                                if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "merg", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                                break;
                            case 'g':
                                if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ener", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                                break;
                            case 'i':
                                if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "nter", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                                break;
                            case 'l':
                                if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ater", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                                break;
                            case 'o':
                                if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "rgan", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                                break;
                            case 'p':
                                if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ast", 3) == 0) { among_var = -1; z->c = c_among + 4; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab11;
                break;
            lab11:
                z->c = v_8;
                {
                    int ret = snowball_out_grouping_U(z, g_v, 97, 121, 1);
                    if (ret < 0) goto lab10;
                    z->c += ret;
                }
                {
                    int ret = snowball_in_grouping_U(z, g_v, 97, 121, 1);
                    if (ret < 0) goto lab10;
                    z->c += ret;
                }
            } while (0);
            i_p1 = z->c;
            {
                int ret = snowball_out_grouping_U(z, g_v, 97, 121, 1);
                if (ret < 0) goto lab10;
                z->c += ret;
            }
            {
                int ret = snowball_in_grouping_U(z, g_v, 97, 121, 1);
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
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case '\'':
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "'s", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                                break;
                            case 's':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "'", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->c = z->l - v_10; goto lab13; }
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
            lab13:
                ;
            }
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 's':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "sse", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ie", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 1) { among_var = 3; z->c = c_among - 1; break; }
                            break;
                        case 'd':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ie", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
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
                            int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 2);
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
                        int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab12;
                        z->c = ret;
                    }
                    {
                        int ret = snowball_out_grouping_b_U(z, g_v, 97, 121, 1);
                        if (ret < 0) goto lab12;
                        z->c -= ret;
                    }
                    {
                        int ret = snowball_slice_del(z);
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
            {
                int c_among = z->c;
                among_var = -1;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'y':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "eedl", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ingl", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "edl", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                            break;
                        case 'd':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ee", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            break;
                        case 'g':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "in", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
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
                                {
                                    int c_among = z->c;
                                    among_var = 0;
                                    if (c_among > z->lb) {
                                        switch (z->p[c_among - 1]) {
                                            case 'c':
                                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "suc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "pro", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ex", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                                break;
                                        }
                                    }
                                }
                                if (!among_var) goto lab18;
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
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 'n':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "eve", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "can", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "in", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                        break;
                                    case 'r':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ear", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "her", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        break;
                                    case 't':
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ou", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                        break;
                                    case 'y':
                                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) goto lab16;
                        switch (among_var) {
                            case 1:
                                {
                                    int v_16 = z->l - z->c;
                                    if (snowball_out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab16;
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
                        int ret = snowball_out_grouping_b_U(z, g_v, 97, 121, 1);
                        if (ret < 0) goto lab15;
                        z->c -= ret;
                    }
                    z->c = z->l - v_17;
                }
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                z->ket = z->c;
                z->bra = z->c;
                {
                    int v_18 = z->l - z->c;
                    {
                        int c_among = z->c;
                        among_var = 3;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 'b':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "b", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'd':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "d", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'f':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "f", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'g':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "g", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'l':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "b", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 'm':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "m", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'n':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'p':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "p", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'r':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "r", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 't':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 'z':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
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
                                if (snowball_in_grouping_b_U(z, g_aeo, 97, 111, 0)) goto lab19;
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
                    int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                    if (ret < 0) goto lab15;
                    z->c = ret;
                }
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
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
            if (snowball_out_grouping_b_U(z, g_v, 97, 121, 0)) goto lab20;
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'l':
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ationa", 6) == 0) { among_var = 7; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "tiona", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 'n':
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "izatio", 6) == 0) { among_var = 6; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "atio", 4) == 0) { among_var = 7; z->c = c_among - 5; break; }
                            break;
                        case 's':
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ivenes", 6) == 0) { among_var = 11; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "fulnes", 6) == 0) { among_var = 9; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ousnes", 6) == 0) { among_var = 10; z->c = c_among - 7; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "lessl", 5) == 0) { among_var = 15; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "bilit", 5) == 0) { among_var = 12; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "full", 4) == 0) { among_var = 9; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ousl", 4) == 0) { among_var = 10; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "entl", 4) == 0) { among_var = 5; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "alit", 4) == 0) { among_var = 8; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ivit", 4) == 0) { among_var = 11; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "anc", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "enc", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abl", 3) == 0) { among_var = 4; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "all", 3) == 0) { among_var = 8; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "og", 2) == 0) { among_var = 14; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "bl", 2) == 0) { among_var = 12; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = 16; z->c = c_among - 2; break; }
                            break;
                        case 'm':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "alis", 4) == 0) { among_var = 8; z->c = c_among - 5; break; }
                            break;
                        case 't':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ogis", 4) == 0) { among_var = 13; z->c = c_among - 5; break; }
                            break;
                        case 'r':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ize", 3) == 0) { among_var = 6; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ato", 3) == 0) { among_var = 7; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
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
                    if (snowball_in_grouping_b_U(z, g_valid_LI, 99, 116, 0)) goto lab22;
                    {
                        int ret = snowball_slice_del(z);
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'l':
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ationa", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "tiona", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ica", 3) == 0) { among_var = 4; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "fu", 2) == 0) { among_var = 5; z->c = c_among - 3; break; }
                            break;
                        case 'e':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "icat", 4) == 0) { among_var = 4; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ativ", 4) == 0) { among_var = 6; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "aliz", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "icit", 4) == 0) { among_var = 4; z->c = c_among - 5; break; }
                            break;
                        case 's':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "nes", 3) == 0) { among_var = 5; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
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
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    if (i_p2 > z->c) goto lab23;
                    {
                        int ret = snowball_slice_del(z);
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 't':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "emen", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "men", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "an", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "en", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'e':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "anc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "enc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ibl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "at", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iz", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "it", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'm':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "is", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'n':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "io", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                            break;
                        case 's':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ou", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'c':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 'l':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 'r':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab24;
            z->bra = z->c;
            if (i_p2 > z->c) goto lab24;
            switch (among_var) {
                case 1:
                    {
                        int ret = snowball_slice_del(z);
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
                        int ret = snowball_slice_del(z);
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'e':
                            if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                            break;
                        case 'l':
                            if (c_among - z->lb >= 1) { among_var = 2; z->c = c_among - 1; break; }
                            break;
                    }
                }
            }
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
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (i_p2 > z->c) goto lab26;
                    if (z->c <= z->lb || z->p[z->c - 1] != 'l') goto lab26;
                    z->c--;
                    {
                        int ret = snowball_slice_del(z);
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
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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

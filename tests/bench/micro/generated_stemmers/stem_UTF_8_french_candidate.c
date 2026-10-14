/* Generated from french.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_french_candidate.h"

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
extern int candidate_french_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

#define s_11 (s_0 + 1)
#define s_21 (s_8 + 1)
#define s_1 (s_9 + 2)
#define s_8 (s_7 + 1)
#define s_12 (s_5 + 1)
#define s_10 (s_17 + 1)
static const symbol s_0[] = { 'q', 'u' };
static const symbol s_2[] = { 'I' };
static const symbol s_3[] = { 'Y' };
static const symbol s_4[] = { 0xC3, 0xAB };
static const symbol s_5[] = { 'H', 'e', 'n', 't' };
static const symbol s_6[] = { 0xC3, 0xAF };
static const symbol s_7[] = { 'H', 'i', 'c' };
static const symbol s_24[] = { 'y' };
static const symbol s_9[] = { 'i', 'q', 'U' };
static const symbol s_22[] = { 0xC3, 0xA9 };
static const symbol s_23[] = { 0xC3, 0xA8 };
static const symbol s_13[] = { 'a', 't' };
static const symbol s_14[] = { 'e', 'u', 'x' };
static const symbol s_15[] = { 'a', 'b', 'l' };
static const symbol s_16[] = { 'e', 'a', 'u' };
static const symbol s_17[] = { 'a', 'l', 'o', 'g' };
static const symbol s_18[] = { 'o', 'u' };
static const symbol s_19[] = { 'a', 'n', 't' };
static const symbol s_20[] = { 0xC3, 0xA7 };

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 130, 103, 8, 5 };

static const unsigned char g_oux_ending[] = { 65, 85 };

static const unsigned char g_elision_char[] = { 131, 14, 131 };

static const unsigned char g_keep_with_s[] = { 1, 65, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128 };

extern int candidate_french_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_p2;
    int i_p1;
    int i_pV;
    {
        int v_1 = z->c;
        z->bra = z->c;
        do {
            if (snowball_in_grouping_U(z, g_elision_char, 99, 122, 0)) goto lab1;
            break;
        lab1:
            if (z->l - z->c < 2 || __builtin_memcmp(z->p + z->c, s_0, 2) != 0) goto lab0;
            z->c += 2;
        } while (0);
        if (z->c == z->l || z->p[z->c] != '\'') goto lab0;
        z->c++;
        z->ket = z->c;
        if (z->c >= z->l) goto lab0;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab0:
        z->c = v_1;
    }
    {
        int v_2 = z->c;
        while (1) {
            int v_3 = z->c;
            while (1) {
                int v_4 = z->c;
                do {
                    int v_5 = z->c;
                    if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab5;
                    z->bra = z->c;
                    do {
                        int v_6 = z->c;
                        if (z->c == z->l || z->p[z->c] != 'u') goto lab6;
                        z->c++;
                        z->ket = z->c;
                        if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab6;
                        {
                            int ret = slice_from_s(z, 1, s_1);
                            if (ret < 0) return ret;
                        }
                        break;
                    lab6:
                        z->c = v_6;
                        if (z->c == z->l || z->p[z->c] != 'i') goto lab7;
                        z->c++;
                        z->ket = z->c;
                        if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab7;
                        {
                            int ret = slice_from_s(z, 1, s_2);
                            if (ret < 0) return ret;
                        }
                        break;
                    lab7:
                        z->c = v_6;
                        if (z->c == z->l || z->p[z->c] != 'y') goto lab5;
                        z->c++;
                        z->ket = z->c;
                        {
                            int ret = slice_from_s(z, 1, s_3);
                            if (ret < 0) return ret;
                        }
                    } while (0);
                    break;
                lab5:
                    z->c = v_5;
                    z->bra = z->c;
                    if (z->l - z->c < 2 || __builtin_memcmp(z->p + z->c, s_4, 2) != 0) goto lab8;
                    z->c += 2;
                    z->ket = z->c;
                    {
                        int ret = slice_from_s(z, 2, s_5);
                        if (ret < 0) return ret;
                    }
                    break;
                lab8:
                    z->c = v_5;
                    z->bra = z->c;
                    if (z->l - z->c < 2 || __builtin_memcmp(z->p + z->c, s_6, 2) != 0) goto lab9;
                    z->c += 2;
                    z->ket = z->c;
                    {
                        int ret = slice_from_s(z, 2, s_7);
                        if (ret < 0) return ret;
                    }
                    break;
                lab9:
                    z->c = v_5;
                    z->bra = z->c;
                    if (z->c == z->l || z->p[z->c] != 'y') goto lab10;
                    z->c++;
                    z->ket = z->c;
                    if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab10;
                    {
                        int ret = slice_from_s(z, 1, s_3);
                        if (ret < 0) return ret;
                    }
                    break;
                lab10:
                    z->c = v_5;
                    if (z->c == z->l || z->p[z->c] != 'q') goto lab4;
                    z->c++;
                    z->bra = z->c;
                    if (z->c == z->l || z->p[z->c] != 'u') goto lab4;
                    z->c++;
                    z->ket = z->c;
                    {
                        int ret = slice_from_s(z, 1, s_1);
                        if (ret < 0) return ret;
                    }
                } while (0);
                z->c = v_4;
                break;
            lab4:
                z->c = v_4;
                {
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab3;
                    z->c = ret;
                }
            }
            continue;
        lab3:
            z->c = v_3;
            break;
        }
        z->c = v_2;
    }
    i_pV = z->l;
    i_p1 = z->l;
    i_p2 = z->l;
    {
        int v_7 = z->c;
        do {
            int v_8 = z->c;
            if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab13;
            if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab13;
            {
                int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                if (ret < 0) goto lab13;
                z->c = ret;
            }
            break;
        lab13:
            z->c = v_8;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 'c':
                            if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ol", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                            break;
                        case 'p':
                            if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ar", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                            break;
                        case 't':
                            if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ap", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                            break;
                        case 'n':
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "i", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab14;
            switch (among_var) {
                case 1:
                    if (snowball_in_grouping_U(z, g_v, 97, 251, 0)) goto lab14;
                    break;
            }
            break;
        lab14:
            z->c = v_8;
            {
                int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                if (ret < 0) goto lab12;
                z->c = ret;
            }
            {
                int ret = snowball_out_grouping_U(z, g_v, 97, 251, 1);
                if (ret < 0) goto lab12;
                z->c += ret;
            }
        } while (0);
        i_pV = z->c;
    lab12:
        z->c = v_7;
    }
    {
        int v_9 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 251, 1);
            if (ret < 0) goto lab15;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 251, 1);
            if (ret < 0) goto lab15;
            z->c += ret;
        }
        i_p1 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 251, 1);
            if (ret < 0) goto lab15;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 251, 1);
            if (ret < 0) goto lab15;
            z->c += ret;
        }
        i_p2 = z->c;
    lab15:
        z->c = v_9;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_10 = z->l - z->c;
        do {
            int v_11 = z->l - z->c;
            {
                int v_12 = z->l - z->c;
                do {
                    int v_13 = z->l - z->c;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 's':
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "issement", 8) == 0) { among_var = 13; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "atrice", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "logie", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "usion", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ation", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ution", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ateur", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ement", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iqUe", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ance", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ence", 4) == 0) { among_var = 5; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "able", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "isme", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "euse", 4) == 0) { among_var = 12; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iste", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ment", 4) == 0) { among_var = 16; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "it\303\251", 4) == 0) { among_var = 7; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ive", 3) == 0) { among_var = 8; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "if", 2) == 0) { among_var = 8; z->c = c_among - 3; break; }
                                    break;
                                case 't':
                                    if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "issemen", 7) == 0) { among_var = 13; z->c = c_among - 8; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ammen", 5) == 0) { among_var = 14; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "emmen", 5) == 0) { among_var = 15; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "emen", 4) == 0) { among_var = 6; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "men", 3) == 0) { among_var = 16; z->c = c_among - 4; break; }
                                    break;
                                case 'e':
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "atric", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "logi", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "iqU", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "anc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "enc", 3) == 0) { among_var = 5; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ism", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "eus", 3) == 0) { among_var = 12; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ist", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 8; z->c = c_among - 3; break; }
                                    break;
                                case 'n':
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "usio", 4) == 0) { among_var = 4; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "atio", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "utio", 4) == 0) { among_var = 4; z->c = c_among - 5; break; }
                                    break;
                                case 'r':
                                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ateu", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                    break;
                                case 'x':
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "eau", 3) == 0) { among_var = 9; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "au", 2) == 0) { among_var = 10; z->c = c_among - 3; break; }
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "eu", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ou", 2) == 0) { among_var = 11; z->c = c_among - 3; break; }
                                    break;
                                case 0xA9:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "it\303", 3) == 0) { among_var = 7; z->c = c_among - 4; break; }
                                    break;
                                case 'f':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 8; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab18;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            {
                                int v_14 = z->l - z->c;
                                z->ket = z->c;
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_8, 2) != 0) { z->c = z->l - v_14; goto lab19; }
                                z->c -= 2;
                                z->bra = z->c;
                                do {
                                    int v_15 = z->l - z->c;
                                    if (i_p2 > z->c) goto lab20;
                                    {
                                        int ret = snowball_slice_del(z);
                                        if (ret < 0) return ret;
                                    }
                                    break;
                                lab20:
                                    z->c = z->l - v_15;
                                    {
                                        int ret = slice_from_s(z, 3, s_9);
                                        if (ret < 0) return ret;
                                    }
                                } while (0);
                            lab19:
                                ;
                            }
                            break;
                        case 3:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = slice_from_s(z, 3, s_10);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 4:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = slice_from_s(z, 1, s_11);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 5:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = slice_from_s(z, 3, s_12);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 6:
                            if (i_pV > z->c) goto lab18;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            {
                                int v_16 = z->l - z->c;
                                z->ket = z->c;
                                {
                                    int c_among = z->c;
                                    among_var = 0;
                                    if (c_among > z->lb) {
                                        switch (z->p[c_among - 1]) {
                                            case 'r':
                                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "I\303\250", 3) == 0) { among_var = 4; z->c = c_among - 4; break; }
                                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "i\303\250", 3) == 0) { among_var = 4; z->c = c_among - 4; break; }
                                                break;
                                            case 'U':
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iq", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                                                break;
                                            case 'l':
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ab", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                                                break;
                                            case 's':
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "eu", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                                break;
                                            case 'v':
                                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                                break;
                                        }
                                    }
                                }
                                if (!among_var) { z->c = z->l - v_16; goto lab21; }
                                z->bra = z->c;
                                switch (among_var) {
                                    case 1:
                                        if (i_p2 > z->c) { z->c = z->l - v_16; goto lab21; }
                                        {
                                            int ret = snowball_slice_del(z);
                                            if (ret < 0) return ret;
                                        }
                                        z->ket = z->c;
                                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_13, 2) != 0) { z->c = z->l - v_16; goto lab21; }
                                        z->c -= 2;
                                        z->bra = z->c;
                                        if (i_p2 > z->c) { z->c = z->l - v_16; goto lab21; }
                                        {
                                            int ret = snowball_slice_del(z);
                                            if (ret < 0) return ret;
                                        }
                                        break;
                                    case 2:
                                        do {
                                            int v_17 = z->l - z->c;
                                            if (i_p2 > z->c) goto lab22;
                                            {
                                                int ret = snowball_slice_del(z);
                                                if (ret < 0) return ret;
                                            }
                                            break;
                                        lab22:
                                            z->c = z->l - v_17;
                                            if (i_p1 > z->c) { z->c = z->l - v_16; goto lab21; }
                                            {
                                                int ret = slice_from_s(z, 3, s_14);
                                                if (ret < 0) return ret;
                                            }
                                        } while (0);
                                        break;
                                    case 3:
                                        if (i_p2 > z->c) { z->c = z->l - v_16; goto lab21; }
                                        {
                                            int ret = snowball_slice_del(z);
                                            if (ret < 0) return ret;
                                        }
                                        break;
                                    case 4:
                                        if (i_pV > z->c) { z->c = z->l - v_16; goto lab21; }
                                        {
                                            int ret = slice_from_s(z, 1, s_8);
                                            if (ret < 0) return ret;
                                        }
                                        break;
                                }
                            lab21:
                                ;
                            }
                            break;
                        case 7:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            {
                                int v_18 = z->l - z->c;
                                z->ket = z->c;
                                {
                                    int c_among = z->c;
                                    among_var = 0;
                                    if (c_among > z->lb) {
                                        switch (z->p[c_among - 1]) {
                                            case 'l':
                                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abi", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                                break;
                                            case 'c':
                                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                                break;
                                            case 'v':
                                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                                                break;
                                        }
                                    }
                                }
                                if (!among_var) { z->c = z->l - v_18; goto lab23; }
                                z->bra = z->c;
                                switch (among_var) {
                                    case 1:
                                        do {
                                            int v_19 = z->l - z->c;
                                            if (i_p2 > z->c) goto lab24;
                                            {
                                                int ret = snowball_slice_del(z);
                                                if (ret < 0) return ret;
                                            }
                                            break;
                                        lab24:
                                            z->c = z->l - v_19;
                                            {
                                                int ret = slice_from_s(z, 3, s_15);
                                                if (ret < 0) return ret;
                                            }
                                        } while (0);
                                        break;
                                    case 2:
                                        do {
                                            int v_20 = z->l - z->c;
                                            if (i_p2 > z->c) goto lab25;
                                            {
                                                int ret = snowball_slice_del(z);
                                                if (ret < 0) return ret;
                                            }
                                            break;
                                        lab25:
                                            z->c = z->l - v_20;
                                            {
                                                int ret = slice_from_s(z, 3, s_9);
                                                if (ret < 0) return ret;
                                            }
                                        } while (0);
                                        break;
                                    case 3:
                                        if (i_p2 > z->c) { z->c = z->l - v_18; goto lab23; }
                                        {
                                            int ret = snowball_slice_del(z);
                                            if (ret < 0) return ret;
                                        }
                                        break;
                                }
                            lab23:
                                ;
                            }
                            break;
                        case 8:
                            if (i_p2 > z->c) goto lab18;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            {
                                int v_21 = z->l - z->c;
                                z->ket = z->c;
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_13, 2) != 0) { z->c = z->l - v_21; goto lab26; }
                                z->c -= 2;
                                z->bra = z->c;
                                if (i_p2 > z->c) { z->c = z->l - v_21; goto lab26; }
                                {
                                    int ret = snowball_slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                z->ket = z->c;
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_8, 2) != 0) { z->c = z->l - v_21; goto lab26; }
                                z->c -= 2;
                                z->bra = z->c;
                                do {
                                    int v_22 = z->l - z->c;
                                    if (i_p2 > z->c) goto lab27;
                                    {
                                        int ret = snowball_slice_del(z);
                                        if (ret < 0) return ret;
                                    }
                                    break;
                                lab27:
                                    z->c = z->l - v_22;
                                    {
                                        int ret = slice_from_s(z, 3, s_9);
                                        if (ret < 0) return ret;
                                    }
                                } while (0);
                            lab26:
                                ;
                            }
                            break;
                        case 9:
                            {
                                int ret = slice_from_s(z, 3, s_16);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 10:
                            if (i_p1 > z->c) goto lab18;
                            {
                                int ret = slice_from_s(z, 2, s_17);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 11:
                            if (snowball_in_grouping_b_U(z, g_oux_ending, 98, 112, 0)) goto lab18;
                            {
                                int ret = slice_from_s(z, 2, s_18);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 12:
                            do {
                                int v_23 = z->l - z->c;
                                if (i_p2 > z->c) goto lab28;
                                {
                                    int ret = snowball_slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                            lab28:
                                z->c = z->l - v_23;
                                if (i_p1 > z->c) goto lab18;
                                {
                                    int ret = slice_from_s(z, 3, s_14);
                                    if (ret < 0) return ret;
                                }
                            } while (0);
                            break;
                        case 13:
                            if (i_p1 > z->c) goto lab18;
                            if (snowball_out_grouping_b_U(z, g_v, 97, 251, 0)) goto lab18;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 14:
                            if (i_pV > z->c) goto lab18;
                            {
                                int ret = slice_from_s(z, 3, s_19);
                                if (ret < 0) return ret;
                            }
                            goto lab18;
                            break;
                        case 15:
                            if (i_pV > z->c) goto lab18;
                            {
                                int ret = slice_from_s(z, 3, s_12);
                                if (ret < 0) return ret;
                            }
                            goto lab18;
                            break;
                        case 16:
                            {
                                int v_24 = z->l - z->c;
                                if (snowball_in_grouping_b_U(z, g_v, 97, 251, 0)) goto lab18;
                                if (i_pV > z->c) goto lab18;
                                z->c = z->l - v_24;
                            }
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            goto lab18;
                            break;
                    }
                    break;
                lab18:
                    z->c = z->l - v_13;
                    {
                        int v_25;
                        if (z->c < i_pV) goto lab29;
                        v_25 = z->lb; z->lb = i_pV;
                        z->ket = z->c;
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 's':
                                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "issante", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ission", 6) == 0) { among_var = 1; z->c = c_among - 7; break; }
                                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "issant", 6) == 0) { among_var = 1; z->c = c_among - 7; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "issai", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "irion", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "isson", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\256me", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "isse", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\256te", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "irai", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iron", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ira", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ie", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 't':
                                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "issaIen", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "iraIen", 6) == 0) { among_var = 1; z->c = c_among - 7; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "issai", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "issan", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "issen", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "irai", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iren", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iron", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 'e':
                                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "issant", 6) == 0) { among_var = 1; z->c = c_among - 7; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "iss", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 'z':
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "issie", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "irie", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "isse", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ire", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        break;
                                    case 'i':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ira", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                                        break;
                                    case 'a':
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ir", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                        break;
                                    case 'r':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->lb = v_25; goto lab29; }
                        z->bra = z->c;
                        if (z->c <= z->lb || z->p[z->c - 1] != 'H') goto lab30;
                        z->c--;
                        { z->lb = v_25; goto lab29; }
                    lab30:
                        if (snowball_out_grouping_b_U(z, g_v, 97, 251, 0)) { z->lb = v_25; goto lab29; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        z->lb = v_25;
                    }
                    break;
                lab29:
                    z->c = z->l - v_13;
                    {
                        int v_26;
                        if (z->c < i_pV) goto lab17;
                        v_26 = z->lb; z->lb = i_pV;
                        z->ket = z->c;
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 's':
                                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "assion", 6) == 0) { among_var = 3; z->c = c_among - 7; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "erion", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\242me", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "aise", 4) == 0) { among_var = 4; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "asse", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ante", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\242te", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "erai", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "eron", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "era", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251e", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "eai", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ion", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ant", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ai", 2) == 0) { among_var = 4; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                                        break;
                                    case 't':
                                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "eraIen", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\303\250ren", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "assen", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "erai", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "aIen", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "eron", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ai", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "an", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\242", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                                        break;
                                    case 'z':
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "assie", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "erie", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ere", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ie", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                        break;
                                    case 'e':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ais", 3) == 0) { among_var = 4; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ass", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ant", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                        break;
                                    case 'i':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "era", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                                        break;
                                    case 'a':
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "er", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                        if (c_among - z->lb >= 1) { among_var = 3; z->c = c_among - 1; break; }
                                        break;
                                    case 'r':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                        break;
                                    case 0xA9:
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->lb = v_26; goto lab17; }
                        z->bra = z->c;
                        z->lb = v_26;
                    }
                    switch (among_var) {
                        case 1:
                            if (i_p2 > z->c) goto lab17;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 3:
                            {
                                int v_27 = z->l - z->c;
                                if (z->c <= z->lb || z->p[z->c - 1] != 'e') { z->c = z->l - v_27; goto lab31; }
                                z->c--;
                                if (i_pV > z->c) { z->c = z->l - v_27; goto lab31; }
                                z->bra = z->c;
                            lab31:
                                ;
                            }
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 4:
                            {
                                int v_28 = z->l - z->c;
                                {
                                    int c_among = z->c;
                                    among_var = 0;
                                    if (c_among > z->lb) {
                                        switch (z->p[c_among - 1]) {
                                            case 'l':
                                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251p", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                                break;
                                            case 'v':
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "au", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                        }
                                    }
                                }
                                if (!among_var) goto lab32;
                                switch (among_var) {
                                    case 1:
                                        {
                                            int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                                            if (ret < 0) goto lab32;
                                            z->c = ret;
                                        }
                                        if (z->c > z->lb) goto lab32;
                                        break;
                                }
                                goto lab17;
                            lab32:
                                z->c = z->l - v_28;
                            }
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                    }
                } while (0);
                z->c = z->l - v_12;
                {
                    int v_29 = z->l - z->c;
                    z->ket = z->c;
                    do {
                        int v_30 = z->l - z->c;
                        if (z->c <= z->lb || z->p[z->c - 1] != 'Y') goto lab34;
                        z->c--;
                        z->bra = z->c;
                        {
                            int ret = slice_from_s(z, 1, s_8);
                            if (ret < 0) return ret;
                        }
                        break;
                    lab34:
                        z->c = z->l - v_30;
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_20, 2) != 0) { z->c = z->l - v_29; goto lab33; }
                        z->c -= 2;
                        z->bra = z->c;
                        {
                            int ret = slice_from_s(z, 1, s_21);
                            if (ret < 0) return ret;
                        }
                    } while (0);
                lab33:
                    ;
                }
            }
            break;
        lab17:
            z->c = z->l - v_11;
            {
                int v_31 = z->l - z->c;
                z->ket = z->c;
                if (z->c <= z->lb || z->p[z->c - 1] != 's') { z->c = z->l - v_31; goto lab35; }
                z->c--;
                z->bra = z->c;
                {
                    int v_32 = z->l - z->c;
                    do {
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_7, 2) != 0) goto lab36;
                        z->c -= 2;
                        break;
                    lab36:
                        if (snowball_out_grouping_b_U(z, g_keep_with_s, 97, 232, 0)) { z->c = z->l - v_31; goto lab35; }
                    } while (0);
                    z->c = z->l - v_32;
                }
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
            lab35:
                ;
            }
            {
                int v_33;
                if (z->c < i_pV) goto lab16;
                v_33 = z->lb; z->lb = i_pV;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'e':
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "I\303\250r", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "i\303\250r", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 1) { among_var = 3; z->c = c_among - 1; break; }
                                break;
                            case 'n':
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "io", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 'r':
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "Ie", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ie", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_33; goto lab16; }
                z->bra = z->c;
                switch (among_var) {
                    case 1:
                        if (i_p2 > z->c) { z->lb = v_33; goto lab16; }
                        do {
                            if (z->c <= z->lb || z->p[z->c - 1] != 's') goto lab37;
                            z->c--;
                            break;
                        lab37:
                            if (z->c <= z->lb || z->p[z->c - 1] != 't') { z->lb = v_33; goto lab16; }
                            z->c--;
                        } while (0);
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 2:
                        {
                            int ret = slice_from_s(z, 1, s_8);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 3:
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        break;
                }
                z->lb = v_33;
            }
        } while (0);
    lab16:
        z->c = z->l - v_10;
    }
    {
        int v_34 = z->l - z->c;
        {
            int v_35 = z->l - z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'l':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "eil", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "el", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 'n':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "en", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "on", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 't':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "et", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab38;
            z->c = z->l - v_35;
        }
        z->ket = z->c;
        {
            int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
            if (ret < 0) goto lab38;
            z->c = ret;
        }
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab38:
        z->c = z->l - v_34;
    }
    {
        int v_36 = z->l - z->c;
        {
            int v_37 = 1;
            while (1) {
                if (snowball_out_grouping_b_U(z, g_v, 97, 251, 0)) goto lab40;
                v_37--;
                continue;
            lab40:
                break;
            }
            if (v_37 > 0) goto lab39;
        }
        z->ket = z->c;
        do {
            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_22, 2) != 0) goto lab41;
            z->c -= 2;
            break;
        lab41:
            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_23, 2) != 0) goto lab39;
            z->c -= 2;
        } while (0);
        z->bra = z->c;
        {
            int ret = slice_from_s(z, 1, s_12);
            if (ret < 0) return ret;
        }
    lab39:
        z->c = z->l - v_36;
    }
    z->c = z->lb;
    {
        int v_38 = z->c;
        while (1) {
            int v_39 = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 7;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 'H':
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "e", 1) == 0) { among_var = 4; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "i", 1) == 0) { among_var = 5; z->c = c_among + 2; break; }
                            if (c_among + 1 <= z->l) { among_var = 6; z->c = c_among + 1; break; }
                            break;
                        case 'I':
                            if (c_among + 1 <= z->l) { among_var = 1; z->c = c_among + 1; break; }
                            break;
                        case 'U':
                            if (c_among + 1 <= z->l) { among_var = 2; z->c = c_among + 1; break; }
                            break;
                        case 'Y':
                            if (c_among + 1 <= z->l) { among_var = 3; z->c = c_among + 1; break; }
                            break;
                    }
                }
            }
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 1, s_8);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 1, s_11);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 1, s_24);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 2, s_4);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = slice_from_s(z, 2, s_6);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 7:
                    {
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab43;
                        z->c = ret;
                    }
                    break;
            }
            continue;
        lab43:
            z->c = v_39;
            break;
        }
        z->c = v_38;
    }
    return 1;
}

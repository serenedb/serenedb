/* Generated from spanish.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_spanish_candidate.h"

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
extern int candidate_spanish_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

#define s_10 (s_0 + 4)
#define s_3 (s_8 + 3)
static const symbol s_0[] = { 'i', 'e', 'n', 'd', 'o' };
static const symbol s_1[] = { 'a', 'n', 'd', 'o' };
static const symbol s_2[] = { 'a', 'r' };
static const symbol s_9[] = { 'a', 't' };
static const symbol s_4[] = { 'i', 'r' };
static const symbol s_5[] = { 'i', 'c' };
static const symbol s_6[] = { 'l', 'o', 'g' };
static const symbol s_7[] = { 'u' };
static const symbol s_8[] = { 'e', 'n', 't', 'e', 'r' };

static const unsigned char g_v[] = { 17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 17, 4, 10 };

extern int candidate_spanish_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_p2;
    int i_p1;
    int i_pV;
    i_pV = z->l;
    i_p1 = z->l;
    i_p2 = z->l;
    {
        int v_1 = z->c;
        do {
            int v_2 = z->c;
            if (snowball_in_grouping_U(z, g_v, 97, 252, 0)) goto lab2;
            do {
                int v_3 = z->c;
                if (snowball_out_grouping_U(z, g_v, 97, 252, 0)) goto lab3;
                {
                    int ret = snowball_out_grouping_U(z, g_v, 97, 252, 1);
                    if (ret < 0) goto lab3;
                    z->c += ret;
                }
                break;
            lab3:
                z->c = v_3;
                if (snowball_in_grouping_U(z, g_v, 97, 252, 0)) goto lab2;
                {
                    int ret = snowball_in_grouping_U(z, g_v, 97, 252, 1);
                    if (ret < 0) goto lab2;
                    z->c += ret;
                }
            } while (0);
            break;
        lab2:
            z->c = v_2;
            if (snowball_out_grouping_U(z, g_v, 97, 252, 0)) goto lab1;
            do {
                int v_4 = z->c;
                if (snowball_out_grouping_U(z, g_v, 97, 252, 0)) goto lab4;
                {
                    int ret = snowball_out_grouping_U(z, g_v, 97, 252, 1);
                    if (ret < 0) goto lab4;
                    z->c += ret;
                }
                break;
            lab4:
                z->c = v_4;
                if (snowball_in_grouping_U(z, g_v, 97, 252, 0)) goto lab1;
                {
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab1;
                    z->c = ret;
                }
            } while (0);
        } while (0);
        i_pV = z->c;
    lab1:
        z->c = v_1;
    }
    {
        int v_5 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        i_p1 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 252, 1);
            if (ret < 0) goto lab5;
            z->c += ret;
        }
        i_p2 = z->c;
    lab5:
        z->c = v_5;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_6 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 's':
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "sela", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "selo", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "la", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "le", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "lo", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "no", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 'a':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "sel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'o':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "sel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'e':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "m", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab6;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'o':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "i\303\251nd", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iend", 4) == 0) { among_var = 6; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "yend", 4) == 0) { among_var = 7; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\241nd", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "and", 3) == 0) { among_var = 6; z->c = c_among - 4; break; }
                        break;
                    case 'r':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\241", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 4; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\255", 2) == 0) { among_var = 5; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab6;
        if (i_pV > z->c) goto lab6;
        switch (among_var) {
            case 1:
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 5, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 4, s_1);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 2, s_2);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 2, s_3);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 2, s_4);
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
                if (z->c <= z->lb || z->p[z->c - 1] != 'u') goto lab6;
                z->c--;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab6:
        z->c = z->l - v_6;
    }
    {
        int v_7 = z->l - z->c;
        do {
            int v_8 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 's':
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "amiento", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "imiento", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "log\303\255a", 6) == 0) { among_var = 3; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "acione", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ucione", 6) == 0) { among_var = 4; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ancia", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "encia", 5) == 0) { among_var = 5; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "adora", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "idade", 5) == 0) { among_var = 8; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "adore", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ista", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "anza", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "able", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ible", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ante", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ismo", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ica", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "osa", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "iva", 3) == 0) { among_var = 9; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ico", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "oso", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ivo", 3) == 0) { among_var = 9; z->c = c_among - 4; break; }
                            break;
                        case 'o':
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "amient", 6) == 0) { among_var = 1; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "imient", 6) == 0) { among_var = 1; z->c = c_among - 7; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ism", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ic", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "os", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 9; z->c = c_among - 3; break; }
                            break;
                        case 'a':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "log\303\255", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "anci", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "enci", 4) == 0) { among_var = 5; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ador", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ist", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "anz", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ic", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "os", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 9; z->c = c_among - 3; break; }
                            break;
                        case 'e':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ament", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ment", 4) == 0) { among_var = 7; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ibl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ant", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                            break;
                        case 'n':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "aci\303\263", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "uci\303\263", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "acio", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ucio", 4) == 0) { among_var = 4; z->c = c_among - 5; break; }
                            break;
                        case 'd':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ida", 3) == 0) { among_var = 8; z->c = c_among - 4; break; }
                            break;
                        case 'r':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ado", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab8;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_9 = z->l - z->c;
                        z->ket = z->c;
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_5, 2) != 0) { z->c = z->l - v_9; goto lab9; }
                        z->c -= 2;
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_9; goto lab9; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab9:
                        ;
                    }
                    break;
                case 3:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = slice_from_s(z, 3, s_6);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = slice_from_s(z, 1, s_7);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = slice_from_s(z, 4, s_8);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    if (i_p1 > z->c) goto lab8;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_10 = z->l - z->c;
                        z->ket = z->c;
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 'c':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                        break;
                                    case 'd':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                        break;
                                    case 's':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                        break;
                                    case 'v':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->c = z->l - v_10; goto lab10; }
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_10; goto lab10; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        switch (among_var) {
                            case 1:
                                z->ket = z->c;
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_9, 2) != 0) { z->c = z->l - v_10; goto lab10; }
                                z->c -= 2;
                                z->bra = z->c;
                                if (i_p2 > z->c) { z->c = z->l - v_10; goto lab10; }
                                {
                                    int ret = snowball_slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                        }
                    lab10:
                        ;
                    }
                    break;
                case 7:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_11 = z->l - z->c;
                        z->ket = z->c;
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 'e':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ibl", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ant", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->c = z->l - v_11; goto lab11; }
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_11; goto lab11; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab11:
                        ;
                    }
                    break;
                case 8:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_12 = z->l - z->c;
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
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 'v':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->c = z->l - v_12; goto lab12; }
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_12; goto lab12; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab12:
                        ;
                    }
                    break;
                case 9:
                    if (i_p2 > z->c) goto lab8;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_13 = z->l - z->c;
                        z->ket = z->c;
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_9, 2) != 0) { z->c = z->l - v_13; goto lab13; }
                        z->c -= 2;
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_13; goto lab13; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab13:
                        ;
                    }
                    break;
            }
            break;
        lab8:
            z->c = z->l - v_8;
            {
                int v_14;
                if (z->c < i_pV) goto lab14;
                v_14 = z->lb; z->lb = i_pV;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'n':
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "yero", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ya", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ye", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 'o':
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "yend", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "y", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 's':
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "yamo", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "yai", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ya", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ye", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 0xB3:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "y\303", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 'a':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "y", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "y", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_14; goto lab14; }
                z->bra = z->c;
                z->lb = v_14;
            }
            if (z->c <= z->lb || z->p[z->c - 1] != 'u') goto lab14;
            z->c--;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab14:
            z->c = z->l - v_8;
            {
                int v_15;
                if (z->c < i_pV) goto lab7;
                v_15 = z->lb; z->lb = i_pV;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 's':
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "i\303\251ramo", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "ar\303\255amo", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "er\303\255amo", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "ir\303\255amo", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "i\303\251semo", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ar\303\255ai", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "er\303\255ai", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ir\303\255ai", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "\303\241bamo", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "\303\241ramo", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "\303\241semo", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ar\303\255a", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "er\303\255a", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ir\303\255a", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ierai", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iesei", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "astei", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "istei", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ar\303\251i", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "er\303\251i", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ir\303\251i", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\303\255amo", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "aremo", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "eremo", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iremo", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iera", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iese", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "abai", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "arai", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\255ai", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "asei", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ar\303\241", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "er\303\241", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ir\303\241", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "aba", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ada", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ida", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ara", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\255a", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ase", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\241i", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251i", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ado", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ido", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "amo", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "emo", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imo", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\255", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 'n':
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ar\303\255a", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "er\303\255a", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ir\303\255a", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iera", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iese", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iero", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ar\303\241", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "er\303\241", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ir\303\241", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "aba", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ara", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\255a", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ase", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "aro", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 'a':
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ar\303\255", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "er\303\255", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ir\303\255", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ier", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ab", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ad", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "id", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ar", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\255", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                break;
                            case 'o':
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iend", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "and", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ad", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "id", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ies", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ast", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ist", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "as", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                break;
                            case 0xA1:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ar\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "er\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ir\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                break;
                            case 0xA9:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ar\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "er\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ir\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                break;
                            case 0xB3:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "i\303", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                                break;
                            case 'd':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                break;
                            case 'r':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_15; goto lab7; }
                z->bra = z->c;
                z->lb = v_15;
            }
            switch (among_var) {
                case 1:
                    {
                        int v_16 = z->l - z->c;
                        if (z->c <= z->lb || z->p[z->c - 1] != 'u') { z->c = z->l - v_16; goto lab15; }
                        z->c--;
                        {
                            int v_17 = z->l - z->c;
                            if (z->c <= z->lb || z->p[z->c - 1] != 'g') { z->c = z->l - v_16; goto lab15; }
                            z->c--;
                            z->c = z->l - v_17;
                        }
                    lab15:
                        ;
                    }
                    z->bra = z->c;
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
            }
        } while (0);
    lab7:
        z->c = z->l - v_7;
    }
    {
        int v_18 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 's':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA1:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0xAD:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB3:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'a':
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                    case 'e':
                        if (c_among - z->lb >= 1) { among_var = 2; z->c = c_among - 1; break; }
                        break;
                    case 'o':
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab16;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                if (i_pV > z->c) goto lab16;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                if (i_pV > z->c) goto lab16;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_19 = z->l - z->c;
                    z->ket = z->c;
                    if (z->c <= z->lb || z->p[z->c - 1] != 'u') { z->c = z->l - v_19; goto lab17; }
                    z->c--;
                    z->bra = z->c;
                    {
                        int v_20 = z->l - z->c;
                        if (z->c <= z->lb || z->p[z->c - 1] != 'g') { z->c = z->l - v_19; goto lab17; }
                        z->c--;
                        z->c = z->l - v_20;
                    }
                    if (i_pV > z->c) { z->c = z->l - v_19; goto lab17; }
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab17:
                    ;
                }
                break;
        }
    lab16:
        z->c = z->l - v_18;
    }
    z->c = z->lb;
    {
        int v_21 = z->c;
        while (1) {
            int v_22 = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 6;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xC3:
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\241", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\251", 1) == 0) { among_var = 2; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\255", 1) == 0) { among_var = 3; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\263", 1) == 0) { among_var = 4; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272", 1) == 0) { among_var = 5; z->c = c_among + 2; break; }
                            break;
                    }
                }
            }
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 1, s_1);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 1, s_3);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 1, s_0);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 1, s_10);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = slice_from_s(z, 1, s_7);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    {
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab19;
                        z->c = ret;
                    }
                    break;
            }
            continue;
        lab19:
            z->c = v_22;
            break;
        }
        z->c = v_21;
    }
    return 1;
}

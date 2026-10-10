/* Generated from german.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_german_candidate.h"

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
                    if (snowball_in_grouping_U(z, g_v, 97, 252, 0)) goto lab2;
                    z->bra = z->c;
                    do {
                        int v_5 = z->c;
                        if (z->c == z->l || z->p[z->c] != 'u') goto lab3;
                        z->c++;
                        z->ket = z->c;
                        if (snowball_in_grouping_U(z, g_v, 97, 252, 0)) goto lab3;
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
                        if (snowball_in_grouping_U(z, g_v, 97, 252, 0)) goto lab2;
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
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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
            {
                int c_among = z->c;
                among_var = 5;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 'a':
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "e", 1) == 0) { among_var = 2; z->c = c_among + 2; break; }
                            break;
                        case 'o':
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "e", 1) == 0) { among_var = 3; z->c = c_among + 2; break; }
                            break;
                        case 'q':
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "u", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                            break;
                        case 'u':
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "e", 1) == 0) { among_var = 4; z->c = c_among + 2; break; }
                            break;
                        case 0xC3:
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\237", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                            break;
                    }
                }
            }
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
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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
                int ret = snowball_skip_utf8(z->p, z->c, z->l, 3);
                if (ret < 0) goto lab5;
                z->c = ret;
            }
            i_x = z->c;
            z->c = v_8;
        }
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
        if (i_p1 >= i_x) goto lab6;
        i_p1 = i_x;
    lab6:
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
        z->c = v_7;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_9 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'n':
                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "erinne", 6) == 0) { among_var = 2; z->c = c_among - 7; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "eri", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "er", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = 5; z->c = c_among - 2; break; }
                        break;
                    case 's':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ln", 2) == 0) { among_var = 5; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 4; z->c = c_among - 1; break; }
                        break;
                    case 'm':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'r':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 'e':
                        if (c_among - z->lb >= 1) { among_var = 3; z->c = c_among - 1; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab8;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab8;
        switch (among_var) {
            case 1:
                if (z->c - z->lb < 4 || __builtin_memcmp(z->p + z->c - 4, s_6, 4) != 0) goto lab9;
                z->c -= 4;
                goto lab8;
            lab9:
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
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_10 = z->l - z->c;
                    z->ket = z->c;
                    if (z->c <= z->lb || z->p[z->c - 1] != 's') { z->c = z->l - v_10; goto lab10; }
                    z->c--;
                    z->bra = z->c;
                    if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_7, 3) != 0) { z->c = z->l - v_10; goto lab10; }
                    z->c -= 3;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab10:
                    ;
                }
                break;
            case 4:
                if (snowball_in_grouping_b_U(z, g_s_ending, 98, 116, 0)) goto lab8;
                {
                    int ret = snowball_slice_del(z);
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
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 't':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "es", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 'n':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'r':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab11;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab11;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                if (snowball_in_grouping_b_U(z, g_st_ending, 98, 116, 0)) goto lab11;
                {
                    int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 3);
                    if (ret < 0) goto lab11;
                    z->c = ret;
                }
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int v_12 = z->l - z->c;
                    if (snowball_in_grouping_b_U(z, g_et_ending, 85, 228, 0)) goto lab11;
                    z->c = z->l - v_12;
                }
                {
                    int v_13 = z->l - z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 'n':
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "geord", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "inter", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "pla", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                    break;
                                case 'k':
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "tic", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                    break;
                                case 'r':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab12;
                    goto lab11;
                lab12:
                    z->c = z->l - v_13;
                }
                {
                    int ret = snowball_slice_del(z);
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
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'h':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "lic", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "isc", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 't':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "hei", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "kei", 3) == 0) { among_var = 4; z->c = c_among - 4; break; }
                        break;
                    case 'd':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "en", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        break;
                    case 'g':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "un", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 'k':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab13;
        z->bra = z->c;
        if (i_p2 > z->c) goto lab13;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_15 = z->l - z->c;
                    z->ket = z->c;
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_9, 2) != 0) { z->c = z->l - v_15; goto lab14; }
                    z->c -= 2;
                    z->bra = z->c;
                    if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab15;
                    z->c--;
                    { z->c = z->l - v_15; goto lab14; }
                lab15:
                    if (i_p2 > z->c) { z->c = z->l - v_15; goto lab14; }
                    {
                        int ret = snowball_slice_del(z);
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
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_16 = z->l - z->c;
                    z->ket = z->c;
                    do {
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_10, 2) != 0) goto lab18;
                        z->c -= 2;
                        break;
                    lab18:
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_11, 2) != 0) { z->c = z->l - v_16; goto lab17; }
                        z->c -= 2;
                    } while (0);
                    z->bra = z->c;
                    if (i_p1 > z->c) { z->c = z->l - v_16; goto lab17; }
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab17:
                    ;
                }
                break;
            case 4:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_17 = z->l - z->c;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 'h':
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "lic", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 'g':
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) { z->c = z->l - v_17; goto lab19; }
                    z->bra = z->c;
                    if (i_p2 > z->c) { z->c = z->l - v_17; goto lab19; }
                    {
                        int ret = snowball_slice_del(z);
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
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'h':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "'sc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 's':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "'", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case '\'':
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab20;
        z->bra = z->c;
        {
            int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
            if (ret < 0) goto lab20;
            z->c = ret;
        }
        if (z->c <= z->lb) goto lab20;
        {
            int ret = snowball_slice_del(z);
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
            {
                int c_among = z->c;
                among_var = 5;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xC3:
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\244", 1) == 0) { among_var = 3; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\266", 1) == 0) { among_var = 4; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\274", 1) == 0) { among_var = 2; z->c = c_among + 2; break; }
                            break;
                        case 'U':
                            if (c_among + 1 <= z->l) { among_var = 2; z->c = c_among + 1; break; }
                            break;
                        case 'Y':
                            if (c_among + 1 <= z->l) { among_var = 1; z->c = c_among + 1; break; }
                            break;
                    }
                }
            }
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
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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

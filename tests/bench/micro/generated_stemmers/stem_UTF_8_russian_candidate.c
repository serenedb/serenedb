/* Generated from russian.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_russian_candidate.h"

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
extern int candidate_russian_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static const symbol s_0[] = { 0xD1, 0x91 };
static const symbol s_1[] = { 0xD0, 0xB5 };
static const symbol s_2[] = { 0xD0, 0xB0 };
static const symbol s_3[] = { 0xD1, 0x8F };
static const symbol s_4[] = { 0xD0, 0xB8 };
static const symbol s_5[] = { 0xD0, 0xBD };

static const unsigned char g_v[] = { 33, 65, 8, 232 };

extern int candidate_russian_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_p2;
    int i_pV;
    {
        int v_1 = z->c;
        while (1) {
            int v_2 = z->c;
            while (1) {
                int v_3 = z->c;
                z->bra = z->c;
                if (z->l - z->c < 2 || __builtin_memcmp(z->p + z->c, s_0, 2) != 0) goto lab2;
                z->c += 2;
                z->ket = z->c;
                z->c = v_3;
                break;
            lab2:
                z->c = v_3;
                {
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab1;
                    z->c = ret;
                }
            }
            {
                int ret = slice_from_s(z, 2, s_1);
                if (ret < 0) return ret;
            }
            continue;
        lab1:
            z->c = v_2;
            break;
        }
        z->c = v_1;
    }
    i_pV = z->l;
    i_p2 = z->l;
    {
        int v_4 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        i_pV = z->c;
        {
            int ret = snowball_in_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        {
            int ret = snowball_out_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        i_p2 = z->c;
    lab4:
        z->c = v_4;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_5;
        if (z->c < i_pV) return 0;
        v_5 = z->lb; z->lb = i_pV;
        {
            int v_6 = z->l - z->c;
            do {
                int v_7 = z->l - z->c;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x8C:
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\321\213\320\262\321\210\320\270\321\201\321", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\320\270\320\262\321\210\320\270\321\201\321", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\320\262\321\210\320\270\321\201\321", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                break;
                            case 0xB8:
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\321\213\320\262\321\210\320", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\320\270\320\262\321\210\320", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\262\321\210\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                break;
                            case 0xB2:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab6;
                z->bra = z->c;
                switch (among_var) {
                    case 1:
                        do {
                            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_2, 2) != 0) goto lab7;
                            z->c -= 2;
                            break;
                        lab7:
                            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_3, 2) != 0) goto lab6;
                            z->c -= 2;
                        } while (0);
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
                break;
            lab6:
                z->c = z->l - v_7;
                {
                    int v_8 = z->l - z->c;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x8C:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\201\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0x8F:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\201\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) { z->c = z->l - v_8; goto lab8; }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab8:
                    ;
                }
                do {
                    int v_9 = z->l - z->c;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x83:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\320\274\321", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\276\320\274\321", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    break;
                                case 0xB8:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\213\320\274\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\274\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    break;
                                case 0xBE:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\320\263\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\276\320\263\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    break;
                                case 0x85:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0x8E:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\203\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\216\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0x8F:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\217\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\260\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xB5:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xB9:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xBC:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab9;
                    z->bra = z->c;
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
                                    case 0x88:
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\213\320\262\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\262\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\262\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        break;
                                    case 0x89:
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\203\321\216\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\216\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 0xBC:
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        break;
                                    case 0xBD:
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\275\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->c = z->l - v_10; goto lab10; }
                        z->bra = z->c;
                        switch (among_var) {
                            case 1:
                                do {
                                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_2, 2) != 0) goto lab11;
                                    z->c -= 2;
                                    break;
                                lab11:
                                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_3, 2) != 0) { z->c = z->l - v_10; goto lab10; }
                                    z->c -= 2;
                                } while (0);
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
                    lab10:
                        ;
                    }
                    break;
                lab9:
                    z->c = z->l - v_9;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0xB5:
                                    if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\321\203\320\271\321\202\320", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                    if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\320\265\320\271\321\202\320", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\321\202\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\321\202\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\271\321\202\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    break;
                                case 0x82:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\203\321\216\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\203\320\265\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\321", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\216\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\217\321", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\321", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    break;
                                case 0x8B:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\320\275\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\275\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0x8C:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\213\321\202\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\321\202\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\321\210\321", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\321\210\321", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\202\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xB0:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\213\320\273\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\273\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\320\275\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\273\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\275\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xB8:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\213\320\273\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\273\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\273\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xBE:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\213\320\273\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\273\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\320\275\320", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\275\320\275\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\273\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\275\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0x8E:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\203\321", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                                    break;
                                case 0xB9:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\203\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0xBB:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0xBC:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\213\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    break;
                                case 0xBD:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab12;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            do {
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_2, 2) != 0) goto lab13;
                                z->c -= 2;
                                break;
                            lab13:
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_3, 2) != 0) goto lab12;
                                z->c -= 2;
                            } while (0);
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
                    break;
                lab12:
                    z->c = z->l - v_9;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0xB8:
                                    if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\320\270\321\217\320\274\320", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\321\217\320\274\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\260\320\274\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x85:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\321\217\321", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\217\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\260\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xB9:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\265\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0xBC:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\321\217\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\270\320\265\320", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\217\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\260\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0x8E:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\214\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x8F:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\214\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\321", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0xB2:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\265\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\276\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    break;
                                case 0xB5:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\321\214\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\320\270\320", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x83:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x8B:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x8C:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0xB0:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0xBE:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab5;
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                } while (0);
            } while (0);
        lab5:
            z->c = z->l - v_6;
        }
        {
            int v_11 = z->l - z->c;
            z->ket = z->c;
            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_4, 2) != 0) { z->c = z->l - v_11; goto lab14; }
            z->c -= 2;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab14:
            ;
        }
        {
            int v_12 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x8C:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\320\276\321\201\321\202\321", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0x82:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\276\321\201\321", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab15;
            z->bra = z->c;
            if (i_p2 > z->c) goto lab15;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab15:
            z->c = z->l - v_12;
        }
        {
            int v_13 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB5:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\320\265\320\271\321\210\320", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0x88:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\320\265\320\271\321", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x8C:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\321", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                            break;
                        case 0xBD:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\320", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab16;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    z->ket = z->c;
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_5, 2) != 0) goto lab16;
                    z->c -= 2;
                    z->bra = z->c;
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_5, 2) != 0) goto lab16;
                    z->c -= 2;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_5, 2) != 0) goto lab16;
                    z->c -= 2;
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
                    break;
            }
        lab16:
            z->c = z->l - v_13;
        }
        z->lb = v_5;
    }
    z->c = z->lb;
    return 1;
}

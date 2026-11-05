/* Generated from tamil.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_tamil_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

typedef struct SN_env SN_env;

struct SN_local {
    struct SN_env z;
    unsigned char b_found_vetrumai_urupu;
};

typedef struct SN_local SN_local;

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
extern int candidate_tamil_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_fix_endings(struct SN_env * z);
static int r_fix_ending(struct SN_env * z);
static int r_fix_va_start(struct SN_env * z);

#define s_9 (s_4 + 3)
#define s_8 (s_11 + 3)
#define s_11 (s_6 + 3)
static const symbol s_0[] = { 0xE0, 0xAE, 0x93 };
static const symbol s_1[] = { 0xE0, 0xAE, 0x92 };
static const symbol s_2[] = { 0xE0, 0xAE, 0x89 };
static const symbol s_3[] = { 0xE0, 0xAE, 0x8A };
static const symbol s_4[] = { 0xE0, 0xAE, 0xB3, 0xE0, 0xAF, 0x8D };
static const symbol s_5[] = { 0xE0, 0xAE, 0xB2, 0xE0, 0xAF, 0x8D };
static const symbol s_6[] = {
    0xE0, 0xAE, 0x9F, 0xE0, 0xAF, 0x81, 0xE0, 0xAE,
    0xAE, 0xE0, 0xAF, 0x8D
};
static const symbol s_7[] = { 0xE0, 0xAF, 0x88 };
static const symbol s_10[] = { 0xE0, 0xAE, 0x8E };
static const symbol s_14[] = { 0xE0, 0xAE, 0x9A };
static const symbol s_12[] = {
    0xE0, 0xAE, 0xBF, 0xE0, 0xAE, 0xA9, 0xE0, 0xAF,
    0x8D
};
static const symbol s_13[] = {
    0xE0, 0xAF, 0x81, 0xE0, 0xAE, 0x99, 0xE0, 0xAF,
    0x8D
};

static int r_fix_va_start(struct SN_env * z) {
    int among_var;
    z->bra = z->c;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among < z->l) {
            switch (z->p[c_among]) {
                case 0xE0:
                    if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\265\340\257\201", 5) == 0) { among_var = 3; z->c = c_among + 6; break; }
                    if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\265\340\257\202", 5) == 0) { among_var = 4; z->c = c_among + 6; break; }
                    if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\265\340\257\212", 5) == 0) { among_var = 2; z->c = c_among + 6; break; }
                    if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\265\340\257\213", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    z->ket = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = slice_from_s(z, 3, s_0);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_from_s(z, 3, s_1);
                if (ret < 0) return ret;
            }
            break;
        case 3:
            {
                int ret = slice_from_s(z, 3, s_2);
                if (ret < 0) return ret;
            }
            break;
        case 4:
            {
                int ret = slice_from_s(z, 3, s_3);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

static int r_fix_endings(struct SN_env * z) {
    {
        int v_1 = z->c;
        while (1) {
            int v_2 = z->c;
            {
                int ret = r_fix_ending(z);
                if (ret == 0) goto lab1;
                if (ret < 0) return ret;
            }
            continue;
        lab1:
            z->c = v_2;
            break;
        }
        z->c = v_1;
    }
    return 1;
}

static int r_fix_ending(struct SN_env * z) {
    int among_var;
    if (len_utf8(z->p) < 4) return 0;
    z->lb = z->c; z->c = z->l;
    do {
        int v_1 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x8D:
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\257\201\340\256\225\340\257\215\340\256\225\340\257", 14) == 0) { among_var = 7; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\237\340\257\215\340\256\225\340\257", 11) == 0) { among_var = 3; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\261\340\257\215\340\256\225\340\257", 11) == 0) { among_var = 4; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\237\340\257\215\340\256\237\340\257", 11) == 0) { among_var = 5; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\244\340\257\215\340\256\244\340\257", 11) == 0) { among_var = 6; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\250\340\257\215\340\256\244\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\237\340\257\215\340\256\252\340\257", 11) == 0) { among_var = 3; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\251\340\257\215\340\256\261\340\257", 11) == 0) { among_var = 4; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\201\340\256\225\340\257", 8) == 0) { among_var = 7; z->c = c_among - 9; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\231\340\257", 5) == 0) { among_var = 9; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\250\340\257", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\257\340\257", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\265\340\257", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xA4:
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\250\340\257\215\340\256", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\251\340\257", 5) == 0) { among_var = 8; z->c = c_among - 6; break; }
                        break;
                    case 0xAF:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab0;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int v_2 = z->l - z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x80:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x88:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xBF:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab0;
                    z->c = z->l - v_2;
                }
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 6, s_4);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 6, s_5);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int ret = slice_from_s(z, 6, s_6);
                    if (ret < 0) return ret;
                }
                break;
            case 6:
                if (!((SN_local *)z)->b_found_vetrumai_urupu) goto lab0;
                if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_7, 3) != 0) goto lab1;
                z->c -= 3;
                goto lab0;
            lab1:
                {
                    int ret = slice_from_s(z, 6, s_8);
                    if (ret < 0) return ret;
                }
                break;
            case 7:
                {
                    int ret = slice_from_s(z, 3, s_9);
                    if (ret < 0) return ret;
                }
                break;
            case 8:
                {
                    int v_3 = z->l - z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x80:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x81:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x82:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x86:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x87:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x88:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xBE:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xBF:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab2;
                    goto lab0;
                lab2:
                    z->c = z->l - v_3;
                }
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 9:
                {
                    int c_among = z->c;
                    among_var = 2;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x88:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 0x8D:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                switch (among_var) {
                    case 1:
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 2:
                        {
                            int ret = slice_from_s(z, 6, s_8);
                            if (ret < 0) return ret;
                        }
                        break;
                }
                break;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        z->ket = z->c;
        if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_9, 3) != 0) return 0;
        z->c -= 3;
        do {
            int v_4 = z->l - z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x95:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0x9A:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0x9F:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xA4:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xAA:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xB1:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab3;
            {
                int v_5 = z->l - z->c;
                if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_9, 3) != 0) { z->c = z->l - v_5; goto lab4; }
                z->c -= 3;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x95:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x9A:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x9F:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0xA4:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0xAA:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0xB1:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->c = z->l - v_5; goto lab4; }
            lab4:
                ;
            }
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab3:
            z->c = z->l - v_4;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x9E:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xA3:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xA8:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xA9:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xAE:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xAF:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xB0:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xB2:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xB3:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xB4:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                        case 0xB5:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab5;
            z->bra = z->c;
            if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_9, 3) != 0) goto lab5;
            z->c -= 3;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab5:
            z->c = z->l - v_4;
            {
                int v_6 = z->l - z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x80:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x81:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x82:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x86:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x87:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x88:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0x8D:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0xBE:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0xBF:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) return 0;
                z->c = z->l - v_6;
            }
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        } while (0);
    } while (0);
    z->c = z->lb;
    return 1;
}

extern int candidate_tamil_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int b_found_a_match;
    ((SN_local *)z)->b_found_vetrumai_urupu = 0;
    {
        int v_1 = z->c;
        {
            int ret = r_fix_ending(z);
            if (ret < 0) return ret;
        }
        z->c = v_1;
    }
    if (len_utf8(z->p) < 5) return 0;
    {
        int v_2 = z->c;
        z->bra = z->c;
        if (z->l - z->c < 3 || __builtin_memcmp(z->p + z->c, s_10, 3) != 0) goto lab0;
        z->c += 3;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among < z->l) {
                switch (z->p[c_among]) {
                    case 0xE0:
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\225", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\231", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\232", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\236", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\244", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\250", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\252", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\256", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\257", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\265", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab0;
        if (z->l - z->c < 3 || __builtin_memcmp(z->p + z->c, s_9, 3) != 0) goto lab0;
        z->c += 3;
        z->ket = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        {
            int v_3 = z->c;
            {
                int ret = r_fix_va_start(z);
                if (ret < 0) return ret;
            }
            z->c = v_3;
        }
    lab0:
        z->c = v_2;
    }
    {
        int v_4 = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among < z->l) {
                switch (z->p[c_among]) {
                    case 0xE0:
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\205", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\207", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\211", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab1;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among < z->l) {
                switch (z->p[c_among]) {
                    case 0xE0:
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\225", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\231", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\232", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\236", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\244", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\250", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\252", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\256", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\257", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\256\265", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab1;
        if (z->l - z->c < 3 || __builtin_memcmp(z->p + z->c, s_9, 3) != 0) goto lab1;
        z->c += 3;
        z->ket = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        {
            int v_5 = z->c;
            {
                int ret = r_fix_va_start(z);
                if (ret < 0) return ret;
            }
            z->c = v_5;
        }
    lab1:
        z->c = v_4;
    }
    if (len_utf8(z->p) < 5) goto lab2;
    z->lb = z->c; z->c = z->l;
    {
        int v_6 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x87:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 0x8B:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 0xBE:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab3;
        z->bra = z->c;
        {
            int ret = slice_from_s(z, 3, s_9);
            if (ret < 0) return ret;
        }
    lab3:
        z->c = z->l - v_6;
    }
    z->c = z->lb;
    {
        int ret = r_fix_endings(z);
        if (ret < 0) return ret;
    }
lab2:
    {
        int v_7 = z->c;
        if (len_utf8(z->p) < 5) goto lab4;
        z->lb = z->c; z->c = z->l;
        z->ket = z->c;
        if (!(eq_s_b(z, 9, s_11))) goto lab4;
        z->bra = z->c;
        {
            int ret = slice_from_s(z, 3, s_9);
            if (ret < 0) return ret;
        }
        z->c = z->lb;
        {
            int v_8 = z->c;
            {
                int ret = r_fix_ending(z);
                if (ret < 0) return ret;
            }
            z->c = v_8;
        }
    lab4:
        z->c = v_7;
    }
    {
        int v_9 = z->c;
        if (len_utf8(z->p) < 5) goto lab5;
        z->lb = z->c; z->c = z->l;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x8D:
                        if (c_among - z->lb >= 21 && __builtin_memcmp(z->p + c_among - 21, "\340\257\206\340\256\262\340\257\215\340\256\262\340\256\276\340\256\256\340\257", 20) == 0) { among_var = 3; z->c = c_among - 21; break; }
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\257\206\340\256\251\340\257\201\340\256\256\340\257", 14) == 0) { among_var = 1; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\257\201\340\256\237\340\256\251\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\277\340\256\237\340\256\256\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\340\256\265\340\256\277\340\256\237\340\257\215\340\256\237\340\257", 17) == 0) { among_var = 3; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\340\256\252\340\256\237\340\257\215\340\256\237\340\256\244\340\257", 17) == 0) { among_var = 3; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\252\340\256\237\340\257\215\340\256\237\340\257", 14) == 0) { among_var = 3; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\257\206\340\256\251\340\257\215\340\256\261\340\257", 14) == 0) { among_var = 1; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\265\340\256\277\340\256\237\340\257", 11) == 0) { among_var = 3; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\237\340\257", 8) == 0) { among_var = 3; z->c = c_among - 9; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\340\256\252\340\256\237\340\256\277\340\256\244\340\256\276\340\256", 17) == 0) { among_var = 3; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\244\340\256\276\340\256", 8) == 0) { among_var = 3; z->c = c_among - 9; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\257\206\340\256", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x88:
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\277\340\256\262\340\257\215\340\256\262\340\257", 14) == 0) { among_var = 1; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\201\340\256\237\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                        break;
                    case 0xA3:
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\252\340\256\237\340\257\215\340\256\237\340\256", 14) == 0) { among_var = 3; z->c = c_among - 15; break; }
                        break;
                    case 0xAF:
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\225\340\257\201\340\256\260\340\256\277\340\256", 14) == 0) { among_var = 3; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\257\201\340\256\237\340\257\210\340\256", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\276\340\256\225\340\256\277\340\256", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\277\340\256\251\340\257\215\340\256\261\340\256", 14) == 0) { among_var = 1; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\252\340\256\261\340\257\215\340\256\261\340\256", 14) == 0) { among_var = 3; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\225\340\256", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\237\340\256", 8) == 0) { among_var = 3; z->c = c_among - 9; break; }
                        break;
                    case 0x9F:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\252\340\256\237\340\257\215\340\256", 11) == 0) { among_var = 3; z->c = c_among - 12; break; }
                        break;
                    case 0xB3:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\257\201\340\256\263\340\257\215\340\256", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xB2:
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\262\340\257\215\340\256", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab5;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 3, s_9);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int v_10 = z->l - z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x80:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x81:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x82:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x86:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x87:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x88:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xBE:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xBF:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab6;
                    goto lab5;
                lab6:
                    z->c = z->l - v_10;
                }
                {
                    int ret = slice_from_s(z, 3, s_9);
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
        z->c = z->lb;
        {
            int ret = r_fix_endings(z);
            if (ret < 0) return ret;
        }
    lab5:
        z->c = v_9;
    }
    {
        int v_11 = z->c;
        ((SN_local *)z)->b_found_vetrumai_urupu = 0;
        if (len_utf8(z->p) < 5) goto lab7;
        z->lb = z->c; z->c = z->l;
        do {
            int v_12 = z->l - z->c;
            {
                int v_13 = z->l - z->c;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x81:
                                if (c_among - z->lb >= 21 && __builtin_memcmp(z->p + c_among - 21, "\340\256\277\340\256\260\340\257\201\340\256\250\340\257\215\340\256\244\340\257", 20) == 0) { among_var = 2; z->c = c_among - 21; break; }
                                if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\277\340\256\251\340\257\215\340\256\261\340\257", 14) == 0) { among_var = 2; z->c = c_among - 15; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\212\340\256\237\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\213\340\256\237\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\244\340\257", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                                break;
                            case 0x8D:
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\256\340\257\201\340\256\251\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\277\340\256\237\340\256\256\340\257", 11) == 0) { among_var = 4; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\256\340\257\207\340\256\261\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\256\340\257\207\340\256\262\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\276\340\256\256\340\256\262\340\257", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\225\340\257\200\340\256\264\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\225\340\256\243\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\277\340\256\251\340\257", 8) == 0) { among_var = 3; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\277\340\256\261\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\262\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\277\340\256\262\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\201\340\256\263\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\262\340\257", 5) == 0) { among_var = 5; z->c = c_among - 6; break; }
                                break;
                            case 0x88:
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\201\340\256\237\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\251\340\257", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                break;
                            case 0x9F:
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\265\340\256\277\340\256", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                break;
                            case 0x80:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = 7; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab8;
                z->bra = z->c;
                switch (among_var) {
                    case 1:
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 2:
                        {
                            int ret = slice_from_s(z, 3, s_9);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 3:
                        if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_8, 3) != 0) goto lab9;
                        z->c -= 3;
                        goto lab8;
                    lab9:
                        {
                            int ret = slice_from_s(z, 3, s_9);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 4:
                        if (len_utf8(z->p) < 7) goto lab8;
                        {
                            int ret = slice_from_s(z, 3, s_9);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 5:
                        {
                            int v_14 = z->l - z->c;
                            {
                                int c_among = z->c;
                                among_var = 0;
                                if (c_among > z->lb) {
                                    switch (z->p[c_among - 1]) {
                                        case 0x80:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x81:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x82:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x86:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x87:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x88:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0xBE:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0xBF:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                    }
                                }
                            }
                            if (!among_var) goto lab10;
                            goto lab8;
                        lab10:
                            z->c = z->l - v_14;
                        }
                        {
                            int ret = slice_from_s(z, 3, s_9);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 6:
                        {
                            int v_15 = z->l - z->c;
                            {
                                int c_among = z->c;
                                among_var = 0;
                                if (c_among > z->lb) {
                                    switch (z->p[c_among - 1]) {
                                        case 0x80:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x81:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x82:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x86:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x87:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0x88:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0xBE:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                        case 0xBF:
                                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                            break;
                                    }
                                }
                            }
                            if (!among_var) goto lab11;
                            goto lab8;
                        lab11:
                            z->c = z->l - v_15;
                        }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        break;
                    case 7:
                        {
                            int ret = slice_from_s(z, 3, s_12);
                            if (ret < 0) return ret;
                        }
                        break;
                }
                z->c = z->l - v_13;
            }
            break;
        lab8:
            z->c = z->l - v_12;
            {
                int v_16 = z->l - z->c;
                z->ket = z->c;
                if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_7, 3) != 0) goto lab7;
                z->c -= 3;
                do {
                    int v_17 = z->l - z->c;
                    {
                        int v_18 = z->l - z->c;
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 0x95:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0x9A:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0x9F:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0xA4:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0xAA:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0xB1:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) goto lab13;
                        goto lab12;
                    lab13:
                        z->c = z->l - v_18;
                    }
                    break;
                lab12:
                    z->c = z->l - v_17;
                    {
                        int v_19 = z->l - z->c;
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 0x95:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0x9A:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0x9F:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0xA4:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0xAA:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                    case 0xB1:
                                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) goto lab7;
                        if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_9, 3) != 0) goto lab7;
                        z->c -= 3;
                        z->c = z->l - v_19;
                    }
                } while (0);
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 3, s_9);
                    if (ret < 0) return ret;
                }
                z->c = z->l - v_16;
            }
        } while (0);
        ((SN_local *)z)->b_found_vetrumai_urupu = 1;
        {
            int v_20 = z->l - z->c;
            z->ket = z->c;
            if (!(eq_s_b(z, 9, s_12))) goto lab14;
            z->bra = z->c;
            {
                int ret = slice_from_s(z, 3, s_9);
                if (ret < 0) return ret;
            }
        lab14:
            z->c = z->l - v_20;
        }
        z->c = z->lb;
        {
            int ret = r_fix_endings(z);
            if (ret < 0) return ret;
        }
    lab7:
        z->c = v_11;
    }
    {
        int v_21 = z->c;
        z->lb = z->c; z->c = z->l;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x8D:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\340\257\201\340\256\231\340\257\215\340\256\225\340\256\263\340\257", 17) == 0) { among_var = 1; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\237\340\257\215\340\256\225\340\256\263\340\257", 14) == 0) { among_var = 3; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\261\340\257\215\340\256\225\340\256\263\340\257", 14) == 0) { among_var = 2; z->c = c_among - 15; break; }
                        if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\225\340\256\263\340\257", 8) == 0) { among_var = 4; z->c = c_among - 9; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab15;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                do {
                    int v_22 = z->l - z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x95:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x9A:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0x9F:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xA4:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xAA:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                                case 0xB1:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab16;
                    {
                        int ret = slice_from_s(z, 9, s_13);
                        if (ret < 0) return ret;
                    }
                    break;
                lab16:
                    z->c = z->l - v_22;
                    {
                        int ret = slice_from_s(z, 3, s_9);
                        if (ret < 0) return ret;
                    }
                } while (0);
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 6, s_5);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 6, s_4);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
        }
        z->c = z->lb;
    lab15:
        z->c = v_21;
    }
    {
        int v_23 = z->c;
        if (len_utf8(z->p) < 5) goto lab17;
        z->lb = z->c; z->c = z->l;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBF:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\252\340\256", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\265\340\256", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab17;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        z->c = z->lb;
    lab17:
        z->c = v_23;
    }
    {
        int v_24 = z->c;
        while (1) {
            int v_25 = z->c;
            b_found_a_match = 0;
            if (len_utf8(z->p) < 5) goto lab19;
            z->lb = z->c; z->c = z->l;
            {
                int v_26 = z->l - z->c;
                {
                    int v_27 = z->l - z->c;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x8D:
                                    if (c_among - z->lb >= 24 && __builtin_memcmp(z->p + c_among - 24, "\340\256\225\340\257\212\340\256\243\340\257\215\340\256\237\340\256\277\340\256\260\340\257", 23) == 0) { among_var = 1; z->c = c_among - 24; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\251\340\257\206\340\256\251\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\251\340\256\276\340\256\251\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\256\340\256\277\340\256\251\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\225\340\257\201\340\256\256\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\237\340\257\201\340\256\256\340\257", 11) == 0) { among_var = 5; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\244\340\257\201\340\256\256\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\261\340\257\201\340\256\256\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\257\200\340\256\257\340\256\260\340\257", 11) == 0) { among_var = 5; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\251\340\256\276\340\256\260\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\256\340\256\276\340\256\260\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\251\340\256\277\340\256\260\340\257", 11) == 0) { among_var = 5; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\251\340\256\276\340\256\263\340\257", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\207\340\256\251\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\251\340\256\251\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\251\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\265\340\256\251\340\257", 8) == 0) { among_var = 2; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\251\340\257", 8) == 0) { among_var = 4; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\206\340\256\256\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\207\340\256\256\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\213\340\256\256\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\251\340\256\256\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\256\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\256\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\257\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\257\200\340\256\260\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\251\340\256\260\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\260\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\265\340\256\260\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\260\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\251\340\256\263\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\263\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\265\340\256\263\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\276\340\256\263\340\257", 8) == 0) { among_var = 5; z->c = c_among - 9; break; }
                                    break;
                                case 0x81:
                                    if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\277\340\256\261\340\257\215\340\256\261\340\257", 14) == 0) { among_var = 1; z->c = c_among - 15; break; }
                                    if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\252\340\256\237\340\257", 8) == 0) { among_var = 1; z->c = c_among - 9; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\225\340\257", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\244\340\257", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                    break;
                                case 0x88:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\251\340\257", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\340\256\265\340\257", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                    break;
                                case 0x95:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                    break;
                                case 0xA4:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                    break;
                                case 0xA9:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                    break;
                                case 0xAA:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                    break;
                                case 0xAF:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                    break;
                                case 0xBE:
                                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = 5; z->c = c_among - 3; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab20;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            {
                                int v_28 = z->l - z->c;
                                {
                                    int c_among = z->c;
                                    among_var = 0;
                                    if (c_among > z->lb) {
                                        switch (z->p[c_among - 1]) {
                                            case 0x85:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x86:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x87:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x88:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x89:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x8A:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x8E:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x8F:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x90:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x92:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x93:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x94:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                        }
                                    }
                                }
                                if (!among_var) goto lab21;
                                goto lab20;
                            lab21:
                                z->c = z->l - v_28;
                            }
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 3:
                            {
                                int v_29 = z->l - z->c;
                                {
                                    int c_among = z->c;
                                    among_var = 0;
                                    if (c_among > z->lb) {
                                        switch (z->p[c_among - 1]) {
                                            case 0x80:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x81:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x82:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x86:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x87:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0x88:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\257", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0xBE:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                            case 0xBF:
                                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\340\256", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                                break;
                                        }
                                    }
                                }
                                if (!among_var) goto lab22;
                                goto lab20;
                            lab22:
                                z->c = z->l - v_29;
                            }
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 4:
                            if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_14, 3) != 0) goto lab23;
                            z->c -= 3;
                            goto lab20;
                        lab23:
                            {
                                int ret = slice_from_s(z, 3, s_9);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 5:
                            {
                                int ret = slice_from_s(z, 3, s_9);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 6:
                            {
                                int v_30 = z->l - z->c;
                                if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_9, 3) != 0) goto lab20;
                                z->c -= 3;
                                z->c = z->l - v_30;
                            }
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                    }
                    b_found_a_match = 1;
                    z->c = z->l - v_27;
                }
            lab20:
                z->c = z->l - v_26;
            }
            {
                int v_31 = z->l - z->c;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x8D:
                                if (c_among - z->lb >= 21 && __builtin_memcmp(z->p + c_among - 21, "\340\256\276\340\256\250\340\256\277\340\256\251\340\257\215\340\256\261\340\257", 20) == 0) { among_var = -1; z->c = c_among - 21; break; }
                                if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\340\256\225\340\256\277\340\256\251\340\257\215\340\256\261\340\257", 17) == 0) { among_var = -1; z->c = c_among - 18; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\340\256\225\340\256\277\340\256\261\340\257", 11) == 0) { among_var = -1; z->c = c_among - 12; break; }
                                break;
                            case 0xB1:
                                if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\340\256\276\340\256\250\340\256\277\340\256\251\340\257\215\340\256", 17) == 0) { among_var = -1; z->c = c_among - 18; break; }
                                if (c_among - z->lb >= 15 && __builtin_memcmp(z->p + c_among - 15, "\340\256\225\340\256\277\340\256\251\340\257\215\340\256", 14) == 0) { among_var = -1; z->c = c_among - 15; break; }
                                if (c_among - z->lb >= 9 && __builtin_memcmp(z->p + c_among - 9, "\340\256\225\340\256\277\340\256", 8) == 0) { among_var = -1; z->c = c_among - 9; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab24;
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                b_found_a_match = 1;
            lab24:
                z->c = z->l - v_31;
            }
            z->c = z->lb;
            {
                int ret = r_fix_endings(z);
                if (ret < 0) return ret;
            }
            if (!b_found_a_match) goto lab19;
            continue;
        lab19:
            z->c = v_25;
            break;
        }
        z->c = v_24;
    }
    return 1;
}

extern struct SN_env * candidate_tamil_UTF_8_create_env(void) {
    return SN_new_env(sizeof(SN_local));
}

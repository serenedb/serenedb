/* Generated from greek.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_greek_candidate.h"

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
extern int candidate_greek_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

#define s_12 (s_0 + 4)
#define s_32 (s_0 + 2)
#define s_5 (s_1 + 4)
#define s_19 (s_1 + 2)
#define s_51 (s_6 + 4)
#define s_16 (s_7 + 8)
#define s_10 (s_14 + 2)
#define s_3 (s_36 + 12)
#define s_13 (s_36 + 6)
#define s_38 (s_37 + 4)
#define s_48 (s_47 + 4)
#define s_21 (s_52 + 4)
#define s_11 (s_54 + 2)
#define s_56 (s_55 + 2)
#define s_31 (s_18 + 2)
#define s_17 (s_42 + 2)
#define s_50 (s_49 + 4)
#define s_25 (s_60 + 4)
#define s_53 (s_2 + 6)
#define s_27 (s_42 + 4)
#define s_54 (s_18 + 6)
#define s_43 (s_20 + 2)
#define s_14 (s_24 + 2)
#define s_23 (s_26 + 2)
#define s_9 (s_57 + 2)
#define s_40 (s_46 + 2)
#define s_52 (s_28 + 4)
#define s_15 (s_35 + 4)
#define s_44 (s_57 + 6)
#define s_24 (s_58 + 4)
#define s_4 (s_6 + 8)
#define s_35 (s_29 + 4)
#define s_59 (s_37 + 6)
#define s_34 (s_39 + 4)
#define s_61 (s_58 + 10)
#define s_33 (s_55 + 8)
#define s_18 (s_26 + 6)
static const symbol s_0[] = {
    0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xBD, 0xCF, 0x89,
    0xCF, 0x83, 0xCF, 0x84
};
static const symbol s_1[] = {
    0xCE, 0xB2, 0xCF, 0x85, 0xCE, 0xB6, 0xCE, 0xB1,
    0xCE, 0xBD, 0xCF, 0x84
};
static const symbol s_2[] = {
    0xCE, 0xB3, 0xCE, 0xB5, 0xCE, 0xB3, 0xCE, 0xBF,
    0xCE, 0xBD, 0xCF, 0x84
};
static const symbol s_55[] = {
    0xCE, 0xB9, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84,
    0xCE, 0xB5, 0xCE, 0xBA, 0xCE, 0xBB, 0xCE, 0xB5,
    0xCE, 0xBA, 0xCF, 0x84
};
static const symbol s_6[] = {
    0xCE, 0xB7, 0xCF, 0x83, 0xCE, 0xB5, 0xCF, 0x84,
    0xCE, 0xB5, 0xCE, 0xB8, 0xCE, 0xBD
};
static const symbol s_7[] = {
    0xCE, 0xB8, 0xCE, 0xB5, 0xCE, 0xB1, 0xCF, 0x84,
    0xCF, 0x81
};
static const symbol s_8[] = { 0xCE, 0xB9, 0xCE, 0xB6 };
static const symbol s_60[] = {
    0xCE, 0xB7, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xB5,
    0xCF, 0x81
};
static const symbol s_58[] = {
    0xCE, 0xBF, 0xCF, 0x85, 0xCF, 0x83, 0xCE, 0xBF,
    0xCE, 0xBB, 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBD
};
static const symbol s_46[] = { 0xCE, 0xB9, 0xCE, 0xBA, 0xCE, 0xBF, 0xCF, 0x81 };
static const symbol s_39[] = {
    0xCE, 0xB9, 0xCF, 0x84, 0xCF, 0x83, 0xCE, 0xBA,
    0xCE, 0xB5, 0xCF, 0x80, 0xCF, 0x84
};
static const symbol s_36[] = {
    0xCE, 0xB1, 0xCE, 0xBB, 0xCE, 0xB5, 0xCE, 0xBE,
    0xCE, 0xB1, 0xCE, 0xBD, 0xCE, 0xB4, 0xCF, 0x81
};
static const symbol s_20[] = { 0xCF, 0x86, 0xCE, 0xB1, 0xCE, 0xB4 };
static const symbol s_22[] = { 0xCF, 0x88 };
static const symbol s_47[] = {
    0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBC,
    0xCE, 0xB5
};
static const symbol s_45[] = { 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xB4 };
static const symbol s_41[] = { 0xCE, 0xB9, 0xCE, 0xB4 };
static const symbol s_26[] = {
    0xCF, 0x86, 0xCF, 0x89, 0xCE, 0xBD, 0xCF, 0x84,
    0xCE, 0xB1, 0xCF, 0x84, 0xCE, 0xBF, 0xCE, 0xBC,
    0xCE, 0xB1, 0xCF, 0x83, 0xCF, 0x84
};
static const symbol s_57[] = {
    0xCE, 0xB7, 0xCE, 0xBA, 0xCF, 0x81, 0xCE, 0xB5,
    0xCE, 0xB4
};
static const symbol s_28[] = {
    0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xB1, 0xCF, 0x81,
    0xCF, 0x87
};
static const symbol s_29[] = {
    0xCE, 0xB9, 0xCF, 0x83, 0xCF, 0x84, 0xCE, 0xBF,
    0xCF, 0x80, 0xCE, 0xB5, 0xCF, 0x81
};
static const symbol s_30[] = { 0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBC };
static const symbol s_62[] = { 0xCE, 0xBF, 0xCF, 0x85, 0xCE, 0xBC };
static const symbol s_37[] = {
    0xCE, 0xB1, 0xCF, 0x81, 0xCE, 0xB1, 0xCE, 0xBA,
    0xCE, 0xBF, 0xCE, 0xBB, 0xCE, 0xBB
};
static const symbol s_49[] = {
    0xCE, 0xB1, 0xCE, 0xB3, 0xCE, 0xB1, 0xCE, 0xBD,
    0xCE, 0xB5
};
static const symbol s_42[] = {
    0xCE, 0xB9, 0xCF, 0x83, 0xCE, 0xBA, 0xCE, 0xB1,
    0xCE, 0xB8, 0xCE, 0xB5, 0xCF, 0x83, 0xCF, 0x84
};

static const unsigned char g_v[] = { 81, 65, 16, 1 };

static const unsigned char g_v2[] = { 81, 65, 0, 1 };

extern int candidate_greek_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int b_test1;
    z->lb = z->c; z->c = z->l;
    {
        int v_1 = z->l - z->c;
        while (1) {
            int v_2 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 25;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x82:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 18; z->c = c_among - 2; break; }
                            break;
                        case 0x86:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0x88:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 5; z->c = c_among - 2; break; }
                            break;
                        case 0x89:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 7; z->c = c_among - 2; break; }
                            break;
                        case 0x8A:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 9; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 7; z->c = c_among - 2; break; }
                            break;
                        case 0x8B:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 20; z->c = c_among - 2; break; }
                            break;
                        case 0x8C:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 15; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 15; z->c = c_among - 2; break; }
                            break;
                        case 0x8D:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 20; z->c = c_among - 2; break; }
                            break;
                        case 0x8E:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 20; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 24; z->c = c_among - 2; break; }
                            break;
                        case 0x8F:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 24; z->c = c_among - 2; break; }
                            break;
                        case 0x90:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 7; z->c = c_among - 2; break; }
                            break;
                        case 0x91:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0x92:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            break;
                        case 0x93:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                            break;
                        case 0x94:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 4; z->c = c_among - 2; break; }
                            break;
                        case 0x95:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 5; z->c = c_among - 2; break; }
                            break;
                        case 0x96:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                            break;
                        case 0x97:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 7; z->c = c_among - 2; break; }
                            break;
                        case 0x98:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 8; z->c = c_among - 2; break; }
                            break;
                        case 0x99:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 9; z->c = c_among - 2; break; }
                            break;
                        case 0x9A:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 10; z->c = c_among - 2; break; }
                            break;
                        case 0x9B:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 11; z->c = c_among - 2; break; }
                            break;
                        case 0x9C:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 12; z->c = c_among - 2; break; }
                            break;
                        case 0x9D:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 13; z->c = c_among - 2; break; }
                            break;
                        case 0x9E:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 14; z->c = c_among - 2; break; }
                            break;
                        case 0x9F:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 15; z->c = c_among - 2; break; }
                            break;
                        case 0xA0:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 16; z->c = c_among - 2; break; }
                            break;
                        case 0xA1:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 17; z->c = c_among - 2; break; }
                            break;
                        case 0xA3:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 18; z->c = c_among - 2; break; }
                            break;
                        case 0xA4:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 19; z->c = c_among - 2; break; }
                            break;
                        case 0xA5:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 20; z->c = c_among - 2; break; }
                            break;
                        case 0xA6:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 21; z->c = c_among - 2; break; }
                            break;
                        case 0xA7:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 22; z->c = c_among - 2; break; }
                            break;
                        case 0xA8:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 23; z->c = c_among - 2; break; }
                            break;
                        case 0xA9:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 24; z->c = c_among - 2; break; }
                            break;
                        case 0xAA:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 9; z->c = c_among - 2; break; }
                            break;
                        case 0xAB:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 20; z->c = c_among - 2; break; }
                            break;
                        case 0xAC:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0xAD:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 5; z->c = c_among - 2; break; }
                            break;
                        case 0xAE:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 7; z->c = c_among - 2; break; }
                            break;
                        case 0xAF:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 9; z->c = c_among - 2; break; }
                            break;
                        case 0xB0:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 20; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 2, s_0);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 2, s_1);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 2, s_2);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 2, s_3);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = slice_from_s(z, 2, s_4);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    {
                        int ret = slice_from_s(z, 2, s_5);
                        if (ret < 0) return ret;
                    }
                    break;
                case 7:
                    {
                        int ret = slice_from_s(z, 2, s_6);
                        if (ret < 0) return ret;
                    }
                    break;
                case 8:
                    {
                        int ret = slice_from_s(z, 2, s_7);
                        if (ret < 0) return ret;
                    }
                    break;
                case 9:
                    {
                        int ret = slice_from_s(z, 2, s_8);
                        if (ret < 0) return ret;
                    }
                    break;
                case 10:
                    {
                        int ret = slice_from_s(z, 2, s_9);
                        if (ret < 0) return ret;
                    }
                    break;
                case 11:
                    {
                        int ret = slice_from_s(z, 2, s_10);
                        if (ret < 0) return ret;
                    }
                    break;
                case 12:
                    {
                        int ret = slice_from_s(z, 2, s_11);
                        if (ret < 0) return ret;
                    }
                    break;
                case 13:
                    {
                        int ret = slice_from_s(z, 2, s_12);
                        if (ret < 0) return ret;
                    }
                    break;
                case 14:
                    {
                        int ret = slice_from_s(z, 2, s_13);
                        if (ret < 0) return ret;
                    }
                    break;
                case 15:
                    {
                        int ret = slice_from_s(z, 2, s_14);
                        if (ret < 0) return ret;
                    }
                    break;
                case 16:
                    {
                        int ret = slice_from_s(z, 2, s_15);
                        if (ret < 0) return ret;
                    }
                    break;
                case 17:
                    {
                        int ret = slice_from_s(z, 2, s_16);
                        if (ret < 0) return ret;
                    }
                    break;
                case 18:
                    {
                        int ret = slice_from_s(z, 2, s_17);
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
                        int ret = slice_from_s(z, 2, s_20);
                        if (ret < 0) return ret;
                    }
                    break;
                case 22:
                    {
                        int ret = slice_from_s(z, 2, s_21);
                        if (ret < 0) return ret;
                    }
                    break;
                case 23:
                    {
                        int ret = slice_from_s(z, 2, s_22);
                        if (ret < 0) return ret;
                    }
                    break;
                case 24:
                    {
                        int ret = slice_from_s(z, 2, s_23);
                        if (ret < 0) return ret;
                    }
                    break;
                case 25:
                    {
                        int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab1;
                        z->c = ret;
                    }
                    break;
            }
            continue;
        lab1:
            z->c = z->l - v_2;
            break;
        }
        z->c = z->l - v_1;
    }
    if (len_utf8(z->p) < 3) return 0;
    b_test1 = 1;
    {
        int v_3 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 20 && __builtin_memcmp(z->p + c_among - 20, "\316\272\316\261\316\270\316\265\317\203\317\204\317\211\317\204\316\277\317", 19) == 0) { among_var = 10; z->c = c_among - 20; break; }
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\316\263\316\265\316\263\316\277\316\275\316\277\317\204\316\277\317", 17) == 0) { among_var = 11; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\272\316\261\316\270\316\265\317\203\317\204\317\211\317", 15) == 0) { among_var = 10; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\265\317\201\316\261\317\204\316\277\317", 13) == 0) { among_var = 7; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\204\316\265\317\201\316\261\317\204\316\277\317", 13) == 0) { among_var = 8; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\317\201\316\265\316\261\317\204\316\277\317", 13) == 0) { among_var = 6; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\263\316\265\316\263\316\277\316\275\316\277\317", 13) == 0) { among_var = 11; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\265\317\201\316\261\317", 9) == 0) { among_var = 7; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\204\316\265\317\201\316\261\317", 9) == 0) { among_var = 8; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\272\317\201\316\265\316\261\317", 9) == 0) { among_var = 6; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\206\317\211\317\204\316\277\317", 9) == 0) { among_var = 9; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\206\317\211\317", 5) == 0) { among_var = 9; z->c = c_among - 6; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 20 && __builtin_memcmp(z->p + c_among - 20, "\316\272\316\261\316\270\316\265\317\203\317\204\317\211\317\204\317\211\316", 19) == 0) { among_var = 10; z->c = c_among - 20; break; }
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\316\263\316\265\316\263\316\277\316\275\316\277\317\204\317\211\316", 17) == 0) { among_var = 11; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\317\204\316\261\317\204\316\277\316\263\316\271\317\211\316", 15) == 0) { among_var = 5; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\265\317\201\316\261\317\204\317\211\316", 13) == 0) { among_var = 7; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\204\316\265\317\201\316\261\317\204\317\211\316", 13) == 0) { among_var = 8; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\317\201\316\265\316\261\317\204\317\211\316", 13) == 0) { among_var = 6; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\316\272\316\261\316\263\316\271\317\211\316", 13) == 0) { among_var = 2; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\316\273\316\277\316\263\316\271\317\211\316", 13) == 0) { among_var = 3; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\206\316\261\316\263\316\271\317\211\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\203\316\277\316\263\316\271\317\211\316", 11) == 0) { among_var = 4; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\206\317\211\317\204\317\211\316", 9) == 0) { among_var = 9; z->c = c_among - 10; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\316\272\316\261\316\270\316\265\317\203\317\204\317\211\317\204\316", 17) == 0) { among_var = 10; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\263\316\265\316\263\316\277\316\275\316\277\317\204\316", 15) == 0) { among_var = 11; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\204\316\261\317\204\316\277\316\263\316\271\316", 13) == 0) { among_var = 5; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\265\317\201\316\261\317\204\316", 11) == 0) { among_var = 7; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\204\316\265\317\201\316\261\317\204\316", 11) == 0) { among_var = 8; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\272\317\201\316\265\316\261\317\204\316", 11) == 0) { among_var = 6; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\203\316\272\316\261\316\263\316\271\316", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\316\273\316\277\316\263\316\271\316", 11) == 0) { among_var = 3; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\206\316\261\316\263\316\271\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\203\316\277\316\263\316\271\316", 9) == 0) { among_var = 4; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\206\317\211\317\204\316", 7) == 0) { among_var = 9; z->c = c_among - 8; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\317\204\316\261\317\204\316\277\316\263\316\271\316\277\317", 15) == 0) { among_var = 5; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\316\272\316\261\316\263\316\271\316\277\317", 13) == 0) { among_var = 2; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\316\273\316\277\316\263\316\271\316\277\317", 13) == 0) { among_var = 3; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\206\316\261\316\263\316\271\316\277\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\203\316\277\316\263\316\271\316\277\317", 11) == 0) { among_var = 4; z->c = c_among - 12; break; }
                        break;
                    case 0xB7:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\265\317\201\316\261\317\204\316", 11) == 0) { among_var = 7; z->c = c_among - 12; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab2;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 4, s_20);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 6, s_17);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 6, s_14);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 4, s_24);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int ret = slice_from_s(z, 8, s_18);
                    if (ret < 0) return ret;
                }
                break;
            case 6:
                {
                    int ret = slice_from_s(z, 6, s_9);
                    if (ret < 0) return ret;
                }
                break;
            case 7:
                {
                    int ret = slice_from_s(z, 6, s_15);
                    if (ret < 0) return ret;
                }
                break;
            case 8:
                {
                    int ret = slice_from_s(z, 6, s_25);
                    if (ret < 0) return ret;
                }
                break;
            case 9:
                {
                    int ret = slice_from_s(z, 4, s_26);
                    if (ret < 0) return ret;
                }
                break;
            case 10:
                {
                    int ret = slice_from_s(z, 12, s_27);
                    if (ret < 0) return ret;
                }
                break;
            case 11:
                {
                    int ret = slice_from_s(z, 10, s_2);
                    if (ret < 0) return ret;
                }
                break;
        }
        b_test1 = 0;
    lab2:
        z->c = z->l - v_3;
    }
    {
        int v_4 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\316\266\316\277\317\205\316\274\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\316\266\316\277\317\205\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\266\316\261\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\266\316\265\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\266\316\261\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\266\316\261\316\275\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\266\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\266\316\265\316\271\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\266\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\266\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\266\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\266\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x89:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\266\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\266\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab3;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x81:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\271\317\200\316\265\317\201\316\277\317", 13) == 0) { among_var = 2; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\263\316\273\317\205\316\272\317\205\317", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\262\316\277\316\273\316\262\316\277\317", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\263\316\273\317\205\316\272\316\277\317", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\277\316\273\317\205\317", 9) == 0) { among_var = 2; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\274\317\200\316\261\317", 9) == 0) { among_var = 2; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\317\200\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\201\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\261\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\272\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\317\205\316\275\316\261\316\270\317\201\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\270\317\201\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\276\316\261\316\275\316\261\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\265\317\201\316\271\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\316\275\316\261\316\274\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\274\317\200\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\262\316\261\316\270\317\205\317\201\316", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\261\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\316\261\317\201\316", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\316\261\317\201\316", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\277\317\201\316", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\274\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\277\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0xB2:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab3;
        if (z->c > z->lb) goto lab3;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 2, s_8);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 4, s_8);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab3:
        z->c = z->l - v_4;
    }
    {
        int v_5 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\211\316\270\316\267\316\272\316\261\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\211\316\270\316\267\316\272\316\261\316\274\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\211\316\270\316\267\316\272\316\261\316\275\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\211\316\270\316\267\316\272\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\211\316\270\316\267\316\272\316\265\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\211\316\270\316\267\316\272\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\211\316\270\316\267\316\272\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab4;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x88:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\205\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x89:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\266\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\262\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\273\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x87:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab4;
        if (z->c > z->lb) goto lab4;
        {
            int ret = slice_from_s(z, 4, s_23);
            if (ret < 0) return ret;
        }
    lab4:
        z->c = z->l - v_5;
    }
    {
        int v_6 = z->l - z->c;
        do {
            int v_7 = z->l - z->c;
            z->ket = z->c;
            if (!(eq_s_b(z, 6, s_28))) goto lab6;
            z->bra = z->c;
            if (z->c > z->lb) goto lab6;
            {
                int ret = slice_from_s(z, 4, s_28);
                if (ret < 0) return ret;
            }
            break;
        lab6:
            z->c = z->l - v_7;
            z->ket = z->c;
        } while (0);
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\261\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\261\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\261\316\275\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
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
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x86:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\316\263\316\271\316\263\316\261\316\275\317\204\316\277\316\261\317", 17) == 0) { among_var = 2; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0x84:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\264\316\267\316\274\316\277\316\272\317\201\316\261\317", 15) == 0) { among_var = 2; z->c = c_among - 16; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\207\316\261\317\201\317\204\316\277\317\200\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\276\316\261\316\275\316\261\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\265\317\201\316\271\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\316\275\316\261\316\274\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\265\316\276\316\261\317\201\317\207\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\274\317\200\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\317\205\316\275\316\261\316\270\317\201\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\270\317\201\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\316\273\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\274\316\265\317\204\316\265\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\265\317\203\317\211\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\200\316\265\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\200\316\277\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\272\316\273\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\261\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\272\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\273\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\263\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\277\317\205\316\272\316\261\316", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\277\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\261\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\263\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab5;
        if (z->c > z->lb) goto lab5;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 2, s_8);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 4, s_28);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab5:
        z->c = z->l - v_6;
    }
    {
        int v_8 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\317\203\316\277\317\205\316\274\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\317\203\316\277\317\205\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\265\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\265\316\271\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x89:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\317\203\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab7;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB1:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\207\316\261\317\201\317\204\316\277\317\200\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\276\316\261\316\275\316\261\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\265\317\201\316\271\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\316\275\316\261\316\274\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\265\316\276\316\261\317\201\317\207\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\274\317\200\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\317\205\316\275\316\261\316\270\317\201\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\270\317\201\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\274\316\265\317\204\316\265\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\265\317\203\317\211\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\200\316\265\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\200\316\277\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\272\316\273\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\261\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\273\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab7;
        if (z->c > z->lb) goto lab7;
        {
            int ret = slice_from_s(z, 2, s_8);
            if (ret < 0) return ret;
        }
    lab7:
        z->c = z->l - v_8;
    }
    {
        int v_9 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\317\203\317\204\316\277\317\205\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\317\204\316\265\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\317\204\316\267\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\317\204\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\317\204\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\317\204\316\277\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\317\204\317\211\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\317\204\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\317\204\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB7:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\317\204\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\317\204\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab8;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBF:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\317\205\316\275\316\261\316\270\317\201\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\265\317\203\317\211\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\261\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\273\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\273\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\205\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\274\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\317\205\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\207\317\211\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\277\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\207\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0x84:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\207\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\272\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\207\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\272\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0x87:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\203\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\261\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\204\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\317\206\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\206\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0xB3:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\317\205\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\265\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0xB8:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\207\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\261\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\261\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\261\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\317\205\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\206\316\271\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\265\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\207\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        break;
                    case 0x88:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\205\317", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                    case 0xB4:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\267\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab8;
        if (z->c > z->lb) goto lab8;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 2, s_8);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 6, s_29);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab8:
        z->c = z->l - v_9;
    }
    {
        int v_10 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\317\203\316\274\316\277\317\205\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\274\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\274\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\274\316\277\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\274\317\211\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\316\274\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
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
        b_test1 = 0;
        do {
            int v_11 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB5:
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\261\316\275\317\204\316\271\316\264\316\261\316\275\316", 15) == 0) { among_var = 2; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\274\316\271\316\272\317\201\316\277\317\203\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\274\316\265\317\204\316\261\317\203\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\200\316\277\316\272\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\316\263\316\272\316\273\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\261\316\275\316", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab10;
            if (z->c > z->lb) goto lab10;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 6, s_30);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 2, s_8);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            break;
        lab10:
            z->c = z->l - v_11;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xBD:
                            if (c_among - z->lb >= 20 && __builtin_memcmp(z->p + c_among - 20, "\316\261\316\273\316\265\316\276\316\261\316\275\316\264\317\201\316\271\316", 19) == 0) { among_var = 8; z->c = c_among - 20; break; }
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\262\317\205\316\266\316\261\316\275\317\204\316\271\316", 15) == 0) { among_var = 9; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\270\316\265\316\261\317\204\317\201\316\271\316", 13) == 0) { among_var = 10; z->c = c_among - 14; break; }
                            break;
                        case 0xBA:
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\261\316\263\316\275\317\211\317\203\317\204\316\271\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\265\316\272\316\273\316\265\316\272\317\204\316\271\316", 15) == 0) { among_var = 5; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\316\272\316\265\317\200\317\204\316\271\316", 13) == 0) { among_var = 6; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\263\316\275\317\211\317\203\317\204\316\271\316", 13) == 0) { among_var = 3; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\204\316\277\316\274\316\271\316", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\204\316\277\317\200\316\271\316", 9) == 0) { among_var = 7; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\316\270\316\275\316\271\316", 9) == 0) { among_var = 4; z->c = c_among - 10; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab9;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 12, s_0);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 8, s_31);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = slice_from_s(z, 10, s_32);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    {
                        int ret = slice_from_s(z, 6, s_4);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    {
                        int ret = slice_from_s(z, 12, s_33);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    {
                        int ret = slice_from_s(z, 10, s_34);
                        if (ret < 0) return ret;
                    }
                    break;
                case 7:
                    {
                        int ret = slice_from_s(z, 6, s_35);
                        if (ret < 0) return ret;
                    }
                    break;
                case 8:
                    {
                        int ret = slice_from_s(z, 16, s_36);
                        if (ret < 0) return ret;
                    }
                    break;
                case 9:
                    {
                        int ret = slice_from_s(z, 12, s_1);
                        if (ret < 0) return ret;
                    }
                    break;
                case 10:
                    {
                        int ret = slice_from_s(z, 10, s_7);
                        if (ret < 0) return ret;
                    }
                    break;
            }
        } while (0);
    lab9:
        z->c = z->l - v_10;
    }
    {
        int v_12 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB1:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\317\205\316\264\316\261\316\272\316\271\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\201\316\261\316\272\316\271\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\205\316\264\316\261\316\272\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\201\316\261\316\272\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab11;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x87:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab11;
        if (z->c > z->lb) goto lab11;
        {
            int ret = slice_from_s(z, 8, s_37);
            if (ret < 0) return ret;
        }
    lab11:
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
                    case 0xB1:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\201\316\261\316\272\316\271\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\204\317\203\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\272\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\204\317\203\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\204\317\203\316\265\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\201\316\261\316\272\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\272\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\204\317\203\317\211\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab12;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_14 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB2:
                            if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\317\204\317\203\316\265\317\207\316\277\317\203\316\273\316\277\316", 17) == 0) { among_var = 1; z->c = c_among - 18; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\316\261\316\274\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\316\273\316\277\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            break;
                        case 0xBD:
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\267\316\263\316\277\317\205\316\274\316\265\316", 13) == 0) { among_var = 2; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\274\316\261\316\272\317\201\317\205\316", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\200\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\263\316\271\316\261\316", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x80:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\272\316\261\317\204\317\201\316\261\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            break;
                        case 0x83:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\275\316\261\316\263\316\272\316\261\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\277\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0xBB:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\204\317\201\316\271\317\200\316\277\316", 11) == 0) { among_var = 2; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\273\316\277\317\205\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\206\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\262\316\261\316", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\263\316", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                            break;
                        case 0x81:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\261\317\204\316\265\317", 9) == 0) { among_var = 2; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\273\316\261\316\262\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\274\316\262\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\275\316\270\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\262\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0x84:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\274\316\277\317\205\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0x86:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\273\316\271\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\317\205\317", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0xBA:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\272\316\261\317\200\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\203\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0xB4:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\261\317\201\316", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                            break;
                        case 0xBC:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\206\316\261\317\201\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\261\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\273\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0x85:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\201\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0xB6:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\204\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            break;
                        case 0x87:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab13;
            if (z->c > z->lb) goto lab13;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 4, s_38);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 6, s_39);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            break;
        lab13:
            z->c = z->l - v_14;
            z->ket = z->c;
            z->bra = z->c;
            if (!(eq_s_b(z, 6, s_40))) goto lab12;
            {
                int ret = slice_from_s(z, 6, s_39);
                if (ret < 0) return ret;
            }
        } while (0);
    lab12:
        z->c = z->l - v_13;
    }
    {
        int v_15 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\264\316\271\317\211\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\264\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\264\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab14;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_16 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xBD:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\271\317\206\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0xBB:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\210\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xBF:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\316\273\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x81:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\271\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab15;
            if (z->c > z->lb) goto lab15;
            {
                int ret = slice_from_s(z, 4, s_41);
                if (ret < 0) return ret;
            }
            break;
        lab15:
            z->c = z->l - v_16;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xBD:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\261\316\271\317\207\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0xB5:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab14;
            {
                int ret = slice_from_s(z, 4, s_41);
                if (ret < 0) return ret;
            }
        } while (0);
    lab14:
        z->c = z->l - v_15;
    }
    {
        int v_17 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\272\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\317\203\316\272\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\316\272\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\317\203\316\272\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab16;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBA:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\206\317\201\316\261\316\263\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\316\262\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\267\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB2:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\271\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB4:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab16;
        if (z->c > z->lb) goto lab16;
        {
            int ret = slice_from_s(z, 6, s_42);
            if (ret < 0) return ret;
        }
    lab16:
        z->c = z->l - v_17;
    }
    {
        int v_18 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\264\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\264\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
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
        {
            int v_19 = z->l - z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x80:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\274\317\200\316\261\316\274\317", 9) == 0) { among_var = -1; z->c = c_among - 10; break; }
                            break;
                        case 0x81:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\261\317\204\316\265\317", 9) == 0) { among_var = -1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\265\316\270\316\265\317", 9) == 0) { among_var = -1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\317\205\317", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                            break;
                        case 0x84:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\275\317\204\316\261\316\275\317", 9) == 0) { among_var = -1; z->c = c_among - 10; break; }
                            break;
                        case 0xB9:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\263\316\271\316\261\316\263\316", 9) == 0) { among_var = -1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\270\316\265\316", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                            break;
                        case 0xBC:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\261\316", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                            break;
                        case 0xBD:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\261\316", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                            break;
                        case 0xBA:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\316", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab18;
            goto lab17;
        lab18:
            z->c = z->l - v_19;
        }
        {
            int saved_c = z->c;
            int ret = insert_s(z, z->c, z->c, 4, s_43);
            z->c = saved_c;
            if (ret < 0) return ret;
        }
    lab17:
        z->c = z->l - v_18;
    }
    {
        int v_20 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\264\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\264\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab19;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x80:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\272\317\201\316\261\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\264\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\267\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\274\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\205\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\271\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab19;
        {
            int ret = slice_from_s(z, 4, s_44);
            if (ret < 0) return ret;
        }
    lab19:
        z->c = z->l - v_20;
    }
    {
        int v_21 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\205\316\264\316\265\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\205\316\264\317\211\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab20;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBA:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\272\316\261\316\273\316\271\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\201\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\265\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\273\316\277\317\205\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\262\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\206\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB3:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\204\317\201\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBE:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\316\273\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x87:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\206\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\206\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\207\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab20;
        {
            int ret = slice_from_s(z, 6, s_45);
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
                    case 0x83:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\211\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\211\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab21;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x81:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB4:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\271\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB8:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab21;
        if (z->c > z->lb) goto lab21;
        {
            int ret = slice_from_s(z, 2, s_4);
            if (ret < 0) return ret;
        }
    lab21:
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
                    case 0x85:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\317\211\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\271\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab22;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        if (snowball_in_grouping_b_U(z, g_v, 945, 969, 0)) goto lab22;
        {
            int ret = slice_from_s(z, 2, s_8);
            if (ret < 0) return ret;
        }
    lab22:
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
                    case 0x85:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\272\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\271\316\272\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\272\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\271\316\272\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab23;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_25 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            if (snowball_in_grouping_b_U(z, g_v, 945, 969, 0)) goto lab24;
            {
                int ret = slice_from_s(z, 4, s_46);
                if (ret < 0) return ret;
            }
            break;
        lab24:
            z->c = z->l - v_25;
            z->ket = z->c;
        } while (0);
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x84:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\274\317\200\316\261\316\263\316\271\316\261\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\271\316\272\316\261\316\275\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\316\265\317\201\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\261\316\274\316\274\316\277\317\207\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\317\205\316\275\316\277\316\274\316\267\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\316\277\317\205\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\317\200\316\277\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\277\317\203\317\204\316\265\316\273\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\272\316\261\316\273\316\273\316\271\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\206\316\271\316\273\316\277\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\317\200\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\274\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\273\316\271\316\261\317\204\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\316\265\317\204\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\316\271\317\204\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\317\200\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\206\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\207\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB4:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\317\201\317\211\317\204\316\277\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\272\316\261\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\203\317\205\316\275\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\275\317\204\316\271\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\206\317\205\316\273\316\277\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\276\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\205\317\200\316\277\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\275\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\261\316\273\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB8:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\275\316\267\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\267\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\317\201\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\204\317\203\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\276\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab23;
        if (z->c > z->lb) goto lab23;
        {
            int ret = slice_from_s(z, 4, s_46);
            if (ret < 0) return ret;
        }
    lab23:
        z->c = z->l - v_24;
    }
    {
        int v_26 = z->l - z->c;
        {
            int v_27 = z->l - z->c;
            z->ket = z->c;
            if (!(eq_s_b(z, 10, s_47))) goto lab26;
            z->bra = z->c;
            if (z->c > z->lb) goto lab26;
            {
                int ret = slice_from_s(z, 8, s_47);
                if (ret < 0) return ret;
            }
        lab26:
            z->c = z->l - v_27;
        }
        {
            int v_28 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB5:
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\267\316\270\316\267\316\272\316\261\316\274\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\205\317\203\316\261\316\274\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\317\203\316\261\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\263\316\261\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\272\316\261\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab27;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            b_test1 = 0;
        lab27:
            z->c = z->l - v_28;
        }
        z->ket = z->c;
        if (!(eq_s_b(z, 6, s_48))) goto lab25;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x84:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\200\316\277\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\275\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\316\271\316\272\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB2:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\316\277\317\205\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB8:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\200\316\277\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\276\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\200\316\277\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x87:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\203\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab25;
        if (z->c > z->lb) goto lab25;
        {
            int ret = slice_from_s(z, 4, s_48);
            if (ret < 0) return ret;
        }
    lab25:
        z->c = z->l - v_26;
    }
    {
        int v_29 = z->l - z->c;
        {
            int v_30 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB5:
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\271\316\277\317\205\316\275\317\204\316\261\316\275\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\317\205\316\275\317\204\316\261\316\275\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\316\275\317\204\316\261\316\275\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\267\316\270\316\267\316\272\316\261\316\275\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\205\317\203\316\261\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\316\275\317\204\316\261\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\316\277\317\204\316\261\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\317\203\316\261\316\275\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\204\316\261\316\275\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\263\316\261\316\275\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\272\316\261\316\275\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab29;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            b_test1 = 0;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x81:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\204\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0x83:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\204\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab29;
            if (z->c > z->lb) goto lab29;
            {
                int ret = slice_from_s(z, 8, s_49);
                if (ret < 0) return ret;
            }
        lab29:
            z->c = z->l - v_30;
        }
        z->ket = z->c;
        if (!(eq_s_b(z, 6, s_50))) goto lab28;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_31 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            if (snowball_in_grouping_b_U(z, g_v2, 945, 969, 0)) goto lab30;
            {
                int ret = slice_from_s(z, 4, s_50);
                if (ret < 0) return ret;
            }
            break;
        lab30:
            z->c = z->l - v_31;
            z->ket = z->c;
        } while (0);
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x87:
                        if (c_among - z->lb >= 22 && __builtin_memcmp(z->p + c_among - 22, "\316\274\316\271\316\272\317\201\316\277\316\262\316\271\316\277\316\274\316\267\317", 21) == 0) { among_var = 1; z->c = c_among - 22; break; }
                        if (c_among - z->lb >= 22 && __builtin_memcmp(z->p + c_among - 22, "\316\274\316\265\316\263\316\273\316\277\316\262\316\271\316\277\316\274\316\267\317", 21) == 0) { among_var = 1; z->c = c_among - 22; break; }
                        if (c_among - z->lb >= 22 && __builtin_memcmp(z->p + c_among - 22, "\316\272\316\261\317\200\316\275\316\277\316\262\316\271\316\277\316\274\316\267\317", 21) == 0) { among_var = 1; z->c = c_among - 22; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\277\316\273\317\205\316\274\316\267\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\262\316\271\316\277\316\274\316\267\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\274\316\267\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\317\207\316\261\316\274\316\267\316\273\316\277\316\264\316\261\317", 17) == 0) { among_var = 1; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\277\316\273\317\205\316\264\316\261\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\205\317\200\316\277\316\272\316\277\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\264\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\204\317\203\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\317\203\316\261\317\201\316\261\316\272\316\261\317\204\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\277\316\273\316\271\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\270\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\262\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x86:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\317\206\317\211\317\204\316\277\317\203\317\204\316\265\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\265\316\275\317\204\316\261\317\201\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\316\277\316\271\316\273\316\261\317\201\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\205\317\200\316\265\317\201\316\267\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\265\317\201\316\267\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\271\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\204\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\201\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB2:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\276\316\267\317\201\316\277\316\272\316\273\316\271\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\210\316\267\316\273\316\277\317\204\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\275\317\204\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\273\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\277\316\273\316\271\316\263\316\277\316\264\316\261\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\274\316\277\317\205\317\203\316\277\317\205\316\273\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\264\317\201\316\261\316\264\316\277\317\205\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\262\317\201\316\261\317\207\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\261\316\274\316\265\317\201\316\271\316\272\316\261\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x84:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\204\317\203\316\261\317\201\316\273\316\261\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\277\317\205\317\201\316\271\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\203\316\277\317\205\316\273\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\274\316\261\316\271\316\275\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\272\316\261\317\203\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\271\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\316\273\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\266\317\211\316\275\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB6:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\316\261\316\273\317\200\316\277\317\205\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\262\316\261\316\270\317\205\316\263\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\316\261\317\204\316\261\316\263\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\210\317\205\317\207\316\277\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\316\273\316\277\316\263\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\272\316\261\317\203\317\204\316\265\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\277\317\201\317\204\316\277\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\273\316\261\316\277\317\200\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\271\317\200\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\265\317\201\316\271\317\204\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\273\316\277\317\205\316\270\316\267\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\272\316\277\317\201\316\274\316\277\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\262\316\265\317\204\316\265\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\263\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB3:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\317\204\317\203\316\271\316\263\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\275\316\277\317\201\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\316\275\316\277\317\201\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\204\317\203\316\271\316\263\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\270\316\271\316\263\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\204\317\201\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\204\317\203\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\204\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\200\316\267\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\203\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB8:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\274\317\211\316\261\316\274\316\265\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\200\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\262\317\201\316\261\317\207\317\205\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\265\316\273\316\265\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\262\316\277\317\205\316\273\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\316\261\317\203\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\275\316\271\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\264\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\271\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab28;
        if (z->c > z->lb) goto lab28;
        {
            int ret = slice_from_s(z, 4, s_50);
            if (ret < 0) return ret;
        }
    lab28:
        z->c = z->l - v_29;
    }
    {
        int v_32 = z->l - z->c;
        {
            int v_33 = z->l - z->c;
            z->ket = z->c;
            if (!(eq_s_b(z, 10, s_6))) goto lab32;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            b_test1 = 0;
        lab32:
            z->c = z->l - v_33;
        }
        z->ket = z->c;
        if (!(eq_s_b(z, 6, s_51))) goto lab31;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_34 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            if (snowball_in_grouping_b_U(z, g_v2, 945, 969, 0)) goto lab33;
            {
                int ret = slice_from_s(z, 4, s_51);
                if (ret < 0) return ret;
            }
            break;
        lab33:
            z->c = z->l - v_34;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB8:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\205\317\200\316\265\317\201\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\271\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\201\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\272\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\275\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\201\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0xB4:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\275\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0xBB:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\211\317\206\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\262\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x81:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\207\317\211\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\262\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\206\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\262\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0x84:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xBA:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\201\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xBD:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\203\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\201\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x87:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab34;
            {
                int ret = slice_from_s(z, 4, s_51);
                if (ret < 0) return ret;
            }
            break;
        lab34:
            z->c = z->l - v_34;
            z->ket = z->c;
        } while (0);
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBB:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\261\317\201\316\261\316\272\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\316\272\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\262\316\261\317\201\316\277\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\262\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x80:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\316\265\317\201\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\272\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\270\316\261\317\201\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\262\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\275\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\317\200\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\317\204\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\262\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x86:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\317\201\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB3:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBA:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\316\277\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB4:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB8:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab31;
        if (z->c > z->lb) goto lab31;
        {
            int ret = slice_from_s(z, 4, s_51);
            if (ret < 0) return ret;
        }
    lab31:
        z->c = z->l - v_32;
    }
    {
        int v_35 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\211\316\275\317\204\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\316\275\317\204\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab35;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_36 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            if (!(eq_s_b(z, 6, s_52))) goto lab36;
            if (z->c > z->lb) goto lab36;
            {
                int ret = slice_from_s(z, 6, s_53);
                if (ret < 0) return ret;
            }
            break;
        lab36:
            z->c = z->l - v_36;
            z->ket = z->c;
            z->bra = z->c;
            if (!(eq_s_b(z, 6, s_9))) goto lab35;
            {
                int ret = slice_from_s(z, 6, s_23);
                if (ret < 0) return ret;
            }
        } while (0);
    lab35:
        z->c = z->l - v_35;
    }
    {
        int v_37 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\316\274\316\261\317\203\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\316\274\316\261\317\203\317\204\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab37;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        if (z->c - z->lb < 4 || __builtin_memcmp(z->p + z->c - 4, s_53, 4) != 0) goto lab37;
        z->c -= 4;
        if (z->c > z->lb) goto lab37;
        {
            int ret = slice_from_s(z, 10, s_54);
            if (ret < 0) return ret;
        }
    lab37:
        z->c = z->l - v_37;
    }
    {
        int v_38 = z->l - z->c;
        {
            int v_39 = z->l - z->c;
            z->ket = z->c;
            if (!(eq_s_b(z, 10, s_55))) goto lab39;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            b_test1 = 0;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x86:
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\261\316\274\316\265\317\204\316\261\316\274\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            break;
                        case 0x80:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\316\272\316\261\317\204\316\261\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\203\317\205\316\274\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\316\274\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab39;
            if (z->c > z->lb) goto lab39;
            {
                int ret = slice_from_s(z, 8, s_55);
                if (ret < 0) return ret;
            }
        lab39:
            z->c = z->l - v_39;
        }
        z->ket = z->c;
        if (!(eq_s_b(z, 8, s_56))) goto lab38;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBB:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\200\316\261\317\201\316\261\316\272\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\316\272\317\204\316\265\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\317\201\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB6:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBC:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBE:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab38;
        if (z->c > z->lb) goto lab38;
        {
            int ret = slice_from_s(z, 8, s_55);
            if (ret < 0) return ret;
        }
    lab38:
        z->c = z->l - v_38;
    }
    {
        int v_40 = z->l - z->c;
        {
            int v_41 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x83:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\316\270\316\267\316\272\316\265\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            break;
                        case 0xB1:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\270\316\267\316\272\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0xB5:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\270\316\267\316\272\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab41;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            b_test1 = 0;
        lab41:
            z->c = z->l - v_41;
        }
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\316\272\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\316\272\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\316\272\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab40;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_42 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xBB:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\203\316\272\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\316\272\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0xB8:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\275\316\261\317\201\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0x86:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab42;
            {
                int ret = slice_from_s(z, 4, s_57);
                if (ret < 0) return ret;
            }
            break;
        lab42:
            z->c = z->l - v_42;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xB8:
                            if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\317\200\316\261\317\201\316\261\316\272\316\261\317\204\316\261\316", 17) == 0) { among_var = 1; z->c = c_among - 18; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\317\201\316\277\317\203\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\271\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab40;
            if (z->c > z->lb) goto lab40;
            {
                int ret = slice_from_s(z, 4, s_57);
                if (ret < 0) return ret;
            }
        } while (0);
    lab40:
        z->c = z->l - v_40;
    }
    {
        int v_43 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\205\317\203\316\265\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\205\317\203\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\205\317\203\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab43;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_44 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x87:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\261\316\275\317\204\316\261\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xBB:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\274\316\261\316\275\317\204\316\271\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\316\261\316\273\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0x81:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\277\316\264\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0x84:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\272\317\205\316\274\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\317\201\317\211\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0x80:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\316\273\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0xB4:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\206\317\201\317\205\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                        case 0xB3:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\206\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\267\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xBC:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab44;
            {
                int ret = slice_from_s(z, 6, s_58);
                if (ret < 0) return ret;
            }
            break;
        lab44:
            z->c = z->l - v_44;
            z->ket = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x81:
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\265\316\275\316\264\316\271\316\261\317\206\316\265\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\275\316\261\317\201\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0x85:
                            if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\264\316\265\317\205\317\204\316\265\317\201\316\265\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\316\261\316\270\316\261\317\201\316\265\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            break;
                        case 0xBD:
                            if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\205\317\200\316\277\317\204\316\265\316\271\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                            break;
                        case 0xB4:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\273\316\261\316\274\317\200\316\271\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\207\316\261\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\274\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xB6:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\264\316\265\317\203\317\200\316\277\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\274\316\265\317\203\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0xBA:
                            if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\206\316\261\317\201\316\274\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\275\316\267\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\263\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x80:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\316\272\316\273\316\271\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0xBC:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\262\317\201\316\277\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 0x84:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0x87:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xB1:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\204\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xB5:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\273\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\264\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 0xB8:
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\271\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            break;
                        case 0xBB:
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab43;
            if (z->c > z->lb) goto lab43;
            {
                int ret = slice_from_s(z, 6, s_58);
                if (ret < 0) return ret;
            }
        } while (0);
    lab43:
        z->c = z->l - v_43;
    }
    {
        int v_45 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x85:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\317\203\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab45;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBD:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\264\317\211\316\264\316\265\316\272\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\274\316\265\316\263\316\261\316\273\316\277\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\207\316\265\317\201\317\203\316\277\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\265\317\201\316\267\316\274\316\277\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\317\200\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab45;
        if (z->c > z->lb) goto lab45;
        {
            int ret = slice_from_s(z, 4, s_6);
            if (ret < 0) return ret;
        }
    lab45:
        z->c = z->l - v_45;
    }
    {
        int v_46 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\263\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\263\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\263\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab46;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        do {
            int v_47 = z->l - z->c;
            z->ket = z->c;
            z->bra = z->c;
            if (!(eq_s_b(z, 8, s_59))) goto lab47;
            {
                int ret = slice_from_s(z, 4, s_0);
                if (ret < 0) return ret;
            }
            break;
        lab47:
            z->c = z->l - v_47;
            do {
                int v_48 = z->l - z->c;
                z->ket = z->c;
                z->bra = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x87:
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\275\316\261\317\205\316\273\316\277\317", 11) == 0) { among_var = -1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\273\316\277\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                break;
                            case 0x84:
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\207\316\277\317\201\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                break;
                            case 0xBD:
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\316\274\316\267\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                break;
                            case 0x86:
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\210\316\277\317", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0xBB:
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\200\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\273\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0x80:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\201\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0x81:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\200\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\206\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab48;
                switch (among_var) {
                    case 1:
                        {
                            int ret = slice_from_s(z, 4, s_0);
                            if (ret < 0) return ret;
                        }
                        break;
                }
                break;
            lab48:
                z->c = z->l - v_48;
                z->ket = z->c;
                z->bra = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x80:
                                if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\317\200\317\201\316\277\317\203\317\211\317\200\316\277\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                                if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\272\317\201\316\277\316\272\316\261\316\273\316\277\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                                if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\317\203\316\271\316\264\316\267\317\201\316\277\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\264\317\201\316\277\317\203\316\277\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\201\317\204\316\271\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\275\317\205\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\265\316\271\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\316\274\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\275\316\265\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\316\273\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\203\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 0x84:
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\316\271\316\274\316\277\317\203\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\275\317\205\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\262\316\261\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\317\201\316\277\317\203\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\275\316\277\316\274\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\264\316\271\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\317\200\316\271\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\203\317\205\316\275\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\205\317\200\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\200\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\316\274\316\277\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 0xB9:
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\261\316\274\316\261\316\273\316\273\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                break;
                            case 0xBD:
                                if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\264\316\265\317\201\316\262\316\265\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0x81:
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\203\317\200\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\207\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\200\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\207\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\204\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 0x86:
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\317\200\316\277\316\273\317\205\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\264\316\267\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\200\316\261\316\274\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\276\316\265\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0xBC:
                                if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\205\316\273\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 0xBB:
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\274\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                            case 0x85:
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\275\316\261\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab46;
                if (z->c > z->lb) goto lab46;
                {
                    int ret = slice_from_s(z, 4, s_0);
                    if (ret < 0) return ret;
                }
            } while (0);
        } while (0);
    lab46:
        z->c = z->l - v_46;
    }
    {
        int v_49 = z->l - z->c;
        z->ket = z->c;
        if (!(eq_s_b(z, 8, s_60))) goto lab49;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x81:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\272\316\277\316\271\316\275\316\277\317\207\317", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\264\317\205\317\203\317\207\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\317\205\317\207\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\207\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\207\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x88:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\200\316\261\316\273\316\271\316\274\317", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\265\316\271\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xB2:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\203\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\203\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab49;
        if (z->c > z->lb) goto lab49;
        {
            int ret = slice_from_s(z, 6, s_60);
            if (ret < 0) return ret;
        }
    lab49:
        z->c = z->l - v_49;
    }
    {
        int v_50 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\317\203\316\277\317\205\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\316\270\316\277\317\205\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\205\316\275\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab50;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 22 && __builtin_memcmp(z->p + c_among - 22, "\317\203\317\204\317\201\316\261\316\262\316\277\316\274\316\277\317\205\317\204\317", 21) == 0) { among_var = 1; z->c = c_among - 22; break; }
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\316\272\316\261\316\272\316\277\316\274\316\277\317\205\317\204\317", 17) == 0) { among_var = 1; z->c = c_among - 18; break; }
                        break;
                    case 0xBD:
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\276\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\317\203\317\200\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        break;
                    case 0x81:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab50;
        if (z->c > z->lb) goto lab50;
        {
            int ret = slice_from_s(z, 6, s_61);
            if (ret < 0) return ret;
        }
    lab50:
        z->c = z->l - v_50;
    }
    {
        int v_51 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xB5:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\317\203\316\277\317\205\316\274\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\316\270\316\277\317\205\316\274\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\205\316\274\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab51;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_test1 = 0;
        z->ket = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x83:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\317\200\316\261\317\201\316\261\317\203\316\277\317\205\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\261\316\273\316\273\316\277\317\203\316\277\317\205\317", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\317\203\316\277\317\205\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        break;
                    case 0xBB:
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\317\211\317\201\316\271\316\277\317\200\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        break;
                    case 0xB6:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x86:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x87:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab51;
        if (z->c > z->lb) goto lab51;
        {
            int ret = slice_from_s(z, 6, s_62);
            if (ret < 0) return ret;
        }
    lab51:
        z->c = z->l - v_51;
    }
    {
        int v_52 = z->l - z->c;
        {
            int v_53 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0x83:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\274\316\261\317\204\316\277\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0xBD:
                            if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\274\316\261\317\204\317\211\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                            break;
                        case 0xB1:
                            if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\274\316\261\317\204\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab53;
            z->bra = z->c;
            {
                int ret = slice_from_s(z, 4, s_11);
                if (ret < 0) return ret;
            }
        lab53:
            z->c = z->l - v_53;
        }
        if (!b_test1) goto lab52;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xBD:
                        if (c_among - z->lb >= 18 && __builtin_memcmp(z->p + c_among - 18, "\316\271\316\277\316\275\317\204\316\277\317\205\317\203\316\261\316", 17) == 0) { among_var = 1; z->c = c_among - 18; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\277\316\275\317\204\316\277\317\205\317\203\316\261\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\271\316\277\317\203\316\261\317\203\317\204\316\261\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\271\316\277\316\274\316\261\317\203\317\204\316\261\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\317\203\316\261\317\203\317\204\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\316\274\316\261\317\203\317\204\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\317\205\316\275\317\204\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\316\277\317\203\316\277\317\205\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\316\277\316\274\316\277\317\205\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\205\316\275\317\204\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\271\316\277\316\275\317\204\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\316\270\316\267\316\272\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\317\203\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\203\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\270\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\316\274\316\277\317\205\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\205\317\203\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\316\275\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\277\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\316\264\317\211\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\317\203\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\204\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\263\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\316\272\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\205\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\211\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0xB5:
                        if (c_among - z->lb >= 16 && __builtin_memcmp(z->p + c_among - 16, "\316\271\316\277\317\205\316\274\316\261\317\203\317\204\316", 15) == 0) { among_var = 1; z->c = c_among - 16; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\265\317\203\316\261\317\203\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\317\203\316\261\317\203\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\277\317\205\316\274\316\261\317\203\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\265\316\274\316\261\317\203\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\267\316\270\316\267\316\272\316\261\317\204\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\203\316\261\317\203\317\204\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\205\317\203\316\261\317\204\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\267\316\270\316\265\316\271\317\204\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\317\203\316\261\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\261\316\263\316\261\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\272\316\261\317\204\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\316\271\317\204\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB1:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\317\203\316\277\317\205\316\275\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\316\274\316\277\317\205\316\275\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\203\316\277\317\205\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\316\274\316\277\317\205\316\275\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\277\317\205\316\274\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB9:
                        if (c_among - z->lb >= 14 && __builtin_memcmp(z->p + c_among - 14, "\316\271\316\277\317\205\316\275\317\204\316\261\316", 13) == 0) { among_var = 1; z->c = c_among - 14; break; }
                        if (c_among - z->lb >= 12 && __builtin_memcmp(z->p + c_among - 12, "\316\277\317\205\316\275\317\204\316\261\316", 11) == 0) { among_var = 1; z->c = c_among - 12; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\265\317\203\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\265\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\316\275\317\204\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\277\317\205\316\274\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\271\316\265\316\274\316\261\316", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\203\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\317\203\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\317\204\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\265\317\204\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\261\316\274\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\316\274\316\261\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\317\203\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\316\270\316\265\316", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\261\316\265\316", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\316", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x83:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\267\316\270\316\265\316\271\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\317\203\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\267\316\264\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\277\317\205\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\265\316\271\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\317\205\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\265\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\267\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 0x89:
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\317\203\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\316\267\316\270\317", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\261\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0x85:
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\316\277\317", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\317", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xB7:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xBF:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\316", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab52;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab52:
        z->c = z->l - v_52;
    }
    {
        int v_54 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x81:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\317\203\317\204\316\265\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\205\317\204\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\211\317\204\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\204\316\265\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                    case 0x84:
                        if (c_among - z->lb >= 10 && __builtin_memcmp(z->p + c_among - 10, "\316\265\317\203\317\204\316\261\317", 9) == 0) { among_var = 1; z->c = c_among - 10; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\205\317\204\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\317\211\317\204\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\316\277\317\204\316\261\317", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab54;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab54:
        z->c = z->l - v_54;
    }
    z->c = z->lb;
    return 1;
}

/* Generated from arabic.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_arabic_candidate.h"

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

static int r_Suffix_Noun_Step2a(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 0x88:
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
                case 0x8A:
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
                case 0xA7:
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    z->bra = z->c;
    if (len_utf8(z->p) < 5) return 0;
    {
        int ret = snowball_slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_Suffix_Noun_Step2b(struct SN_env * z) {
    z->ket = z->c;
    if (z->c - z->lb < 4 || __builtin_memcmp(z->p + z->c - 4, s_0, 4) != 0) return 0;
    z->c -= 4;
    z->bra = z->c;
    if (len_utf8(z->p) < 5) return 0;
    {
        int ret = snowball_slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_Suffix_Noun_Step2c1(struct SN_env * z) {
    z->ket = z->c;
    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_1, 2) != 0) return 0;
    z->c -= 2;
    z->bra = z->c;
    if (len_utf8(z->p) < 4) return 0;
    {
        int ret = snowball_slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_Suffix_Verb_Step2a(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 0xA7:
                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\330\252\331\205\330", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\206\330", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\330\252\330", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
                case 0x86:
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\210\331", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\212\331", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\330\247\331", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\330\252\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
                case 0x8A:
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
                case 0xAA:
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            if (len_utf8(z->p) < 4) return 0;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            if (len_utf8(z->p) < 5) return 0;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        case 3:
            if (len_utf8(z->p) < 6) return 0;
            {
                int ret = snowball_slice_del(z);
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
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among < z->l) {
                switch (z->p[c_among]) {
                    case 0xD8:
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\250\330\247\331\204", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\247\331\204", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                        break;
                    case 0xD9:
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\203\330\247\331\204", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\204\331\204", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                        break;
                }
            }
        }
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
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among < z->l) {
                        switch (z->p[c_among]) {
                            case 0xEF:
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\200", 2) == 0) { among_var = 12; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\201", 2) == 0) { among_var = 16; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\202", 2) == 0) { among_var = 16; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\203", 2) == 0) { among_var = 13; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\204", 2) == 0) { among_var = 13; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\205", 2) == 0) { among_var = 17; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\206", 2) == 0) { among_var = 17; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\207", 2) == 0) { among_var = 14; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\210", 2) == 0) { among_var = 14; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\211", 2) == 0) { among_var = 15; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\212", 2) == 0) { among_var = 15; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\213", 2) == 0) { among_var = 15; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\214", 2) == 0) { among_var = 15; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\215", 2) == 0) { among_var = 18; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\216", 2) == 0) { among_var = 18; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\217", 2) == 0) { among_var = 19; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\220", 2) == 0) { among_var = 19; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\221", 2) == 0) { among_var = 19; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\222", 2) == 0) { among_var = 19; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\223", 2) == 0) { among_var = 20; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\224", 2) == 0) { among_var = 20; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\225", 2) == 0) { among_var = 21; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\226", 2) == 0) { among_var = 21; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\227", 2) == 0) { among_var = 21; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\230", 2) == 0) { among_var = 21; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\231", 2) == 0) { among_var = 22; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\232", 2) == 0) { among_var = 22; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\233", 2) == 0) { among_var = 22; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\234", 2) == 0) { among_var = 22; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\235", 2) == 0) { among_var = 23; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\236", 2) == 0) { among_var = 23; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\237", 2) == 0) { among_var = 23; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\240", 2) == 0) { among_var = 23; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\241", 2) == 0) { among_var = 24; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\242", 2) == 0) { among_var = 24; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\243", 2) == 0) { among_var = 24; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\244", 2) == 0) { among_var = 24; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\245", 2) == 0) { among_var = 25; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\246", 2) == 0) { among_var = 25; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\247", 2) == 0) { among_var = 25; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\250", 2) == 0) { among_var = 25; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\251", 2) == 0) { among_var = 26; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\252", 2) == 0) { among_var = 26; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\253", 2) == 0) { among_var = 27; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\254", 2) == 0) { among_var = 27; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\255", 2) == 0) { among_var = 28; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\256", 2) == 0) { among_var = 28; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\257", 2) == 0) { among_var = 29; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\260", 2) == 0) { among_var = 29; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\261", 2) == 0) { among_var = 30; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\262", 2) == 0) { among_var = 30; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\263", 2) == 0) { among_var = 30; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\264", 2) == 0) { among_var = 30; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\265", 2) == 0) { among_var = 31; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\266", 2) == 0) { among_var = 31; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\267", 2) == 0) { among_var = 31; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\270", 2) == 0) { among_var = 31; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\271", 2) == 0) { among_var = 32; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\272", 2) == 0) { among_var = 32; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\273", 2) == 0) { among_var = 32; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\274", 2) == 0) { among_var = 32; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\275", 2) == 0) { among_var = 33; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\276", 2) == 0) { among_var = 33; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\272\277", 2) == 0) { among_var = 33; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\200", 2) == 0) { among_var = 33; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\201", 2) == 0) { among_var = 34; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\202", 2) == 0) { among_var = 34; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\203", 2) == 0) { among_var = 34; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\204", 2) == 0) { among_var = 34; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\205", 2) == 0) { among_var = 35; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\206", 2) == 0) { among_var = 35; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\207", 2) == 0) { among_var = 35; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\210", 2) == 0) { among_var = 35; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\211", 2) == 0) { among_var = 36; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\212", 2) == 0) { among_var = 36; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\213", 2) == 0) { among_var = 36; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\214", 2) == 0) { among_var = 36; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\215", 2) == 0) { among_var = 37; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\216", 2) == 0) { among_var = 37; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\217", 2) == 0) { among_var = 37; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\220", 2) == 0) { among_var = 37; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\221", 2) == 0) { among_var = 38; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\222", 2) == 0) { among_var = 38; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\223", 2) == 0) { among_var = 38; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\224", 2) == 0) { among_var = 38; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\225", 2) == 0) { among_var = 39; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\226", 2) == 0) { among_var = 39; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\227", 2) == 0) { among_var = 39; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\230", 2) == 0) { among_var = 39; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\231", 2) == 0) { among_var = 40; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\232", 2) == 0) { among_var = 40; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\233", 2) == 0) { among_var = 40; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\234", 2) == 0) { among_var = 40; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\235", 2) == 0) { among_var = 41; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\236", 2) == 0) { among_var = 41; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\237", 2) == 0) { among_var = 41; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\240", 2) == 0) { among_var = 41; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\241", 2) == 0) { among_var = 42; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\242", 2) == 0) { among_var = 42; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\243", 2) == 0) { among_var = 42; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\244", 2) == 0) { among_var = 42; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\245", 2) == 0) { among_var = 43; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\246", 2) == 0) { among_var = 43; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\247", 2) == 0) { among_var = 43; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\250", 2) == 0) { among_var = 43; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\251", 2) == 0) { among_var = 44; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\252", 2) == 0) { among_var = 44; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\253", 2) == 0) { among_var = 44; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\254", 2) == 0) { among_var = 44; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\255", 2) == 0) { among_var = 45; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\256", 2) == 0) { among_var = 45; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\257", 2) == 0) { among_var = 46; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\260", 2) == 0) { among_var = 46; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\261", 2) == 0) { among_var = 47; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\262", 2) == 0) { among_var = 47; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\263", 2) == 0) { among_var = 47; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\264", 2) == 0) { among_var = 47; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\265", 2) == 0) { among_var = 51; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\266", 2) == 0) { among_var = 51; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\267", 2) == 0) { among_var = 49; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\270", 2) == 0) { among_var = 49; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\271", 2) == 0) { among_var = 50; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\272", 2) == 0) { among_var = 50; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\273", 2) == 0) { among_var = 48; z->c = c_among + 3; break; }
                                if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\273\274", 2) == 0) { among_var = 48; z->c = c_among + 3; break; }
                                break;
                            case 0xD9:
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\200", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\213", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\214", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\215", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\216", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\217", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\220", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\221", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\222", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\240", 1) == 0) { among_var = 2; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\241", 1) == 0) { among_var = 3; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\242", 1) == 0) { among_var = 4; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243", 1) == 0) { among_var = 5; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\244", 1) == 0) { among_var = 6; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\245", 1) == 0) { among_var = 7; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\246", 1) == 0) { among_var = 8; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\247", 1) == 0) { among_var = 9; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\250", 1) == 0) { among_var = 10; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\251", 1) == 0) { among_var = 11; z->c = c_among + 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab4;
                z->ket = z->c;
                switch (among_var) {
                    case 1:
                        {
                            int ret = snowball_slice_del(z);
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
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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
                        {
                            int c_among = z->c;
                            among_var = 0;
                            if (c_among > z->lb) {
                                switch (z->p[c_among - 1]) {
                                    case 0x88:
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\331\203\331\205\331", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                        break;
                                    case 0xA7:
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\331\203\331\205\330", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\331\207\331\205\330", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\206\330", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\207\330", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        break;
                                    case 0x85:
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\203\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\207\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        break;
                                    case 0x86:
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\203\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\207\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        break;
                                    case 0x8A:
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\206\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                        break;
                                    case 0x83:
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 0x87:
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) goto lab8;
                        z->bra = z->c;
                        switch (among_var) {
                            case 1:
                                if (len_utf8(z->p) < 4) goto lab8;
                                {
                                    int ret = snowball_slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                            case 2:
                                if (len_utf8(z->p) < 5) goto lab8;
                                {
                                    int ret = snowball_slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                            case 3:
                                if (len_utf8(z->p) < 6) goto lab8;
                                {
                                    int ret = snowball_slice_del(z);
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
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0x88:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\330\252\331\205\331", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab10;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            if (len_utf8(z->p) < 4) goto lab10;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            if (len_utf8(z->p) < 6) goto lab10;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                    }
                    break;
                lab10:
                    z->c = z->l - v_10;
                    {
                        int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab7;
                        z->c = ret;
                    }
                } while (0);
                break;
            lab7:
                z->c = z->l - v_7;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0x85:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\330\252\331", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0xA7:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\210\330", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab11;
                z->bra = z->c;
                if (len_utf8(z->p) < 5) goto lab11;
                {
                    int ret = snowball_slice_del(z);
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
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_19, 2) != 0) goto lab14;
                    z->c -= 2;
                    z->bra = z->c;
                    if (len_utf8(z->p) < 4) goto lab14;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                lab14:
                    z->c = z->l - v_12;
                    if (b_is_defined) goto lab15;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 0xA7:
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\331\203\331\205\330", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\331\207\331\205\330", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\206\330", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\207\330", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    break;
                                case 0x85:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\203\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\207\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    break;
                                case 0x86:
                                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\331\207\331", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                                    break;
                                case 0x83:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x87:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                                case 0x8A:
                                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\331", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                    break;
                            }
                        }
                    }
                    if (!among_var) goto lab15;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            if (len_utf8(z->p) < 4) goto lab15;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            if (len_utf8(z->p) < 5) goto lab15;
                            {
                                int ret = snowball_slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 3:
                            if (len_utf8(z->p) < 6) goto lab15;
                            {
                                int ret = snowball_slice_del(z);
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
                            int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                            if (ret < 0) goto lab15;
                            z->c = ret;
                        }
                    } while (0);
                    break;
                lab15:
                    z->c = z->l - v_12;
                    z->ket = z->c;
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_41, 2) != 0) goto lab19;
                    z->c -= 2;
                    z->bra = z->c;
                    if (len_utf8(z->p) < 6) goto lab19;
                    {
                        int ret = snowball_slice_del(z);
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
            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_45, 2) != 0) goto lab12;
            z->c -= 2;
            z->bra = z->c;
            if (len_utf8(z->p) < 3) goto lab12;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab12:
            z->c = z->l - v_6;
            z->ket = z->c;
            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_44, 2) != 0) goto lab5;
            z->c -= 2;
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xD8:
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243\330\242", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243\330\243", 3) == 0) { among_var = 1; z->c = c_among + 4; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243\330\244", 3) == 0) { among_var = 1; z->c = c_among + 4; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243\330\245", 3) == 0) { among_var = 4; z->c = c_among + 4; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243\330\247", 3) == 0) { among_var = 3; z->c = c_among + 4; break; }
                            break;
                    }
                }
            }
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xD9:
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\201", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\210", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) { z->c = v_17; goto lab25; }
            z->ket = z->c;
            if (len_utf8(z->p) < 4) { z->c = v_17; goto lab25; }
            if (z->l - z->c < 2 || __builtin_memcmp(z->p + z->c, s_0, 2) != 0) goto lab26;
            z->c += 2;
            { z->c = v_17; goto lab25; }
        lab26:
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab25:
            ;
        }
        do {
            int v_18 = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xD8:
                            if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\250\330\247\331\204", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\247\331\204", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                            break;
                        case 0xD9:
                            if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\203\330\247\331\204", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\204\331\204", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab27;
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    if (len_utf8(z->p) < 6) goto lab27;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (len_utf8(z->p) < 5) goto lab27;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
            }
            break;
        lab27:
            z->c = v_18;
            if (!b_is_noun) goto lab28;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xD8:
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\250\330\247", 3) == 0) { among_var = -1; z->c = c_among + 4; break; }
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\250\330\250", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                            if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\250", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                            break;
                        case 0xD9:
                            if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\203\331\203", 3) == 0) { among_var = 3; z->c = c_among + 4; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab28;
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    if (len_utf8(z->p) < 4) goto lab28;
                    {
                        int ret = snowball_slice_del(z);
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
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among < z->l) {
                        switch (z->p[c_among]) {
                            case 0xD8:
                                if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\263\330\243", 3) == 0) { among_var = 4; z->c = c_among + 4; break; }
                                if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\263\330\252", 3) == 0) { among_var = 2; z->c = c_among + 4; break; }
                                if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\263\331\206", 3) == 0) { among_var = 3; z->c = c_among + 4; break; }
                                if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\263\331\212", 3) == 0) { among_var = 1; z->c = c_among + 4; break; }
                                break;
                        }
                    }
                }
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
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 0xD8:
                            if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\252\330\263\330\252", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                            break;
                        case 0xD9:
                            if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\206\330\263\330\252", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                            if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\212\330\263\330\252", 5) == 0) { among_var = 1; z->c = c_among + 6; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab23;
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
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xA2:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA3:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA4:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA5:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA6:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\330", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab31;
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
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among < z->l) {
                        switch (z->p[c_among]) {
                            case 0xD8:
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\242", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\243", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\244", 1) == 0) { among_var = 2; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\245", 1) == 0) { among_var = 1; z->c = c_among + 2; break; }
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "\246", 1) == 0) { among_var = 3; z->c = c_among + 2; break; }
                                break;
                        }
                    }
                }
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
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
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

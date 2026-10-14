/* Generated from italian.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_italian_ascii_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

typedef struct SN_env SN_env;

#if defined(__GNUC__) || defined(__clang__)
#define SNOWBALL_UNUSED __attribute__((unused))
#else
#define SNOWBALL_UNUSED
#endif

static inline SNOWBALL_UNUSED int snowball_grouping_contains(const unsigned char * s, int min, int max, int ch) {
    return ch >= min && ch <= max &&
           (s[(ch - min) >> 3] & (1u << ((ch - min) & 7))) != 0;
}

static inline SNOWBALL_UNUSED int snowball_in_grouping_U(SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        if (z->c >= z->l) return -1;
        int ch = z->p[z->c];
        int width = 1;
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
        if (snowball_grouping_contains(s, min, max, ch)) return width;
        z->c -= width;
    } while (repeat);
    return 0;
}

static inline SNOWBALL_UNUSED int snowball_skip_utf8(const symbol * p, int c, int limit, int n) {
    (void)p;
    return n >= 0 && n <= limit - c ? c + n : -1;
}

static inline SNOWBALL_UNUSED int snowball_skip_b_utf8(const symbol * p, int c, int limit, int n) {
    (void)p;
    return n >= 0 && n <= c - limit ? c - n : -1;
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
extern int candidate_italian_ascii_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

#define s_6 (s_5 + 1)
static const symbol s_0[] = { 0xC3, 0xA0 };
static const symbol s_1[] = { 0xC3, 0xA8 };
static const symbol s_2[] = { 0xC3, 0xAC };
static const symbol s_3[] = { 0xC3, 0xB2 };
static const symbol s_4[] = { 0xC3, 0xB9 };
static const symbol s_5[] = { 'q', 'U' };
static const symbol s_7[] = { 'I' };
static const symbol s_8[] = { 'd', 'i', 'v', 'a', 'n' };
static const symbol s_9[] = { 'e', 'n', 't', 'e' };
static const symbol s_10[] = { 'i', 'c' };
static const symbol s_11[] = { 'l', 'o', 'g' };
static const symbol s_12[] = { 'u' };
static const symbol s_13[] = { 'a', 't' };

static const unsigned char g_v[] = { 17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 128, 8, 2, 1 };

static const unsigned char g_AEIO[] = { 17, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 128, 8, 2 };

static const unsigned char g_CG[] = { 17 };

extern int candidate_italian_ascii_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_p2;
    int i_p1;
    int i_pV;
    {
        int v_1 = z->c;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among < z->l) {
                switch (z->p[c_among]) {
                    case 'q':
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "uell'", 5) == 0) { among_var = -1; z->c = c_among + 6; break; }
                        if (c_among + 6 <= z->l && __builtin_memcmp(z->p + c_among + 1, "uest'", 5) == 0) { among_var = -1; z->c = c_among + 6; break; }
                        break;
                    case 'd':
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "all'", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ell'", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "'", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                        break;
                    case 'n':
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ell'", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        break;
                    case 's':
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ull'", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "'", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                        break;
                    case 't':
                        if (c_among + 5 <= z->l && __builtin_memcmp(z->p + c_among + 1, "utt'", 4) == 0) { among_var = -1; z->c = c_among + 5; break; }
                        if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "'", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                        break;
                    case 'a':
                        if (c_among + 4 <= z->l && __builtin_memcmp(z->p + c_among + 1, "ll'", 3) == 0) { among_var = -1; z->c = c_among + 4; break; }
                        break;
                    case 'g':
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "l'", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        break;
                    case 'u':
                        if (c_among + 3 <= z->l && __builtin_memcmp(z->p + c_among + 1, "n'", 2) == 0) { among_var = -1; z->c = c_among + 3; break; }
                        break;
                    case 'l':
                        if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "'", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                        break;
                    case 'm':
                        if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "'", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                        break;
                    case 'v':
                        if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "'", 1) == 0) { among_var = -1; z->c = c_among + 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab0;
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
        {
            int v_3 = z->c;
            while (1) {
                int v_4 = z->c;
                z->bra = z->c;
                {
                    int c_among = z->c;
                    among_var = 7;
                    if (c_among < z->l) {
                        switch (z->p[c_among]) {
                            case 'q':
                                if (c_among + 2 <= z->l && __builtin_memcmp(z->p + c_among + 1, "u", 1) == 0) { among_var = 6; z->c = c_among + 2; break; }
                                break;
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
                            int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                            if (ret < 0) goto lab2;
                            z->c = ret;
                        }
                        break;
                }
                continue;
            lab2:
                z->c = v_4;
                break;
            }
            z->c = v_3;
        }
        while (1) {
            int v_5 = z->c;
            while (1) {
                int v_6 = z->c;
                if (snowball_in_grouping_U(z, g_v, 97, 249, 0)) goto lab4;
                z->bra = z->c;
                do {
                    int v_7 = z->c;
                    if (z->c == z->l || z->p[z->c] != 'u') goto lab5;
                    z->c++;
                    z->ket = z->c;
                    if (snowball_in_grouping_U(z, g_v, 97, 249, 0)) goto lab5;
                    {
                        int ret = slice_from_s(z, 1, s_6);
                        if (ret < 0) return ret;
                    }
                    break;
                lab5:
                    z->c = v_7;
                    if (z->c == z->l || z->p[z->c] != 'i') goto lab4;
                    z->c++;
                    z->ket = z->c;
                    if (snowball_in_grouping_U(z, g_v, 97, 249, 0)) goto lab4;
                    {
                        int ret = slice_from_s(z, 1, s_7);
                        if (ret < 0) return ret;
                    }
                } while (0);
                z->c = v_6;
                break;
            lab4:
                z->c = v_6;
                {
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab3;
                    z->c = ret;
                }
            }
            continue;
        lab3:
            z->c = v_5;
            break;
        }
        z->c = v_2;
    }
    i_pV = z->l;
    i_p1 = z->l;
    i_p2 = z->l;
    {
        int v_8 = z->c;
        do {
            int v_9 = z->c;
            if (snowball_in_grouping_U(z, g_v, 97, 249, 0)) goto lab8;
            do {
                int v_10 = z->c;
                if (snowball_out_grouping_U(z, g_v, 97, 249, 0)) goto lab9;
                {
                    int ret = snowball_out_grouping_U(z, g_v, 97, 249, 1);
                    if (ret < 0) goto lab9;
                    z->c += ret;
                }
                break;
            lab9:
                z->c = v_10;
                if (snowball_in_grouping_U(z, g_v, 97, 249, 0)) goto lab8;
                {
                    int ret = snowball_in_grouping_U(z, g_v, 97, 249, 1);
                    if (ret < 0) goto lab8;
                    z->c += ret;
                }
            } while (0);
            break;
        lab8:
            z->c = v_9;
            if (!(eq_s(z, 5, s_8))) goto lab10;
            break;
        lab10:
            if (snowball_out_grouping_U(z, g_v, 97, 249, 0)) goto lab7;
            do {
                int v_11 = z->c;
                if (snowball_out_grouping_U(z, g_v, 97, 249, 0)) goto lab11;
                {
                    int ret = snowball_out_grouping_U(z, g_v, 97, 249, 1);
                    if (ret < 0) goto lab11;
                    z->c += ret;
                }
                break;
            lab11:
                z->c = v_11;
                if (snowball_in_grouping_U(z, g_v, 97, 249, 0)) goto lab7;
                {
                    int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab7;
                    z->c = ret;
                }
            } while (0);
        } while (0);
        i_pV = z->c;
    lab7:
        z->c = v_8;
    }
    {
        int v_12 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 249, 1);
            if (ret < 0) goto lab12;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 249, 1);
            if (ret < 0) goto lab12;
            z->c += ret;
        }
        i_p1 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 249, 1);
            if (ret < 0) goto lab12;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 249, 1);
            if (ret < 0) goto lab12;
            z->c += ret;
        }
        i_p2 = z->c;
    lab12:
        z->c = v_12;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_13 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'a':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "gliel", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "cel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "tel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "vel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'e':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "gliel", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "glien", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "cel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "tel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "vel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "cen", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "men", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "sen", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ten", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ven", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'i':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "gliel", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "cel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "tel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "vel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "gl", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "c", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "m", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "v", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'o':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "gliel", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "cel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "tel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "vel", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab13;
        z->bra = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'o':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "and", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "end", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                    case 'r':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab13;
        if (i_pV > z->c) goto lab13;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_9);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab13:
        z->c = z->l - v_13;
    }
    {
        int v_14 = z->l - z->c;
        do {
            int v_15 = z->l - z->c;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'e':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "atric", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "usion", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "azion", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "uzion", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ament", 5) == 0) { among_var = 7; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "logi", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "abil", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ibil", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ator", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ment", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ich", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ant", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ist", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "anz", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "enz", 3) == 0) { among_var = 5; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ic", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "os", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 9; z->c = c_among - 3; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "atric", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "usion", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "azion", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "uzion", 5) == 0) { among_var = 4; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ament", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iment", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "abil", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ibil", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ator", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ich", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ism", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ant", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ist", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ic", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "os", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 9; z->c = c_among - 3; break; }
                            break;
                        case 'o':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "ament", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iment", 5) == 0) { among_var = 6; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ism", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ic", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "os", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 9; z->c = c_among - 3; break; }
                            break;
                        case 'a':
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "logi", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ist", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "anz", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "enz", 3) == 0) { among_var = 5; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ic", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "os", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 9; z->c = c_among - 3; break; }
                            break;
                        case 0xA0:
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ist\303", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "it\303", 3) == 0) { among_var = 8; z->c = c_among - 4; break; }
                            break;
                        case 0xA8:
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ist\303", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            break;
                        case 0xAC:
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ist\303", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab15;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    if (i_p2 > z->c) goto lab15;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (i_p2 > z->c) goto lab15;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_16 = z->l - z->c;
                        z->ket = z->c;
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_10, 2) != 0) { z->c = z->l - v_16; goto lab16; }
                        z->c -= 2;
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_16; goto lab16; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab16:
                        ;
                    }
                    break;
                case 3:
                    if (i_p2 > z->c) goto lab15;
                    {
                        int ret = slice_from_s(z, 3, s_11);
                        if (ret < 0) return ret;
                    }
                    break;
                case 4:
                    if (i_p2 > z->c) goto lab15;
                    {
                        int ret = slice_from_s(z, 1, s_12);
                        if (ret < 0) return ret;
                    }
                    break;
                case 5:
                    if (i_p2 > z->c) goto lab15;
                    {
                        int ret = slice_from_s(z, 4, s_9);
                        if (ret < 0) return ret;
                    }
                    break;
                case 6:
                    if (i_pV > z->c) goto lab15;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 7:
                    if (i_p1 > z->c) goto lab15;
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
                                    case 'l':
                                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "abi", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                        break;
                                    case 'c':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
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
                        if (!among_var) { z->c = z->l - v_17; goto lab17; }
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_17; goto lab17; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        switch (among_var) {
                            case 1:
                                z->ket = z->c;
                                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_13, 2) != 0) { z->c = z->l - v_17; goto lab17; }
                                z->c -= 2;
                                z->bra = z->c;
                                if (i_p2 > z->c) { z->c = z->l - v_17; goto lab17; }
                                {
                                    int ret = snowball_slice_del(z);
                                    if (ret < 0) return ret;
                                }
                                break;
                        }
                    lab17:
                        ;
                    }
                    break;
                case 8:
                    if (i_p2 > z->c) goto lab15;
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
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                    case 'v':
                                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                        break;
                                }
                            }
                        }
                        if (!among_var) { z->c = z->l - v_18; goto lab18; }
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_18; goto lab18; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab18:
                        ;
                    }
                    break;
                case 9:
                    if (i_p2 > z->c) goto lab15;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_19 = z->l - z->c;
                        z->ket = z->c;
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_13, 2) != 0) { z->c = z->l - v_19; goto lab19; }
                        z->c -= 2;
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_19; goto lab19; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        z->ket = z->c;
                        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_10, 2) != 0) { z->c = z->l - v_19; goto lab19; }
                        z->c -= 2;
                        z->bra = z->c;
                        if (i_p2 > z->c) { z->c = z->l - v_19; goto lab19; }
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                    lab19:
                        ;
                    }
                    break;
            }
            break;
        lab15:
            z->c = z->l - v_15;
            {
                int v_20;
                if (z->c < i_pV) goto lab14;
                v_20 = z->lb; z->lb = i_pV;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'o':
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "erebber", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "irebber", 7) == 0) { among_var = 1; z->c = c_among - 8; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "assim", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "eremm", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iremm", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iscan", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "erann", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "irann", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "iscon", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "asser", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "esser", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "isser", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "avam", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "evam", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ivam", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "erem", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "irem", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "avan", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "evan", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ivan", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "aron", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "eron", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iron", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "isc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "and", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "end", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "Yam", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "iam", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "amm", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "emm", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imm", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "an", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "on", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "at", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "it", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ut", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "av", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ev", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "erebb", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "irebb", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "erest", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "irest", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "avat", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "evat", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "ivat", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "eret", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "iret", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "isc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "end", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ass", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ar", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "er", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ir", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "at", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "et", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "it", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ut", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 'i':
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "erest", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "irest", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "era", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ira", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "isc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "end", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ere", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ire", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ass", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "at", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "it", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ut", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "av", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ev", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 'a':
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "isc", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "end", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "at", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "it", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ut", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "av", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ev", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "iv", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                            case 0xA0:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "er\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ir\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 0xB2:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "er\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ir\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                                break;
                            case 'r':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_20; goto lab14; }
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                z->lb = v_20;
            }
        } while (0);
    lab14:
        z->c = z->l - v_14;
    }
    {
        int v_21 = z->l - z->c;
        {
            int v_22 = z->l - z->c;
            z->ket = z->c;
            if (snowball_in_grouping_b_U(z, g_AEIO, 97, 242, 0)) { z->c = z->l - v_22; goto lab21; }
            z->bra = z->c;
            if (i_pV > z->c) { z->c = z->l - v_22; goto lab21; }
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            z->ket = z->c;
            if (z->c <= z->lb || z->p[z->c - 1] != 'i') { z->c = z->l - v_22; goto lab21; }
            z->c--;
            z->bra = z->c;
            if (i_pV > z->c) { z->c = z->l - v_22; goto lab21; }
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab21:
            ;
        }
        {
            int v_23 = z->l - z->c;
            z->ket = z->c;
            if (z->c <= z->lb || z->p[z->c - 1] != 'h') { z->c = z->l - v_23; goto lab22; }
            z->c--;
            z->bra = z->c;
            if (snowball_in_grouping_b_U(z, g_CG, 99, 103, 0)) { z->c = z->l - v_23; goto lab22; }
            if (i_pV > z->c) { z->c = z->l - v_23; goto lab22; }
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab22:
            ;
        }
        z->c = z->l - v_21;
    }
    z->c = z->lb;
    {
        int v_24 = z->c;
        while (1) {
            int v_25 = z->c;
            z->bra = z->c;
            {
                int c_among = z->c;
                among_var = 3;
                if (c_among < z->l) {
                    switch (z->p[c_among]) {
                        case 'I':
                            if (c_among + 1 <= z->l) { among_var = 1; z->c = c_among + 1; break; }
                            break;
                        case 'U':
                            if (c_among + 1 <= z->l) { among_var = 2; z->c = c_among + 1; break; }
                            break;
                    }
                }
            }
            z->ket = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_from_s(z, 1, s_10);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    {
                        int ret = slice_from_s(z, 1, s_12);
                        if (ret < 0) return ret;
                    }
                    break;
                case 3:
                    {
                        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                        if (ret < 0) goto lab24;
                        z->c = ret;
                    }
                    break;
            }
            continue;
        lab24:
            z->c = v_25;
            break;
        }
        z->c = v_24;
    }
    return 1;
}

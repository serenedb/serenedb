/* Generated from turkish.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_turkish_ascii_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

typedef struct SN_env SN_env;

struct SN_local {
    struct SN_env z;
    int i_vowel_harmony_result;
    int i_vowel_harmony_cursor;
};

typedef struct SN_local SN_local;

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
extern int candidate_turkish_ascii_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_stem_suffix_chain_before_ki(struct SN_env * z);
static int r_1(struct SN_env * z);
static int r_2(struct SN_env * z);
static int r_mark_ysA(struct SN_env * z);
static int r_mark_ymUs_(struct SN_env * z);
static int r_mark_yDU(struct SN_env * z);
static int r_mark_yUz(struct SN_env * z);
static int r_mark_yUm(struct SN_env * z);
static int r_mark_possessives(struct SN_env * z);
static int r_mark_sUnUz(struct SN_env * z);
static int r_mark_sUn(struct SN_env * z);
static int r_mark_sU(struct SN_env * z);
static int r_mark_nUn(struct SN_env * z);
static int r_mark_ndA(struct SN_env * z);
static int r_mark_lArI(struct SN_env * z);
static int r_mark_lAr(struct SN_env * z);
static int r_mark_DUr(struct SN_env * z);
static int r_mark_DA(struct SN_env * z);
static int r_check_vowel_harmony(struct SN_env * z);

#define s_5 (s_0 + 1)
static const symbol s_0[] = { 'k', 'i' };
static const symbol s_1[] = { 'k', 'e', 'n' };
static const symbol s_2[] = { 'a', 'd' };
static const symbol s_3[] = { 's', 'o', 'y' };
static const symbol s_4[] = { 0xC4, 0xB1 };
static const symbol s_6[] = { 'u' };
static const symbol s_7[] = { 0xC3, 0xB6 };
static const symbol s_8[] = { 0xC3, 0xBC };
static const symbol s_9[] = { 'p' };
static const symbol s_10[] = { 0xC3, 0xA7 };
static const symbol s_11[] = { 't' };

static const unsigned char g_vowel[] = { 17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 8, 0, 0, 0, 0, 0, 0, 1 };

static const unsigned char g_U[] = { 1, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 1 };

static const unsigned char g_vowel1[] = { 1, 64, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };

static const unsigned char g_vowel2[] = { 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 130 };

static const unsigned char g_vowel3[] = { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };

static const unsigned char g_vowel4[] = { 17 };

static const unsigned char g_vowel5[] = { 65 };

static const unsigned char g_vowel6[] = { 65 };

static int r_check_vowel_harmony(struct SN_env * z) {
    int among_var;
    do {
        if (((SN_local *)z)->i_vowel_harmony_cursor != z->c) goto lab0;
        break;
    lab0:
        ((SN_local *)z)->i_vowel_harmony_cursor = z->c;
        ((SN_local *)z)->i_vowel_harmony_result = 0;
        {
            int v_1 = z->l - z->c;
            {
                int v_2 = z->l - z->c;
                if (snowball_out_grouping_b_U(z, g_vowel, 97, 305, 1) < 0) goto lab1;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0xB1:
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\304", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                                break;
                            case 0xB6:
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                                break;
                            case 0xBC:
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                                break;
                            case 'a':
                                if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 1) { among_var = 2; z->c = c_among - 1; break; }
                                break;
                            case 'i':
                                if (c_among - z->lb >= 1) { among_var = 4; z->c = c_among - 1; break; }
                                break;
                            case 'o':
                                if (c_among - z->lb >= 1) { among_var = 5; z->c = c_among - 1; break; }
                                break;
                            case 'u':
                                if (c_among - z->lb >= 1) { among_var = 5; z->c = c_among - 1; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab1;
                switch (among_var) {
                    case 1:
                        if (snowball_out_grouping_b_U(z, g_vowel1, 97, 305, 1) < 0) goto lab1;
                        break;
                    case 2:
                        if (snowball_out_grouping_b_U(z, g_vowel2, 101, 252, 1) < 0) goto lab1;
                        break;
                    case 3:
                        if (snowball_out_grouping_b_U(z, g_vowel3, 97, 305, 1) < 0) goto lab1;
                        break;
                    case 4:
                        if (snowball_out_grouping_b_U(z, g_vowel4, 101, 105, 1) < 0) goto lab1;
                        break;
                    case 5:
                        if (snowball_out_grouping_b_U(z, g_vowel5, 111, 117, 1) < 0) goto lab1;
                        break;
                    case 6:
                        if (snowball_out_grouping_b_U(z, g_vowel6, 246, 252, 1) < 0) goto lab1;
                        break;
                }
                ((SN_local *)z)->i_vowel_harmony_result = 1;
                z->c = z->l - v_2;
            }
        lab1:
            z->c = z->l - v_1;
        }
    } while (0);
    return ((SN_local *)z)->i_vowel_harmony_result == 1;
}

static int r_1(struct SN_env * z) {
    do {
        int v_1 = z->l - z->c;
        if (z->c <= z->lb || z->p[z->c - 1] != 'n') goto lab0;
        z->c--;
        {
            int v_2 = z->l - z->c;
            if (snowball_in_grouping_b_U(z, g_vowel, 97, 305, 0)) goto lab0;
            z->c = z->l - v_2;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        if (z->c <= z->lb || z->p[z->c - 1] != 'n') goto lab1;
        z->c--;
        return 0;
    lab1:
        {
            int v_3 = z->l - z->c;
            {
                int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                if (ret < 0) return 0;
                z->c = ret;
            }
            if (snowball_in_grouping_b_U(z, g_vowel, 97, 305, 0)) return 0;
            z->c = z->l - v_3;
        }
    } while (0);
    return 1;
}

static int r_2(struct SN_env * z) {
    do {
        int v_1 = z->l - z->c;
        if (z->c <= z->lb || z->p[z->c - 1] != 'y') goto lab0;
        z->c--;
        {
            int v_2 = z->l - z->c;
            if (snowball_in_grouping_b_U(z, g_vowel, 97, 305, 0)) goto lab0;
            z->c = z->l - v_2;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        if (z->c <= z->lb || z->p[z->c - 1] != 'y') goto lab1;
        z->c--;
        return 0;
    lab1:
        {
            int v_3 = z->l - z->c;
            {
                int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                if (ret < 0) return 0;
                z->c = ret;
            }
            if (snowball_in_grouping_b_U(z, g_vowel, 97, 305, 0)) return 0;
            z->c = z->l - v_3;
        }
    } while (0);
    return 1;
}

static int r_mark_possessives(struct SN_env * z) {
    int among_var;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'z':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "m\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "n\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "m\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "n\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mi", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ni", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "nu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'm':
                    if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                    break;
                case 'n':
                    if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    do {
        int v_1 = z->l - z->c;
        if (snowball_in_grouping_b_U(z, g_U, 105, 305, 0)) goto lab0;
        {
            int v_2 = z->l - z->c;
            if (snowball_out_grouping_b_U(z, g_vowel, 97, 305, 0)) goto lab0;
            z->c = z->l - v_2;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        if (snowball_in_grouping_b_U(z, g_U, 105, 305, 0)) goto lab1;
        return 0;
    lab1:
        {
            int v_3 = z->l - z->c;
            {
                int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                if (ret < 0) return 0;
                z->c = ret;
            }
            if (snowball_out_grouping_b_U(z, g_vowel, 97, 305, 0)) return 0;
            z->c = z->l - v_3;
        }
    } while (0);
    return 1;
}

static int r_mark_sU(struct SN_env * z) {
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    if (snowball_in_grouping_b_U(z, g_U, 105, 305, 0)) return 0;
    do {
        int v_1 = z->l - z->c;
        if (z->c <= z->lb || z->p[z->c - 1] != 's') goto lab0;
        z->c--;
        {
            int v_2 = z->l - z->c;
            if (snowball_in_grouping_b_U(z, g_vowel, 97, 305, 0)) goto lab0;
            z->c = z->l - v_2;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        if (z->c <= z->lb || z->p[z->c - 1] != 's') goto lab1;
        z->c--;
        return 0;
    lab1:
        {
            int v_3 = z->l - z->c;
            {
                int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                if (ret < 0) return 0;
                z->c = ret;
            }
            if (snowball_in_grouping_b_U(z, g_vowel, 97, 305, 0)) return 0;
            z->c = z->l - v_3;
        }
    } while (0);
    return 1;
}

static int r_mark_lArI(struct SN_env * z) {
    int among_var;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 0xB1:
                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "lar\304", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                    break;
                case 'i':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ler", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_nUn(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'n':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\304\261", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\274", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return r_1(z);
}

static int r_mark_DA(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'a':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "d", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'e':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "d", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_ndA(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'a':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "nd", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'e':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "nd", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_yUm(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'm':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\304\261", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\274", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return r_2(z);
}

static int r_mark_sUn(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'n':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "s\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "s\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "si", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "su", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_yUz(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'z':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\304\261", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\274", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return r_2(z);
}

static int r_mark_sUnUz(struct SN_env * z) {
    int among_var;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'z':
                    if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "s\304\261n\304\261", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                    if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "s\303\274n\303\274", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "sini", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "sunu", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_lAr(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'r':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "la", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "le", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_DUr(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'r':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "di", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ti", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "du", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "tu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_mark_yDU(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'k':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "di", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ti", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "du", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "tu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'm':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "di", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ti", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "du", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "tu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'n':
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "d\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "di", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ti", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "du", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "tu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 0xB1:
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "d\304", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "t\304", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 0xBC:
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "d\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "t\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'i':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "d", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'u':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "d", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return r_2(z);
}

static int r_mark_ysA(struct SN_env * z) {
    int among_var;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'k':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "sa", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "se", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'm':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "sa", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "se", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'n':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "sa", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "se", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    break;
                case 'a':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'e':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return r_2(z);
}

static int r_mark_ymUs_(struct SN_env * z) {
    int among_var;
    {
        int ret = r_check_vowel_harmony(z);
        if (ret == 0) return ret;
    }
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 0x9F:
                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "m\304\261\305", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                    if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "m\303\274\305", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mi\305", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mu\305", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return r_2(z);
}

static int r_stem_suffix_chain_before_ki(struct SN_env * z) {
    z->ket = z->c;
    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_0, 2) != 0) return 0;
    z->c -= 2;
    do {
        int v_1 = z->l - z->c;
        if (!r_mark_DA(z)) goto lab0;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        {
            int v_2 = z->l - z->c;
            z->ket = z->c;
            do {
                int v_3 = z->l - z->c;
                if (!r_mark_lAr(z)) goto lab2;
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_4 = z->l - z->c;
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_4; goto lab3; }
                        if (ret < 0) return ret;
                    }
                lab3:
                    ;
                }
                break;
            lab2:
                z->c = z->l - v_3;
                if (!r_mark_possessives(z)) { z->c = z->l - v_2; goto lab1; }
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_5 = z->l - z->c;
                    z->ket = z->c;
                    if (!r_mark_lAr(z)) { z->c = z->l - v_5; goto lab4; }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_5; goto lab4; }
                        if (ret < 0) return ret;
                    }
                lab4:
                    ;
                }
            } while (0);
        lab1:
            ;
        }
        break;
    lab0:
        z->c = z->l - v_1;
        if (!r_mark_nUn(z)) goto lab5;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        {
            int v_6 = z->l - z->c;
            z->ket = z->c;
            do {
                int v_7 = z->l - z->c;
                if (!r_mark_lArI(z)) goto lab7;
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            lab7:
                z->c = z->l - v_7;
                z->ket = z->c;
                do {
                    int v_8 = z->l - z->c;
                    if (!r_mark_possessives(z)) goto lab9;
                    break;
                lab9:
                    z->c = z->l - v_8;
                    if (!r_mark_sU(z)) goto lab8;
                } while (0);
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_9 = z->l - z->c;
                    z->ket = z->c;
                    if (!r_mark_lAr(z)) { z->c = z->l - v_9; goto lab10; }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_9; goto lab10; }
                        if (ret < 0) return ret;
                    }
                lab10:
                    ;
                }
                break;
            lab8:
                z->c = z->l - v_7;
                {
                    int ret = r_stem_suffix_chain_before_ki(z);
                    if (ret == 0) { z->c = z->l - v_6; goto lab6; }
                    if (ret < 0) return ret;
                }
            } while (0);
        lab6:
            ;
        }
        break;
    lab5:
        z->c = z->l - v_1;
        {
            int ret = r_mark_ndA(z);
            if (ret == 0) return ret;
        }
        do {
            int v_10 = z->l - z->c;
            if (!r_mark_lArI(z)) goto lab11;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab11:
            z->c = z->l - v_10;
            if (!r_mark_sU(z)) goto lab12;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_11 = z->l - z->c;
                z->ket = z->c;
                if (!r_mark_lAr(z)) { z->c = z->l - v_11; goto lab13; }
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int ret = r_stem_suffix_chain_before_ki(z);
                    if (ret == 0) { z->c = z->l - v_11; goto lab13; }
                    if (ret < 0) return ret;
                }
            lab13:
                ;
            }
            break;
        lab12:
            z->c = z->l - v_10;
            {
                int ret = r_stem_suffix_chain_before_ki(z);
                if (ret <= 0) return ret;
            }
        } while (0);
    } while (0);
    return 1;
}

extern int candidate_turkish_ascii_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int b_0;
    ((SN_local *)z)->i_vowel_harmony_cursor = -1;
    ((SN_local *)z)->i_vowel_harmony_result = 0;
    {
        int v_1 = z->c;
        z->bra = z->c;
        while (1) {
            int v_2 = z->c;
            if (z->c == z->l || z->p[z->c] != '\'') goto lab3;
            z->c++;
            goto lab2;
        lab3:
            z->c = v_2;
            break;
        lab2:
            z->c = v_2;
            {
                int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                if (ret < 0) goto lab1;
                z->c = ret;
            }
        }
        z->ket = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab1:
        z->c = v_1;
    }
    {
        int v_3 = z->c;
        {
            int ret = snowball_skip_utf8(z->p, z->c, z->l, 2);
            if (ret < 0) goto lab4;
            z->c = ret;
        }
        while (1) {
            int v_4 = z->c;
            if (z->c == z->l || z->p[z->c] != '\'') goto lab5;
            z->c++;
            z->c = v_4;
            break;
        lab5:
            z->c = v_4;
            {
                int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
                if (ret < 0) goto lab4;
                z->c = ret;
            }
        }
        z->bra = z->c;
        z->c = z->l;
        z->ket = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab4:
        z->c = v_3;
    }
    {
        int v_5 = z->c;
        {
            int i; for (i = 2; i > 0; i--) {
                {
                    int ret = snowball_out_grouping_U(z, g_vowel, 97, 305, 1);
                    if (ret < 0) return 0;
                    z->c += ret;
                }
            }
        }
        z->c = v_5;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_6 = z->l - z->c;
        z->ket = z->c;
        b_0 = 1;
        do {
            int v_7 = z->l - z->c;
            do {
                int v_8 = z->l - z->c;
                if (!r_mark_ymUs_(z)) goto lab8;
                break;
            lab8:
                z->c = z->l - v_8;
                if (!r_mark_yDU(z)) goto lab9;
                break;
            lab9:
                z->c = z->l - v_8;
                if (!r_mark_ysA(z)) goto lab10;
                break;
            lab10:
                z->c = z->l - v_8;
                if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_1, 3) != 0) goto lab7;
                z->c -= 3;
                if (!r_2(z)) goto lab7;
            } while (0);
            break;
        lab7:
            z->c = z->l - v_7;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'a':
                            if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "cas\304\261n", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                            break;
                        case 'e':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "cesin", 5) == 0) { among_var = -1; z->c = c_among - 6; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab11;
            do {
                int v_9 = z->l - z->c;
                if (!r_mark_sUnUz(z)) goto lab12;
                break;
            lab12:
                z->c = z->l - v_9;
                if (!r_mark_lAr(z)) goto lab13;
                break;
            lab13:
                z->c = z->l - v_9;
                if (!r_mark_yUm(z)) goto lab14;
                break;
            lab14:
                z->c = z->l - v_9;
                if (!r_mark_sUn(z)) goto lab15;
                break;
            lab15:
                z->c = z->l - v_9;
                if (!r_mark_yUz(z)) goto lab16;
                break;
            lab16:
                z->c = z->l - v_9;
            } while (0);
            if (!r_mark_ymUs_(z)) goto lab11;
            break;
        lab11:
            z->c = z->l - v_7;
            if (!r_mark_lAr(z)) goto lab17;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_10 = z->l - z->c;
                z->ket = z->c;
                do {
                    int v_11 = z->l - z->c;
                    if (!r_mark_DUr(z)) goto lab19;
                    break;
                lab19:
                    z->c = z->l - v_11;
                    if (!r_mark_yDU(z)) goto lab20;
                    break;
                lab20:
                    z->c = z->l - v_11;
                    if (!r_mark_ysA(z)) goto lab21;
                    break;
                lab21:
                    z->c = z->l - v_11;
                    if (!r_mark_ymUs_(z)) { z->c = z->l - v_10; goto lab18; }
                } while (0);
            lab18:
                ;
            }
            b_0 = 0;
            break;
        lab17:
            z->c = z->l - v_7;
            if (!r_check_vowel_harmony(z)) goto lab22;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'z':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "n\304\261", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "n\303\274", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ni", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "nu", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab22;
            do {
                int v_12 = z->l - z->c;
                if (!r_mark_yDU(z)) goto lab23;
                break;
            lab23:
                z->c = z->l - v_12;
                if (!r_mark_ysA(z)) goto lab22;
            } while (0);
            break;
        lab22:
            z->c = z->l - v_7;
            do {
                int v_13 = z->l - z->c;
                if (!r_mark_sUnUz(z)) goto lab25;
                break;
            lab25:
                z->c = z->l - v_13;
                if (!r_mark_yUz(z)) goto lab26;
                break;
            lab26:
                z->c = z->l - v_13;
                if (!r_mark_sUn(z)) goto lab27;
                break;
            lab27:
                z->c = z->l - v_13;
                if (!r_mark_yUm(z)) goto lab24;
            } while (0);
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_14 = z->l - z->c;
                z->ket = z->c;
                if (!r_mark_ymUs_(z)) { z->c = z->l - v_14; goto lab28; }
            lab28:
                ;
            }
            break;
        lab24:
            z->c = z->l - v_7;
            if (!r_mark_DUr(z)) goto lab6;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_15 = z->l - z->c;
                z->ket = z->c;
                do {
                    int v_16 = z->l - z->c;
                    if (!r_mark_sUnUz(z)) goto lab30;
                    break;
                lab30:
                    z->c = z->l - v_16;
                    if (!r_mark_lAr(z)) goto lab31;
                    break;
                lab31:
                    z->c = z->l - v_16;
                    if (!r_mark_yUm(z)) goto lab32;
                    break;
                lab32:
                    z->c = z->l - v_16;
                    if (!r_mark_sUn(z)) goto lab33;
                    break;
                lab33:
                    z->c = z->l - v_16;
                    if (!r_mark_yUz(z)) goto lab34;
                    break;
                lab34:
                    z->c = z->l - v_16;
                } while (0);
                if (!r_mark_ymUs_(z)) { z->c = z->l - v_15; goto lab29; }
            lab29:
                ;
            }
        } while (0);
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab6:
        z->c = z->l - v_6;
    }
    if (!b_0) return 0;
    {
        int v_17 = z->l - z->c;
        do {
            int v_18 = z->l - z->c;
            z->ket = z->c;
            if (!r_mark_lAr(z)) goto lab36;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_19 = z->l - z->c;
                {
                    int ret = r_stem_suffix_chain_before_ki(z);
                    if (ret == 0) { z->c = z->l - v_19; goto lab37; }
                    if (ret < 0) return ret;
                }
            lab37:
                ;
            }
            break;
        lab36:
            z->c = z->l - v_18;
            z->ket = z->c;
            if (!r_check_vowel_harmony(z)) goto lab38;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'a':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "c", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                            break;
                        case 'e':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "c", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab38;
            if (!r_1(z)) goto lab38;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_20 = z->l - z->c;
                do {
                    int v_21 = z->l - z->c;
                    z->ket = z->c;
                    if (!r_mark_lArI(z)) goto lab40;
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                lab40:
                    z->c = z->l - v_21;
                    z->ket = z->c;
                    do {
                        int v_22 = z->l - z->c;
                        if (!r_mark_possessives(z)) goto lab42;
                        break;
                    lab42:
                        z->c = z->l - v_22;
                        if (!r_mark_sU(z)) goto lab41;
                    } while (0);
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_23 = z->l - z->c;
                        z->ket = z->c;
                        if (!r_mark_lAr(z)) { z->c = z->l - v_23; goto lab43; }
                        z->bra = z->c;
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        {
                            int ret = r_stem_suffix_chain_before_ki(z);
                            if (ret == 0) { z->c = z->l - v_23; goto lab43; }
                            if (ret < 0) return ret;
                        }
                    lab43:
                        ;
                    }
                    break;
                lab41:
                    z->c = z->l - v_21;
                    z->ket = z->c;
                    if (!r_mark_lAr(z)) { z->c = z->l - v_20; goto lab39; }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_20; goto lab39; }
                        if (ret < 0) return ret;
                    }
                } while (0);
            lab39:
                ;
            }
            break;
        lab38:
            z->c = z->l - v_18;
            z->ket = z->c;
            do {
                int v_24 = z->l - z->c;
                if (!r_mark_ndA(z)) goto lab45;
                break;
            lab45:
                z->c = z->l - v_24;
                if (!r_check_vowel_harmony(z)) goto lab44;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'a':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab44;
            } while (0);
            do {
                int v_25 = z->l - z->c;
                if (!r_mark_lArI(z)) goto lab46;
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            lab46:
                z->c = z->l - v_25;
                if (!r_mark_sU(z)) goto lab47;
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_26 = z->l - z->c;
                    z->ket = z->c;
                    if (!r_mark_lAr(z)) { z->c = z->l - v_26; goto lab48; }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_26; goto lab48; }
                        if (ret < 0) return ret;
                    }
                lab48:
                    ;
                }
                break;
            lab47:
                z->c = z->l - v_25;
                {
                    int ret = r_stem_suffix_chain_before_ki(z);
                    if (ret == 0) goto lab44;
                    if (ret < 0) return ret;
                }
            } while (0);
            break;
        lab44:
            z->c = z->l - v_18;
            z->ket = z->c;
            do {
                int v_27 = z->l - z->c;
                if (!r_check_vowel_harmony(z)) goto lab50;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'n':
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "nda", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "nde", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab50;
                break;
            lab50:
                z->c = z->l - v_27;
                if (!r_check_vowel_harmony(z)) goto lab49;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0xB1:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "n\304", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 0xBC:
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "n\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                            case 'i':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                            case 'u':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab49;
            } while (0);
            do {
                int v_28 = z->l - z->c;
                if (!r_mark_sU(z)) goto lab51;
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_29 = z->l - z->c;
                    z->ket = z->c;
                    if (!r_mark_lAr(z)) { z->c = z->l - v_29; goto lab52; }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_29; goto lab52; }
                        if (ret < 0) return ret;
                    }
                lab52:
                    ;
                }
                break;
            lab51:
                z->c = z->l - v_28;
                if (!r_mark_lArI(z)) goto lab49;
            } while (0);
            break;
        lab49:
            z->c = z->l - v_18;
            z->ket = z->c;
            if (!r_check_vowel_harmony(z)) goto lab53;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'n':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "da", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ta", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "de", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "te", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
            if (!among_var) goto lab53;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_30 = z->l - z->c;
                z->ket = z->c;
                do {
                    int v_31 = z->l - z->c;
                    if (!r_mark_possessives(z)) goto lab55;
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_32 = z->l - z->c;
                        z->ket = z->c;
                        if (!r_mark_lAr(z)) { z->c = z->l - v_32; goto lab56; }
                        z->bra = z->c;
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        {
                            int ret = r_stem_suffix_chain_before_ki(z);
                            if (ret == 0) { z->c = z->l - v_32; goto lab56; }
                            if (ret < 0) return ret;
                        }
                    lab56:
                        ;
                    }
                    break;
                lab55:
                    z->c = z->l - v_31;
                    if (!r_mark_lAr(z)) goto lab57;
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_33 = z->l - z->c;
                        {
                            int ret = r_stem_suffix_chain_before_ki(z);
                            if (ret == 0) { z->c = z->l - v_33; goto lab58; }
                            if (ret < 0) return ret;
                        }
                    lab58:
                        ;
                    }
                    break;
                lab57:
                    z->c = z->l - v_31;
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_30; goto lab54; }
                        if (ret < 0) return ret;
                    }
                } while (0);
            lab54:
                ;
            }
            break;
        lab53:
            z->c = z->l - v_18;
            z->ket = z->c;
            do {
                int v_34 = z->l - z->c;
                if (!r_mark_nUn(z)) goto lab60;
                break;
            lab60:
                z->c = z->l - v_34;
                if (!r_check_vowel_harmony(z)) goto lab59;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'a':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab59;
                if (!r_2(z)) goto lab59;
            } while (0);
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_35 = z->l - z->c;
                do {
                    int v_36 = z->l - z->c;
                    z->ket = z->c;
                    if (!r_mark_lAr(z)) goto lab62;
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) goto lab62;
                        if (ret < 0) return ret;
                    }
                    break;
                lab62:
                    z->c = z->l - v_36;
                    z->ket = z->c;
                    do {
                        int v_37 = z->l - z->c;
                        if (!r_mark_possessives(z)) goto lab64;
                        break;
                    lab64:
                        z->c = z->l - v_37;
                        if (!r_mark_sU(z)) goto lab63;
                    } while (0);
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_38 = z->l - z->c;
                        z->ket = z->c;
                        if (!r_mark_lAr(z)) { z->c = z->l - v_38; goto lab65; }
                        z->bra = z->c;
                        {
                            int ret = snowball_slice_del(z);
                            if (ret < 0) return ret;
                        }
                        {
                            int ret = r_stem_suffix_chain_before_ki(z);
                            if (ret == 0) { z->c = z->l - v_38; goto lab65; }
                            if (ret < 0) return ret;
                        }
                    lab65:
                        ;
                    }
                    break;
                lab63:
                    z->c = z->l - v_36;
                    {
                        int ret = r_stem_suffix_chain_before_ki(z);
                        if (ret == 0) { z->c = z->l - v_35; goto lab61; }
                        if (ret < 0) return ret;
                    }
                } while (0);
            lab61:
                ;
            }
            break;
        lab59:
            z->c = z->l - v_18;
            z->ket = z->c;
            if (!r_mark_lArI(z)) goto lab66;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab66:
            z->c = z->l - v_18;
            {
                int ret = r_stem_suffix_chain_before_ki(z);
                if (ret == 0) goto lab67;
                if (ret < 0) return ret;
            }
            break;
        lab67:
            z->c = z->l - v_18;
            z->ket = z->c;
            do {
                int v_39 = z->l - z->c;
                if (!r_mark_DA(z)) goto lab69;
                break;
            lab69:
                z->c = z->l - v_39;
                if (!r_check_vowel_harmony(z)) goto lab70;
                if (snowball_in_grouping_b_U(z, g_U, 105, 305, 0)) goto lab70;
                if (!r_2(z)) goto lab70;
                break;
            lab70:
                z->c = z->l - v_39;
                if (!r_check_vowel_harmony(z)) goto lab68;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'a':
                                if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                                break;
                            case 'e':
                                if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab68;
                if (!r_2(z)) goto lab68;
            } while (0);
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_40 = z->l - z->c;
                z->ket = z->c;
                do {
                    int v_41 = z->l - z->c;
                    if (!r_mark_possessives(z)) goto lab72;
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_42 = z->l - z->c;
                        z->ket = z->c;
                        if (!r_mark_lAr(z)) { z->c = z->l - v_42; goto lab73; }
                    lab73:
                        ;
                    }
                    break;
                lab72:
                    z->c = z->l - v_41;
                    if (!r_mark_lAr(z)) { z->c = z->l - v_40; goto lab71; }
                } while (0);
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                z->ket = z->c;
                {
                    int ret = r_stem_suffix_chain_before_ki(z);
                    if (ret == 0) { z->c = z->l - v_40; goto lab71; }
                    if (ret < 0) return ret;
                }
            lab71:
                ;
            }
            break;
        lab68:
            z->c = z->l - v_18;
            z->ket = z->c;
            do {
                int v_43 = z->l - z->c;
                if (!r_mark_possessives(z)) goto lab74;
                break;
            lab74:
                z->c = z->l - v_43;
                if (!r_mark_sU(z)) goto lab35;
            } while (0);
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
            {
                int v_44 = z->l - z->c;
                z->ket = z->c;
                if (!r_mark_lAr(z)) { z->c = z->l - v_44; goto lab75; }
                z->bra = z->c;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int ret = r_stem_suffix_chain_before_ki(z);
                    if (ret == 0) { z->c = z->l - v_44; goto lab75; }
                    if (ret < 0) return ret;
                }
            lab75:
                ;
            }
        } while (0);
    lab35:
        z->c = z->l - v_17;
    }
    z->c = z->lb;
    z->lb = z->c; z->c = z->l;
    {
        int v_45 = z->l - z->c;
        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_2, 2) != 0) goto lab76;
        z->c -= 2;
        {
            int v_46 = z->l - z->c;
            if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_3, 3) != 0) { z->c = z->l - v_46; goto lab77; }
            z->c -= 3;
        lab77:
            ;
        }
        if (z->c > z->lb) goto lab76;
        return 0;
    lab76:
        z->c = z->l - v_45;
    }
    {
        int v_47 = z->l - z->c;
        z->ket = z->c;
        z->bra = z->c;
        do {
            if (z->c <= z->lb || z->p[z->c - 1] != 'd') goto lab79;
            z->c--;
            break;
        lab79:
            if (z->c <= z->lb || z->p[z->c - 1] != 'g') goto lab78;
            z->c--;
        } while (0);
        if (snowball_out_grouping_b_U(z, g_vowel, 97, 305, 1) < 0) goto lab78;
        do {
            int v_48 = z->l - z->c;
            do {
                if (z->c <= z->lb || z->p[z->c - 1] != 'a') goto lab81;
                z->c--;
                break;
            lab81:
                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_4, 2) != 0) goto lab80;
                z->c -= 2;
            } while (0);
            {
                int ret = slice_from_s(z, 2, s_4);
                if (ret < 0) return ret;
            }
            break;
        lab80:
            z->c = z->l - v_48;
            do {
                if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab83;
                z->c--;
                break;
            lab83:
                if (z->c <= z->lb || z->p[z->c - 1] != 'i') goto lab82;
                z->c--;
            } while (0);
            {
                int ret = slice_from_s(z, 1, s_5);
                if (ret < 0) return ret;
            }
            break;
        lab82:
            z->c = z->l - v_48;
            do {
                if (z->c <= z->lb || z->p[z->c - 1] != 'o') goto lab85;
                z->c--;
                break;
            lab85:
                if (z->c <= z->lb || z->p[z->c - 1] != 'u') goto lab84;
                z->c--;
            } while (0);
            {
                int ret = slice_from_s(z, 1, s_6);
                if (ret < 0) return ret;
            }
            break;
        lab84:
            z->c = z->l - v_48;
            do {
                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_7, 2) != 0) goto lab86;
                z->c -= 2;
                break;
            lab86:
                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_8, 2) != 0) goto lab78;
                z->c -= 2;
            } while (0);
            {
                int ret = slice_from_s(z, 2, s_8);
                if (ret < 0) return ret;
            }
        } while (0);
    lab78:
        z->c = z->l - v_47;
    }
    {
        int v_49 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x9F:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\304", 1) == 0) { among_var = 4; z->c = c_among - 2; break; }
                        break;
                    case 'b':
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                    case 'c':
                        if (c_among - z->lb >= 1) { among_var = 2; z->c = c_among - 1; break; }
                        break;
                    case 'd':
                        if (c_among - z->lb >= 1) { among_var = 3; z->c = c_among - 1; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab87;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 1, s_9);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 2, s_10);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 1, s_11);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab87:
        z->c = z->l - v_49;
    }
    z->c = z->lb;
    return 1;
}

extern struct SN_env * candidate_turkish_ascii_UTF_8_create_env(void) {
    return SN_new_env(sizeof(SN_local));
}

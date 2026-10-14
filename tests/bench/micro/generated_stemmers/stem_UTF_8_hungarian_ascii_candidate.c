/* Generated from hungarian.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_hungarian_ascii_candidate.h"

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
extern int candidate_hungarian_ascii_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_double(struct SN_env * z);
static int r_undouble(struct SN_env * z);

static const symbol s_0[] = { 'a' };
static const symbol s_1[] = { 'e' };

static const unsigned char g_v[] = { 17, 65, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 17, 36, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1 };

static int r_double(struct SN_env * z) {
    int among_var;
    {
        int v_1 = z->l - z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 's':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "cc", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "zz", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'y':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "gg", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ll", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "nn", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "tt", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 'z':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ss", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "z", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'b':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "b", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'c':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "c", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'd':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "d", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'f':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "f", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'g':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "g", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'j':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "j", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'k':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "k", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'l':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "l", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'm':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "m", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'n':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'p':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "p", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'r':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "r", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 't':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'v':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "v", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) return 0;
        z->c = z->l - v_1;
    }
    return 1;
}

static int r_undouble(struct SN_env * z) {
    {
        int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
        if (ret < 0) return 0;
        z->c = ret;
    }
    z->ket = z->c;
    {
        int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
        if (ret < 0) return 0;
        z->c = ret;
    }
    z->bra = z->c;
    {
        int ret = snowball_slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

extern int candidate_hungarian_ascii_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_p1;
    {
        int v_1 = z->c;
        i_p1 = z->l;
        do {
            int v_2 = z->c;
            if (snowball_in_grouping_U(z, g_v, 97, 369, 0)) goto lab1;
            {
                int v_3 = z->c;
                {
                    int ret = snowball_in_grouping_U(z, g_v, 97, 369, 1);
                    if (ret < 0) goto lab2;
                    z->c += ret;
                }
                i_p1 = z->c;
            lab2:
                z->c = v_3;
            }
            break;
        lab1:
            z->c = v_2;
            {
                int ret = snowball_out_grouping_U(z, g_v, 97, 369, 1);
                if (ret < 0) goto lab0;
                z->c += ret;
            }
            i_p1 = z->c;
        } while (0);
    lab0:
        z->c = v_1;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_4 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'l':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab3;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab3;
        if (!r_double(z)) goto lab3;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        {
            int ret = r_undouble(z);
            if (ret == 0) goto lab3;
            if (ret < 0) return ret;
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
                    case 'n':
                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "k\303\251ppe", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ba", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "be", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\266", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                        break;
                    case 't':
                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "ank\303\251n", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "enk\303\251n", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "onk\303\251n", 6) == 0) { among_var = -1; z->c = c_among - 7; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "k\303\251n", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251r", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\266", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                        break;
                    case 'p':
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "k\303\251p", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                        break;
                    case 'l':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "b\305\221", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "r\305\221", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\305\221", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "n\303\241", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "n\303\251", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "b\303\263", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "r\303\263", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "t\303\263", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "va", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ve", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\274", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'z':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "h\303\266", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "he", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ho", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 'k':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "na", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ne", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 'r':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ko", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 0xA1:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "v\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "v\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                        break;
                    case 'a':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "b", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "r", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'e':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "b", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "r", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                    case 'g':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab4;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab4;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xA1:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab4;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab4;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_1);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab4:
        z->c = z->l - v_5;
    }
    {
        int v_6 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 't':
                        if (c_among - z->lb >= 8 && __builtin_memcmp(z->p + c_among - 8, "\303\241nk\303\251n", 7) == 0) { among_var = 2; z->c = c_among - 8; break; }
                        break;
                    case 'n':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\241", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab5;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab5;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 1, s_1);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab5:
        z->c = z->l - v_6;
    }
    {
        int v_7 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'l':
                        if (c_among - z->lb >= 7 && __builtin_memcmp(z->p + c_among - 7, "\303\251st\303\274", 6) == 0) { among_var = 3; z->c = c_among - 7; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\303\241stu", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "est\303\274", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "astu", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "st\303\274", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "stu", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab6;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab6;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 1, s_1);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab6:
        z->c = z->l - v_7;
    }
    {
        int v_8 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0xA1:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab7;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab7;
        if (!r_double(z)) goto lab7;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        {
            int ret = r_undouble(z);
            if (ret == 0) goto lab7;
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
                    case 'i':
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\241\303\251", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\251\303\251", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\241k\303", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\251k\303", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\266k\303", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ak\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ek\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ok\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251\303", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "k\303", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab8;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab8;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_1);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 1, s_0);
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
                    case 'k':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\303\251j\303\274", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\241ju", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\241n", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251n", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\274n", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "j\303\274", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "un", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ju", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\274", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'd':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\241", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\266", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                    case 'm':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\241", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                    case 'a':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "j", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                    case 'e':
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "j", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                    case 0xA1:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                        break;
                    case 0xA9:
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "\303", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        break;
                    case 'o':
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab9;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab9;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 1, s_1);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab9:
        z->c = z->l - v_10;
    }
    {
        int v_11 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'k':
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "jeite", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\303\251ite", 5) == 0) { among_var = 3; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "jaito", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "\303\241ito", 5) == 0) { among_var = 2; z->c = c_among - 6; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "eite", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "jain", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "jein", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\241in", 4) == 0) { among_var = 2; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "\303\251in", 4) == 0) { among_var = 3; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "aito", 4) == 0) { among_var = 1; z->c = c_among - 5; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ite", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "jai", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "jei", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\241i", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251i", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ain", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ein", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ai", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ei", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "in", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'd':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "jai", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "jei", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\241i", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251i", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ai", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ei", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'm':
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "jai", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "jei", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\241i", 3) == 0) { among_var = 2; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\251i", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ai", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ei", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        break;
                    case 'i':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ja", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "je", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\241", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 1; z->c = c_among - 1; break; }
                        break;
                }
            }
        }
        if (!among_var) goto lab10;
        z->bra = z->c;
        if (i_p1 > z->c) goto lab10;
        switch (among_var) {
            case 1:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 1, s_1);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab10:
        z->c = z->l - v_11;
    }
    {
        int v_12 = z->l - z->c;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 'k':
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\241", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\251", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\266", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = 3; z->c = c_among - 2; break; }
                        if (c_among - z->lb >= 1) { among_var = 3; z->c = c_among - 1; break; }
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
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_1);
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
    lab11:
        z->c = z->l - v_12;
    }
    z->c = z->lb;
    return 1;
}

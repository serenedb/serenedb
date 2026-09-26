/* Generated from finnish.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_finnish_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

typedef struct SN_env SN_env;

struct SN_local {
    struct SN_env z;
    symbol * s_x;
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
extern int candidate_finnish_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_O_(struct SN_env * z);
static int r_A_(struct SN_env * z);
static int r_U(struct SN_env * z);
static int r_O(struct SN_env * z);
static int r_I(struct SN_env * z);
static int r_E(struct SN_env * z);
static int r_A(struct SN_env * z);
static int r_VI(struct SN_env * z);
static int r_LV(struct SN_env * z);

#define s_5 (s_4 + 2)
static const symbol s_0[] = { 0xC3, 0xA4 };
static const symbol s_1[] = { 0xC3, 0xB6 };
static const symbol s_2[] = { 0xC3, 0xB8 };
static const symbol s_3[] = { 'k', 's', 'e' };
static const symbol s_4[] = { 'k', 's', 'i', 'e' };
static const symbol s_6[] = { 'p', 'o' };

static const unsigned short a_7[] = {
    0x0000 , 0xA461 , 0x0046 , 0x0000 , 0x0000 , 0x0000 , 0x0062 , 0x0000 ,
    0x0000 , 0x0000 , 0x0069 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x006D ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00F6 , 0x0002 , 0x746C ,
    0x0051 , 0x0000 , 0xC001 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0054 ,
    0x0057 , 0x0000 , 0x6C6C , 0xC001 , 0x0000 , 0x7373 , 0xC001 , 0x3FFF ,
    0x746C , 0xC001 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0xC001 , 0xFFFD , 0x0000 , 0x6C6E , 0x0051 , 0x0066 , 0x0000 , 0x6969 ,
    0xC001 , 0x0000 , 0x0002 , 0xC001 , S(736B), 0x0001 , 0xB661 , 0x00C5 ,
    0x0000 , 0x0000 , 0x0000 , 0x00C8 , 0x0000 , 0x0000 , 0x0000 , 0x00E1 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00E8 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x00EB , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x00EE , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00F2 , 0x0000 , 0x6868 , 0xC000 ,
    0x0000 , 0x7464 , 0xBFFF , 0x00DB , 0x0000 , 0x0000 , 0xBFFD , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x00DE , 0x0000 , 0x7373 , 0xBFFE , 0x0000 , 0x7474 ,
    0xBFFF , 0x0000 , 0x6869 , 0xBFFC , 0x00E5 , 0x0000 , 0x7373 , 0xBFFF ,
    0x0000 , 0x6868 , 0xBFFB , 0x0000 , 0x6868 , 0xBFFA , 0x0000 , 0x0002 ,
    0xBFF9 , S(C368), 0x0000 , 0x0002 , 0xBFF8 , S(C368), 0x0000 , 0xC3C3 ,
    0x00F9 , 0x0002 , 0x746C , 0x0051 , 0x0000 , 0xC001 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0054 , 0x0057
};

static const unsigned char g_AEI[] = { 17, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8 };

static const unsigned char g_C[] = { 119, 223, 119, 1 };

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32 };

static const unsigned char g_particle_end[] = { 17, 97, 24, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32 };

static int r_LV(struct SN_env * z) {
    int among_var;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 0xA4:
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\244\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    break;
                case 0xB6:
                    if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "\303\266\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                    break;
                case 'a':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'e':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'i':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'o':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case 'u':
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_VI(struct SN_env * z) {
    int among_var;
    {
        int c_among = z->c;
        among_var = 0;
        if (c_among > z->lb) {
            switch (z->p[c_among - 1]) {
                case 'i':
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\244", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\266", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "i", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "o", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "u", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                    break;
                case '\'':
                    if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                    break;
            }
        }
    }
    if (!among_var) return 0;
    return 1;
}

static int r_A(struct SN_env * z) {
    do {
        if (z->c <= z->lb || z->p[z->c - 1] != 'a') goto lab0;
        z->c--;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_E(struct SN_env * z) {
    do {
        if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab0;
        z->c--;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_I(struct SN_env * z) {
    do {
        if (z->c <= z->lb || z->p[z->c - 1] != 'i') goto lab0;
        z->c--;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_O(struct SN_env * z) {
    do {
        if (z->c <= z->lb || z->p[z->c - 1] != 'o') goto lab0;
        z->c--;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_U(struct SN_env * z) {
    do {
        if (z->c <= z->lb || z->p[z->c - 1] != 'u') goto lab0;
        z->c--;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_A_(struct SN_env * z) {
    do {
        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_0, 2) != 0) goto lab0;
        z->c -= 2;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_O_(struct SN_env * z) {
    do {
        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_1, 2) != 0) goto lab0;
        z->c -= 2;
        break;
    lab0:
        if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_2, 2) != 0) goto lab1;
        z->c -= 2;
        break;
    lab1:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

extern int candidate_finnish_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int b_ending_removed;
    int i_p2;
    int i_p1;
    {
        int v_1 = z->c;
        i_p1 = z->l;
        i_p2 = z->l;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        i_p1 = z->c;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        i_p2 = z->c;
    lab0:
        z->c = v_1;
    }
    b_ending_removed = 0;
    z->lb = z->c; z->c = z->l;
    {
        int v_2 = z->l - z->c;
        {
            int v_3;
            if (z->c < i_p1) goto lab1;
            v_3 = z->lb; z->lb = i_p1;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'n':
                            if (c_among - z->lb >= 6 && __builtin_memcmp(z->p + c_among - 6, "k\303\244\303\244", 5) == 0) { among_var = 1; z->c = c_among - 6; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "kaa", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "h\303\244", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ha", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ki", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "st", 2) == 0) { among_var = 2; z->c = c_among - 3; break; }
                            break;
                        case 0xA4:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "p\303", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 0xB6:
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "k\303", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'a':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "p", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                        case 'o':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "k", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) { z->lb = v_3; goto lab1; }
            z->bra = z->c;
            z->lb = v_3;
        }
        switch (among_var) {
            case 1:
                if (snowball_in_grouping_b_U(z, g_particle_end, 97, 246, 0)) goto lab1;
                break;
            case 2:
                if (i_p2 > z->c) goto lab1;
                break;
        }
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab1:
        z->c = z->l - v_2;
    }
    {
        int v_4 = z->l - z->c;
        {
            int v_5;
            if (z->c < i_p1) goto lab2;
            v_5 = z->lb; z->lb = i_p1;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xA4:
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ns\303", 3) == 0) { among_var = 3; z->c = c_among - 4; break; }
                            break;
                        case 'a':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ns", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                            break;
                        case 'e':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mm", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "nn", 2) == 0) { among_var = 3; z->c = c_among - 3; break; }
                            break;
                        case 'n':
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "\303\244", 2) == 0) { among_var = 5; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "a", 1) == 0) { among_var = 4; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "e", 1) == 0) { among_var = 6; z->c = c_among - 2; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = 2; z->c = c_among - 2; break; }
                            if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "s", 1) == 0) { among_var = 1; z->c = c_among - 2; break; }
                            break;
                    }
                }
            }
            if (!among_var) { z->lb = v_5; goto lab2; }
            z->bra = z->c;
            z->lb = v_5;
        }
        switch (among_var) {
            case 1:
                if (z->c <= z->lb || z->p[z->c - 1] != 'k') goto lab3;
                z->c--;
                goto lab2;
            lab3:
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
                z->ket = z->c;
                if (z->c - z->lb < 3 || __builtin_memcmp(z->p + z->c - 3, s_3, 3) != 0) goto lab2;
                z->c -= 3;
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 3, s_4);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'a':
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ll", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ss", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "lt", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "st", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "n", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                if (c_among - z->lb >= 2 && __builtin_memcmp(z->p + c_among - 2, "t", 1) == 0) { among_var = -1; z->c = c_among - 2; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab2;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 0xA4:
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ll\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ss\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "lt\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "st\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "n\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "t\303", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab2;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 6:
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'e':
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ll", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "in", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) goto lab2;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
        }
    lab2:
        z->c = z->l - v_4;
    }
    {
        int v_6 = z->l - z->c;
        {
            int v_7;
            if (z->c < i_p1) goto lab4;
            v_7 = z->lb; z->lb = i_p1;
            z->ket = z->c;
            {
                int c0 = z->c;
                among_var = find_among_b(z, a_7);
                if ((among_var & 0x4000)) {
                    int c = z->c;
                    switch (among_var & 0xF) {
                        case 0: {
                            int ret = r_A(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 1: {
                            int ret = r_VI(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 2: {
                            int ret = r_LV(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 3: {
                            int ret = r_E(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 4: {
                            int ret = r_I(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 5: {
                            int ret = r_O(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 6: {
                            int ret = r_U(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 7: {
                            int ret = r_A_(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                        case 8: {
                            int ret = r_O_(z);
                            if (ret > 0) { z->c = c; among_var = 16383; break; }
                            z->c = c0 - 1;
                            among_var = 1;
                            break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_7; goto lab4; }
            }
            z->bra = z->c;
            z->lb = v_7;
        }
        switch (among_var) {
            case 1:
                {
                    int v_8 = z->l - z->c;
                    {
                        int v_9 = z->l - z->c;
                        do {
                            int v_10 = z->l - z->c;
                            if (!r_LV(z)) goto lab6;
                            break;
                        lab6:
                            z->c = z->l - v_10;
                            if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_5, 2) != 0) { z->c = z->l - v_8; goto lab5; }
                            z->c -= 2;
                        } while (0);
                        z->c = z->l - v_9;
                        {
                            int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                            if (ret < 0) { z->c = z->l - v_8; goto lab5; }
                            z->c = ret;
                        }
                    }
                    z->bra = z->c;
                lab5:
                    ;
                }
                break;
            case 2:
                if (snowball_in_grouping_b_U(z, g_v, 97, 246, 0)) goto lab4;
                if (snowball_in_grouping_b_U(z, g_C, 98, 122, 0)) goto lab4;
                break;
            case 3:
                if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab4;
                z->c--;
                break;
        }
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
        b_ending_removed = 1;
    lab4:
        z->c = z->l - v_6;
    }
    {
        int v_11 = z->l - z->c;
        {
            int v_12;
            if (z->c < i_p2) goto lab7;
            v_12 = z->lb; z->lb = i_p2;
            z->ket = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 0xA4:
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "imm\303", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 5 && __builtin_memcmp(z->p + c_among - 5, "imp\303", 4) == 0) { among_var = -1; z->c = c_among - 5; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "ej\303", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mm\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "mp\303", 3) == 0) { among_var = 1; z->c = c_among - 4; break; }
                            break;
                        case 'a':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imm", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imp", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "ej", 2) == 0) { among_var = -1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mm", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mp", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                        case 'i':
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imm", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imp", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mm", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mp", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                            break;
                    }
                }
            }
            if (!among_var) { z->lb = v_12; goto lab7; }
            z->bra = z->c;
            z->lb = v_12;
        }
        switch (among_var) {
            case 1:
                if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_6, 2) != 0) goto lab8;
                z->c -= 2;
                goto lab7;
            lab8:
                break;
        }
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab7:
        z->c = z->l - v_11;
    }
    do {
        if (!b_ending_removed) goto lab9;
        {
            int v_13 = z->l - z->c;
            {
                int v_14;
                if (z->c < i_p1) goto lab10;
                v_14 = z->lb; z->lb = i_p1;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'i':
                                if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                                break;
                            case 'j':
                                if (c_among - z->lb >= 1) { among_var = -1; z->c = c_among - 1; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_14; goto lab10; }
                z->bra = z->c;
                z->lb = v_14;
            }
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab10:
            z->c = z->l - v_13;
        }
        break;
    lab9:
        {
            int v_15 = z->l - z->c;
            {
                int v_16;
                if (z->c < i_p1) goto lab11;
                v_16 = z->lb; z->lb = i_p1;
                z->ket = z->c;
                if (z->c <= z->lb || z->p[z->c - 1] != 't') { z->lb = v_16; goto lab11; }
                z->c--;
                z->bra = z->c;
                {
                    int v_17 = z->l - z->c;
                    if (snowball_in_grouping_b_U(z, g_v, 97, 246, 0)) { z->lb = v_16; goto lab11; }
                    z->c = z->l - v_17;
                }
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                z->lb = v_16;
            }
            {
                int v_18;
                if (z->c < i_p2) goto lab11;
                v_18 = z->lb; z->lb = i_p2;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'a':
                                if (c_among - z->lb >= 4 && __builtin_memcmp(z->p + c_among - 4, "imm", 3) == 0) { among_var = -1; z->c = c_among - 4; break; }
                                if (c_among - z->lb >= 3 && __builtin_memcmp(z->p + c_among - 3, "mm", 2) == 0) { among_var = 1; z->c = c_among - 3; break; }
                                break;
                        }
                    }
                }
                if (!among_var) { z->lb = v_18; goto lab11; }
                z->bra = z->c;
                z->lb = v_18;
            }
            switch (among_var) {
                case 1:
                    if (z->c - z->lb < 2 || __builtin_memcmp(z->p + z->c - 2, s_6, 2) != 0) goto lab12;
                    z->c -= 2;
                    goto lab11;
                lab12:
                    break;
            }
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab11:
            z->c = z->l - v_15;
        }
    } while (0);
    {
        int v_19 = z->l - z->c;
        {
            int v_20;
            if (z->c < i_p1) goto lab13;
            v_20 = z->lb; z->lb = i_p1;
            {
                int v_21 = z->l - z->c;
                {
                    int v_22 = z->l - z->c;
                    if (!r_LV(z)) goto lab14;
                    z->c = z->l - v_22;
                    z->ket = z->c;
                    {
                        int ret = snowball_skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab14;
                        z->c = ret;
                    }
                    z->bra = z->c;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                }
            lab14:
                z->c = z->l - v_21;
            }
            {
                int v_23 = z->l - z->c;
                z->ket = z->c;
                if (snowball_in_grouping_b_U(z, g_AEI, 97, 228, 0)) goto lab15;
                z->bra = z->c;
                if (snowball_in_grouping_b_U(z, g_C, 98, 122, 0)) goto lab15;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
            lab15:
                z->c = z->l - v_23;
            }
            {
                int v_24 = z->l - z->c;
                z->ket = z->c;
                if (z->c <= z->lb || z->p[z->c - 1] != 'j') goto lab16;
                z->c--;
                z->bra = z->c;
                do {
                    if (z->c <= z->lb || z->p[z->c - 1] != 'o') goto lab17;
                    z->c--;
                    break;
                lab17:
                    if (z->c <= z->lb || z->p[z->c - 1] != 'u') goto lab16;
                    z->c--;
                } while (0);
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
            lab16:
                z->c = z->l - v_24;
            }
            {
                int v_25 = z->l - z->c;
                z->ket = z->c;
                if (z->c <= z->lb || z->p[z->c - 1] != 'o') goto lab18;
                z->c--;
                z->bra = z->c;
                if (z->c <= z->lb || z->p[z->c - 1] != 'j') goto lab18;
                z->c--;
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
            lab18:
                z->c = z->l - v_25;
            }
            z->lb = v_20;
        }
        {
            int v_26 = z->l - z->c;
            if (snowball_in_grouping_b_U(z, g_v, 97, 246, 1) < 0) goto lab19;
            z->ket = z->c;
            if (snowball_in_grouping_b_U(z, g_C, 98, 122, 0)) goto lab19;
            z->bra = z->c;
            {
                int ret = slice_to(z, &((SN_local *)z)->s_x);
                if (ret < 0) return ret;
            }
            if (!(eq_v_b(z, ((SN_local *)z)->s_x))) goto lab19;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab19:
            z->c = z->l - v_26;
        }
        z->ket = z->c;
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') goto lab13;
        z->c--;
        z->bra = z->c;
        {
            int ret = snowball_slice_del(z);
            if (ret < 0) return ret;
        }
    lab13:
        z->c = z->l - v_19;
    }
    z->c = z->lb;
    return 1;
}

extern struct SN_env * candidate_finnish_UTF_8_create_env(void) {
    struct SN_env * z = SN_new_env(sizeof(SN_local));
    if (z) {
        if ((((SN_local *)z)->s_x = create_s()) == NULL) {
            candidate_finnish_UTF_8_close_env(z);
            return NULL;
        }
    }
    return z;
}

extern void candidate_finnish_UTF_8_close_env(struct SN_env * z) {
    if (!z) return;
    lose_s(((SN_local *)z)->s_x);
    SN_delete_env(z);
}


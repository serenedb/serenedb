/* Generated from hindi.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_hindi_candidate.h"

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
extern int candidate_hindi_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_CONSONANT(struct SN_env * z);


static const unsigned char g_consonant[] = { 255, 255, 255, 255, 159, 0, 0, 0, 248, 7 };

static int r_CONSONANT(struct SN_env * z) {
    return !snowball_in_grouping_b_U(z, g_consonant, 2325, 2399, 0);
}

extern int candidate_hindi_UTF_8_stem(struct SN_env * z) {
    int among_var;
    {
        int ret = snowball_skip_utf8(z->p, z->c, z->l, 1);
        if (ret < 0) return 0;
        z->c = ret;
    }
    z->lb = z->c; z->c = z->l;
    z->ket = z->c;
    {
        int c0 = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among - 0 > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x80:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA5:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x97:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x82:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x82:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA5:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0x87:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA5:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0x8A:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0x8F:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x87:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x8B:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x8F:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x86:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBE:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x93:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x86:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBE:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xA4:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16384; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xA8:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16384; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x81:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x86:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xBE:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0xAF:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x87:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBF:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                                case 0xA5:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x82:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x80:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA5:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0xA4:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16385; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x85:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0x86:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBE:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x86:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x81:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x89:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x87:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA5:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x88:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x8B:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA5:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0xAF:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x87:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBF:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x8F:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x81:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x89:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0xA4:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16386; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x85:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xA8:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16386; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x85:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x93:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x81:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x89:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0xA4:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16386; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x85:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xA8:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16386; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x85:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xBE:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0xAF:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x87:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBF:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                                case 0xA5:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x85:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x86:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x87:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                                case 0xA5:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x97:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x82:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x87:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA5:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0x8F:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x8B:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x93:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x86:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBE:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xA4:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16384; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xA8:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16384; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x88:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x86:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xBE:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x89:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x8A:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x8B:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA5:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x8D:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA5:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x8F:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x86:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0x87:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xBE:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xBF:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0x93:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x86:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xBE:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16383; z->c = c_among - 6;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0xB0:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x95:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16387; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0xBE:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                if (c_among - 3 > z->lb) {
                                                    switch (z->p[c_among - 4]) {
                                                        case 0x97:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x82:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x82:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA5:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0x8A:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            if (c_among - 12 > z->lb) {
                                                                                                                                                                switch (z->p[c_among - 13]) {
                                                                                                                                                                    case 0x86:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                    case 0xBE:
                                                                                                                                                                        if (c_among - 13 > z->lb) {
                                                                                                                                                                            switch (z->p[c_among - 14]) {
                                                                                                                                                                                case 0xA4:
                                                                                                                                                                                    if (c_among - 14 > z->lb) {
                                                                                                                                                                                        switch (z->p[c_among - 15]) {
                                                                                                                                                                                            case 0xE0:
                                                                                                                                                                                                among_var = 16383; z->c = c_among - 15;
                                                                                                                                                                                                break;
                                                                                                                                                                                        }
                                                                                                                                                                                    }
                                                                                                                                                                                    break;
                                                                                                                                                                            }
                                                                                                                                                                        }
                                                                                                                                                                        break;
                                                                                                                                                                }
                                                                                                                                                            }
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x87:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA5:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x8F:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        if (c_among - 9 > z->lb) {
                                                                                                                            switch (z->p[c_among - 10]) {
                                                                                                                                case 0x86:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                                case 0xBE:
                                                                                                                                    if (c_among - 10 > z->lb) {
                                                                                                                                        switch (z->p[c_among - 11]) {
                                                                                                                                            case 0xA4:
                                                                                                                                                if (c_among - 11 > z->lb) {
                                                                                                                                                    switch (z->p[c_among - 12]) {
                                                                                                                                                        case 0xE0:
                                                                                                                                                            among_var = 16383; z->c = c_among - 12;
                                                                                                                                                            break;
                                                                                                                                                    }
                                                                                                                                                }
                                                                                                                                                break;
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                    break;
                                                                                                                            }
                                                                                                                        }
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xA4:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16384; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xA8:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    among_var = 16384; z->c = c_among - 6;
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x85:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                        case 0xAF:
                                                            if (c_among - 4 > z->lb) {
                                                                switch (z->p[c_among - 5]) {
                                                                    case 0xA4:
                                                                        if (c_among - 5 > z->lb) {
                                                                            switch (z->p[c_among - 6]) {
                                                                                case 0xE0:
                                                                                    if (c_among - 6 > z->lb) {
                                                                                        switch (z->p[c_among - 7]) {
                                                                                            case 0x86:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                            case 0xBE:
                                                                                                if (c_among - 7 > z->lb) {
                                                                                                    switch (z->p[c_among - 8]) {
                                                                                                        case 0xA4:
                                                                                                            if (c_among - 8 > z->lb) {
                                                                                                                switch (z->p[c_among - 9]) {
                                                                                                                    case 0xE0:
                                                                                                                        among_var = 16383; z->c = c_among - 9;
                                                                                                                        break;
                                                                                                                }
                                                                                                            }
                                                                                                            break;
                                                                                                    }
                                                                                                }
                                                                                                break;
                                                                                        }
                                                                                    }
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                }
                                                            }
                                                            break;
                                                    }
                                                }
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                    case 0xBF:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xA4:
                                    if (c_among - 2 > z->lb) {
                                        switch (z->p[c_among - 3]) {
                                            case 0xE0:
                                                among_var = 16383; z->c = c_among - 3;
                                                break;
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                }
            }
        }
        if ((among_var & 0x4000)) {
            int c = z->c;
            switch (among_var & 0x3) {
                case 0: {
                    int ret = r_CONSONANT(z);
                    if (ret > 0) { z->c = c; among_var = 16383; break; }
                    z->c = c0 - 3;
                    among_var = 16383;
                    break;
                }
                case 1: {
                    int ret = r_CONSONANT(z);
                    if (ret > 0) { z->c = c; among_var = 16383; break; }
                    z->c = c0 - 6;
                    among_var = 16383;
                    break;
                }
                case 2: {
                    int ret = r_CONSONANT(z);
                    if (ret > 0) { z->c = c; among_var = 16383; break; }
                    z->c = c0 - 9;
                    among_var = 16383;
                    break;
                }
                case 3: {
                    int ret = r_CONSONANT(z);
                    if (ret > 0) { z->c = c; among_var = 16383; break; }
                    among_var = 0;
                    break;
                }
            }
        }
        if (!among_var) return 0;
    }
    z->bra = z->c;
    {
        int ret = snowball_slice_del(z);
        if (ret < 0) return ret;
    }
    z->c = z->lb;
    return 1;
}

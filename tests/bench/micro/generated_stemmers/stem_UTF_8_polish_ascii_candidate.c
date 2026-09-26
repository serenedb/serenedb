/* Generated from polish.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_polish_ascii_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

typedef struct SN_env SN_env;

struct SN_local {
    struct SN_env z;
    int i_p1;
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
extern int candidate_polish_ascii_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static int r_R1(struct SN_env * z);

static const symbol s_0[] = { 's' };
static const symbol s_1[] = { 0xC5, 0x82 };
static const symbol s_2[] = { 'c' };
static const symbol s_3[] = { 'n' };
static const symbol s_4[] = { 'z' };

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 1 };

static int r_R1(struct SN_env * z) {
    return ((SN_local *)z)->i_p1 <= z->c;
}

extern int candidate_polish_ascii_UTF_8_stem(struct SN_env * z) {
    int among_var;
    {
        int v_1 = z->c;
        ((SN_local *)z)->i_p1 = z->l;
        {
            int ret = snowball_out_grouping_U(z, g_v, 97, 281, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        {
            int ret = snowball_in_grouping_U(z, g_v, 97, 281, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        ((SN_local *)z)->i_p1 = z->c;
    lab0:
        z->c = v_1;
    }
    do {
        int v_2 = z->c;
        {
            int ret = snowball_skip_utf8(z->p, z->c, z->l, 2);
            if (ret < 0) goto lab1;
            z->c = ret;
        }
        z->lb = z->c; z->c = z->l;
        {
            int v_3 = z->l - z->c;
            {
                int v_4;
                if (z->c < ((SN_local *)z)->i_p1) goto lab2;
                v_4 = z->lb; z->lb = ((SN_local *)z)->i_p1;
                z->ket = z->c;
                {
                    int c_among = z->c;
                    among_var = 0;
                    if (c_among - 0 > z->lb) {
                        switch (z->p[c_among - 1]) {
                            case 'e':
                                if (c_among - 1 > z->lb) {
                                    switch (z->p[c_among - 2]) {
                                        case 'i':
                                            if (c_among - 2 > z->lb) {
                                                switch (z->p[c_among - 3]) {
                                                    case 'c':
                                                        if (c_among - 3 > z->lb) {
                                                            switch (z->p[c_among - 4]) {
                                                                case 0x9B:
                                                                    if (c_among - 4 > z->lb) {
                                                                        switch (z->p[c_among - 5]) {
                                                                            case 0xC5:
                                                                                if (c_among - 5 > z->lb) {
                                                                                    switch (z->p[c_among - 6]) {
                                                                                        case 'y':
                                                                                            if (c_among - 6 > z->lb) {
                                                                                                switch (z->p[c_among - 7]) {
                                                                                                    case 'b':
                                                                                                        among_var = 1; z->c = c_among - 7;
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
                            case 'm':
                                if (c_among - 1 > z->lb) {
                                    switch (z->p[c_among - 2]) {
                                        case 'y':
                                            if (c_among - 2 > z->lb) {
                                                switch (z->p[c_among - 3]) {
                                                    case 'b':
                                                        among_var = 1; z->c = c_among - 3;
                                                        break;
                                                }
                                            }
                                            break;
                                    }
                                }
                                break;
                            case 'y':
                                if (c_among - 1 > z->lb) {
                                    switch (z->p[c_among - 2]) {
                                        case 'b':
                                            among_var = 1; z->c = c_among - 2;
                                            break;
                                        case 'm':
                                            if (c_among - 2 > z->lb) {
                                                switch (z->p[c_among - 3]) {
                                                    case 0x9B:
                                                        if (c_among - 3 > z->lb) {
                                                            switch (z->p[c_among - 4]) {
                                                                case 0xC5:
                                                                    if (c_among - 4 > z->lb) {
                                                                        switch (z->p[c_among - 5]) {
                                                                            case 'y':
                                                                                if (c_among - 5 > z->lb) {
                                                                                    switch (z->p[c_among - 6]) {
                                                                                        case 'b':
                                                                                            among_var = 1; z->c = c_among - 6;
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
                            case 0x9B:
                                if (c_among - 1 > z->lb) {
                                    switch (z->p[c_among - 2]) {
                                        case 0xC5:
                                            if (c_among - 2 > z->lb) {
                                                switch (z->p[c_among - 3]) {
                                                    case 'y':
                                                        if (c_among - 3 > z->lb) {
                                                            switch (z->p[c_among - 4]) {
                                                                case 'b':
                                                                    among_var = 1; z->c = c_among - 4;
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
                }
                if (!among_var) { z->lb = v_4; goto lab2; }
                z->bra = z->c;
                z->lb = v_4;
            }
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab2:
            z->c = z->l - v_3;
        }
        z->ket = z->c;
        {
            int c0 = z->c;
            {
                int c_among = z->c;
                among_var = 0;
                if (c_among - 0 > z->lb) {
                    switch (z->p[c_among - 1]) {
                        case 'a':
                            among_var = 16384; z->c = c_among - 1;
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'c':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0x85:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC4:
                                                                among_var = 1; z->c = c_among - 4;
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'j':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'a':
                                                                                        among_var = 1; z->c = c_among - 6;
                                                                                        break;
                                                                                }
                                                                            }
                                                                            break;
                                                                        case 'z':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 's':
                                                                                        among_var = 2; z->c = c_among - 6;
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
                                    case 'i':
                                        among_var = 16385; z->c = c_among - 2;
                                        break;
                                    case 'z':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 's':
                                                    among_var = 1; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'j':
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'e':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'i':
                                                                                        among_var = 1; z->c = c_among - 6;
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
                                    case 0x82:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0xC5:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'a':
                                                                among_var = 1; z->c = c_among - 4;
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'i':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            break;
                                                                    }
                                                                }
                                                                break;
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
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
                        case 'c':
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 0x85:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0xC4:
                                                    among_var = 1; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'j':
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'a':
                                                                            among_var = 1; z->c = c_among - 5;
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
                        case 'e':
                            among_var = 16384; z->c = c_among - 1;
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'c':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0x85:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC4:
                                                                among_var = 1; z->c = c_among - 4;
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'j':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'a':
                                                                                        among_var = 1; z->c = c_among - 6;
                                                                                        break;
                                                                                }
                                                                            }
                                                                            break;
                                                                        case 'z':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 's':
                                                                                        among_var = 2; z->c = c_among - 6;
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
                                    case 'i':
                                        among_var = 16385; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'c':
                                                    among_var = 1; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'a':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                            case 'e':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                            case 'j':
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'a':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            break;
                                                                    }
                                                                }
                                                                break;
                                                            case 0x9B:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 0xC5:
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'i':
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 'l':
                                                                                                    among_var = 4; z->c = c_among - 7;
                                                                                                    if (c_among - 7 > z->lb) {
                                                                                                        switch (z->p[c_among - 8]) {
                                                                                                            case 'a':
                                                                                                                among_var = 1; z->c = c_among - 8;
                                                                                                                break;
                                                                                                            case 'e':
                                                                                                                if (c_among - 8 > z->lb) {
                                                                                                                    switch (z->p[c_among - 9]) {
                                                                                                                        case 'i':
                                                                                                                            among_var = 1; z->c = c_among - 9;
                                                                                                                            break;
                                                                                                                    }
                                                                                                                }
                                                                                                                break;
                                                                                                            case 'i':
                                                                                                                among_var = 1; z->c = c_among - 8;
                                                                                                                break;
                                                                                                        }
                                                                                                    }
                                                                                                    break;
                                                                                            }
                                                                                        }
                                                                                        break;
                                                                                    case 'y':
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 0x82:
                                                                                                    if (c_among - 7 > z->lb) {
                                                                                                        switch (z->p[c_among - 8]) {
                                                                                                            case 0xC5:
                                                                                                                among_var = 4; z->c = c_among - 8;
                                                                                                                if (c_among - 8 > z->lb) {
                                                                                                                    switch (z->p[c_among - 9]) {
                                                                                                                        case 'a':
                                                                                                                            among_var = 1; z->c = c_among - 9;
                                                                                                                            if (c_among - 9 > z->lb) {
                                                                                                                                switch (z->p[c_among - 10]) {
                                                                                                                                    case 'i':
                                                                                                                                        among_var = 1; z->c = c_among - 10;
                                                                                                                                        break;
                                                                                                                                }
                                                                                                                            }
                                                                                                                            break;
                                                                                                                        case 'i':
                                                                                                                            among_var = 1; z->c = c_among - 9;
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
                                    case 'z':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 's':
                                                    among_var = 1; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'j':
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'e':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'i':
                                                                                        among_var = 1; z->c = c_among - 6;
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
                        case 'h':
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'c':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 16384; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 16386; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 'i':
                                                    among_var = 5; z->c = c_among - 3;
                                                    break;
                                                case 'y':
                                                    among_var = 5; z->c = c_among - 3;
                                                    break;
                                            }
                                        }
                                        break;
                                }
                            }
                            break;
                        case 'i':
                            among_var = 16384; z->c = c_among - 1;
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'l':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 'e':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 'i':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                            }
                                        }
                                        break;
                                    case 'm':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 16385; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 16387; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 'i':
                                                    among_var = 5; z->c = c_among - 3;
                                                    break;
                                                case 'y':
                                                    among_var = 5; z->c = c_among - 3;
                                                    break;
                                            }
                                        }
                                        break;
                                    case 'w':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'o':
                                                    among_var = 16385; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 16387; z->c = c_among - 4;
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
                        case 'j':
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'a':
                                        among_var = 1; z->c = c_among - 2;
                                        break;
                                    case 'e':
                                        among_var = 5; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'i':
                                                    among_var = 5; z->c = c_among - 3;
                                                    break;
                                            }
                                        }
                                        break;
                                }
                            }
                            break;
                        case 'm':
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'a':
                                        among_var = 1; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0x82:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC5:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'a':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'i':
                                                                                        among_var = 1; z->c = c_among - 6;
                                                                                        break;
                                                                                }
                                                                            }
                                                                            break;
                                                                        case 'i':
                                                                            among_var = 1; z->c = c_among - 5;
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
                                    case 'e':
                                        among_var = 16384; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'i':
                                                    among_var = 16388; z->c = c_among - 3;
                                                    break;
                                                case 0x82:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC5:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'a':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'i':
                                                                                        among_var = 1; z->c = c_among - 6;
                                                                                        break;
                                                                                }
                                                                            }
                                                                            break;
                                                                        case 'i':
                                                                            among_var = 1; z->c = c_among - 5;
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
                                    case 'i':
                                        among_var = 5; z->c = c_among - 2;
                                        break;
                                    case 'o':
                                        among_var = 16384; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'i':
                                                    among_var = 16388; z->c = c_among - 3;
                                                    break;
                                            }
                                        }
                                        break;
                                    case 'y':
                                        among_var = 5; z->c = c_among - 2;
                                        break;
                                }
                            }
                            break;
                        case 'o':
                            among_var = 16384; z->c = c_among - 1;
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'g':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'e':
                                                    among_var = 5; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 5; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                            }
                                        }
                                        break;
                                    case 0x82:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0xC5:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'a':
                                                                among_var = 1; z->c = c_among - 4;
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'i':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            break;
                                                                    }
                                                                }
                                                                break;
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
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
                        case 'u':
                            among_var = 16384; z->c = c_among - 1;
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'i':
                                        among_var = 16385; z->c = c_among - 2;
                                        break;
                                    case 'm':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'e':
                                                    among_var = 5; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 5; z->c = c_among - 4;
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
                        case 'w':
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 0xB3:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0xC3:
                                                    among_var = 16384; z->c = c_among - 3;
                                                    break;
                                            }
                                        }
                                        break;
                                }
                            }
                            break;
                        case 'y':
                            among_var = 5; z->c = c_among - 1;
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 'm':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 'e':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 'i':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 0x9B:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC5:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'i':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'l':
                                                                                        among_var = 4; z->c = c_among - 6;
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 'a':
                                                                                                    among_var = 1; z->c = c_among - 7;
                                                                                                    break;
                                                                                                case 'e':
                                                                                                    if (c_among - 7 > z->lb) {
                                                                                                        switch (z->p[c_among - 8]) {
                                                                                                            case 'i':
                                                                                                                among_var = 1; z->c = c_among - 8;
                                                                                                                break;
                                                                                                        }
                                                                                                    }
                                                                                                    break;
                                                                                                case 'i':
                                                                                                    among_var = 1; z->c = c_among - 7;
                                                                                                    break;
                                                                                            }
                                                                                        }
                                                                                        break;
                                                                                }
                                                                            }
                                                                            break;
                                                                        case 'y':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 0x82:
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 0xC5:
                                                                                                    among_var = 4; z->c = c_among - 7;
                                                                                                    if (c_among - 7 > z->lb) {
                                                                                                        switch (z->p[c_among - 8]) {
                                                                                                            case 'a':
                                                                                                                among_var = 1; z->c = c_among - 8;
                                                                                                                if (c_among - 8 > z->lb) {
                                                                                                                    switch (z->p[c_among - 9]) {
                                                                                                                        case 'i':
                                                                                                                            among_var = 1; z->c = c_among - 9;
                                                                                                                            break;
                                                                                                                    }
                                                                                                                }
                                                                                                                break;
                                                                                                            case 'i':
                                                                                                                among_var = 1; z->c = c_among - 8;
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
                                    case 0x82:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 0xC5:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'a':
                                                                among_var = 1; z->c = c_among - 4;
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'i':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            break;
                                                                    }
                                                                }
                                                                break;
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
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
                        case 'z':
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 's':
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 'e':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 'i':
                                                    among_var = 1; z->c = c_among - 3;
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
                                    case 0xC5:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 1; z->c = c_among - 3;
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 'i':
                                                    among_var = 1; z->c = c_among - 3;
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
                                    case 0xC4:
                                        among_var = 16384; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'c':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0x85:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 0xC4:
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'j':
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 'a':
                                                                                                    among_var = 1; z->c = c_among - 7;
                                                                                                    break;
                                                                                            }
                                                                                        }
                                                                                        break;
                                                                                    case 'z':
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 's':
                                                                                                    among_var = 2; z->c = c_among - 7;
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
                                                case 'i':
                                                    among_var = 16388; z->c = c_among - 3;
                                                    break;
                                                case 'j':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'a':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 'z':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 's':
                                                                among_var = 3; z->c = c_among - 4;
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'j':
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'e':
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 'i':
                                                                                                    among_var = 1; z->c = c_among - 7;
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
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 0xC4:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 'e':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 'i':
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 'i':
                                                    among_var = 1; z->c = c_among - 3;
                                                    break;
                                                case 0x85:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC4:
                                                                among_var = 1; z->c = c_among - 4;
                                                                break;
                                                        }
                                                    }
                                                    break;
                                                case 0x9B:
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0xC5:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 'a':
                                                                            among_var = 1; z->c = c_among - 5;
                                                                            break;
                                                                        case 'e':
                                                                            among_var = 1; z->c = c_among - 5;
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
                        case 0x99:
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 0xC4:
                                        among_var = 1; z->c = c_among - 2;
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'z':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 's':
                                                                among_var = 2; z->c = c_among - 4;
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
                        case 0x9B:
                            if (c_among - 1 > z->lb) {
                                switch (z->p[c_among - 2]) {
                                    case 0xC5:
                                        if (c_among - 2 > z->lb) {
                                            switch (z->p[c_among - 3]) {
                                                case 'a':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0x82:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 0xC5:
                                                                            among_var = 4; z->c = c_among - 5;
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'a':
                                                                                        among_var = 1; z->c = c_among - 6;
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 'i':
                                                                                                    among_var = 1; z->c = c_among - 7;
                                                                                                    break;
                                                                                            }
                                                                                        }
                                                                                        break;
                                                                                    case 'i':
                                                                                        among_var = 1; z->c = c_among - 6;
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
                                                case 'e':
                                                    if (c_among - 3 > z->lb) {
                                                        switch (z->p[c_among - 4]) {
                                                            case 0x82:
                                                                if (c_among - 4 > z->lb) {
                                                                    switch (z->p[c_among - 5]) {
                                                                        case 0xC5:
                                                                            among_var = 4; z->c = c_among - 5;
                                                                            if (c_among - 5 > z->lb) {
                                                                                switch (z->p[c_among - 6]) {
                                                                                    case 'a':
                                                                                        among_var = 1; z->c = c_among - 6;
                                                                                        if (c_among - 6 > z->lb) {
                                                                                            switch (z->p[c_among - 7]) {
                                                                                                case 'i':
                                                                                                    among_var = 1; z->c = c_among - 7;
                                                                                                    break;
                                                                                            }
                                                                                        }
                                                                                        break;
                                                                                    case 'i':
                                                                                        among_var = 1; z->c = c_among - 6;
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
            }
            if ((among_var & 0x4000)) do {
                int c = z->c;
                switch (among_var & 0x7) {
                    case 0: {
                        int ret = r_R1(z);
                        if (ret > 0) { z->c = c; among_var = 1; break; }
                        among_var = 0;
                        break;
                    }
                    case 1: {
                        int ret = r_R1(z);
                        if (ret > 0) { z->c = c; among_var = 1; break; }
                        z->c = c0 - 1;
                        among_var = 16384;
                        continue;
                    }
                    case 2: {
                        int ret = r_R1(z);
                        if (ret > 0) { z->c = c; among_var = 1; break; }
                        z->c = c0 - 3;
                        among_var = 16384;
                        continue;
                    }
                    case 3: {
                        int ret = r_R1(z);
                        if (ret > 0) { z->c = c; among_var = 1; break; }
                        z->c = c0 - 3;
                        among_var = 16385;
                        continue;
                    }
                    case 4: {
                        int ret = r_R1(z);
                        if (ret > 0) { z->c = c; among_var = 1; break; }
                        z->c = c0 - 2;
                        among_var = 16384;
                        continue;
                    }
                }
                break;
            } while (1);
            if (!among_var) goto lab1;
        }
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
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                do {
                    int v_5 = z->l - z->c;
                    if (((SN_local *)z)->i_p1 > z->c) goto lab3;
                    {
                        int ret = snowball_slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                lab3:
                    z->c = z->l - v_5;
                    {
                        int ret = slice_from_s(z, 1, s_0);
                        if (ret < 0) return ret;
                    }
                } while (0);
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 2, s_1);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                {
                    int ret = snowball_slice_del(z);
                    if (ret < 0) return ret;
                }
                {
                    int v_6 = z->l - z->c;
                    z->ket = z->c;
                    {
                        int c_among = z->c;
                        among_var = 0;
                        if (c_among - 0 > z->lb) {
                            switch (z->p[c_among - 1]) {
                                case 'c':
                                    if (c_among - 1 > z->lb) {
                                        switch (z->p[c_among - 2]) {
                                            case 0x85:
                                                if (c_among - 2 > z->lb) {
                                                    switch (z->p[c_among - 3]) {
                                                        case 0xC4:
                                                            among_var = 1; z->c = c_among - 3;
                                                            if (c_among - 3 > z->lb) {
                                                                switch (z->p[c_among - 4]) {
                                                                    case 'j':
                                                                        if (c_among - 4 > z->lb) {
                                                                            switch (z->p[c_among - 5]) {
                                                                                case 'a':
                                                                                    among_var = 1; z->c = c_among - 5;
                                                                                    break;
                                                                            }
                                                                        }
                                                                        break;
                                                                    case 'z':
                                                                        if (c_among - 4 > z->lb) {
                                                                            switch (z->p[c_among - 5]) {
                                                                                case 's':
                                                                                    among_var = 2; z->c = c_among - 5;
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
                                case 'z':
                                    if (c_among - 1 > z->lb) {
                                        switch (z->p[c_among - 2]) {
                                            case 's':
                                                among_var = 1; z->c = c_among - 2;
                                                if (c_among - 2 > z->lb) {
                                                    switch (z->p[c_among - 3]) {
                                                        case 'j':
                                                            if (c_among - 3 > z->lb) {
                                                                switch (z->p[c_among - 4]) {
                                                                    case 'e':
                                                                        if (c_among - 4 > z->lb) {
                                                                            switch (z->p[c_among - 5]) {
                                                                                case 'i':
                                                                                    among_var = 1; z->c = c_among - 5;
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
                    }
                    if (!among_var) { z->c = z->l - v_6; goto lab4; }
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
                                int ret = slice_from_s(z, 1, s_0);
                                if (ret < 0) return ret;
                            }
                            break;
                    }
                lab4:
                    ;
                }
                break;
        }
        {
            int v_7 = z->l - z->c;
            z->ket = z->c;
            if (z->c <= z->lb || z->p[z->c - 1] != '\'') { z->c = z->l - v_7; goto lab5; }
            z->c--;
            z->bra = z->c;
            {
                int ret = snowball_slice_del(z);
                if (ret < 0) return ret;
            }
        lab5:
            ;
        }
        z->c = z->lb;
        break;
    lab1:
        z->c = v_2;
        z->lb = z->c; z->c = z->l;
        z->ket = z->c;
        {
            int c_among = z->c;
            among_var = 0;
            if (c_among - 0 > z->lb) {
                switch (z->p[c_among - 1]) {
                    case 0x84:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xC5:
                                    among_var = 2; z->c = c_among - 2;
                                    break;
                            }
                        }
                        break;
                    case 0x87:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xC4:
                                    among_var = 1; z->c = c_among - 2;
                                    break;
                            }
                        }
                        break;
                    case 0x9B:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xC5:
                                    among_var = 3; z->c = c_among - 2;
                                    break;
                            }
                        }
                        break;
                    case 0xBA:
                        if (c_among - 1 > z->lb) {
                            switch (z->p[c_among - 2]) {
                                case 0xC5:
                                    among_var = 4; z->c = c_among - 2;
                                    break;
                            }
                        }
                        break;
                }
            }
        }
        if (!among_var) return 0;
        z->bra = z->c;
        if (z->c <= z->lb) return 0;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_from_s(z, 1, s_2);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_from_s(z, 1, s_3);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_from_s(z, 1, s_0);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                {
                    int ret = slice_from_s(z, 1, s_4);
                    if (ret < 0) return ret;
                }
                break;
        }
        z->c = z->lb;
    } while (0);
    return 1;
}

extern struct SN_env * candidate_polish_ascii_UTF_8_create_env(void) {
    return SN_new_env(sizeof(SN_local));
}

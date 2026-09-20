/* Generated from finnish.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_finnish_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

struct SN_local {
    struct SN_env z;
    symbol * s_x;
};

typedef struct SN_local SN_local;

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

static const unsigned short a_0[] = {
    0x0000 , 0xB661 , 0x0058 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x005B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x005F ,
    0x00A9 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00B7 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00B3 ,
    0x0000 , 0x7070 , 0xFFFF , 0x0000 , 0x0002 , 0xFFFE , S(7473), 0x0000 ,
    0xA461 , 0x00A5 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x00A9 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00AC , 0x0000 , 0x6168 , 0x00A9 ,
    0xFFFF , 0x0000 , 0x6B6B , 0xFFFF , 0x0000 , 0xC3C3 , 0x00AF , 0x0000 ,
    0x68A4 , 0xFFFF , 0x00B3 , 0x0000 , 0x0002 , 0xFFFF , S(C36B), 0x0000 ,
    0x0002 , 0xFFFF , S(C370)
};

static const unsigned short a_1[] = {
    0x0000 , 0x6161 , 0x0003 , 0x0000 , 0x746C , 0x000E , 0x0000 , 0xC001 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0011 , 0x0014 , 0x0000 , 0x6C6C ,
    0xC001 , 0x0000 , 0x7373 , 0xC001 , 0x3FFF , 0x6C73 , 0xC001 , 0xC001
};

static const unsigned short a_2[] = {
    0x0000 , 0x0002 , 0x0004 , S(A4C3), 0x0000 , 0x746C , 0x000F , 0x0000 ,
    0xC001 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0012 , 0x0015 , 0x0000 ,
    0x6C6C , 0xC001 , 0x0000 , 0x7373 , 0xC001 , 0x3FFF , 0x6C73 , 0xC001 ,
    0xC001
};

static const unsigned short a_3[] = {
    0x0000 , 0x6565 , 0x0003 , 0x0000 , 0x6C6E , 0x0007 , 0x000A , 0x0000 ,
    0x6C6C , 0xC001 , 0x0000 , 0x6969 , 0xC001
};

static const unsigned short a_4[] = {
    0x0000 , 0xA461 , 0x0046 , 0x0000 , 0x0000 , 0x0000 , 0x004A , 0x0000 ,
    0x0000 , 0x0000 , 0x0054 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0058 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00A1 , 0x0000 , 0x0002 ,
    0xFFFD , S(736E), 0x0000 , 0x6D6E , 0x004E , 0x0051 , 0x0000 , 0x6D6D ,
    0xFFFD , 0x0000 , 0x6E6E , 0xFFFD , 0x0000 , 0x6E73 , 0xFFFE , 0xFFFF ,
    0x0000 , 0xA461 , 0xFFFC , 0x0000 , 0x0000 , 0x0000 , 0xFFFA , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x009E , 0x0000 , 0xC3C3 ,
    0xFFFB , 0x0000 , 0x0003 , 0xFFFD , S(736E), S(00C3)
};

static const unsigned short a_5[] = {
    0x0000 , 0xB661 , 0x0058 , 0x0000 , 0x0000 , 0x0000 , 0x005B , 0x0000 ,
    0x0000 , 0x0000 , 0x005E , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0061 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0064 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0067 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x006C ,
    0x0000 , 0x6161 , 0xC001 , 0x0000 , 0x6565 , 0xC001 , 0x0000 , 0x6969 ,
    0xC001 , 0x0000 , 0x6F6F , 0xC001 , 0x0000 , 0x7575 , 0xC001 , 0x0000 ,
    0x0003 , 0xC001 , S(A4C3), S(00C3), 0x0000 , 0x0003 , 0xC001 , S(B6C3),
    S(00C3)
};

static const unsigned short a_6[] = {
    0x0000 , 0x2769 , 0xC001 , 0x0004 , 0x0000 , 0xB661 , 0xC001 , 0x0000 ,
    0x0000 , 0x0000 , 0xC001 , 0x0000 , 0x0000 , 0x0000 , 0xC001 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0xC001 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0xC001 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x005C , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x005C , 0x0000 , 0xC3C3 , 0xC001
};

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

static const unsigned short a_8[] = {
    0x0000 , 0xA461 , 0x0046 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x005B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x005F , 0x0000 , 0x706A ,
    0x004F , 0x0000 , 0x0000 , 0x0052 , 0x0000 , 0x0000 , 0x0058 , 0x0000 ,
    0x6565 , 0xC001 , 0x0000 , 0x6D6D , 0x0055 , 0x0001 , 0x6969 , 0xC001 ,
    0x0000 , 0x6D6D , 0x0055 , 0x0000 , 0x6D70 , 0x0058 , 0x0058 , 0x0000 ,
    0xC3C3 , 0x0062 , 0x0000 , 0x706A , 0x004F , 0x0000 , 0x0000 , 0x0058 ,
    0x0000 , 0x0000 , 0x0058
};

static const unsigned short a_10[] = {
    0x0000 , 0x0003 , 0x0005 , S(6D6D), S(0061), 0x0001 , 0x6969 , 0xC001
};

static const unsigned char g_AEI[] = { 17, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8 };

static const unsigned char g_C[] = { 119, 223, 119, 1 };

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32 };

static const unsigned char g_particle_end[] = { 17, 97, 24, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32 };

static int r_LV(struct SN_env * z) {
    return find_among_b(z, a_5) != 0;
}

static int r_VI(struct SN_env * z) {
    if (z->c <= z->lb || (z->p[z->c - 1] != 39 && z->p[z->c - 1] != 105)) return 0;
    return find_among_b(z, a_6) != 0;
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
        if (!(eq_s_b(z, 2, s_0))) goto lab0;
        break;
    lab0:
        if (z->c <= z->lb || z->p[z->c - 1] != '\'') return 0;
        z->c--;
    } while (0);
    return 1;
}

static int r_O_(struct SN_env * z) {
    do {
        if (!(eq_s_b(z, 2, s_1))) goto lab0;
        break;
    lab0:
        if (!(eq_s_b(z, 2, s_2))) goto lab1;
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
            int ret = out_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        {
            int ret = in_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        i_p1 = z->c;
        {
            int ret = out_grouping_U(z, g_v, 97, 246, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        {
            int ret = in_grouping_U(z, g_v, 97, 246, 1);
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
            among_var = find_among_b(z, a_0);
            if (!among_var) { z->lb = v_3; goto lab1; }
            z->bra = z->c;
            z->lb = v_3;
        }
        switch (among_var) {
            case 1:
                if (in_grouping_b_U(z, g_particle_end, 97, 246, 0)) goto lab1;
                break;
            case 2:
                if (i_p2 > z->c) goto lab1;
                break;
        }
        {
            int ret = slice_del(z);
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
            among_var = find_among_b(z, a_4);
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
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                z->ket = z->c;
                if (!(eq_s_b(z, 3, s_3))) goto lab2;
                z->bra = z->c;
                {
                    int ret = slice_from_s(z, 3, s_4);
                    if (ret < 0) return ret;
                }
                break;
            case 3:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 4:
                if (z->c - 1 <= z->lb || z->p[z->c - 1] != 97) goto lab2;
                if (!find_among_b(z, a_1)) goto lab2;
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 5:
                if (z->c - 2 <= z->lb || z->p[z->c - 1] != 164) goto lab2;
                if (!find_among_b(z, a_2)) goto lab2;
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 6:
                if (z->c - 2 <= z->lb || z->p[z->c - 1] != 101) goto lab2;
                if (!find_among_b(z, a_3)) goto lab2;
                {
                    int ret = slice_del(z);
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
                            if (!(eq_s_b(z, 2, s_5))) { z->c = z->l - v_8; goto lab5; }
                        } while (0);
                        z->c = z->l - v_9;
                        {
                            int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
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
                if (in_grouping_b_U(z, g_v, 97, 246, 0)) goto lab4;
                if (in_grouping_b_U(z, g_C, 98, 122, 0)) goto lab4;
                break;
            case 3:
                if (z->c <= z->lb || z->p[z->c - 1] != 'e') goto lab4;
                z->c--;
                break;
        }
        {
            int ret = slice_del(z);
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
            among_var = find_among_b(z, a_8);
            if (!among_var) { z->lb = v_12; goto lab7; }
            z->bra = z->c;
            z->lb = v_12;
        }
        switch (among_var) {
            case 1:
                if (!(eq_s_b(z, 2, s_6))) goto lab8;
                goto lab7;
            lab8:
                break;
        }
        {
            int ret = slice_del(z);
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
                if (z->c <= z->lb || (z->p[z->c - 1] != 105 && z->p[z->c - 1] != 106)) { z->lb = v_14; goto lab10; }
                z->c--;
                z->bra = z->c;
                z->lb = v_14;
            }
            {
                int ret = slice_del(z);
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
                    if (in_grouping_b_U(z, g_v, 97, 246, 0)) { z->lb = v_16; goto lab11; }
                    z->c = z->l - v_17;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                z->lb = v_16;
            }
            {
                int v_18;
                if (z->c < i_p2) goto lab11;
                v_18 = z->lb; z->lb = i_p2;
                z->ket = z->c;
                if (z->c - 2 <= z->lb || z->p[z->c - 1] != 97) { z->lb = v_18; goto lab11; }
                among_var = find_among_b(z, a_10);
                if (!among_var) { z->lb = v_18; goto lab11; }
                z->bra = z->c;
                z->lb = v_18;
            }
            switch (among_var) {
                case 1:
                    if (!(eq_s_b(z, 2, s_6))) goto lab12;
                    goto lab11;
                lab12:
                    break;
            }
            {
                int ret = slice_del(z);
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
                        int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
                        if (ret < 0) goto lab14;
                        z->c = ret;
                    }
                    z->bra = z->c;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                }
            lab14:
                z->c = z->l - v_21;
            }
            {
                int v_23 = z->l - z->c;
                z->ket = z->c;
                if (in_grouping_b_U(z, g_AEI, 97, 228, 0)) goto lab15;
                z->bra = z->c;
                if (in_grouping_b_U(z, g_C, 98, 122, 0)) goto lab15;
                {
                    int ret = slice_del(z);
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
                    int ret = slice_del(z);
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
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
            lab18:
                z->c = z->l - v_25;
            }
            z->lb = v_20;
        }
        {
            int v_26 = z->l - z->c;
            if (in_grouping_b_U(z, g_v, 97, 246, 1) < 0) goto lab19;
            z->ket = z->c;
            if (in_grouping_b_U(z, g_C, 98, 122, 0)) goto lab19;
            z->bra = z->c;
            {
                int ret = slice_to(z, &((SN_local *)z)->s_x);
                if (ret < 0) return ret;
            }
            if (!(eq_v_b(z, ((SN_local *)z)->s_x))) goto lab19;
            {
                int ret = slice_del(z);
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
            int ret = slice_del(z);
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


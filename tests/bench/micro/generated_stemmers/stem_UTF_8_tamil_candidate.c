/* Generated from tamil.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_tamil_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

struct SN_local {
    struct SN_env z;
    unsigned char b_found_vetrumai_urupu;
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

static const unsigned short a_0[] = {
    0x0000 , 0x0005 , 0x0006 , S(AEE0), S(E0B5), S(00AF), 0x0000 , 0x8B81 ,
    0xFFFD , 0xFFFC , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0xFFFE , 0xFFFF
};

static const unsigned short a_1[] = {
    0x0000 , 0x0002 , 0x0004 , S(AEE0), 0x0000 , 0xB595 , 0xC001 , 0x0000 ,
    0x0000 , 0x0000 , 0xC001 , 0xC001 , 0x0000 , 0x0000 , 0x0000 , 0xC001 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0xC001 , 0x0000 , 0x0000 ,
    0x0000 , 0xC001 , 0x0000 , 0xC001 , 0x0000 , 0x0000 , 0x0000 , 0xC001 ,
    0xC001 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0xC001
};

static const unsigned short a_2[] = {
    0x0000 , 0xBF80 , 0x0042 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0042 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0046 , 0x0000 , 0x0002 , 0xC001 , S(AFE0), 0x0000 , 0x0002 ,
    0xC001 , S(AEE0)
};

static const unsigned short a_3[] = {
    0x0000 , 0xBF80 , 0x0042 , 0x0042 , 0x0042 , 0x0000 , 0x0000 , 0x0000 ,
    0x0042 , 0x0042 , 0x0042 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0046 , 0x0046 , 0x0000 , 0x0002 , 0xC001 , S(AFE0), 0x0000 , 0x0002 ,
    0xC001 , S(AEE0)
};

static const unsigned short a_4[] = {
    0x0002 , 0x888D , 0x0004 , 0x0004 , 0x0000 , 0x0002 , 0xFFFF , S(AFE0)
};

static const unsigned short a_5[] = {
    0x0000 , 0xB581 , 0x0037 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x003D , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00D0 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x00BA , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00BA , 0x0000 ,
    0x0005 , 0xFFF8 , S(AEE0), S(E0A9), S(00AF), 0x0000 , 0x0002 , 0x0041 ,
    S(AFE0), 0x0000 , 0xB595 , 0x0064 , 0x0000 , 0x0000 , 0x0000 , 0x00A1 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00A5 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x00AC , 0x0000 , 0x0000 , 0x0000 , 0x00BA , 0x0000 ,
    0x00BE , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00C5 , 0x0000 , 0x00C9 ,
    0x0000 , 0x0000 , 0x0000 , 0x00BA , 0x0000 , 0x0002 , 0x0068 , S(AEE0),
    0x0000 , 0x818D , 0x006C , 0x0070 , 0x0000 , 0x0002 , 0xFFF9 , S(AFE0),
    0x0000 , 0x0002 , 0x0074 , S(AFE0), 0x0000 , 0xB195 , 0x0093 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0099 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x009D , 0x0000 , 0x0005 , 0xFFF9 , S(AFE0), S(E081),
    S(00AE), 0x0000 , 0x0002 , 0xFFFD , S(AEE0), 0x0000 , 0x0002 , 0xFFFC ,
    S(AEE0), 0x0000 , 0x0002 , 0xFFF7 , S(AEE0), 0x0000 , 0x0008 , 0xFFFB ,
    S(AEE0), S(E09F), S(8DAF), S(AEE0), 0x0000 , 0x0005 , 0x00B2 , S(AFE0),
    S(E08D), S(00AE), 0x0000 , 0xA4A8 , 0x00B6 , 0x00BA , 0x0000 , 0x0002 ,
    0xFFFA , S(AEE0), 0x0000 , 0x0002 , 0xFFFF , S(AEE0), 0x0000 , 0x0008 ,
    0xFFFD , S(AEE0), S(E09F), S(8DAF), S(AEE0), 0x0000 , 0x0002 , 0xFFFE ,
    S(AEE0), 0x0000 , 0x0008 , 0xFFFC , S(AEE0), S(E0A9), S(8DAF), S(AEE0),
    0x0000 , 0x0008 , 0xFFFF , S(AEE0), S(E0A8), S(8DAF), S(AEE0)
};

static const unsigned short a_6[] = {
    0x0000 , 0xB195 , 0x001F , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x001F ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x001F , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x001F , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x001F ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x001F , 0x0000 ,
    0x0002 , 0xC001 , S(AEE0)
};

static const unsigned short a_7[] = {
    0x0000 , 0xB59E , 0x001A , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x001A ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x001A , 0x001A , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x001A , 0x001A , 0x001A , 0x0000 , 0x001A , 0x001A ,
    0x001A , 0x001A , 0x0000 , 0x0002 , 0xC001 , S(AEE0)
};

static const unsigned short a_8[] = {
    0x0000 , 0xBF80 , 0x0042 , 0x0042 , 0x0042 , 0x0000 , 0x0000 , 0x0000 ,
    0x0042 , 0x0042 , 0x0042 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0042 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0046 , 0x0046 , 0x0000 , 0x0002 , 0xC001 , S(AFE0), 0x0000 , 0x0002 ,
    0xC001 , S(AEE0)
};

static const unsigned short a_9[] = {
    0x0000 , 0x0002 , 0x0004 , S(AEE0), 0x0000 , 0x8985 , 0xC001 , 0x0000 ,
    0xC001 , 0x0000 , 0xC001
};

static const unsigned short a_10[] = {
    0x0000 , 0x0009 , 0x0008 , S(AEE0), S(E095), S(B3AE), S(AFE0), S(008D),
    0x0004 , 0x0003 , 0x000D , S(AFE0), S(008D), 0x0000 , 0xB199 , 0x0028 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x002E , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0032 ,
    0x0000 , 0x0005 , 0xFFFF , S(AFE0), S(E081), S(00AE), 0x0000 , 0x0002 ,
    0xFFFD , S(AEE0), 0x0000 , 0x0002 , 0xFFFE , S(AEE0)
};

static const unsigned short a_11[] = {
    0x0000 , 0xBE87 , 0x003A , 0x0000 , 0x0000 , 0x0000 , 0x003A , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x003E , 0x0000 , 0x0002 , 0xC001 , S(AFE0), 0x0000 , 0x0002 ,
    0xC001 , S(AEE0)
};

static const unsigned short a_12[] = {
    0x0000 , 0x0003 , 0x0005 , S(AEE0), S(00BF), 0x0000 , 0xAAB5 , 0x0009 ,
    0x0009 , 0x0000 , 0x0002 , 0xC001 , S(AEE0)
};

static const unsigned short a_13[] = {
    0x0000 , 0xBF81 , 0x0041 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x00BA , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00D1 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x013B , 0x0000 , 0x0000 , 0x0000 , 0x00A7 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0144 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x015E , 0x0000 , 0x0000 , 0x0182 , 0x0189 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0192 , 0x0000 , 0x0002 , 0x0045 , S(AFE0), 0x0000 , 0xB19F , 0x005A ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00A7 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x00B1 , 0x0000 , 0x0002 , 0x005E , S(AEE0), 0x0000 , 0xBF8D ,
    0x0093 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x009D , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x00A1 , 0x0000 , 0x0005 , 0x0099 , S(AEE0), S(E09F),
    S(00AF), 0x0000 , 0xAABF , 0x009D , 0x00A1 , 0x0000 , 0x0002 , 0xFFFD ,
    S(AEE0), 0x0000 , 0x0005 , 0xFFFD , S(AEE0), S(E0B5), S(00AE), 0x0000 ,
    0x000E , 0xFFFD , S(AEE0), S(E0AA), S(9FAE), S(AFE0), S(E08D), S(9FAE),
    S(AEE0), 0x0000 , 0x000B , 0xFFFF , S(AFE0), S(E086), S(A9AE), S(AFE0),
    S(E08D), S(00AE), 0x0000 , 0x0002 , 0x00BE , S(AFE0), 0x0000 , 0x9FB2 ,
    0x00C2 , 0x00C8 , 0x0000 , 0x0005 , 0xFFFF , S(AFE0), S(E081), S(00AE),
    0x0000 , 0x000B , 0xFFFF , S(AEE0), S(E0BF), S(B2AE), S(AFE0), S(E08D),
    S(00AE), 0x0000 , 0x0002 , 0x00D5 , S(AFE0), 0x0000 , 0xA9AE , 0x00D9 ,
    0x00E0 , 0x0000 , 0x0008 , 0xFFFF , S(AFE0), S(E081), S(9FAE), S(AEE0),
    0x0000 , 0x0002 , 0x00E4 , S(AEE0), 0x0000 , 0xBE81 , 0x0124 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x012B , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0131 , 0x0000 , 0x0008 , 0xFFFF , S(AFE0),
    S(E086), S(A9AE), S(AFE0), 0x0000 , 0x0005 , 0xFFFF , S(AEE0), S(E0BF),
    S(00AE), 0x0000 , 0x000E , 0xFFFD , S(AFE0), S(E086), S(B2AE), S(AFE0),
    S(E08D), S(B2AE), S(AEE0), 0x0000 , 0x000B , 0xFFFD , S(AEE0), S(E0AA),
    S(9FAE), S(AFE0), S(E08D), S(00AE), 0x0000 , 0x0002 , 0x0148 , S(AEE0),
    0x0000 , 0x86BE , 0x014C , 0x0150 , 0x0000 , 0x0002 , 0xFFFF , S(AFE0),
    0x0000 , 0x0005 , 0x0156 , S(AEE0), S(E0A4), S(00AE), 0x0003 , 0x0009 ,
    0xFFFD , S(AEE0), S(E0AA), S(9FAE), S(AEE0), S(00BF), 0x0000 , 0x0002 ,
    0x0162 , S(AEE0), 0x0000 , 0x88BF , 0x0166 , 0x016D , 0x0000 , 0x0008 ,
    0xFFFF , S(AFE0), S(E081), S(9FAE), S(AFE0), 0x0000 , 0x0002 , 0x0171 ,
    S(AEE0), 0x0000 , 0x95B0 , 0x0175 , 0x017B , 0x0000 , 0x0005 , 0xFFFF ,
    S(AEE0), S(E0BE), S(00AE), 0x0000 , 0x0008 , 0xFFFD , S(AEE0), S(E095),
    S(81AF), S(AEE0), 0x0000 , 0x0008 , 0xFFFE , S(AEE0), S(E0B2), S(8DAF),
    S(AEE0), 0x0000 , 0x000B , 0xFFFF , S(AFE0), S(E081), S(B3AE), S(AFE0),
    S(E08D), S(00AE), 0x0000 , 0x0002 , 0x0196 , S(AEE0), 0x0000 , 0xB195 ,
    0x0175 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x01B5 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x01BB , 0x0000 , 0x0005 , 0xFFFD ,
    S(AEE0), S(E0AA), S(00AE), 0x0000 , 0x0005 , 0x01C1 , S(AFE0), S(E08D),
    S(00AE), 0x0000 , 0xA9B1 , 0x012B , 0x01B5
};

static const unsigned short a_14[] = {
    0x0000 , 0x9F80 , 0x0022 , 0x0026 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0063 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0075 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0104 , 0x0000 , 0x0002 , 0xFFF9 , S(AFE0), 0x0000 , 0x0002 ,
    0x002A , S(AFE0), 0x0000 , 0xB19F , 0x003F , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x004B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x005A , 0x0000 ,
    0x0002 , 0x0043 , S(AEE0), 0x0000 , 0x8A8B , 0x0047 , 0x0047 , 0x0000 ,
    0x0002 , 0xFFFE , S(AFE0), 0x0000 , 0x0002 , 0x004F , S(AEE0), 0x0006 ,
    0x000F , 0xFFFE , S(AEE0), S(E0BF), S(B0AE), S(AFE0), S(E081), S(A8AE),
    S(AFE0), S(008D), 0x0000 , 0x000B , 0xFFFE , S(AEE0), S(E0BF), S(A9AE),
    S(AFE0), S(E08D), S(00AE), 0x0000 , 0x0002 , 0x0067 , S(AFE0), 0x0000 ,
    0x9FA9 , 0x006B , 0x0071 , 0x0000 , 0x0005 , 0xFFFE , S(AFE0), S(E081),
    S(00AE), 0x0000 , 0x0002 , 0xFFFF , S(AEE0), 0x0000 , 0x0002 , 0x0079 ,
    S(AFE0), 0x0000 , 0xB4A3 , 0x008D , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0093 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00A5 , 0x0000 ,
    0x0000 , 0x00AC , 0x00B8 , 0x006B , 0x00FD , 0x0000 , 0x0005 , 0xFFFF ,
    S(AEE0), S(E095), S(00AE), 0x0000 , 0x0002 , 0x0097 , S(AEE0), 0x0000 ,
    0x81BF , 0x009B , 0x00A1 , 0x0000 , 0x0005 , 0xFFFF , S(AEE0), S(E0AE),
    S(00AF), 0x0000 , 0x0002 , 0xFFFD , S(AEE0), 0x0000 , 0x0008 , 0xFFFC ,
    S(AEE0), S(E0BF), S(9FAE), S(AEE0), 0x0000 , 0x0002 , 0x00B0 , S(AEE0),
    0x0000 , 0x87BF , 0x009B , 0x00B4 , 0x0000 , 0x0002 , 0xFFFE , S(AEE0),
    0x0000 , 0x0002 , 0x00BC , S(AEE0), 0x0005 , 0xBF87 , 0x009B , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00F7 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00B4 , 0x00B4 , 0x0000 ,
    0x0005 , 0xFFFE , S(AEE0), S(E0BE), S(00AE), 0x0000 , 0x0008 , 0xFFFF ,
    S(AEE0), S(E095), S(80AF), S(AEE0), 0x0000 , 0x0008 , 0xFFFE , S(AEE0),
    S(E0B5), S(BFAE), S(AEE0)
};

static const unsigned short a_15[] = {
    0x0000 , 0x9485 , 0x0012 , 0x0012 , 0x0012 , 0x0012 , 0x0012 , 0x0012 ,
    0x0000 , 0x0000 , 0x0000 , 0x0012 , 0x0012 , 0x0012 , 0x0000 , 0x0012 ,
    0x0012 , 0x0012 , 0x0000 , 0x0002 , 0xC001 , S(AEE0)
};

static const unsigned short a_16[] = {
    0x0000 , 0xBE81 , 0x0040 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x007A , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0086 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0082 , 0x0082 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0082 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x015B ,
    0x0000 , 0x0002 , 0x0044 , S(AFE0), 0x0000 , 0xB195 , 0x0063 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0067 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x006D , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0071 , 0x0000 , 0x0002 , 0xFFFA , S(AEE0), 0x0000 ,
    0x0005 , 0xFFFF , S(AEE0), S(E0AA), S(00AE), 0x0000 , 0x0002 , 0xFFFD ,
    S(AEE0), 0x0000 , 0x000B , 0xFFFF , S(AEE0), S(E0BF), S(B1AE), S(AFE0),
    S(E08D), S(00AE), 0x0000 , 0x0002 , 0x007E , S(AFE0), 0x0000 , 0xA9B5 ,
    0x0082 , 0x0082 , 0x0000 , 0x0002 , 0xFFFF , S(AEE0), 0x0000 , 0x0002 ,
    0x008A , S(AFE0), 0x0000 , 0xB3A9 , 0x0097 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x00F4 , 0x015F , 0x0165 , 0x0000 , 0x0000 , 0x01CB , 0x0000 ,
    0x0002 , 0x009B , S(AEE0), 0x0000 , 0xBF86 , 0x00D7 , 0x00DD , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0082 , 0x0082 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00E1 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00E5 , 0x00EE , 0x0000 ,
    0x0005 , 0xFFFF , S(AEE0), S(E0A9), S(00AF), 0x0000 , 0x0002 , 0xFFFB ,
    S(AFE0), 0x0000 , 0x0002 , 0xFFFE , S(AEE0), 0x0000 , 0x0002 , 0x00E9 ,
    S(AEE0), 0x0004 , 0x0003 , 0xFFFF , S(AEE0), S(00A9), 0x0000 , 0x0005 ,
    0xFFFF , S(AEE0), S(E0AE), S(00AE), 0x0000 , 0x0002 , 0x00F8 , S(AEE0),
    0x0000 , 0xBE81 , 0x0138 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00DD ,
    0x00DD , 0x0000 , 0x0000 , 0x0000 , 0x00DD , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0082 , 0x0082 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x015B ,
    0x0000 , 0x0002 , 0x013C , S(AFE0), 0x0000 , 0xB195 , 0x0082 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x015B , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0082 , 0x0000 , 0x0002 , 0xFFFB , S(AEE0), 0x0000 ,
    0x0005 , 0xFFFB , S(AEE0), S(E0BE), S(00AE), 0x0000 , 0x0002 , 0x0169 ,
    S(AEE0), 0x0000 , 0xBF80 , 0x00DD , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x0082 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x01AB , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0082 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x01B1 , 0x01B9 , 0x0000 , 0x0005 , 0xFFFB , S(AFE0), S(E080),
    S(00AE), 0x0000 , 0x0002 , 0x01B5 , S(AEE0), 0x0005 , 0xA9AE , 0x0082 ,
    0x0082 , 0x0000 , 0x0002 , 0x01BD , S(AEE0), 0x0000 , 0x9FA9 , 0x01C1 ,
    0x015B , 0x0000 , 0x000E , 0xFFFF , S(AEE0), S(E095), S(8AAF), S(AEE0),
    S(E0A3), S(8DAF), S(AEE0), 0x0000 , 0x0002 , 0x01CF , S(AEE0), 0x0000 ,
    0xBEA9 , 0x0082 , 0x0082 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x01E7 , 0x0000 ,
    0x0002 , 0x01EB , S(AEE0), 0x0005 , 0x0003 , 0xFFFF , S(AEE0), S(00A9)
};

static const unsigned short a_17[] = {
    0x0000 , 0x8DB1 , 0x0004 , 0x0029 , 0x0000 , 0x0005 , 0x000A , S(AEE0),
    S(E0B1), S(00AF), 0x0000 , 0x8DBF , 0x000E , 0x0023 , 0x0000 , 0x0008 ,
    0x0015 , S(AEE0), S(E0BF), S(A9AE), S(AFE0), 0x0000 , 0x95A8 , 0x0019 ,
    0x001D , 0x0000 , 0x0002 , 0xC001 , S(AEE0), 0x0000 , 0x0005 , 0xC001 ,
    S(AEE0), S(E0BE), S(00AE), 0x0000 , 0x0005 , 0xC001 , S(AEE0), S(E095),
    S(00AE), 0x0000 , 0x0002 , 0x002D , S(AEE0), 0x0000 , 0x8DBF , 0x0031 ,
    0x0023 , 0x0000 , 0x0008 , 0x0038 , S(AEE0), S(E0BF), S(A9AE), S(AFE0),
    0x0000 , 0x95A8 , 0x0019 , 0x001D
};

static int r_fix_va_start(struct SN_env * z) {
    int among_var;
    z->bra = z->c;
    if (z->c + 5 >= z->l || z->p[z->c + 5] >> 5 != 4 || !((3078 >> (z->p[z->c + 5] & 0x1f)) & 1)) return 0;
    among_var = find_among(z, a_0);
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
        among_var = find_among_b(z, a_5);
        if (!among_var) goto lab0;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 2:
                {
                    int v_2 = z->l - z->c;
                    if (!find_among_b(z, a_2)) goto lab0;
                    z->c = z->l - v_2;
                }
                {
                    int ret = slice_del(z);
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
                if (!(eq_s_b(z, 3, s_7))) goto lab1;
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
                    if (!find_among_b(z, a_3)) goto lab2;
                    goto lab0;
                lab2:
                    z->c = z->l - v_3;
                }
                {
                    int ret = slice_del(z);
                    if (ret < 0) return ret;
                }
                break;
            case 9:
                if (z->c - 2 <= z->lb || (z->p[z->c - 1] != 136 && z->p[z->c - 1] != 141)) among_var = 2; else
                among_var = find_among_b(z, a_4);
                switch (among_var) {
                    case 1:
                        {
                            int ret = slice_del(z);
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
        if (!(eq_s_b(z, 3, s_9))) return 0;
        do {
            int v_4 = z->l - z->c;
            if (!find_among_b(z, a_6)) goto lab3;
            {
                int v_5 = z->l - z->c;
                if (!(eq_s_b(z, 3, s_9))) { z->c = z->l - v_5; goto lab4; }
                if (!find_among_b(z, a_6)) { z->c = z->l - v_5; goto lab4; }
            lab4:
                ;
            }
            z->bra = z->c;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab3:
            z->c = z->l - v_4;
            if (!find_among_b(z, a_7)) goto lab5;
            z->bra = z->c;
            if (!(eq_s_b(z, 3, s_9))) goto lab5;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
            break;
        lab5:
            z->c = z->l - v_4;
            {
                int v_6 = z->l - z->c;
                if (!find_among_b(z, a_8)) return 0;
                z->c = z->l - v_6;
            }
            z->bra = z->c;
            {
                int ret = slice_del(z);
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
        if (!(eq_s(z, 3, s_10))) goto lab0;
        if (!find_among(z, a_1)) goto lab0;
        if (!(eq_s(z, 3, s_9))) goto lab0;
        z->ket = z->c;
        {
            int ret = slice_del(z);
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
        if (z->c + 2 >= z->l || z->p[z->c + 2] >> 5 != 4 || !((672 >> (z->p[z->c + 2] & 0x1f)) & 1)) goto lab1;
        if (!find_among(z, a_9)) goto lab1;
        if (!find_among(z, a_1)) goto lab1;
        if (!(eq_s(z, 3, s_9))) goto lab1;
        z->ket = z->c;
        {
            int ret = slice_del(z);
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
        if (!find_among_b(z, a_11)) goto lab3;
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
        among_var = find_among_b(z, a_13);
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
                    if (!find_among_b(z, a_3)) goto lab6;
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
                    int ret = slice_del(z);
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
                if (z->c - 2 <= z->lb || z->p[z->c - 1] >> 5 != 4 || !((-2147475197 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab8;
                among_var = find_among_b(z, a_14);
                if (!among_var) goto lab8;
                z->bra = z->c;
                switch (among_var) {
                    case 1:
                        {
                            int ret = slice_del(z);
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
                        if (!(eq_s_b(z, 3, s_8))) goto lab9;
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
                            if (!find_among_b(z, a_3)) goto lab10;
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
                            if (!find_among_b(z, a_3)) goto lab11;
                            goto lab8;
                        lab11:
                            z->c = z->l - v_15;
                        }
                        {
                            int ret = slice_del(z);
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
                if (!(eq_s_b(z, 3, s_7))) goto lab7;
                do {
                    int v_17 = z->l - z->c;
                    {
                        int v_18 = z->l - z->c;
                        if (!find_among_b(z, a_6)) goto lab13;
                        goto lab12;
                    lab13:
                        z->c = z->l - v_18;
                    }
                    break;
                lab12:
                    z->c = z->l - v_17;
                    {
                        int v_19 = z->l - z->c;
                        if (!find_among_b(z, a_6)) goto lab7;
                        if (!(eq_s_b(z, 3, s_9))) goto lab7;
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
        if (z->c - 8 <= z->lb || z->p[z->c - 1] != 141) goto lab15;
        among_var = find_among_b(z, a_10);
        if (!among_var) goto lab15;
        z->bra = z->c;
        switch (among_var) {
            case 1:
                do {
                    int v_22 = z->l - z->c;
                    if (!find_among_b(z, a_6)) goto lab16;
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
                    int ret = slice_del(z);
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
        if (z->c - 5 <= z->lb || z->p[z->c - 1] != 191) goto lab17;
        if (!find_among_b(z, a_12)) goto lab17;
        z->bra = z->c;
        {
            int ret = slice_del(z);
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
                    among_var = find_among_b(z, a_16);
                    if (!among_var) goto lab20;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 2:
                            {
                                int v_28 = z->l - z->c;
                                if (z->c - 2 <= z->lb || z->p[z->c - 1] >> 5 != 4 || !((1951712 >> (z->p[z->c - 1] & 0x1f)) & 1)) goto lab21;
                                if (!find_among_b(z, a_15)) goto lab21;
                                goto lab20;
                            lab21:
                                z->c = z->l - v_28;
                            }
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 3:
                            {
                                int v_29 = z->l - z->c;
                                if (!find_among_b(z, a_3)) goto lab22;
                                goto lab20;
                            lab22:
                                z->c = z->l - v_29;
                            }
                            {
                                int ret = slice_del(z);
                                if (ret < 0) return ret;
                            }
                            break;
                        case 4:
                            if (!(eq_s_b(z, 3, s_14))) goto lab23;
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
                                if (!(eq_s_b(z, 3, s_9))) goto lab20;
                                z->c = z->l - v_30;
                            }
                            {
                                int ret = slice_del(z);
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
                if (z->c - 8 <= z->lb || (z->p[z->c - 1] != 141 && z->p[z->c - 1] != 177)) goto lab24;
                if (!find_among_b(z, a_17)) goto lab24;
                z->bra = z->c;
                {
                    int ret = slice_del(z);
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

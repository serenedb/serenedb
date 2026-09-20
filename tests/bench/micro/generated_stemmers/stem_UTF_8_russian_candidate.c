/* Generated from russian.sbl by Snowball 3.1.1 - https://snowballstem.org/ */

#include "stem_UTF_8_russian_candidate.h"

#include <stddef.h>

#include "runtime/snowball_runtime.h"

#ifdef SNOWBALL_BIGENDIAN
#define S(W) ((0x##W & 0xff) << 8 | 0x##W >> 8)
#else
#define S(W) (0x##W)
#endif

#ifdef __cplusplus
extern "C" {
#endif
extern int candidate_russian_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif

static const symbol s_0[] = { 0xD1, 0x91 };
static const symbol s_1[] = { 0xD0, 0xB5 };
static const symbol s_2[] = { 0xD0, 0xB0 };
static const symbol s_3[] = { 0xD1, 0x8F };
static const symbol s_4[] = { 0xD0, 0xB8 };
static const symbol s_5[] = { 0xD0, 0xBD };

static const unsigned short a_0[] = {
    0x0000 , 0xB88C , 0x002F , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0041 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0048 , 0x0000 ,
    0x0009 , 0x0037 , S(B2D0), S(88D1), S(B8D0), S(81D1), S(00D1), 0x0001 ,
    0x8BB8 , 0x003B , 0x003E , 0x0000 , 0xD1D1 , 0xFFFE , 0x0000 , 0xD0D0 ,
    0xFFFE , 0x0000 , 0xD0D0 , 0x0044 , 0x0001 , 0x8BB8 , 0x003B , 0x003E ,
    0x0000 , 0x0005 , 0x0044 , S(B2D0), S(88D1), S(00D0)
};

static const unsigned short a_1[] = {
    0x0000 , 0xBE83 , 0x003E , 0x0000 , 0x004A , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0054 , 0x0095 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x009C , 0x0000 , 0x0000 , 0x00D5 ,
    0x00DE , 0x0000 , 0x0000 , 0x00DE , 0x0000 , 0x00E1 , 0x0000 , 0x0003 ,
    0x0043 , S(BCD0), S(00D1), 0x0000 , 0xB5BE , 0x0047 , 0x0047 , 0x0000 ,
    0xD0D0 , 0xFFFF , 0x0000 , 0xD1D1 , 0x004D , 0x0000 , 0x8BB8 , 0x0051 ,
    0x0047 , 0x0000 , 0xD1D1 , 0xFFFF , 0x0000 , 0xD1D1 , 0x0057 , 0x0000 ,
    0xBE83 , 0x0051 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0051 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0047 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0047 , 0x0000 , 0xD1D1 , 0x0098 ,
    0x0000 , 0x8FB0 , 0x0051 , 0x0047 , 0x0000 , 0xD0D0 , 0x009F , 0x0000 ,
    0xBE8B , 0x0051 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0047 , 0x0000 , 0x0000 , 0x0047 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0047 , 0x0000 , 0x0003 , 0x00DA ,
    S(BCD0), S(00D0), 0x0000 , 0x8BB8 , 0x0051 , 0x0047 , 0x0000 , 0xD0D0 ,
    0x009F , 0x0000 , 0x0003 , 0x00E6 , S(B3D0), S(00D0), 0x0000 , 0xB5BE ,
    0x0047 , 0x0047
};

static const unsigned short a_2[] = {
    0x0000 , 0xBD88 , 0x0038 , 0x0047 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0052 , 0x0057 ,
    0x0000 , 0x0003 , 0x003D , S(B2D0), S(00D1), 0x0001 , 0x8BB8 , 0x0041 ,
    0x0044 , 0x0000 , 0xD1D1 , 0xFFFE , 0x0000 , 0xD0D0 , 0xFFFE , 0x0000 ,
    0xD1D1 , 0x004A , 0x0001 , 0x0002 , 0x004E , S(8ED1), 0x0001 , 0x0002 ,
    0xFFFE , S(83D1), 0x0000 , 0x0003 , 0xFFFF , S(B5D0), S(00D0), 0x0000 ,
    0x0003 , 0xFFFF , S(BDD0), S(00D0)
};

static const unsigned short a_3[] = {
    0x0000 , 0x8C8F , 0x0004 , 0x0004 , 0x0000 , 0x0003 , 0xFFFF , S(81D1),
    S(00D1)
};

static const unsigned short a_4[] = {
    0x0000 , 0xBE82 , 0x003F , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0082 , 0x008B , 0x0000 , 0x00A3 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x00AA , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x00B7 , 0x0000 , 0x0000 ,
    0x00CA , 0x00CF , 0x0000 , 0x00B1 , 0x00D2 , 0x00B4 , 0x0105 , 0x0000 ,
    0xD1D1 , 0x0042 , 0x0000 , 0xB88B , 0x0072 , 0x0000 , 0x0000 , 0x0075 ,
    0x0072 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x007C , 0x0000 ,
    0x0000 , 0x007F , 0x0000 , 0xD1D1 , 0xFFFE , 0x0000 , 0xD1D1 , 0x0078 ,
    0x0001 , 0x0002 , 0xFFFE , S(83D1), 0x0000 , 0xD0D0 , 0x0078 , 0x0000 ,
    0xD0D0 , 0xFFFE , 0x0000 , 0x0003 , 0x0087 , S(BDD0), S(00D1), 0x0001 ,
    0x0002 , 0xFFFE , S(B5D0), 0x0000 , 0xD1D1 , 0x008E , 0x0000 , 0x8288 ,
    0x0092 , 0x0099 , 0x0000 , 0xD1D1 , 0x0095 , 0x0001 , 0x8BB8 , 0x0072 ,
    0x007F , 0x0000 , 0xD1D1 , 0x009C , 0x0000 , 0xB5B8 , 0x00A0 , 0x007F ,
    0x0000 , 0xD0D0 , 0xFFFF , 0x0000 , 0xD1D1 , 0x00A6 , 0x0002 , 0x0002 ,
    0xFFFE , S(83D1), 0x0000 , 0xD0D0 , 0x00AD , 0x0000 , 0xBBBD , 0x00B1 ,
    0x00B4 , 0x0000 , 0xD0D0 , 0x0095 , 0x0000 , 0xD0D0 , 0x0087 , 0x0000 ,
    0x0003 , 0x00BC , S(82D1), S(00D0), 0x0000 , 0xB9B5 , 0x00A0 , 0x0000 ,
    0x0000 , 0x007F , 0x00C3 , 0x0000 , 0xD0D0 , 0x00C6 , 0x0001 , 0x83B5 ,
    0x0072 , 0x007F , 0x0000 , 0x0003 , 0x0095 , S(BBD0), S(00D0), 0x0000 ,
    0xD0D0 , 0x00C6 , 0x0000 , 0xD0D0 , 0x00D5 , 0x0000 , 0xB88B , 0x0072 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x00A0 , 0x0000 , 0x0000 , 0x007F , 0x0000 , 0xD0D0 , 0x0108 ,
    0x0000 , 0xBBBD , 0x00B1 , 0x010C , 0x0000 , 0xD0D0 , 0x010F , 0x0001 ,
    0xB5BD , 0x007F , 0x00A0
};

static const unsigned short a_5[] = {
    0x0000 , 0xBE83 , 0x003E , 0x0000 , 0x0041 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x003E , 0x003E , 0x0000 , 0x0052 , 0x0059 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x004F ,
    0x0000 , 0x005C , 0x0000 , 0x0000 , 0x0063 , 0x0000 , 0x0000 , 0x0066 ,
    0x007D , 0x0000 , 0x0000 , 0x008F , 0x0000 , 0x004F , 0x0000 , 0xD1D1 ,
    0xFFFF , 0x0000 , 0xD1D1 , 0x0044 , 0x0000 , 0x8FB0 , 0x0048 , 0x004F ,
    0x0000 , 0xD1D1 , 0x004B , 0x0001 , 0x0002 , 0xFFFF , S(B8D0), 0x0000 ,
    0xD0D0 , 0xFFFF , 0x0000 , 0xD1D1 , 0x0055 , 0x0001 , 0x8CB8 , 0x003E ,
    0x004F , 0x0000 , 0xD1D1 , 0x0055 , 0x0000 , 0xD0D0 , 0x005F , 0x0000 ,
    0xB5BE , 0x004F , 0x004F , 0x0000 , 0xD0D0 , 0x0055 , 0x0000 , 0xD0D0 ,
    0x0069 , 0x0001 , 0xBCB5 , 0x004F , 0x0000 , 0x0000 , 0x004F , 0x0000 ,
    0x0000 , 0x0000 , 0x0073 , 0x0000 , 0xD0D0 , 0x0076 , 0x0000 , 0x8FB0 ,
    0x007A , 0x004F , 0x0000 , 0xD1D1 , 0x004B , 0x0000 , 0xD0D0 , 0x0080 ,
    0x0001 , 0xBEB5 , 0x008C , 0x0000 , 0x0000 , 0x004F , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x004F , 0x0000 , 0xD0D0 , 0x004B , 0x0000 ,
    0xD0D0 , 0x0092 , 0x0000 , 0xBE8F , 0x007A , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x004F , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x008C , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x004F
};

static const unsigned short a_6[] = {
    0x0000 , 0x828C , 0x0004 , 0x000A , 0x0000 , 0x0005 , 0xFFFF , S(BED0),
    S(81D1), S(00D1), 0x0000 , 0x0007 , 0xFFFF , S(BED0), S(81D1), S(82D1),
    S(00D1)
};

static const unsigned short a_7[] = {
    0x0000 , 0xBD88 , 0x0038 , 0x0000 , 0x0000 , 0x0000 , 0x003E , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0041 ,
    0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0000 , 0x0048 ,
    0x0000 , 0x0005 , 0xFFFF , S(B5D0), S(B9D0), S(00D1), 0x0000 , 0xD1D1 ,
    0xFFFD , 0x0000 , 0x0007 , 0xFFFF , S(B5D0), S(B9D0), S(88D1), S(00D0),
    0x0000 , 0xD0D0 , 0xFFFE
};

static const unsigned char g_v[] = { 33, 65, 8, 232 };

extern int candidate_russian_UTF_8_stem(struct SN_env * z) {
    int among_var;
    int i_p2;
    int i_pV;
    {
        int v_1 = z->c;
        while (1) {
            int v_2 = z->c;
            while (1) {
                int v_3 = z->c;
                z->bra = z->c;
                if (!(eq_s(z, 2, s_0))) goto lab2;
                z->ket = z->c;
                z->c = v_3;
                break;
            lab2:
                z->c = v_3;
                {
                    int ret = skip_utf8(z->p, z->c, z->l, 1);
                    if (ret < 0) goto lab1;
                    z->c = ret;
                }
            }
            {
                int ret = slice_from_s(z, 2, s_1);
                if (ret < 0) return ret;
            }
            continue;
        lab1:
            z->c = v_2;
            break;
        }
        z->c = v_1;
    }
    i_pV = z->l;
    i_p2 = z->l;
    {
        int v_4 = z->c;
        {
            int ret = out_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        i_pV = z->c;
        {
            int ret = in_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        {
            int ret = out_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        {
            int ret = in_grouping_U(z, g_v, 1072, 1103, 1);
            if (ret < 0) goto lab4;
            z->c += ret;
        }
        i_p2 = z->c;
    lab4:
        z->c = v_4;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_5;
        if (z->c < i_pV) return 0;
        v_5 = z->lb; z->lb = i_pV;
        {
            int v_6 = z->l - z->c;
            do {
                int v_7 = z->l - z->c;
                z->ket = z->c;
                among_var = find_among_b(z, a_0);
                if (!among_var) goto lab6;
                z->bra = z->c;
                switch (among_var) {
                    case 1:
                        do {
                            if (!(eq_s_b(z, 2, s_2))) goto lab7;
                            break;
                        lab7:
                            if (!(eq_s_b(z, 2, s_3))) goto lab6;
                        } while (0);
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
                        break;
                }
                break;
            lab6:
                z->c = z->l - v_7;
                {
                    int v_8 = z->l - z->c;
                    z->ket = z->c;
                    if (z->c - 3 <= z->lb || (z->p[z->c - 1] != 140 && z->p[z->c - 1] != 143)) { z->c = z->l - v_8; goto lab8; }
                    if (!find_among_b(z, a_3)) { z->c = z->l - v_8; goto lab8; }
                    z->bra = z->c;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                lab8:
                    ;
                }
                do {
                    int v_9 = z->l - z->c;
                    z->ket = z->c;
                    if (!find_among_b(z, a_1)) goto lab9;
                    z->bra = z->c;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    {
                        int v_10 = z->l - z->c;
                        z->ket = z->c;
                        among_var = find_among_b(z, a_2);
                        if (!among_var) { z->c = z->l - v_10; goto lab10; }
                        z->bra = z->c;
                        switch (among_var) {
                            case 1:
                                do {
                                    if (!(eq_s_b(z, 2, s_2))) goto lab11;
                                    break;
                                lab11:
                                    if (!(eq_s_b(z, 2, s_3))) { z->c = z->l - v_10; goto lab10; }
                                } while (0);
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
                                break;
                        }
                    lab10:
                        ;
                    }
                    break;
                lab9:
                    z->c = z->l - v_9;
                    z->ket = z->c;
                    among_var = find_among_b(z, a_4);
                    if (!among_var) goto lab12;
                    z->bra = z->c;
                    switch (among_var) {
                        case 1:
                            do {
                                if (!(eq_s_b(z, 2, s_2))) goto lab13;
                                break;
                            lab13:
                                if (!(eq_s_b(z, 2, s_3))) goto lab12;
                            } while (0);
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
                            break;
                    }
                    break;
                lab12:
                    z->c = z->l - v_9;
                    z->ket = z->c;
                    if (!find_among_b(z, a_5)) goto lab5;
                    z->bra = z->c;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                } while (0);
            } while (0);
        lab5:
            z->c = z->l - v_6;
        }
        {
            int v_11 = z->l - z->c;
            z->ket = z->c;
            if (!(eq_s_b(z, 2, s_4))) { z->c = z->l - v_11; goto lab14; }
            z->bra = z->c;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
        lab14:
            ;
        }
        {
            int v_12 = z->l - z->c;
            z->ket = z->c;
            if (z->c - 5 <= z->lb || (z->p[z->c - 1] != 130 && z->p[z->c - 1] != 140)) goto lab15;
            if (!find_among_b(z, a_6)) goto lab15;
            z->bra = z->c;
            if (i_p2 > z->c) goto lab15;
            {
                int ret = slice_del(z);
                if (ret < 0) return ret;
            }
        lab15:
            z->c = z->l - v_12;
        }
        {
            int v_13 = z->l - z->c;
            z->ket = z->c;
            among_var = find_among_b(z, a_7);
            if (!among_var) goto lab16;
            z->bra = z->c;
            switch (among_var) {
                case 1:
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    z->ket = z->c;
                    if (!(eq_s_b(z, 2, s_5))) goto lab16;
                    z->bra = z->c;
                    if (!(eq_s_b(z, 2, s_5))) goto lab16;
                    {
                        int ret = slice_del(z);
                        if (ret < 0) return ret;
                    }
                    break;
                case 2:
                    if (!(eq_s_b(z, 2, s_5))) goto lab16;
                    {
                        int ret = slice_del(z);
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
        lab16:
            z->c = z->l - v_13;
        }
        z->lb = v_5;
    }
    z->c = z->lb;
    return 1;
}

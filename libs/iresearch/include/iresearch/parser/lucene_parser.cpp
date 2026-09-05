/* clang-format off */
/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 42 "libs/iresearch/include/iresearch/parser/lucene_parser.y"

#include "parser.hpp"

#pragma clang diagnostic ignored "-Wunused-but-set-variable"

#line 77 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "lucene_parser.hpp"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_TERM = 3,                       /* TERM  */
  YYSYMBOL_REGEX = 4,                      /* REGEX  */
  YYSYMBOL_PREFIX = 5,                     /* PREFIX  */
  YYSYMBOL_WILDCARD = 6,                   /* WILDCARD  */
  YYSYMBOL_STAR = 7,                       /* STAR  */
  YYSYMBOL_NUMBER = 8,                     /* NUMBER  */
  YYSYMBOL_FLOAT = 9,                      /* FLOAT  */
  YYSYMBOL_GAP = 10,                       /* GAP  */
  YYSYMBOL_AND = 11,                       /* AND  */
  YYSYMBOL_OR = 12,                        /* OR  */
  YYSYMBOL_NOT = 13,                       /* NOT  */
  YYSYMBOL_TO = 14,                        /* TO  */
  YYSYMBOL_LPAREN = 15,                    /* LPAREN  */
  YYSYMBOL_RPAREN = 16,                    /* RPAREN  */
  YYSYMBOL_LBRACKET = 17,                  /* LBRACKET  */
  YYSYMBOL_RBRACKET = 18,                  /* RBRACKET  */
  YYSYMBOL_LBRACE = 19,                    /* LBRACE  */
  YYSYMBOL_RBRACE = 20,                    /* RBRACE  */
  YYSYMBOL_COLON = 21,                     /* COLON  */
  YYSYMBOL_CARET = 22,                     /* CARET  */
  YYSYMBOL_PLUS = 23,                      /* PLUS  */
  YYSYMBOL_MINUS = 24,                     /* MINUS  */
  YYSYMBOL_AT = 25,                        /* AT  */
  YYSYMBOL_QUOTE = 26,                     /* QUOTE  */
  YYSYMBOL_LT = 27,                        /* LT  */
  YYSYMBOL_LE = 28,                        /* LE  */
  YYSYMBOL_GT = 29,                        /* GT  */
  YYSYMBOL_GE = 30,                        /* GE  */
  YYSYMBOL_EQ = 31,                        /* EQ  */
  YYSYMBOL_FUZZY = 32,                     /* FUZZY  */
  YYSYMBOL_FN_NGRAM = 33,                  /* FN_NGRAM  */
  YYSYMBOL_FN_PHRASE = 34,                 /* FN_PHRASE  */
  YYSYMBOL_FN_WILDCARD = 35,               /* FN_WILDCARD  */
  YYSYMBOL_FN_FUZZY = 36,                  /* FN_FUZZY  */
  YYSYMBOL_FN_OR = 37,                     /* FN_OR  */
  YYSYMBOL_FN_UNORDERED = 38,              /* FN_UNORDERED  */
  YYSYMBOL_FN_ATLEAST = 39,                /* FN_ATLEAST  */
  YYSYMBOL_FN_ORDERED = 40,                /* FN_ORDERED  */
  YYSYMBOL_FN_MAXGAPS = 41,                /* FN_MAXGAPS  */
  YYSYMBOL_FN_MAXWIDTH = 42,               /* FN_MAXWIDTH  */
  YYSYMBOL_FN_OTHER = 43,                  /* FN_OTHER  */
  YYSYMBOL_YYACCEPT = 44,                  /* $accept  */
  YYSYMBOL_query = 45,                     /* query  */
  YYSYMBOL_clause_list = 46,               /* clause_list  */
  YYSYMBOL_mod_clause = 47,                /* mod_clause  */
  YYSYMBOL_term_expr = 48,                 /* term_expr  */
  YYSYMBOL_field_prefix = 49,              /* field_prefix  */
  YYSYMBOL_field_name = 50,                /* field_name  */
  YYSYMBOL_boosted_expr = 51,              /* boosted_expr  */
  YYSYMBOL_modified_term = 52,             /* modified_term  */
  YYSYMBOL_base_term = 53,                 /* base_term  */
  YYSYMBOL_group = 54,                     /* group  */
  YYSYMBOL_55_1 = 55,                      /* @1  */
  YYSYMBOL_phrase = 56,                    /* phrase  */
  YYSYMBOL_57_2 = 57,                      /* $@2  */
  YYSYMBOL_phrase_body = 58,               /* phrase_body  */
  YYSYMBOL_59_3 = 59,                      /* $@3  */
  YYSYMBOL_phrase_part = 60,               /* phrase_part  */
  YYSYMBOL_ngram_expr = 61,                /* ngram_expr  */
  YYSYMBOL_62_4 = 62,                      /* $@4  */
  YYSYMBOL_63_5 = 63,                      /* $@5  */
  YYSYMBOL_64_6 = 64,                      /* $@6  */
  YYSYMBOL_65_7 = 65,                      /* $@7  */
  YYSYMBOL_66_8 = 66,                      /* $@8  */
  YYSYMBOL_67_9 = 67,                      /* $@9  */
  YYSYMBOL_68_10 = 68,                     /* $@10  */
  YYSYMBOL_69_11 = 69,                     /* $@11  */
  YYSYMBOL_fn_terms = 70,                  /* fn_terms  */
  YYSYMBOL_fn_source = 71,                 /* fn_source  */
  YYSYMBOL_fn_args = 72,                   /* fn_args  */
  YYSYMBOL_threshold = 73,                 /* threshold  */
  YYSYMBOL_ngram_terms = 74,               /* ngram_terms  */
  YYSYMBOL_range_expr = 75,                /* range_expr  */
  YYSYMBOL_range_bound = 76                /* range_bound  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;



/* Unqualified %code blocks.  */
#line 48 "libs/iresearch/include/iresearch/parser/lucene_parser.y"

int yylex(YYSTYPE* yylval);
void yyerror(sdb::ParserContext& ctx, const char *s);

#line 193 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"

#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if !defined yyoverflow

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  61
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   555

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  44
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  33
/* YYNRULES -- Number of rules.  */
#define YYNRULES  94
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  167

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   298


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    95,    95,    99,   100,   101,   102,   106,   107,   108,
     109,   113,   114,   115,   118,   119,   120,   121,   125,   126,
     130,   138,   139,   142,   149,   153,   156,   163,   164,   165,
     166,   167,   168,   169,   170,   171,   172,   173,   174,   178,
     178,   188,   188,   193,   194,   195,   195,   200,   201,   202,
     203,   213,   213,   215,   215,   217,   218,   219,   220,   221,
     225,   225,   227,   227,   229,   229,   231,   231,   236,   236,
     238,   238,   240,   250,   251,   255,   256,   257,   258,   259,
     260,   263,   265,   266,   267,   271,   272,   276,   277,   281,
     283,   285,   287,   292,   293
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "TERM", "REGEX",
  "PREFIX", "WILDCARD", "STAR", "NUMBER", "FLOAT", "GAP", "AND", "OR",
  "NOT", "TO", "LPAREN", "RPAREN", "LBRACKET", "RBRACKET", "LBRACE",
  "RBRACE", "COLON", "CARET", "PLUS", "MINUS", "AT", "QUOTE", "LT", "LE",
  "GT", "GE", "EQ", "FUZZY", "FN_NGRAM", "FN_PHRASE", "FN_WILDCARD",
  "FN_FUZZY", "FN_OR", "FN_UNORDERED", "FN_ATLEAST", "FN_ORDERED",
  "FN_MAXGAPS", "FN_MAXWIDTH", "FN_OTHER", "$accept", "query",
  "clause_list", "mod_clause", "term_expr", "field_prefix", "field_name",
  "boosted_expr", "modified_term", "base_term", "group", "@1", "phrase",
  "$@2", "phrase_body", "$@3", "phrase_part", "ngram_expr", "$@4", "$@5",
  "$@6", "$@7", "$@8", "$@9", "$@10", "$@11", "fn_terms", "fn_source",
  "fn_args", "threshold", "ngram_terms", "range_expr", "range_bound", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-91)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-21)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     166,   200,   -91,   -91,   -91,     7,   -91,   -91,   207,   -91,
      72,    72,   207,   207,   -91,    26,    33,    38,    46,    51,
      59,    62,    68,    69,    70,    79,    55,   125,   -91,   -91,
     207,    85,   -91,    76,   -91,    86,    77,   -91,   -91,   -91,
     128,   -91,   166,   -91,   -91,   127,   129,   -91,   -91,    20,
      27,   -91,    99,   136,   -91,   -91,   137,   -91,   138,   139,
     -91,   -91,   166,   166,   -91,   -91,   -91,    72,    72,    72,
      72,   -91,    27,   142,   -91,   -91,    84,    72,    72,   120,
     -91,   -91,    28,   -91,   -91,   -91,   -91,    20,   140,   141,
     160,     8,   512,   512,   -91,   512,   113,   114,   248,   -91,
     -91,   -91,   -91,   -91,   -91,   123,   -91,   -91,   -14,    -9,
     -91,   -91,   -91,   -91,   174,    40,   -91,   -91,   -91,   162,
     -91,   -91,   -91,   -91,   -91,   -91,   -91,   289,   -91,   330,
     512,   371,   165,   167,   -91,   -91,   -91,   -91,   -91,   -91,
     -91,   -91,   -91,    20,   -91,     4,   -91,   -91,   -91,   -91,
     -91,   412,   -91,   -91,   -91,   -91,   -91,   -91,   -91,   512,
     512,   453,   494,   168,   170,   -91,   -91
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,    27,    31,    32,    33,    38,    28,    29,     0,    39,
       0,     0,     0,     0,    41,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     2,     3,     7,
       0,     0,    11,    21,    24,    36,    30,    35,    34,    25,
       0,    10,     0,    93,    94,     0,     0,     8,     9,     0,
       0,    53,     0,     0,    60,    62,     0,    66,     0,     0,
      81,     1,     0,     0,     4,    13,    18,     0,     0,     0,
       0,    19,     0,     0,    26,    12,     0,     0,     0,    47,
      48,    49,     0,    43,    85,    86,    51,     0,     0,     0,
       0,     0,     0,     0,    64,     0,     0,     0,     0,     5,
       6,    14,    15,    16,    17,    22,    37,    40,     0,     0,
      50,    45,    42,    44,     0,     0,    57,    56,    55,     0,
      58,    75,    78,    76,    77,    79,    80,     0,    73,     0,
       0,     0,     0,     0,    83,    84,    72,    82,    23,    89,
      91,    92,    90,     0,    87,     0,    54,    59,    61,    74,
      63,     0,    67,    68,    70,    46,    88,    52,    65,     0,
       0,     0,     0,     0,     0,    69,    71
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
     -91,   -91,   145,    10,     9,   -91,   -91,   -91,   -91,   -91,
     -91,   -91,     0,   -91,   101,   -91,   -63,     2,   -91,   -91,
     -91,   -91,   -91,   -91,   -91,   -91,   -90,   -80,   -91,   119,
     -91,   -91,   -10
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    42,   125,    49,    82,   143,    83,   126,   114,    87,
      92,    93,   130,    95,   159,   160,   127,   128,    98,    86,
     145,    38,    45
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      36,    46,    37,   129,   139,   131,   140,   156,    36,   141,
      37,   142,    36,    36,    37,    37,   119,    41,   137,   113,
     157,    47,    48,    79,   120,    80,    81,    36,    40,    37,
      36,    79,    37,    80,    81,    84,    85,    64,   111,    65,
     151,    50,    36,    79,    37,    80,    81,   149,    51,   149,
     111,   149,   113,    52,   112,    61,   146,   101,   102,   103,
     104,    53,    36,    36,    37,    37,    54,   108,   109,   161,
     162,   149,    99,   100,    55,    43,    36,    56,    37,    44,
     155,   149,   149,    57,    58,    59,    64,     1,     2,     3,
       4,     5,     6,     7,    60,    62,    63,     8,    72,     9,
     107,    10,    88,    11,    89,    90,    66,    12,    13,    74,
      14,    73,    67,    68,    69,    70,    71,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,     1,     2,
       3,     4,     5,     6,     7,    75,    62,    63,     8,    91,
       9,    77,    10,    78,    11,    94,    96,    97,    12,    13,
     106,    14,   110,   132,   133,   138,   116,   117,    15,    16,
      17,    18,    19,    20,    21,    22,    23,    24,    25,     1,
       2,     3,     4,     5,     6,     7,   118,   144,   147,     8,
     153,     9,   154,    10,   165,    11,   166,    76,   115,    12,
      13,   105,    14,     0,     0,     0,     0,     0,     0,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
       1,     2,     3,     4,     5,     6,     7,     0,     0,     0,
       0,   -20,     9,     0,    10,     0,    11,   -20,   -20,   -20,
     -20,   -20,    39,    14,     0,     0,     0,     0,     0,     0,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,   121,   122,   123,   124,     0,   134,   135,     0,     0,
       0,     0,     0,     0,   136,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    14,     0,     0,     0,     0,     0,
       0,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,   121,   122,   123,   124,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   148,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    14,     0,     0,     0,     0,
       0,     0,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,   121,   122,   123,   124,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   150,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    14,     0,     0,     0,
       0,     0,     0,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,   121,   122,   123,   124,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   152,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    14,     0,     0,
       0,     0,     0,     0,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    24,    25,   121,   122,   123,   124,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   158,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    14,     0,
       0,     0,     0,     0,     0,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,   121,   122,   123,   124,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   163,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    14,
       0,     0,     0,     0,     0,     0,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    24,    25,   121,   122,   123,
     124,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     164,     0,     0,     0,     0,   121,   122,   123,   124,     0,
      14,     0,     0,     0,     0,     0,     0,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    14,     0,
       0,     0,     0,     0,     0,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25
};

static const yytype_int16 yycheck[] =
{
       0,    11,     0,    93,    18,    95,    20,     3,     8,    18,
       8,    20,    12,    13,    12,    13,     8,     8,    98,    82,
      16,    12,    13,     3,    16,     5,     6,    27,    21,    27,
      30,     3,    30,     5,     6,     8,     9,    27,    10,    30,
     130,    15,    42,     3,    42,     5,     6,   127,    15,   129,
      10,   131,   115,    15,    26,     0,    16,    67,    68,    69,
      70,    15,    62,    63,    62,    63,    15,    77,    78,   159,
     160,   151,    62,    63,    15,     3,    76,    15,    76,     7,
     143,   161,   162,    15,    15,    15,    76,     3,     4,     5,
       6,     7,     8,     9,    15,    11,    12,    13,    22,    15,
      16,    17,     3,    19,     5,     6,    21,    23,    24,    32,
      26,    25,    27,    28,    29,    30,    31,    33,    34,    35,
      36,    37,    38,    39,    40,    41,    42,    43,     3,     4,
       5,     6,     7,     8,     9,     7,    11,    12,    13,     3,
      15,    14,    17,    14,    19,     8,     8,     8,    23,    24,
       8,    26,    32,    40,    40,    32,    16,    16,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,     3,
       4,     5,     6,     7,     8,     9,    16,     3,    16,    13,
      15,    15,    15,    17,    16,    19,    16,    42,    87,    23,
      24,    72,    26,    -1,    -1,    -1,    -1,    -1,    -1,    33,
      34,    35,    36,    37,    38,    39,    40,    41,    42,    43,
       3,     4,     5,     6,     7,     8,     9,    -1,    -1,    -1,
      -1,    21,    15,    -1,    17,    -1,    19,    27,    28,    29,
      30,    31,    32,    26,    -1,    -1,    -1,    -1,    -1,    -1,
      33,    34,    35,    36,    37,    38,    39,    40,    41,    42,
      43,     3,     4,     5,     6,    -1,     8,     9,    -1,    -1,
      -1,    -1,    -1,    -1,    16,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    26,    -1,    -1,    -1,    -1,    -1,
      -1,    33,    34,    35,    36,    37,    38,    39,    40,    41,
      42,    43,     3,     4,     5,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    16,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    26,    -1,    -1,    -1,    -1,
      -1,    -1,    33,    34,    35,    36,    37,    38,    39,    40,
      41,    42,    43,     3,     4,     5,     6,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    16,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    26,    -1,    -1,    -1,
      -1,    -1,    -1,    33,    34,    35,    36,    37,    38,    39,
      40,    41,    42,    43,     3,     4,     5,     6,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    16,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    26,    -1,    -1,
      -1,    -1,    -1,    -1,    33,    34,    35,    36,    37,    38,
      39,    40,    41,    42,    43,     3,     4,     5,     6,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    16,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    26,    -1,
      -1,    -1,    -1,    -1,    -1,    33,    34,    35,    36,    37,
      38,    39,    40,    41,    42,    43,     3,     4,     5,     6,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    16,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    26,
      -1,    -1,    -1,    -1,    -1,    -1,    33,    34,    35,    36,
      37,    38,    39,    40,    41,    42,    43,     3,     4,     5,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      16,    -1,    -1,    -1,    -1,     3,     4,     5,     6,    -1,
      26,    -1,    -1,    -1,    -1,    -1,    -1,    33,    34,    35,
      36,    37,    38,    39,    40,    41,    42,    43,    26,    -1,
      -1,    -1,    -1,    -1,    -1,    33,    34,    35,    36,    37,
      38,    39,    40,    41,    42,    43
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     4,     5,     6,     7,     8,     9,    13,    15,
      17,    19,    23,    24,    26,    33,    34,    35,    36,    37,
      38,    39,    40,    41,    42,    43,    45,    46,    47,    48,
      49,    50,    51,    52,    53,    54,    56,    61,    75,    32,
      21,    48,    55,     3,     7,    76,    76,    48,    48,    57,
      15,    15,    15,    15,    15,    15,    15,    15,    15,    15,
      15,     0,    11,    12,    47,    48,    21,    27,    28,    29,
      30,    31,    22,    25,    32,     7,    46,    14,    14,     3,
       5,     6,    58,    60,     8,     9,    73,    63,     3,     5,
       6,     3,    64,    65,     8,    67,     8,     8,    72,    47,
      47,    76,    76,    76,    76,    73,     8,    16,    76,    76,
      32,    10,    26,    60,    62,    58,    16,    16,    16,     8,
      16,     3,     4,     5,     6,    56,    61,    70,    71,    70,
      66,    70,    40,    40,     8,     9,    16,    71,    32,    18,
      20,    18,    20,    59,     3,    74,    16,    16,    16,    71,
      16,    70,    16,    15,    15,    60,     3,    16,    16,    68,
      69,    70,    70,    16,    16,    16,    16
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    44,    45,    46,    46,    46,    46,    47,    47,    47,
      47,    48,    48,    48,    48,    48,    48,    48,    49,    49,
      50,    51,    51,    51,    52,    52,    52,    53,    53,    53,
      53,    53,    53,    53,    53,    53,    53,    53,    53,    55,
      54,    57,    56,    58,    58,    59,    58,    60,    60,    60,
      60,    62,    61,    63,    61,    61,    61,    61,    61,    61,
      64,    61,    65,    61,    66,    61,    67,    61,    68,    61,
      69,    61,    61,    70,    70,    71,    71,    71,    71,    71,
      71,    72,    72,    72,    72,    73,    73,    74,    74,    75,
      75,    75,    75,    76,    76
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     1,     2,     3,     3,     1,     2,     2,
       2,     1,     3,     2,     3,     3,     3,     3,     2,     2,
       1,     1,     3,     4,     1,     2,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     1,     0,
       4,     0,     4,     1,     2,     0,     4,     1,     1,     1,
       2,     0,     6,     0,     5,     4,     4,     4,     4,     5,
       0,     5,     0,     5,     0,     6,     0,     5,     0,     9,
       0,     9,     4,     1,     2,     1,     1,     1,     1,     1,
       1,     0,     2,     2,     2,     1,     1,     1,     2,     5,
       5,     5,     5,     1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (ctx, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, ctx); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, sdb::ParserContext& ctx)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (ctx);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, sdb::ParserContext& ctx)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, ctx);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule, sdb::ParserContext& ctx)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)], ctx);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, ctx); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, sdb::ParserContext& ctx)
{
  YY_USE (yyvaluep);
  YY_USE (ctx);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (sdb::ParserContext& ctx)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* query: clause_list  */
#line 95 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.EndClauseList(); }
#line 1346 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 3: /* clause_list: mod_clause  */
#line 99 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddClause(sdb::Conjunction::Or); }
#line 1352 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 4: /* clause_list: clause_list mod_clause  */
#line 100 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddClause(sdb::Conjunction::Or); }
#line 1358 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 5: /* clause_list: clause_list AND mod_clause  */
#line 101 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddClause(sdb::Conjunction::And); }
#line 1364 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 6: /* clause_list: clause_list OR mod_clause  */
#line 102 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddClause(sdb::Conjunction::Or); }
#line 1370 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 7: /* mod_clause: term_expr  */
#line 106 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.last_mod = sdb::Modifier::None; }
#line 1376 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 8: /* mod_clause: PLUS term_expr  */
#line 107 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.last_mod = sdb::Modifier::Required; }
#line 1382 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 9: /* mod_clause: MINUS term_expr  */
#line 108 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.last_mod = sdb::Modifier::Not; }
#line 1388 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 10: /* mod_clause: NOT term_expr  */
#line 109 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.last_mod = sdb::Modifier::Not; }
#line 1394 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 11: /* term_expr: boosted_expr  */
#line 113 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1400 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 12: /* term_expr: STAR COLON STAR  */
#line 114 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddAll(); }
#line 1406 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 13: /* term_expr: field_prefix term_expr  */
#line 115 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1412 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 14: /* term_expr: field_name LT range_bound  */
#line 118 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange("*", (yyvsp[0].sv), false, false); }
#line 1418 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 15: /* term_expr: field_name LE range_bound  */
#line 119 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange("*", (yyvsp[0].sv), false, true); }
#line 1424 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 16: /* term_expr: field_name GT range_bound  */
#line 120 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[0].sv), "*", false, false); }
#line 1430 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 17: /* term_expr: field_name GE range_bound  */
#line 121 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[0].sv), "*", true, false); }
#line 1436 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 20: /* field_name: TERM  */
#line 130 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      if (!ctx.CheckField((yyvsp[0].sv))) {
                                        YYABORT;
                                      }
                                    }
#line 1446 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 21: /* boosted_expr: modified_term  */
#line 138 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1452 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 22: /* boosted_expr: modified_term CARET threshold  */
#line 139 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyvsp[-2].filter)->SetBoost((yyvsp[0].fnum)); (yyval.filter) = (yyvsp[-2].filter); }
#line 1458 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 23: /* boosted_expr: modified_term CARET threshold FUZZY  */
#line 143 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyvsp[-3].filter)->SetBoost((yyvsp[-1].fnum));
                                      (yyval.filter) = &ctx.ApplyFuzzy((yyvsp[-3].filter), (yyvsp[0].fuzzy).has_value,
                                                           (yyvsp[0].fuzzy).value); }
#line 1466 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 24: /* modified_term: base_term  */
#line 149 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1472 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 25: /* modified_term: TERM FUZZY  */
#line 153 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].fuzzy).has_value
                                          ? &ctx.AddFuzzySimilarity((yyvsp[-1].sv), (yyvsp[0].fuzzy).value)
                                          : &ctx.AddFuzzy((yyvsp[-1].sv), 2); }
#line 1480 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 26: /* modified_term: phrase FUZZY  */
#line 156 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { if ((yyvsp[0].fuzzy).has_value) {
                                        ctx.SetSlop((yyvsp[-1].filter), static_cast<int>((yyvsp[0].fuzzy).value));
                                      }
                                      (yyval.filter) = (yyvsp[-1].filter); }
#line 1489 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 27: /* base_term: TERM  */
#line 163 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddTerm((yyvsp[0].sv)); }
#line 1495 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 28: /* base_term: NUMBER  */
#line 164 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddTerm((yyvsp[0].num).text); }
#line 1501 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 29: /* base_term: FLOAT  */
#line 165 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddTerm((yyvsp[0].flt).text); }
#line 1507 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 30: /* base_term: phrase  */
#line 166 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1513 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 31: /* base_term: REGEX  */
#line 167 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRegex((yyvsp[0].sv)); }
#line 1519 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 32: /* base_term: PREFIX  */
#line 168 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddPrefix((yyvsp[0].sv)); }
#line 1525 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 33: /* base_term: WILDCARD  */
#line 169 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddWildcard((yyvsp[0].sv)); }
#line 1531 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 34: /* base_term: range_expr  */
#line 170 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1537 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 35: /* base_term: ngram_expr  */
#line 171 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1543 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 36: /* base_term: group  */
#line 172 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1549 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 37: /* base_term: group AT NUMBER  */
#line 173 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.SetMinMatch((yyvsp[-2].filter), (yyvsp[0].num).value); (yyval.filter) = (yyvsp[-2].filter); }
#line 1555 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 38: /* base_term: STAR  */
#line 174 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddFieldExists(); }
#line 1561 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 39: /* @1: %empty  */
#line 178 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyval.filter) = ctx.current_root;
                                      ctx.current_root = &ctx.BeginGroup();
                                    }
#line 1570 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 40: /* group: LPAREN @1 clause_list RPAREN  */
#line 182 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndGroup((yyvsp[-2].filter)); }
#line 1576 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 41: /* $@2: %empty  */
#line 188 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginPhrase(); }
#line 1582 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 42: /* phrase: QUOTE $@2 phrase_body QUOTE  */
#line 189 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndPhrase(); }
#line 1588 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 45: /* $@3: %empty  */
#line 195 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.SetGap((yyvsp[0].gap).min, (yyvsp[0].gap).max); }
#line 1594 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 47: /* phrase_part: TERM  */
#line 200 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddPhraseTerm((yyvsp[0].sv)); }
#line 1600 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 48: /* phrase_part: PREFIX  */
#line 201 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddPhrasePrefix((yyvsp[0].sv)); }
#line 1606 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 49: /* phrase_part: WILDCARD  */
#line 202 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddPhraseWildcard((yyvsp[0].sv)); }
#line 1612 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 50: /* phrase_part: TERM FUZZY  */
#line 203 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddPhraseFuzzy(
                                        (yyvsp[-1].sv), (yyvsp[0].fuzzy).has_value
                                              ? static_cast<int>((yyvsp[0].fuzzy).value)
                                              : 2); }
#line 1621 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 51: /* $@4: %empty  */
#line 213 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginNGram((yyvsp[0].fnum)); }
#line 1627 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 52: /* ngram_expr: FN_NGRAM LPAREN threshold $@4 ngram_terms RPAREN  */
#line 214 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndNGram(); }
#line 1633 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 53: /* $@5: %empty  */
#line 215 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginPhrase(); }
#line 1639 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 54: /* ngram_expr: FN_PHRASE LPAREN $@5 phrase_body RPAREN  */
#line 216 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndPhrase(); }
#line 1645 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 55: /* ngram_expr: FN_WILDCARD LPAREN WILDCARD RPAREN  */
#line 217 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                          { (yyval.filter) = &ctx.AddWildcard((yyvsp[-1].sv)); }
#line 1651 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 56: /* ngram_expr: FN_WILDCARD LPAREN PREFIX RPAREN  */
#line 218 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                          { (yyval.filter) = &ctx.AddWildcard((yyvsp[-1].sv)); }
#line 1657 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 57: /* ngram_expr: FN_WILDCARD LPAREN TERM RPAREN  */
#line 219 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                          { (yyval.filter) = &ctx.AddWildcard((yyvsp[-1].sv)); }
#line 1663 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 58: /* ngram_expr: FN_FUZZY LPAREN TERM RPAREN  */
#line 220 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                          { (yyval.filter) = &ctx.AddFuzzy((yyvsp[-1].sv), 2); }
#line 1669 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 59: /* ngram_expr: FN_FUZZY LPAREN TERM NUMBER RPAREN  */
#line 221 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                          { (yyval.filter) = &ctx.AddFuzzy((yyvsp[-2].sv), (yyvsp[-1].num).value); }
#line 1675 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 60: /* $@6: %empty  */
#line 225 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginFn(); }
#line 1681 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 61: /* ngram_expr: FN_OR LPAREN $@6 fn_terms RPAREN  */
#line 226 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndFnAny(); }
#line 1687 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 62: /* $@7: %empty  */
#line 227 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginFn(); }
#line 1693 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 63: /* ngram_expr: FN_UNORDERED LPAREN $@7 fn_terms RPAREN  */
#line 228 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndFnAll(); }
#line 1699 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 64: /* $@8: %empty  */
#line 229 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginFn(); }
#line 1705 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 65: /* ngram_expr: FN_ATLEAST LPAREN NUMBER $@8 fn_terms RPAREN  */
#line 230 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndFnAtLeast((yyvsp[-3].num).value); }
#line 1711 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 66: /* $@9: %empty  */
#line 231 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.BeginFn(); }
#line 1717 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 67: /* ngram_expr: FN_ORDERED LPAREN $@9 fn_terms RPAREN  */
#line 232 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndFnOrdered(); }
#line 1723 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 68: /* $@10: %empty  */
#line 236 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                                 { ctx.BeginFn(); }
#line 1729 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 69: /* ngram_expr: FN_MAXGAPS LPAREN NUMBER FN_ORDERED LPAREN $@10 fn_terms RPAREN RPAREN  */
#line 237 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndFnMaxGaps((yyvsp[-6].num).value); }
#line 1735 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 70: /* $@11: %empty  */
#line 238 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                                  { ctx.BeginFn(); }
#line 1741 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 71: /* ngram_expr: FN_MAXWIDTH LPAREN NUMBER FN_ORDERED LPAREN $@11 fn_terms RPAREN RPAREN  */
#line 239 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.EndFnMaxWidth((yyvsp[-6].num).value); }
#line 1747 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 72: /* ngram_expr: FN_OTHER LPAREN fn_args RPAREN  */
#line 240 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                      { ctx.Unsupported((yyvsp[-3].sv)); (yyval.filter) = nullptr; }
#line 1753 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 75: /* fn_source: TERM  */
#line 255 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddFnTerm((yyvsp[0].sv)); }
#line 1759 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 76: /* fn_source: PREFIX  */
#line 256 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddFnOther("a prefix"); }
#line 1765 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 77: /* fn_source: WILDCARD  */
#line 257 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddFnOther("a wildcard"); }
#line 1771 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 78: /* fn_source: REGEX  */
#line 258 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddFnOther("a regular expression"); }
#line 1777 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 79: /* fn_source: phrase  */
#line 259 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddFnOther("a phrase"); }
#line 1783 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 80: /* fn_source: ngram_expr  */
#line 260 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddFnOther("a function"); }
#line 1789 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 85: /* threshold: NUMBER  */
#line 271 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.fnum) = static_cast<float>((yyvsp[0].num).value); }
#line 1795 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 86: /* threshold: FLOAT  */
#line 272 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.fnum) = (yyvsp[0].flt).value; }
#line 1801 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 87: /* ngram_terms: TERM  */
#line 276 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddNGram((yyvsp[0].sv)); }
#line 1807 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 88: /* ngram_terms: ngram_terms TERM  */
#line 277 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { ctx.AddNGram((yyvsp[0].sv)); }
#line 1813 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 89: /* range_expr: LBRACKET range_bound TO range_bound RBRACKET  */
#line 282 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), true, true); }
#line 1819 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 90: /* range_expr: LBRACE range_bound TO range_bound RBRACE  */
#line 284 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), false, false); }
#line 1825 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 91: /* range_expr: LBRACKET range_bound TO range_bound RBRACE  */
#line 286 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), true, false); }
#line 1831 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 92: /* range_expr: LBRACE range_bound TO range_bound RBRACKET  */
#line 288 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), false, true); }
#line 1837 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 93: /* range_bound: TERM  */
#line 292 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.sv) = (yyvsp[0].sv); }
#line 1843 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 94: /* range_bound: STAR  */
#line 293 "libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.sv) = (yyvsp[0].sv); }
#line 1849 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;


#line 1853 "libs/iresearch/include/iresearch/parser/lucene_parser.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (ctx, YY_("syntax error"));
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, ctx);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, ctx);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (ctx, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, ctx);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, ctx);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 296 "libs/iresearch/include/iresearch/parser/lucene_parser.y"


void yyerror(sdb::ParserContext& ctx, const char *s) {
    ctx.error_message = s;
}

extern void LexerSetInput(std::string_view input);
extern void LexerCleanup(void);

bool sdb::ParseQuery(sdb::ParserContext& ctx, std::string_view input) {
    LexerSetInput(input);
    int result = yyparse(ctx);
    LexerCleanup();
    return result == 0;
}

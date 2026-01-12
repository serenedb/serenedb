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
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 40 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"


#include "parser.h"
#include "basics/error.h"

int yylex(void);
void yyerror(sdb::ParserContext& ctx, const char *s);

#line 80 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"

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
  YYSYMBOL_PHRASE = 4,                     /* PHRASE  */
  YYSYMBOL_REGEX = 5,                      /* REGEX  */
  YYSYMBOL_PREFIX = 6,                     /* PREFIX  */
  YYSYMBOL_SUFFIX = 7,                     /* SUFFIX  */
  YYSYMBOL_WILDCARD = 8,                   /* WILDCARD  */
  YYSYMBOL_STAR = 9,                       /* STAR  */
  YYSYMBOL_NUMBER = 10,                    /* NUMBER  */
  YYSYMBOL_FLOAT = 11,                     /* FLOAT  */
  YYSYMBOL_AND = 12,                       /* AND  */
  YYSYMBOL_OR = 13,                        /* OR  */
  YYSYMBOL_NOT = 14,                       /* NOT  */
  YYSYMBOL_TO = 15,                        /* TO  */
  YYSYMBOL_LPAREN = 16,                    /* LPAREN  */
  YYSYMBOL_RPAREN = 17,                    /* RPAREN  */
  YYSYMBOL_LBRACKET = 18,                  /* LBRACKET  */
  YYSYMBOL_RBRACKET = 19,                  /* RBRACKET  */
  YYSYMBOL_LBRACE = 20,                    /* LBRACE  */
  YYSYMBOL_RBRACE = 21,                    /* RBRACE  */
  YYSYMBOL_COLON = 22,                     /* COLON  */
  YYSYMBOL_CARET = 23,                     /* CARET  */
  YYSYMBOL_TILDE = 24,                     /* TILDE  */
  YYSYMBOL_PLUS = 25,                      /* PLUS  */
  YYSYMBOL_MINUS = 26,                     /* MINUS  */
  YYSYMBOL_YYACCEPT = 27,                  /* $accept  */
  YYSYMBOL_query = 28,                     /* query  */
  YYSYMBOL_clause_list = 29,               /* clause_list  */
  YYSYMBOL_30_1 = 30,                      /* @1  */
  YYSYMBOL_31_2 = 31,                      /* $@2  */
  YYSYMBOL_clause = 32,                    /* clause  */
  YYSYMBOL_33_3 = 33,                      /* @3  */
  YYSYMBOL_34_4 = 34,                      /* @4  */
  YYSYMBOL_35_5 = 35,                      /* $@5  */
  YYSYMBOL_36_6 = 36,                      /* @6  */
  YYSYMBOL_37_7 = 37,                      /* @7  */
  YYSYMBOL_term_expr = 38,                 /* term_expr  */
  YYSYMBOL_39_8 = 39,                      /* @8  */
  YYSYMBOL_boosted_expr = 40,              /* boosted_expr  */
  YYSYMBOL_modified_term = 41,             /* modified_term  */
  YYSYMBOL_base_term = 42,                 /* base_term  */
  YYSYMBOL_43_9 = 43,                      /* @9  */
  YYSYMBOL_range_expr = 44,                /* range_expr  */
  YYSYMBOL_range_bound = 45                /* range_bound  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




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
typedef yytype_int8 yy_state_t;

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
#define YYFINAL  32
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   101

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  27
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  19
/* YYNRULES -- Number of rules.  */
#define YYNRULES  43
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  62

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   281


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
      25,    26
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint8 yyrline[] =
{
       0,    77,    77,    87,    88,    89,    89,   106,   106,   116,
     117,   118,   117,   127,   127,   134,   135,   134,   147,   148,
     148,   159,   160,   164,   171,   172,   173,   174,   175,   179,
     180,   181,   182,   183,   184,   185,   186,   186,   197,   199,
     201,   203,   208,   209
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
  "\"end of file\"", "error", "\"invalid token\"", "TERM", "PHRASE",
  "REGEX", "PREFIX", "SUFFIX", "WILDCARD", "STAR", "NUMBER", "FLOAT",
  "AND", "OR", "NOT", "TO", "LPAREN", "RPAREN", "LBRACKET", "RBRACKET",
  "LBRACE", "RBRACE", "COLON", "CARET", "TILDE", "PLUS", "MINUS",
  "$accept", "query", "clause_list", "@1", "$@2", "clause", "@3", "@4",
  "$@5", "@6", "@7", "term_expr", "@8", "boosted_expr", "modified_term",
  "base_term", "@9", "range_expr", "range_bound", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-22)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int8 yypact[] =
{
       3,   -21,   -20,   -22,   -22,   -22,   -22,   -22,   -22,     9,
       9,   -22,   -22,     5,    57,   -22,   -22,   -22,    -9,   -22,
     -22,   -22,    20,    32,   -22,     3,   -22,   -22,    28,    29,
       3,   -22,   -22,   -22,   -22,   -22,    14,    81,   -22,   -22,
       3,    33,     9,     9,   -22,     3,     3,     3,   -22,   -22,
     -22,   -22,   -22,    -6,     1,   -22,   -22,   -22,   -22,   -22,
     -22,   -22
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,    29,    30,    31,    32,    33,    34,    10,    36,     0,
       0,    13,    15,     0,     2,     3,     9,    18,    21,    24,
      35,    19,    25,    27,    11,     0,    42,    43,     0,     0,
       0,    16,     1,     5,     7,     4,     0,     0,    26,    28,
       0,     0,     0,     0,    14,     0,     0,     0,    22,    23,
      20,    12,    37,     0,     0,    17,     6,     8,    38,    40,
      41,    39
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -22,   -22,    23,   -22,   -22,   -14,   -22,   -22,   -22,   -22,
     -22,    15,   -22,   -22,   -22,   -22,   -22,   -22,    -8
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,    13,    14,    46,    47,    15,    24,    40,    30,    31,
      45,    16,    37,    17,    18,    19,    25,    20,    28
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      35,    21,    29,    22,    23,    32,     1,     2,     3,     4,
       5,     6,    26,    58,    36,    59,    44,     7,    27,     8,
      60,     9,    61,    10,    48,    49,    51,    35,    11,    12,
      38,    55,    56,    57,    53,    54,     1,     2,     3,     4,
       5,     6,    39,    42,    43,    33,    34,     7,    41,     8,
      52,     9,    50,    10,     0,     0,     0,     0,    11,    12,
       1,     2,     3,     4,     5,     6,     0,     0,     0,    33,
      34,     7,     0,     8,     0,     9,     0,    10,     0,     0,
       0,     0,    11,    12,     1,     2,     3,     4,     5,     6,
       0,     0,     0,     0,     0,     0,     0,     8,     0,     9,
       0,    10
};

static const yytype_int8 yycheck[] =
{
      14,    22,    10,    24,    24,     0,     3,     4,     5,     6,
       7,     8,     3,    19,    23,    21,    30,    14,     9,    16,
      19,    18,    21,    20,    10,    11,    40,    41,    25,    26,
      10,    45,    46,    47,    42,    43,     3,     4,     5,     6,
       7,     8,    10,    15,    15,    12,    13,    14,    25,    16,
      17,    18,    37,    20,    -1,    -1,    -1,    -1,    25,    26,
       3,     4,     5,     6,     7,     8,    -1,    -1,    -1,    12,
      13,    14,    -1,    16,    -1,    18,    -1,    20,    -1,    -1,
      -1,    -1,    25,    26,     3,     4,     5,     6,     7,     8,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    16,    -1,    18,
      -1,    20
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     4,     5,     6,     7,     8,    14,    16,    18,
      20,    25,    26,    28,    29,    32,    38,    40,    41,    42,
      44,    22,    24,    24,    33,    43,     3,     9,    45,    45,
      35,    36,     0,    12,    13,    32,    23,    39,    10,    10,
      34,    29,    15,    15,    32,    37,    30,    31,    10,    11,
      38,    32,    17,    45,    45,    32,    32,    32,    19,    21,
      19,    21
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    27,    28,    29,    29,    30,    29,    31,    29,    32,
      33,    34,    32,    35,    32,    36,    37,    32,    38,    39,
      38,    40,    40,    40,    41,    41,    41,    41,    41,    42,
      42,    42,    42,    42,    42,    42,    43,    42,    44,    44,
      44,    44,    45,    45
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     1,     2,     0,     4,     0,     4,     1,
       0,     0,     4,     0,     3,     0,     0,     4,     1,     0,
       4,     1,     3,     3,     1,     2,     3,     2,     3,     1,
       1,     1,     1,     1,     1,     1,     0,     4,     5,     5,
       5,     5,     1,     1
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


/* Lookahead token kind.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (sdb::ParserContext& ctx)
{
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
      yychar = yylex ();
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
#line 77 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      if (ctx.required_and) {
                                        ctx.current_parent = ctx.required_and;
                                        ctx.required_and = nullptr;
                                      }
                                      (yyval.filter) = (yyvsp[0].filter);
                                    }
#line 1166 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 3: /* clause_list: clause  */
#line 87 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1172 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 4: /* clause_list: clause_list clause  */
#line 88 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1178 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 5: /* @1: %empty  */
#line 89 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      if (ctx.required_and) {
                                        ctx.current_parent = ctx.required_and;
                                        ctx.required_and = nullptr;
                                      }
                                      // Pop the previous term from current parent
                                      auto prev = ctx.current_parent->PopBack();
                                      (yyval.parent) = ctx.current_parent;
                                      // Create And and add the previous term to it
                                      auto& and_filter = ctx.current_parent->add<irs::And>();
                                      and_filter.add(std::move(prev));
                                      ctx.current_parent = &and_filter;
                                    }
#line 1196 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 6: /* clause_list: clause_list AND @1 clause  */
#line 102 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyval.filter) = ctx.current_parent;
                                      ctx.current_parent = (yyvsp[-1].parent);
                                    }
#line 1205 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 7: /* $@2: %empty  */
#line 106 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      if (ctx.required_and) {
                                        ctx.current_parent = ctx.required_and;
                                        ctx.required_and = nullptr;
                                      }
                                    }
#line 1216 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 8: /* clause_list: clause_list OR $@2 clause  */
#line 112 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1222 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 9: /* clause: term_expr  */
#line 116 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1228 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 10: /* @3: %empty  */
#line 117 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.parent) = ctx.current_parent; }
#line 1234 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 11: /* @4: %empty  */
#line 118 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      auto& not_filter = (yyvsp[0].parent)->add<irs::Not>();
                                      ctx.current_parent = &not_filter.filter<irs::Or>();
                                      (yyval.filter) = &not_filter;
                                    }
#line 1244 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 12: /* clause: NOT @3 @4 clause  */
#line 123 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      ctx.current_parent = (yyvsp[-2].parent);
                                      (yyval.filter) = (yyvsp[-1].filter);
                                    }
#line 1253 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 13: /* $@5: %empty  */
#line 127 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      if (!ctx.required_and) {
                                        ctx.required_and = &ctx.current_parent->add<irs::And>();
                                        ctx.current_parent = ctx.required_and;
                                      }
                                    }
#line 1264 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 14: /* clause: PLUS $@5 clause  */
#line 133 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1270 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 15: /* @6: %empty  */
#line 134 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.parent) = ctx.current_parent; }
#line 1276 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 16: /* @7: %empty  */
#line 135 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      auto& not_filter = (yyvsp[0].parent)->add<irs::Not>();
                                      ctx.current_parent = &not_filter.filter<irs::Or>();
                                      (yyval.filter) = &not_filter;
                                    }
#line 1286 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 17: /* clause: MINUS @6 @7 clause  */
#line 140 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      ctx.current_parent = (yyvsp[-2].parent);
                                      (yyval.filter) = (yyvsp[-1].filter);
                                    }
#line 1295 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 18: /* term_expr: boosted_expr  */
#line 147 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1301 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 19: /* @8: %empty  */
#line 148 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyval.sv) = {ctx.default_field.data(), ctx.default_field.size()};
                                      ctx.default_field = (yyvsp[-1].sv);
                                    }
#line 1310 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 20: /* term_expr: TERM COLON @8 term_expr  */
#line 152 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      ctx.default_field = (yyvsp[-1].sv);
                                      (yyval.filter) = (yyvsp[0].filter);
                                    }
#line 1319 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 21: /* boosted_expr: modified_term  */
#line 159 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1325 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 22: /* boosted_expr: modified_term CARET NUMBER  */
#line 160 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyvsp[-2].filter)->boost(static_cast<float>((yyvsp[0].num)));
                                      (yyval.filter) = (yyvsp[-2].filter);
                                    }
#line 1334 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 23: /* boosted_expr: modified_term CARET FLOAT  */
#line 164 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyvsp[-2].filter)->boost((yyvsp[0].fnum));
                                      (yyval.filter) = (yyvsp[-2].filter);
                                    }
#line 1343 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 24: /* modified_term: base_term  */
#line 171 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1349 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 25: /* modified_term: TERM TILDE  */
#line 172 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddFuzzy((yyvsp[-1].sv), 2); }
#line 1355 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 26: /* modified_term: TERM TILDE NUMBER  */
#line 173 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddFuzzy((yyvsp[-2].sv), (yyvsp[0].num)); }
#line 1361 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 27: /* modified_term: PHRASE TILDE  */
#line 174 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddPhrase((yyvsp[-1].sv), 0); }
#line 1367 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 28: /* modified_term: PHRASE TILDE NUMBER  */
#line 175 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddPhrase((yyvsp[-2].sv), (yyvsp[0].num)); }
#line 1373 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 29: /* base_term: TERM  */
#line 179 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddTerm((yyvsp[0].sv)); }
#line 1379 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 30: /* base_term: PHRASE  */
#line 180 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddPhrase((yyvsp[0].sv), 0); }
#line 1385 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 31: /* base_term: REGEX  */
#line 181 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddWildcard((yyvsp[0].sv)); }
#line 1391 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 32: /* base_term: PREFIX  */
#line 182 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddPrefix((yyvsp[0].sv)); }
#line 1397 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 33: /* base_term: SUFFIX  */
#line 183 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddWildcard((yyvsp[0].sv)); }
#line 1403 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 34: /* base_term: WILDCARD  */
#line 184 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddWildcard((yyvsp[0].sv)); }
#line 1409 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 35: /* base_term: range_expr  */
#line 185 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = (yyvsp[0].filter); }
#line 1415 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 36: /* @9: %empty  */
#line 186 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyval.parent) = ctx.current_parent;
                                      ctx.current_parent = &ctx.current_parent->add<irs::Or>();
                                    }
#line 1424 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 37: /* base_term: LPAREN @9 clause_list RPAREN  */
#line 190 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    {
                                      (yyval.filter) = ctx.current_parent;
                                      ctx.current_parent = (yyvsp[-2].parent);
                                    }
#line 1433 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 38: /* range_expr: LBRACKET range_bound TO range_bound RBRACKET  */
#line 198 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), true, true); }
#line 1439 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 39: /* range_expr: LBRACE range_bound TO range_bound RBRACE  */
#line 200 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), false, false); }
#line 1445 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 40: /* range_expr: LBRACKET range_bound TO range_bound RBRACE  */
#line 202 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), true, false); }
#line 1451 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 41: /* range_expr: LBRACE range_bound TO range_bound RBRACKET  */
#line 204 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.filter) = &ctx.AddRange((yyvsp[-3].sv), (yyvsp[-1].sv), false, true); }
#line 1457 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 42: /* range_bound: TERM  */
#line 208 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.sv) = (yyvsp[0].sv); }
#line 1463 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;

  case 43: /* range_bound: STAR  */
#line 209 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"
                                    { (yyval.sv) = (yyvsp[0].sv); }
#line 1469 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"
    break;


#line 1473 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.cpp"

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

#line 212 "/home/abrama/projects/search-benchmark-game/engines/iresearch/serenedb/libs/iresearch/include/iresearch/parser/lucene_parser.y"


void yyerror(sdb::ParserContext& ctx, const char *s) {
    ctx.error_message = s;
}

extern void LexerSetInput(std::string_view input);
extern void LexerCleanup(void);

sdb::Result sdb::ParseQuery(sdb::ParserContext& ctx, std::string_view input) {
    LexerSetInput(input);
    int result = yyparse(ctx);
    LexerCleanup();
    if (result != 0) {
        return {sdb::ERROR_BAD_PARAMETER, std::move(ctx.error_message)};
    }
    return {};
}

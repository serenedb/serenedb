%skeleton "lalr1.cc"
%require "3.2"
%defines
%define api.value.type variant
%define api.token.constructor  
%define parse.assert
%define parse.error verbose

%code top {
    #if defined(__GNUC__) || defined(__clang__)
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wunused-but-set-variable"
    #endif
}

%code requires {
    #include "ast.h"
    using namespace sdb;
    typedef void* yyscan_t;
}

%param { yyscan_t scanner }
%param { ParserContext& ctx }
%param { int& rootIndex }

%code {
    yy::parser::symbol_type yylex(yyscan_t scanner, ParserContext& ctx, int& rootIndex);
}

%token <std::string_view> TERM PHRASE
%token <double> NUMBER
%token AND OR NOT PLUS MINUS COLON LPAREN RPAREN TO LBRACKET RBRACKET LBRACE RBRACE TILDE CARAT END 0

%type <int> query or_expr and_expr implicit_expr unary_expr modifier_expr field_or_atom atom range

%nonassoc TILDE
%nonassoc NUMBER

%%

input:
    query END { rootIndex = $1; }
    ;

query:
    or_expr { $$ = $1; }
    ;

or_expr:
      and_expr
    | or_expr OR and_expr { $$ = ctx.AddOp(NodeType::Or, $1, $3); }
    ;

and_expr:
      implicit_expr
    | and_expr AND implicit_expr { $$ = ctx.AddOp(NodeType::And, $1, $3); }
    ;

implicit_expr:
      unary_expr
    | implicit_expr unary_expr { $$ = ctx.AddOp(NodeType::Or, $1, $2); }
    ;

unary_expr:
      modifier_expr
    | NOT unary_expr   { $$ = ctx.AddOp(NodeType::Not, $2, -1); }
    | PLUS unary_expr  { $$ = ctx.AddOp(NodeType::Required, $2, -1); }
    | MINUS unary_expr { $$ = ctx.AddOp(NodeType::Prohibited, $2, -1); }
    ;

modifier_expr:
      field_or_atom
    | modifier_expr CARAT NUMBER { $$ = ctx.AddModifier(NodeType::Boost, $1, $3); }
    | modifier_expr TILDE        { $$ = ctx.AddModifier(NodeType::Fuzzy, $1, NAN); }
    | modifier_expr TILDE NUMBER { $$ = ctx.AddModifier(NodeType::Fuzzy, $1, $3); }
    ;

field_or_atom:
      atom
    | TERM COLON atom { 
          int name = ctx.AddString(NodeType::Term, $1);
          $$ = ctx.AddOp(NodeType::Field, name, $3); 
      }
    ;

atom:
      TERM    { $$ = ctx.AddString(NodeType::Term, $1); }
    | PHRASE  { $$ = ctx.AddString(NodeType::Phrase, $1); }
    | NUMBER  { $$ = ctx.AddNumber($1); }
    | range   { $$ = $1; }
    | LPAREN or_expr RPAREN { $$ = $2; }
    ;

range:
      LBRACKET atom TO atom RBRACKET { $$ = ctx.AddOp(NodeType::RangeInclusive, $2, $4); }
    | LBRACE   atom TO atom RBRACE   { $$ = ctx.AddOp(NodeType::RangeExclusive, $2, $4); }
    | LBRACKET atom TO atom RBRACE   { $$ = ctx.AddOp(NodeType::RangeIncExc, $2, $4); }
    | LBRACE   atom TO atom RBRACKET { $$ = ctx.AddOp(NodeType::RangeExcInc, $2, $4); }
    ;

%%

void yy::parser::error(const std::string& m) {
    std::cerr << "Parser Error: " << m << '\n';
}

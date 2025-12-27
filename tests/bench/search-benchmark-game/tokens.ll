%{
#include "ast.h"
#include "grammar.hpp"
#include <charconv>

#undef YY_DECL
#define YY_DECL yy::parser::symbol_type yylex(yyscan_t yyscanner)

using namespace sdb;
%}

%option reentrant noyywrap nounput noinput batch prefix="serene_"

NUMBER  [0-9]+(\.[0-9]+)?
ID      ([a-zA-Z0-9_*?.\-]|(\\.))+

%%

[ \t\r\n]+   { }

"AND"        { return yy::parser::make_AND(); }
"OR"         { return yy::parser::make_OR(); }
"NOT"        { return yy::parser::make_NOT(); }
"TO"         { return yy::parser::make_TO(); }
"+"          { return yy::parser::make_PLUS(); }
"-"          { return yy::parser::make_MINUS(); }
":"          { return yy::parser::make_COLON(); }
"("          { return yy::parser::make_LPAREN(); }
")"          { return yy::parser::make_RPAREN(); }
"["          { return yy::parser::make_LBRACKET(); }
"]"          { return yy::parser::make_RBRACKET(); }
"{"          { return yy::parser::make_LBRACE(); }
"}"          { return yy::parser::make_RBRACE(); }
"~"          { return yy::parser::make_TILDE(); }
"^"          { return yy::parser::make_CARAT(); }

{NUMBER}     { 
               double v = 0.0;
               std::from_chars(yytext, yytext + yyleng, v);
               return yy::parser::make_NUMBER(v); 
             }

{ID}         { return yy::parser::make_TERM(std::string_view(yytext, yyleng)); }
\"[^"]*\"    { return yy::parser::make_PHRASE(std::string_view(yytext + 1, yyleng - 2)); }

.            { } 
<<EOF>>      { return yy::parser::make_END(); }

%%

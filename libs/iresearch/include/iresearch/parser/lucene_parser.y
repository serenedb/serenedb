/*
 DISCLAIMER

 Copyright 2025 SereneDB GmbH, Berlin, Germany

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 Copyright holder is SereneDB GmbH, Berlin, Germany
*/

%define api.pure full

%code requires {
#include "iresearch/search/filter.hpp"
#include "iresearch/search/boolean_filter.hpp"

#include "basics/down_cast.h"

#include <cstddef>
#include <string_view>

namespace irs { class Filter; }
namespace sdb { struct ParserContext; }

struct StringSpan {
    const char* data;
    size_t len;
    operator std::string_view() const { return {data, len}; }
};
}

%{
#include "parser.h"
#include "basics/error.h"

#pragma clang diagnostic ignored "-Wunused-but-set-variable"
%}

%code {
int yylex(YYSTYPE* yylval);
void yyerror(sdb::ParserContext& ctx, const char *s);
}

%parse-param { sdb::ParserContext& ctx }

%union {
    StringSpan sv;
    int num;
    float fnum;
    irs::FilterWithBoost* filter;
}

%token <sv> TERM PHRASE REGEX PREFIX SUFFIX WILDCARD STAR
%token <num> NUMBER
%token <fnum> FLOAT
%token AND OR NOT TO
%token LPAREN RPAREN LBRACKET RBRACKET LBRACE RBRACE
%token COLON CARET TILDE PLUS MINUS

%type <filter> modified_term base_term range_expr
%type <sv> range_bound

%left OR
%left AND
%right NOT
%right PLUS MINUS

%%

query:
    clause_list
    ;

clause_list:
    mod_clause                      { ctx.AddClause(sdb::Conjunction::Or); }
    | clause_list mod_clause        { ctx.AddClause(sdb::Conjunction::Or); }
    | clause_list AND mod_clause    { ctx.AddClause(sdb::Conjunction::And); }
    | clause_list OR mod_clause     { ctx.AddClause(sdb::Conjunction::Or); }
    ;

mod_clause:
    term_expr                       { ctx.last_mod = sdb::Modifier::None; }
    | PLUS term_expr                { ctx.last_mod = sdb::Modifier::Required; }
    | MINUS term_expr               { ctx.last_mod = sdb::Modifier::Not; }
    | NOT term_expr                 { ctx.last_mod = sdb::Modifier::Not; }
    ;

term_expr:
    boosted_expr
    | TERM COLON                    {
                                      $<sv>$ = {ctx.default_field.data(), ctx.default_field.size()};
                                      ctx.default_field = $1;
                                    }
      term_expr                     { ctx.default_field = $<sv>3; }
    ;

boosted_expr:
    modified_term
    | modified_term CARET NUMBER    { $1->boost(static_cast<float>($3)); }
    | modified_term CARET FLOAT     { $1->boost($3); }
    ;

modified_term:
    base_term                       { $$ = $1; }
    | TERM TILDE                    { $$ = &ctx.AddFuzzy($1, 2); }
    | TERM TILDE NUMBER             { $$ = &ctx.AddFuzzy($1, $3); }
    | PHRASE TILDE                  { $$ = &ctx.AddPhrase($1, 0); }
    | PHRASE TILDE NUMBER           { $$ = &ctx.AddPhrase($1, $3); }
    ;

base_term:
    TERM                            { $$ = &ctx.AddTerm($1); }
    | PHRASE                        { $$ = &ctx.AddPhrase($1, 0); }
    | REGEX                         { $$ = &ctx.AddWildcard($1); }
    | PREFIX                        { $$ = &ctx.AddPrefix($1); }
    | SUFFIX                        { $$ = &ctx.AddWildcard($1); }
    | WILDCARD                      { $$ = &ctx.AddWildcard($1); }
    | range_expr                    { $$ = $1; }
    | LPAREN                        {
                                      $<filter>$ = ctx.current_root;
                                      ctx.current_root = &ctx.current_root->GetOptional().add<irs::MixedBooleanFilter>();
                                    }
        clause_list RPAREN          {
                                      $$ = ctx.current_root;
                                      ctx.current_root = sdb::basics::downCast<irs::MixedBooleanFilter>($<filter>2);
                                    }
    ;

range_expr:
    LBRACKET range_bound TO range_bound RBRACKET
                                    { $$ = &ctx.AddRange($2, $4, true, true); }
    | LBRACE range_bound TO range_bound RBRACE
                                    { $$ = &ctx.AddRange($2, $4, false, false); }
    | LBRACKET range_bound TO range_bound RBRACE
                                    { $$ = &ctx.AddRange($2, $4, true, false); }
    | LBRACE range_bound TO range_bound RBRACKET
                                    { $$ = &ctx.AddRange($2, $4, false, true); }
    ;

range_bound:
    TERM                            { $$ = $1; }
    | STAR                          { $$ = $1; }
    ;

%%

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

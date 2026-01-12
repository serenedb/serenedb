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

%code requires {
#include <iresearch/search/filter.hpp>
#include <iresearch/search/boolean_filter.hpp>

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

int yylex(void);
void yyerror(sdb::ParserContext& ctx, const char *s);
%}

%parse-param { sdb::ParserContext& ctx }

%union {
    StringSpan sv;
    int num;
    float fnum;
    irs::FilterWithBoost* filter;
    irs::BooleanFilter* parent;
}

%token <sv> TERM PHRASE REGEX PREFIX SUFFIX WILDCARD STAR
%token <num> NUMBER
%token <fnum> FLOAT
%token AND OR NOT TO
%token LPAREN RPAREN LBRACKET RBRACKET LBRACE RBRACE
%token COLON CARET TILDE PLUS MINUS

%type <filter> query clause_list clause term_expr boosted_expr modified_term base_term range_expr
%type <sv> range_bound

%left OR
%left AND
%right NOT
%right PLUS MINUS

%%

query:
    clause_list                     {
                                      if (ctx.required_and) {
                                        ctx.current_parent = ctx.required_and;
                                        ctx.required_and = nullptr;
                                      }
                                      $$ = $1;
                                    }
    ;

clause_list:
    clause                          { $$ = $1; }
    | clause_list clause            { $$ = $2; }
    | clause_list AND               {
                                      if (ctx.required_and) {
                                        ctx.current_parent = ctx.required_and;
                                        ctx.required_and = nullptr;
                                      }
                                      // Pop the previous term from current parent
                                      auto prev = ctx.current_parent->PopBack();
                                      $<parent>$ = ctx.current_parent;
                                      // Create And and add the previous term to it
                                      auto& and_filter = ctx.current_parent->add<irs::And>();
                                      and_filter.add(std::move(prev));
                                      ctx.current_parent = &and_filter;
                                    }
        clause                      {
                                      $$ = ctx.current_parent;
                                      ctx.current_parent = $<parent>3;
                                    }
    | clause_list OR                {
                                      if (ctx.required_and) {
                                        ctx.current_parent = ctx.required_and;
                                        ctx.required_and = nullptr;
                                      }
                                    }
        clause                      { $$ = $4; }
    ;

clause:
    term_expr                       { $$ = $1; }
    | NOT                           { $<parent>$ = ctx.current_parent; }
                                    {
                                      auto& not_filter = $<parent>2->add<irs::Not>();
                                      ctx.current_parent = &not_filter.filter<irs::Or>();
                                      $<filter>$ = &not_filter;
                                    }
        clause                      {
                                      ctx.current_parent = $<parent>2;
                                      $$ = $<filter>3;
                                    }
    | PLUS                          {
                                      if (!ctx.required_and) {
                                        ctx.required_and = &ctx.current_parent->add<irs::And>();
                                        ctx.current_parent = ctx.required_and;
                                      }
                                    }
        clause                      { $$ = $3; }
    | MINUS                         { $<parent>$ = ctx.current_parent; }
                                    {
                                      auto& not_filter = $<parent>2->add<irs::Not>();
                                      ctx.current_parent = &not_filter.filter<irs::Or>();
                                      $<filter>$ = &not_filter;
                                    }
        clause                      {
                                      ctx.current_parent = $<parent>2;
                                      $$ = $<filter>3;
                                    }
    ;

term_expr:
    boosted_expr                    { $$ = $1; }
    | TERM COLON                    {
                                      $<sv>$ = {ctx.default_field.data(), ctx.default_field.size()};
                                      ctx.default_field = $1;
                                    }
      term_expr                     {
                                      ctx.default_field = $<sv>3;
                                      $$ = $4;
                                    }
    ;

boosted_expr:
    modified_term                   { $$ = $1; }
    | modified_term CARET NUMBER    {
                                      $1->boost(static_cast<float>($3));
                                      $$ = $1;
                                    }
    | modified_term CARET FLOAT     {
                                      $1->boost($3);
                                      $$ = $1;
                                    }
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
                                      $<parent>$ = ctx.current_parent;
                                      ctx.current_parent = &ctx.current_parent->add<irs::Or>();
                                    }
        clause_list RPAREN          {
                                      $$ = ctx.current_parent;
                                      ctx.current_parent = $<parent>2;
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

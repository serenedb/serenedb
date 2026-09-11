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
#include "parser.hpp"

#pragma clang diagnostic ignored "-Wunused-but-set-variable"
%}

%code {
int yylex(YYSTYPE* yylval);
void yyerror(sdb::ParserContext& ctx, const char *s);
}

%parse-param { sdb::ParserContext& ctx }

%union {
    StringSpan sv;
    // A number is a count where one is asked for and a term everywhere else,
    // so it carries both what it means and what it said.
    struct { int value; StringSpan text; } num;
    struct { bool has_value; float value; } fuzzy;
    // A float is a threshold where one is asked for and a term everywhere
    // else, the same way a number is.
    struct { float value; StringSpan text; } flt;
    float fnum;
    struct { int min; int max; } gap;
    irs::Filter* filter;
}

%token <sv> TERM REGEX PREFIX WILDCARD STAR
%token <num> NUMBER
%token <flt> FLOAT
%token <gap> GAP
%token AND OR NOT TO
%token LPAREN RPAREN LBRACKET RBRACKET LBRACE RBRACE
%token COLON CARET PLUS MINUS AT QUOTE LT LE GT GE EQ
%token <fuzzy> FUZZY
%token FN_NGRAM FN_PHRASE FN_WILDCARD FN_FUZZY
%token FN_OR FN_UNORDERED FN_ATLEAST FN_ORDERED FN_MAXGAPS FN_MAXWIDTH
%token <sv> FN_OTHER

%type <filter> term_expr boosted_expr modified_term base_term range_expr
%type <filter> group phrase
%type <filter> ngram_expr
%type <fnum> threshold
%type <sv> range_bound

%left OR
%left AND
%right NOT
%right PLUS MINUS

%%

query:
    clause_list                     { ctx.EndClauseList(); }
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
    boosted_expr                    { $$ = $1; }
    | STAR COLON STAR               { $$ = &ctx.AddAll(); }
    | field_prefix term_expr        { $$ = $2; }
    // What Lucene's flexible parser spells `field<5`: a range with one end
    // left open.
    | field_name LT range_bound     { $$ = &ctx.AddRange("*", $3, false, false); }
    | field_name LE range_bound     { $$ = &ctx.AddRange("*", $3, false, true); }
    | field_name GT range_bound     { $$ = &ctx.AddRange($3, "*", false, false); }
    | field_name GE range_bound     { $$ = &ctx.AddRange($3, "*", true, false); }
    ;

field_prefix:
    field_name COLON
    | field_name EQ
    ;

field_name:
    TERM                            {
                                      if (!ctx.CheckField($1)) {
                                        YYABORT;
                                      }
                                    }
    ;

boosted_expr:
    modified_term                   { $$ = $1; }
    | modified_term CARET threshold { $1->SetBoost($3); $$ = $1; }
    // Lucene takes the two suffixes in either order, and a fuzziness read
    // after a boost has to reach a term that is already built.
    | modified_term CARET threshold FUZZY
                                    { $1->SetBoost($3);
                                      $$ = &ctx.ApplyFuzzy($1, $4.has_value,
                                                           $4.value); }
    ;

modified_term:
    base_term                       { $$ = $1; }
    // A distance of two is what a bare `~` means, and a value of one or more
    // is that many edits where a value below one is Lucene's older
    // similarity -- `AddFuzzySimilarity` tells them apart the way Lucene does.
    | TERM FUZZY                    { $$ = $2.has_value
                                          ? &ctx.AddFuzzySimilarity($1, $2.value)
                                          : &ctx.AddFuzzy($1, 2); }
    | phrase FUZZY                  { if ($2.has_value) {
                                        ctx.SetSlop($1, static_cast<int>($2.value));
                                      }
                                      $$ = $1; }
    ;

base_term:
    TERM                            { $$ = &ctx.AddTerm($1); }
    | NUMBER                        { $$ = &ctx.AddTerm($1.text); }
    | FLOAT                         { $$ = &ctx.AddTerm($1.text); }
    | phrase                        { $$ = $1; }
    | REGEX                         { $$ = &ctx.AddRegex($1); }
    | PREFIX                        { $$ = &ctx.AddPrefix($1); }
    | WILDCARD                      { $$ = &ctx.AddWildcard($1); }
    | range_expr                    { $$ = $1; }
    | ngram_expr                    { $$ = $1; }
    | group                         { $$ = $1; }
    | group AT NUMBER               { ctx.SetMinMatch($1, $3.value); $$ = $1; }
    | STAR                          { $$ = &ctx.AddFieldExists(); }
    ;

group:
    LPAREN                          {
                                      $<filter>$ = ctx.current_root;
                                      ctx.current_root = &ctx.BeginGroup();
                                    }
        clause_list RPAREN          { $$ = &ctx.EndGroup($<filter>2); }
    ;

// A phrase is a list of parts, each of them the same thing a clause outside
// quotes may be, and a gap says how far apart two of them sit.
phrase:
    QUOTE                           { ctx.BeginPhrase(); }
        phrase_body QUOTE           { $$ = &ctx.EndPhrase(); }
    ;

phrase_body:
    phrase_part
    | phrase_body phrase_part
    | phrase_body GAP               { ctx.SetGap($2.min, $2.max); }
        phrase_part
    ;

phrase_part:
    TERM                            { ctx.AddPhraseTerm($1); }
    | PREFIX                        { ctx.AddPhrasePrefix($1); }
    | WILDCARD                      { ctx.AddPhraseWildcard($1); }
    | TERM FUZZY                    { ctx.AddPhraseFuzzy(
                                        $1, $2.has_value
                                              ? static_cast<int>($2.value)
                                              : 2); }
    ;

// An n-gram similarity: how much of a sequence of terms a document has to
// carry. Lucene has no such filter, so this follows the shape its own
// extensions take -- a named function over a parenthesized argument list.
ngram_expr:
    FN_NGRAM LPAREN threshold       { ctx.BeginNGram($3); }
        ngram_terms RPAREN          { $$ = &ctx.EndNGram(); }
    | FN_PHRASE LPAREN              { ctx.BeginPhrase(); }
        phrase_body RPAREN          { $$ = &ctx.EndPhrase(); }
    | FN_WILDCARD LPAREN WILDCARD RPAREN  { $$ = &ctx.AddWildcard($3); }
    | FN_WILDCARD LPAREN PREFIX RPAREN    { $$ = &ctx.AddWildcard($3); }
    | FN_WILDCARD LPAREN TERM RPAREN      { $$ = &ctx.AddWildcard($3); }
    | FN_FUZZY LPAREN TERM RPAREN         { $$ = &ctx.AddFuzzy($3, 2); }
    | FN_FUZZY LPAREN TERM NUMBER RPAREN  { $$ = &ctx.AddFuzzy($3, $4.value); }
    // Which documents these hold is a question this engine can answer, even
    // though it has no intervals to compose: a set of terms, joined by how
    // many of them a document needs and in what order they must lie.
    | FN_OR LPAREN                  { ctx.BeginFn(); }
        fn_terms RPAREN             { $$ = &ctx.EndFnAny(); }
    | FN_UNORDERED LPAREN           { ctx.BeginFn(); }
        fn_terms RPAREN             { $$ = &ctx.EndFnAll(); }
    | FN_ATLEAST LPAREN NUMBER      { ctx.BeginFn(); }
        fn_terms RPAREN             { $$ = &ctx.EndFnAtLeast($3.value); }
    | FN_ORDERED LPAREN             { ctx.BeginFn(); }
        fn_terms RPAREN             { $$ = &ctx.EndFnOrdered(); }
    // A bound on the gaps is a bound on the distance only where there is one
    // pair to measure; over more of them it bounds their total, which a
    // phrase cannot say.
    | FN_MAXGAPS LPAREN NUMBER FN_ORDERED LPAREN { ctx.BeginFn(); }
        fn_terms RPAREN RPAREN      { $$ = &ctx.EndFnMaxGaps($3.value); }
    | FN_MAXWIDTH LPAREN NUMBER FN_ORDERED LPAREN { ctx.BeginFn(); }
        fn_terms RPAREN RPAREN      { $$ = &ctx.EndFnMaxWidth($3.value); }
    | FN_OTHER LPAREN fn_args RPAREN  { ctx.Unsupported($1); $$ = nullptr; }
    ;

// What an interval function is applied to, read but not acted on: the
// argument list is consumed so the message names the function rather than
// whatever came after it.
// What an interval function is applied to. Lucene composes these freely;
// here a term is a source this engine can answer for, and anything else is
// read so that the refusal can say what it was rather than where it stopped.
fn_terms:
    fn_source
    | fn_terms fn_source
    ;

fn_source:
    TERM                            { ctx.AddFnTerm($1); }
    | PREFIX                        { ctx.AddFnOther("a prefix"); }
    | WILDCARD                      { ctx.AddFnOther("a wildcard"); }
    | REGEX                         { ctx.AddFnOther("a regular expression"); }
    | phrase                        { ctx.AddFnOther("a phrase"); }
    | ngram_expr                    { ctx.AddFnOther("a function"); }
    ;

fn_args:
    /* empty */
    | fn_args fn_source
    | fn_args NUMBER
    | fn_args FLOAT
    ;

threshold:
    NUMBER                          { $$ = static_cast<float>($1.value); }
    | FLOAT                         { $$ = $1.value; }
    ;

ngram_terms:
    TERM                            { ctx.AddNGram($1); }
    | ngram_terms TERM              { ctx.AddNGram($2); }
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

bool sdb::ParseQuery(sdb::ParserContext& ctx, std::string_view input) {
    LexerSetInput(input);
    int result = yyparse(ctx);
    LexerCleanup();
    return result == 0;
}

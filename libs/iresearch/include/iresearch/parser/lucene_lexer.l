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

%{

#include <fast_float/fast_float.h>

#include <cstring>
#include <string_view>

#include "lucene_parser.hpp"

static YY_BUFFER_STATE current_buffer = nullptr;
%}

%option noyywrap
%option nounput
%option noinput
%option bison-bridge

/* Inside quotes the words are words: `and` is a term there rather than an
   operator, and a phrase is a list of parts rather than one string to be
   taken apart afterwards. */
%x PHRASE_BODY

/* Between brackets a bound is almost anything, as Lucene reads it: a date or
   a hyphenated word is a bound there, not an expression. */
%x RANGE

DIGIT       [0-9]
ALNUM       [a-zA-Z0-9_]
WILDCHAR    [*?]
/* A backslash protects whatever follows it, as Lucene's `_ESCAPED_CHAR`
   does: the set of characters worth escaping is not the parser's to guess. */
ESCAPED     \\.
/* Lucene's `_TERM_START_CHAR` and `_TERM_CHAR`, which say what a term is by
   what it is not: everything the syntax has not claimed. That is how `café`,
   `u.s.a` and `don't` are terms, and why the characters this parser reads as
   operators -- `@` for a minimum match, `<`, `>` and `=` for a comparison --
   are excluded where Lucene's classic parser keeps them. A `+` or a `-` is an
   operator only where a term begins, and `&` and `|` only where they pair. */
TERMSTART   ([^ \t\r\n+\-!(){}\[\]:\^"~*?\\/@<>=&|]|{ESCAPED})
TERMCHAR    ({TERMSTART}|[+\-&|])
/* Inside quotes the space and the quote end a word, and nothing else does. */
PWORD       ([^ \t\r\n"~*?\\]|{ESCAPED})

%%

[ \t\r\n]+                  { /* Skip whitespace */ }

[+\-!]/[ \t\r\n]             {
                              /* A lone `+`, `-` or `!` is a term: what was
                                 pasted in is searched for, rather than read
                                 as an operator with nothing to apply to. */
                              yylval->sv = {yytext, 1};
                              return TERM;
                            }

"AND"|"&&"                  { return AND; }
"OR"|"||"                   { return OR; }
"NOT"|"!"                   { return NOT; }
"TO"                        { return TO; }
"fn:ngram"                  { return FN_NGRAM; }
"fn:phrase"                 { return FN_PHRASE; }
"fn:or"                     { return FN_OR; }
"fn:unordered"              { return FN_UNORDERED; }
"fn:atLeast"|"fn:atleast"   { return FN_ATLEAST; }
"fn:ordered"                { return FN_ORDERED; }
"fn:maxgaps"|"fn:maxGaps"   { return FN_MAXGAPS; }
"fn:maxwidth"|"fn:maxWidth" { return FN_MAXWIDTH; }
"fn:wildcard"               { return FN_WILDCARD; }
"fn:fuzzyTerm"|"fn:fuzzyterm"  { return FN_FUZZY; }
"fn:"{ALNUM}+               {
                              /* An interval function Lucene has and this
                                 engine has no algebra for. It is read, and
                                 refused where it would be executed. */
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return FN_OTHER;
                            }

"("                         { return LPAREN; }
")"                         { return RPAREN; }
"["                         { BEGIN(RANGE); return LBRACKET; }
"{"                         { BEGIN(RANGE); return LBRACE; }

<RANGE>[ \t\r\n]+          { /* Skip whitespace */ }
<RANGE>"]"                   { BEGIN(INITIAL); return RBRACKET; }
<RANGE>"}"                   { BEGIN(INITIAL); return RBRACE; }
<RANGE>"TO"                  { return TO; }

<RANGE>\"([^\"]|\\\")+\"      {
                              /* A quoted bound holds what a bare one cannot:
                                 spaces, brackets, the word TO. */
                              yylval->sv = {yytext + 1, static_cast<size_t>(yyleng - 2)};
                              return TERM;
                            }

<RANGE>[^ \t\r\n\]}]+       {
                              /* Anything up to the delimiter, which is how
                                 a date or a hyphenated bound gets through. */
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return TERM;
                            }
":"                         { return COLON; }
"^"                         { return CARET; }
"~"{DIGIT}*("."{DIGIT}+)?   {
                              /* Lucene's `FUZZY_SLOP`: an edit distance after
                                 a term, a slop after a phrase, and the two
                                 are one token so that `the~2` cannot read as
                                 a fuzzy `the` beside the term `2`. */
                              yylval->fuzzy.has_value = yyleng > 1;
                              if (yylval->fuzzy.has_value) {
                                fast_float::from_chars(yytext + 1,
                                                       yytext + yyleng,
                                                       yylval->fuzzy.value);
                              }
                              return FUZZY;
                            }
"@"                         { return AT; }
"<="                        { return LE; }
">="                        { return GE; }
"<"                         { return LT; }
">"                         { return GT; }
"="                         { return EQ; }
"+"                         { return PLUS; }
"-"                         { return MINUS; }

{DIGIT}+"."{DIGIT}+         {
                              fast_float::from_chars(yytext, yytext + yyleng,
                                                     yylval->flt.value);
                              yylval->flt.text = {yytext,
                                                  static_cast<size_t>(yyleng)};
                              return FLOAT;
                            }

{DIGIT}+                    {
                              fast_float::from_chars(yytext, yytext + yyleng,
                                                     yylval->num.value);
                              yylval->num.text = {yytext,
                                                  static_cast<size_t>(yyleng)};
                              return NUMBER;
                            }

\"                          { BEGIN(PHRASE_BODY); return QUOTE; }

<PHRASE_BODY>\"             { BEGIN(INITIAL); return QUOTE; }

<PHRASE_BODY>[ \t\r\n]+    { /* Skip whitespace */ }

<PHRASE_BODY>{DIGIT}+"-"{DIGIT}+  {
                              /* A gap: how far the part after it may sit from
                                 the part before it, rather than next to it.
                                 The pattern already said both are digits, so
                                 neither conversion can fail. */
                              const char* dash = std::strchr(yytext, '-');
                              const char* end = yytext + yyleng;
                              fast_float::from_chars(yytext, dash,
                                                     yylval->gap.min);
                              fast_float::from_chars(dash + 1, end,
                                                     yylval->gap.max);
                              return GAP;
                            }

<PHRASE_BODY>"~"{DIGIT}*("."{DIGIT}+)?  {
                              /* Lucene's `FUZZY_SLOP`: an edit distance after
                                 a term, a slop after a phrase, and the two
                                 are one token so that `the~2` cannot read as
                                 a fuzzy `the` beside the term `2`. */
                              yylval->fuzzy.has_value = yyleng > 1;
                              if (yylval->fuzzy.has_value) {
                                fast_float::from_chars(yytext + 1,
                                                       yytext + yyleng,
                                                       yylval->fuzzy.value);
                              }
                              return FUZZY;
                            }

<PHRASE_BODY>{PWORD}+\*     {
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return PREFIX;
                            }

<PHRASE_BODY>{PWORD}*{WILDCHAR}({PWORD}|{WILDCHAR})*  {
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return WILDCARD;
                            }

<PHRASE_BODY>{PWORD}+       {
                              /* A word of a phrase is anything that is not a
                                 space, a quote, or one of the characters that
                                 mean something here -- `war-torn` and `2` are
                                 words, and what they become is the analyzer's
                                 answer rather than the lexer's. */
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return TERM;
                            }

<PHRASE_BODY>.               {
                              /* The parser turns this into a message on the
                                 context, which is where every other failure
                                 to read a query is reported. */
                              return YYUNDEF;
                            }

\/([^\/\\]|\\.)*\/          {
                              /* Regex pattern - point to content inside slashes */
                              yylval->sv = {yytext + 1, static_cast<size_t>(yyleng - 2)};
                              return REGEX;
                            }

{TERMSTART}{TERMCHAR}*\*    {
                              /* A word with a star at its end and none
                                 within, which is what a term character class
                                 that holds neither `*` nor `?` already says. */
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return PREFIX;
                            }

"*"                         {
                              /* Standalone star (for range bounds) */
                              yylval->sv = {yytext, 1};
                              return STAR;
                            }

({TERMSTART}{TERMCHAR}*)?{WILDCHAR}({TERMCHAR}|{WILDCHAR})*  {
                              /* A pattern anywhere in the word, a leading one
                                 included -- Lucene's classic parser refuses
                                 that one, and nothing downstream needs it to. */
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return WILDCARD;
                            }

{TERMSTART}{TERMCHAR}*      {
                              yylval->sv = {yytext, static_cast<size_t>(yyleng)};
                              return TERM;
                            }

.                           { return YYUNDEF; }

%%

void LexerSetInput(std::string_view input) {
    if (current_buffer) {
        yy_delete_buffer(current_buffer);
    }
    /* A query that failed to parse while inside quotes left the scanner
       there, and the one after it would be read as the rest of a phrase --
       silently, since a word is a word in both. Every query starts outside. */
    BEGIN(INITIAL);
    current_buffer = yy_scan_bytes(input.data(), static_cast<int>(input.size()));
}

void LexerCleanup() {
    if (current_buffer) {
        yy_delete_buffer(current_buffer);
        current_buffer = nullptr;
    }
}

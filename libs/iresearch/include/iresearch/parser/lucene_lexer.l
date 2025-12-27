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

#include <cstdlib>
#include <cstring>
#include <string_view>
#include "lucene_parser.hpp"

static YY_BUFFER_STATE current_buffer = nullptr;
%}

%option noyywrap
%option nounput
%option noinput

DIGIT       [0-9]
ALPHA       [a-zA-Z_]
ALNUM       [a-zA-Z0-9_]
WILDCHAR    [*?]
ESCAPED     \\[+\-&|!(){}[\]^"~*?:\\/]

%%

[ \t\r\n]+                  { /* Skip whitespace */ }

"AND"|"&&"                  { return AND; }
"OR"|"||"                   { return OR; }
"NOT"|"!"                   { return NOT; }
"TO"                        { return TO; }

"("                         { return LPAREN; }
")"                         { return RPAREN; }
"["                         { return LBRACKET; }
"]"                         { return RBRACKET; }
"{"                         { return LBRACE; }
"}"                         { return RBRACE; }
":"                         { return COLON; }
"^"                         { return CARET; }
"~"                         { return TILDE; }
"+"                         { return PLUS; }
"-"                         { return MINUS; }

{DIGIT}+"."{DIGIT}+         {
                              yylval.fnum = std::strtof(yytext, nullptr);
                              return FLOAT;
                            }

{DIGIT}+                    {
                              yylval.num = std::atoi(yytext);
                              return NUMBER;
                            }

\"([^\"\\]|\\.)*\"          {
                              /* Quoted phrase - point to content inside quotes */
                              yylval.sv = {yytext + 1, static_cast<size_t>(yyleng - 2)};
                              return PHRASE;
                            }

\/([^\/\\]|\\.)*\/          {
                              /* Regex pattern - point to content inside slashes */
                              yylval.sv = {yytext + 1, static_cast<size_t>(yyleng - 2)};
                              return REGEX;
                            }

({ALNUM}|{ESCAPED})+\*      {
                              /* Check if it's a pure prefix (ends with single *, no other wildcards) */
                              bool has_inner_wild = false;
                              for (int i = 0; i < yyleng - 1; i++) {
                                  if (yytext[i] == '\\') { i++; continue; }
                                  if (yytext[i] == '*' || yytext[i] == '?') {
                                      has_inner_wild = true;
                                      break;
                                  }
                              }
                              yylval.sv = {yytext, static_cast<size_t>(yyleng)};
                              return has_inner_wild ? WILDCARD : PREFIX;
                            }

\*({ALNUM}|{ESCAPED})+      {
                              /* Check if it's a pure suffix (starts with single *, no other wildcards) */
                              bool has_inner_wild = false;
                              for (int i = 1; i < yyleng; i++) {
                                  if (yytext[i] == '\\') { i++; continue; }
                                  if (yytext[i] == '*' || yytext[i] == '?') {
                                      has_inner_wild = true;
                                      break;
                                  }
                              }
                              yylval.sv = {yytext, static_cast<size_t>(yyleng)};
                              return has_inner_wild ? WILDCARD : SUFFIX;
                            }

"*"                         {
                              /* Standalone star (for range bounds) */
                              yylval.sv = {yytext, 1};
                              return STAR;
                            }

({ALNUM}|{ESCAPED})*({WILDCHAR}({ALNUM}|{ESCAPED})*)+  {
                              /* General wildcard pattern with * or ? */
                              yylval.sv = {yytext, static_cast<size_t>(yyleng)};
                              return WILDCARD;
                            }

({ALPHA}|{ESCAPED})({ALNUM}|{ESCAPED})*  {
                              yylval.sv = {yytext, static_cast<size_t>(yyleng)};
                              return TERM;
                            }

{DIGIT}+({ALNUM}|{ESCAPED})*  {
                              /* Terms starting with digits (like dates) */
                              yylval.sv = {yytext, static_cast<size_t>(yyleng)};
                              return TERM;
                            }

.                           {
                              std::fprintf(stderr, "Unexpected character: %c\n", yytext[0]);
                            }

%%

void LexerSetInput(std::string_view input) {
    if (current_buffer) {
        yy_delete_buffer(current_buffer);
    }
    current_buffer = yy_scan_bytes(input.data(), static_cast<int>(input.size()));
}

void LexerCleanup() {
    if (current_buffer) {
        yy_delete_buffer(current_buffer);
        current_buffer = nullptr;
    }
}

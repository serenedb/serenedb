////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/parser/parser.h>

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "catalog/mangling.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {
namespace {

enum class WsTokKind { Word, Phrase, Or };

struct WsToken {
  WsTokKind kind;
  std::string text;
  bool negated = false;
};

std::vector<WsToken> LexWebsearch(std::string_view text) {
  std::vector<WsToken> out;
  size_t i = 0;
  const size_t n = text.size();
  while (i < n) {
    while (i < n && std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    if (i >= n) {
      break;
    }

    bool neg = false;
    if (text[i] == '-') {
      neg = true;
      ++i;
      if (i >= n || std::isspace(static_cast<unsigned char>(text[i]))) {
        // lone '-' -- ignore
        continue;
      }
    }

    if (text[i] == '"') {
      ++i;
      size_t start = i;
      while (i < n && text[i] != '"') {
        ++i;
      }
      // Drop empty quoted segments (`""` or `-""`) -- they have no
      // searchable content and would otherwise reach BuildFtsPhrase
      // which rejects empty input.
      if (i > start) {
        out.push_back(
          {WsTokKind::Phrase, std::string{text.substr(start, i - start)}, neg});
      }
      if (i < n) {
        ++i;  // consume closing quote
      }
      continue;
    }

    size_t start = i;
    while (i < n && !std::isspace(static_cast<unsigned char>(text[i])) &&
           text[i] != '"') {
      ++i;
    }
    if (i == start) {
      // No characters consumed (shouldn't happen given the outer
      // whitespace skip + EOF check, but guards against future edits).
      continue;
    }
    std::string word{text.substr(start, i - start)};
    // `OR` keyword: case-insensitive, exactly 2 chars, and only when it
    // isn't negated (the user wrote `-OR` or similar).
    if (!neg && word.size() == 2 && (word[0] == 'o' || word[0] == 'O') &&
        (word[1] == 'r' || word[1] == 'R')) {
      out.push_back({WsTokKind::Or, {}, false});
    } else {
      out.push_back({WsTokKind::Word, std::move(word), neg});
    }
  }
  return out;
}

// Groups OR-chained atoms. Stray OR tokens (at start / end / between
// other ORs) are silently dropped.
std::vector<std::vector<WsToken>> GroupWebsearch(
  const std::vector<WsToken>& tokens) {
  std::vector<std::vector<WsToken>> groups;
  size_t i = 0;
  while (i < tokens.size()) {
    if (tokens[i].kind == WsTokKind::Or) {
      ++i;
      continue;
    }
    std::vector<WsToken> group;
    group.push_back(tokens[i]);
    ++i;
    while (i + 1 < tokens.size() && tokens[i].kind == WsTokKind::Or &&
           tokens[i + 1].kind != WsTokKind::Or) {
      group.push_back(tokens[i + 1]);
      i += 2;
    }
    groups.push_back(std::move(group));
  }
  return groups;
}

void ParseWebsearchQuery(std::string_view text,
                         const SearchColumnInfo& column_info,
                         const FilterContext& ctx, irs::BooleanFilter& parent) {
  const auto groups = GroupWebsearch(LexWebsearch(text));
  if (groups.empty()) {
    AddFilter<irs::Empty>(parent);
    return;
  }

  auto emit_atom = [&](const WsToken& tok, irs::BooleanFilter& into,
                       const FilterContext& c) {
    auto ac = c;
    ac.negated = c.negated ^ tok.negated;
    if (tok.kind == WsTokKind::Phrase) {
      BuildFtsPhrase(into, ac, column_info, tok.text);
    } else {
      BuildFtsTokens(into, ac, column_info, tok.text, /*require_all=*/false);
    }
  };

  auto emit_group = [&](const std::vector<WsToken>& group,
                        irs::BooleanFilter& into, const FilterContext& c) {
    if (group.size() == 1) {
      emit_atom(group[0], into, c);
      return;
    }
    auto& or_group =
      c.negated ? Negate<irs::Or>(into) : AddFilter<irs::Or>(into);
    or_group.boost(c.boost);
    auto inner = c;
    inner.negated = false;
    inner.boost = irs::kNoBoost;
    for (const auto& tok : group) {
      emit_atom(tok, or_group, inner);
    }
  };

  if (groups.size() == 1) {
    emit_group(groups[0], parent, ctx);
    return;
  }

  auto& and_group =
    ctx.negated ? Negate<irs::And>(parent) : AddFilter<irs::And>(parent);
  and_group.boost(ctx.boost);
  auto inner = ctx;
  inner.negated = false;
  inner.boost = irs::kNoBoost;
  for (const auto& group : groups) {
    emit_group(group, and_group, inner);
  }
}

}  // namespace

void FromPlainToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                        const SearchColumnInfo& column_info,
                        const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: plainto_tsquery('quick fox'). AND-semantics over tokens.";
  SDB_ASSERT(func.children.size() == 1);
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "plainto_tsquery text", text);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  BuildFtsTokens(parent, ctx, column_info, text, /*require_all=*/true);
}

// websearch_to_tsquery(text): PG-style web-search syntax (quoted
// phrases, OR keyword, leading `-` for NOT).
void FromWebsearchToTsquery(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: websearch_to_tsquery('\"quick fox\" -slow OR fast').";
  SDB_ASSERT(func.children.size() == 1);
  std::string text;
  if (auto r =
        GetVarcharArg(*func.children[0], "websearch_to_tsquery text", text);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  ParseWebsearchQuery(text, column_info, ctx, parent);
}

// tsquery_phrase(q1, q2 [, distance]): function form of `##`, PG
// semantics (distance = lexemes apart, N=1 = adjacent). Same shape as
// the `##` walker: flatten + emit.
void FromTsqueryPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::BoundFunctionExpression& func) {
  PhraseSeq seq;
  FlattenPhraseSeq(func, seq);
  EmitPhraseSeq(parent, ctx, column_info, seq);
}

void FromToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                   const SearchColumnInfo& column_info,
                   const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: to_tsquery('field:foo AND bar*'). Lucene syntax: "
    "AND/OR/NOT, +/-, prefix/wildcard/regex, ranges, ^N, ~N.";
  SDB_ASSERT(func.children.size() == 1);
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "to_tsquery text", text);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  auto& mixed = ctx.negated ? Negate<irs::MixedBooleanFilter>(parent)
                            : AddFilter<irs::MixedBooleanFilter>(parent);
  mixed.boost(ctx.boost);
  sdb::ParserContext parser_ctx{mixed, field_name, ctx.tokenizer};
  parser_ctx.strict_field = true;
  if (auto r = sdb::ParseQuery(parser_ctx, text); !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("to_tsquery parse error: ", r.errorMessage()),
                    ERR_HINT(kSyntaxHint));
  }
}

}  // namespace sdb::connector

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

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/assert.h"
#include "catalog/mangling.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search.h"
#include "string.h"
#include "ts_common.hpp"

namespace sdb::connector {

TSQueryOp ClassifyTSQueryFunction(std::string_view name);

namespace {

PhraseGap ParsePhraseGap(const duckdb::Value& val, std::string_view label,
                         std::string_view hint,
                         std::optional<size_t> arg_index = std::nullopt) {
  auto error = [&](auto&&... msg) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(label, std::forward<decltype(msg)>(msg)...,
              arg_index ? absl::StrCat(" (argument ", *arg_index, ")")
                        : std::string{}),
      ERR_HINT(hint));
  };
  auto coerce = [&](const duckdb::Value& v) -> int64_t {
    duckdb::Value out;
    if (v.IsNull() || !v.type().IsNumeric() ||
        !v.DefaultTryCastAs(duckdb::LogicalType::BIGINT, out,
                            /*error_message=*/nullptr, /*strict=*/true)) {
      error(" gap must be a non-null non-negative integer, got ", v.ToString(),
            " of type ", v.type().ToString());
    }
    auto raw = out.GetValue<int64_t>();
    if (raw < 0) {
      error(" gap must be >= 0, got ", raw);
    }
    return raw;
  };

  const auto id = val.type().id();
  if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::ARRAY) {
    const auto& children = id == duckdb::LogicalTypeId::ARRAY
                             ? duckdb::ArrayValue::GetChildren(val)
                             : duckdb::ListValue::GetChildren(val);
    if (children.size() != 2) {
      error(" interval gap must be a 2-element list [min, max], got ",
            children.size(), " elements");
    }
    auto min = coerce(children[0]);
    auto max = coerce(children[1]);
    if (min > max) {
      error(" interval gap must satisfy 0 <= min <= max, got [", min, ", ", max,
            "]");
    }
    return {.min = static_cast<size_t>(min) + 1,
            .max = static_cast<size_t>(max) + 1};
  }
  auto raw = coerce(val);
  return {.min = static_cast<size_t>(raw) + 1,
          .max = static_cast<size_t>(raw) + 1};
}

}  // namespace

void FromPhrase(irs::BooleanFilter& filter, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_phrase('quick brown fox') or ts_phrase('a', 1, 'b'). "
    "INTEGER / INTEGER[] gap allowed between text args.";
  SDB_ASSERT(!func.children.empty());

  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_phrase field is not VARCHAR"),
                    ERR_HINT(kSyntaxHint));
  }

  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_phrase field should have Positions and Frequency features "
              "enabled"),
      ERR_HINT("Recreate the inverted index with both `Positions` and "
               "`Frequency` features attached to the column."));
  }

  std::string field_name;
  MakeFieldName(column_info.field_id, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(filter)
                             : AddFilter<irs::ByPhrase>(filter);
  phrase.boost(ctx.boost);
  *phrase.mutable_field() = field_name;
  auto* opts = phrase.mutable_options();
  auto& analyzer = ctx.tokenizer;
  const irs::TermAttr* token = irs::get<irs::TermAttr>(analyzer);

  std::optional<PhraseGap> pending_gap;

  for (size_t i = 0; i < func.children.size(); ++i) {
    const auto* const_val = TryGetConstant(*func.children[i]);
    if (!const_val) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_phrase argument ", i, " must be a constant"),
                      ERR_HINT(kSyntaxHint));
    }
    if (const_val->type().id() == duckdb::LogicalTypeId::VARCHAR ||
        const_val->type().id() == duckdb::LogicalTypeId::BLOB) {
      auto text = duckdb::StringValue::Get(*const_val);
      analyzer.reset(std::string_view{text});
      while (analyzer.next()) {
        if (pending_gap) {
          // First token of a new text pattern: apply pending gap.
          opts
            ->push_back<irs::ByTermOptions>(pending_gap->min, pending_gap->max)
            .term.assign(token->value);
        } else {
          // No pending gap: first term or adjacent token within same
          // pattern.
          opts->push_back<irs::ByTermOptions>().term.assign(token->value);
        }
        pending_gap.reset();
      }
      continue;
    }
    if (opts->empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_phrase gap at argument ", i,
                              " must be preceded by a text pattern"),
                      ERR_HINT(kSyntaxHint));
    }
    if (pending_gap) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_phrase has consecutive gaps at argument ", i),
                      ERR_HINT(kSyntaxHint));
    }
    pending_gap = ParsePhraseGap(*const_val, "ts_phrase", kSyntaxHint, i);
  }

  if (pending_gap) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_phrase ends with a gap; a text pattern must follow each gap"),
      ERR_HINT(kSyntaxHint));
  }
  if (opts->empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_phrase text arguments produced no searchable terms"),
      ERR_HINT("All ts_phrase text arguments tokenised to nothing (e.g. "
               "all-stopword input). Provide at least one searchable term."));
  }
}

namespace {

void EmitPhraseTokens(irs::ByPhraseOptions& options, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text, PhraseGap base_gap) {
  auto& analyzer = ctx.tokenizer;
  if (!analyzer.reset(text)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_phrase failed to analyse '", text, "'"),
                    ERR_HINT("The column's analyzer rejected the input text."));
  }
  const auto* token = irs::get<irs::TermAttr>(analyzer);
  bool first = true;
  while (analyzer.next()) {
    const PhraseGap g = first ? base_gap : PhraseGap{1, 1};
    auto& part = options.push_back<irs::ByTermOptions>(g.min, g.max);
    part.term.assign(token->value);
    first = false;
  }
  if (first) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_phrase('", text, "') produced no tokens after analysis"),
      ERR_HINT("All tokens were stripped (e.g. all-stopword input). Provide "
               "at least one searchable term."));
  }
}

}  // namespace

void BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view text) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_phrase field is not VARCHAR"));
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_phrase field should have Positions and Frequency features "
              "enabled"),
      ERR_HINT("Recreate the inverted index with both `Positions` and "
               "`Frequency` features attached to the column."));
  }
  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  std::string field_name;
  MakeFieldName(column_info.field_id, field_name);
  search::mangling::MangleString(field_name);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);
  EmitPhraseTokens(*phrase.mutable_options(), ctx, column_info, text,
                   PhraseGap{});
}

PhraseGap ParsePhraseSeqGap(const duckdb::Expression& expr) {
  static constexpr std::string_view kHint =
    "Use a literal INTEGER (e.g. ts_phrase('a') ## 1 ## 'b') or a "
    "2-element INTEGER[] interval.";
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("## gap must be a constant"), ERR_HINT(kHint));
  }
  return ParsePhraseGap(*val, "##", kHint);
}

namespace {

PhraseGap ParseTsqueryPhraseDistance(const duckdb::Expression& expr) {
  static constexpr std::string_view kHint =
    "Use a literal positive INTEGER (e.g. tsquery_phrase(q1, q2, 3)). "
    "Distance follows PG semantics: 1 = adjacent.";
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("tsquery_phrase distance must be a constant"),
                    ERR_HINT(kHint));
  }
  const auto id = val->type().id();
  if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::ARRAY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("tsquery_phrase distance must be a scalar INTEGER, got "
              "LIST/ARRAY"),
      ERR_HINT(kHint));
  }
  duckdb::Value out;
  if (val->IsNull() || !val->type().IsNumeric() ||
      !val->DefaultTryCastAs(duckdb::LogicalType::BIGINT, out,
                             /*error_message=*/nullptr, /*strict=*/true)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("tsquery_phrase distance must be a non-null integer, got ",
              val->ToString(), " of type ", val->type().ToString()),
      ERR_HINT(kHint));
  }
  const auto raw = out.GetValue<int64_t>();
  if (raw < 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("tsquery_phrase distance must be >= 1, got ", raw),
                    ERR_HINT(kHint));
  }
  return {.min = static_cast<size_t>(raw), .max = static_cast<size_t>(raw)};
}

bool IsPhraseSeqGapType(const duckdb::Expression& expr) {
  const auto& type = expr.GetReturnType();
  const auto id = type.id();
  return type.IsNumeric() || id == duckdb::LogicalTypeId::LIST ||
         id == duckdb::LogicalTypeId::ARRAY;
}

bool IsPhraseSeqNode(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (unwrapped.GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  return f.function.GetName() == kTSQueryPhraseSeq ||
         f.function.GetName() == kTsqueryPhrase;
}

}  // namespace

void AttachPart(PhraseSeq& seq, const duckdb::Expression& next) {
  if (IsPhraseSeqNode(next)) {
    PhraseSeq sub;
    FlattenPhraseSeq(next, sub);
    if (sub.parts.empty()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("## produced no parts"),
        ERR_HINT("Each side of `##` must contribute at least one phrase "
                 "part."));
    }
    for (size_t i = 0; i < sub.parts.size(); ++i) {
      if (!seq.parts.empty() && i == 0) {
        seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
        seq.pending.reset();
      } else if (i > 0) {
        seq.gaps.push_back(sub.gaps[i - 1]);
      }
      seq.parts.push_back(sub.parts[i]);
    }
    seq.pending = sub.pending;
    return;
  }
  if (!seq.parts.empty()) {
    seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
    seq.pending.reset();
  }
  seq.parts.push_back(&next);
}

void FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (!IsPhraseSeqNode(unwrapped)) {
    if (IsPhraseSeqGapType(unwrapped)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("## gap must appear between two phrase parts"),
        ERR_HINT("Example: ts_phrase('a') ## 1 ## 'b'. A gap (INTEGER or "
                 "INTEGER[]) is only valid between TSQUERY parts."));
    }
    seq.parts.push_back(&unwrapped);
    return;
  }
  const auto& func = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  if (func.function.GetName() == kTsqueryPhrase) {
    SDB_ASSERT(func.children.size() == 2 || func.children.size() == 3);
    FlattenPhraseSeq(*func.children[0], seq);
    if (func.children.size() == 3) {
      if (seq.pending) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("tsquery_phrase distance follows a pending phrase gap"),
          ERR_HINT("A `##` gap must be followed by a part, not by another "
                   "tsquery_phrase distance."));
      }
      seq.pending = ParseTsqueryPhraseDistance(*func.children[2]);
    }
    AttachPart(seq, *func.children[1]);
    return;
  }
  // `##` operator: left-associative binary tree.
  SDB_ASSERT(func.function.GetName() == kTSQueryPhraseSeq);
  SDB_ASSERT(func.children.size() == 2);
  FlattenPhraseSeq(*func.children[0], seq);
  const auto& right = *func.children[1];
  if (IsPhraseSeqGapType(right)) {
    if (seq.pending) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("## gap must be followed by a phrase part"),
        ERR_HINT("Consecutive gaps are not allowed; place a TSQUERY part "
                 "between them."));
    }
    seq.pending = ParsePhraseSeqGap(right);
    return;
  }
  AttachPart(seq, right);
}

// Emits the flattened phrase sequence as an irs::ByPhrase under `parent`.
void EmitPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                   const SearchColumnInfo& column_info, const PhraseSeq& seq) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_phrase('hello') ## 1 ## 'world'. Gap is optional "
    "INTEGER / INTEGER[].";
  if (seq.parts.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("## phrase has no parts"), ERR_HINT(kSyntaxHint));
  }
  if (seq.pending) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("## trailing gap must be followed by a phrase "
                            "part"),
                    ERR_HINT(kSyntaxHint));
  }
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("## field is not VARCHAR"), ERR_HINT(kSyntaxHint));
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("## field should have Positions and Frequency features "
              "enabled"),
      ERR_HINT("Recreate the inverted index with both `Positions` and "
               "`Frequency` features attached to the column."));
  }

  std::string field_name;
  MakeFieldName(column_info.field_id, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);

  auto* options = phrase.mutable_options();

  // Emit one phrase part per input, using the gap offsets. First part
  // is always at offset 0 (iresearch normalises this internally). Each
  // case in the switch is self-contained: it parses the part's args,
  // pushes a phrase-part Options slot at (gap.offs_min, gap.offs_max),
  // and breaks. The push_back overload with (offs_min, offs_max) is
  // the interval form; we always use it (rather than the single-arg
  // shorthand) to keep offset semantics consistent between exact and
  // range gaps.
  for (size_t i = 0; i < seq.parts.size(); ++i) {
    const auto& part_expr_ref = UnwrapTSQueryCast(*seq.parts[i]);
    const PhraseGap gap = i > 0 ? seq.gaps[i - 1] : PhraseGap{};

    TSQueryOp leaf_op;
    const duckdb::BoundFunctionExpression* f = nullptr;
    std::string bare_text;
    if (part_expr_ref.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      const auto& val =
        part_expr_ref.Cast<duckdb::BoundConstantExpression>().value;
      if (val.IsNull() || (val.type().id() != duckdb::LogicalTypeId::VARCHAR &&
                           val.type().id() != duckdb::LogicalTypeId::BLOB)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("## part must be a VARCHAR constant"),
                        ERR_HINT(kSyntaxHint));
      }
      bare_text = duckdb::StringValue::Get(val);
      leaf_op = TSQueryOp::Term;
    } else if (part_expr_ref.GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      f = &part_expr_ref.Cast<duckdb::BoundFunctionExpression>();
      leaf_op = ClassifyTSQueryFunction(f->function.GetName());
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("## part expression class: ",
                static_cast<int>(part_expr_ref.GetExpressionClass())),
        ERR_HINT(kSyntaxHint));
    }

    auto get_text_arg = [&] {
      std::string out;
      if (!f) {
        out = bare_text;
        return out;
      }
      if (f->children.size() != 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("## ", f->function.GetName(),
                  " phrase part expects 1 argument, got ", f->children.size()),
          ERR_HINT(kSyntaxHint));
      }
      if (auto r = GetVarcharArg(*f->children[0], "## phrase part text", out);
          !r.ok()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
      }
      return out;
    };

    switch (leaf_op) {
      case TSQueryOp::Term: {
        auto text = get_text_arg();
        options->push_back<irs::ByTermOptions>(gap.min, gap.max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Prefix: {
        auto text = get_text_arg();
        options->push_back<irs::ByPrefixOptions>(gap.min, gap.max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Like: {
        auto text = get_text_arg();
        auto pattern = LikeEscapePattern(text, '\\');
        options->push_back<irs::ByWildcardOptions>(gap.min, gap.max)
          .term.assign(
            irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
        break;
      }
      case TSQueryOp::Fuzzy: {
        auto args = ParseLevenshteinArgs(*f);
        FillByEditDistanceOptions(
          args,
          options->push_back<irs::ByEditDistanceOptions>(gap.min, gap.max));
        break;
      }
      case TSQueryOp::Phrase: {
        // Nested ts_phrase('x y z') -> tokenise via column analyzer and
        // emit one term part per token. The FIRST token uses the
        // incoming gap; subsequent tokens are strictly adjacent. Shared
        // with BuildFtsPhrase via EmitPhraseTokens.
        if (f->children.empty() || f->children.size() > 2) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("## ts_phrase phrase part expects 1 or 2 arguments "
                    "(text[, slop]), got ",
                    f->children.size()),
            ERR_HINT(kSyntaxHint));
        }
        std::string phrase_text;
        if (auto r =
              GetVarcharArg(*f->children[0], "## ts_phrase text", phrase_text);
            !r.ok()) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
        }
        EmitPhraseTokens(*options, ctx, column_info, phrase_text, gap);
        break;
      }
      case TSQueryOp::Any: {
        // ts_any as a phrase part -> ByTermsOptions slot with the
        // listed terms as alternatives at this phrase position. Only
        // `ts_any([list])` and `ts_any([list], 1)` are accepted:
        // iresearch's phrase filter ignores min_match for a
        // ByTermsOptions slot (a single position holds at most one
        // token, so min_match > 1 is unsatisfiable).
        std::vector<const duckdb::Expression*> sub_args;
        std::vector<duckdb::unique_ptr<duckdb::Expression>> sub_synth;
        std::optional<size_t> sub_min_match;
        ExtractAnyAllOfArgs(*f, /*is_any=*/true, sub_args, sub_synth,
                            sub_min_match);
        if (sub_min_match && *sub_min_match != 1) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("## ts_any phrase part requires min_match=1 (got ",
                    *sub_min_match,
                    "); a phrase position can match only "
                    "one token"),
            ERR_HINT("Drop the min_match argument or set it to 1."));
        }
        auto& terms_opts =
          options->push_back<irs::ByTermsOptions>(gap.min, gap.max);
        terms_opts.min_match = 1;
        for (const auto* arg : sub_args) {
          std::string term_text;
          if (auto r = GetVarcharArg(UnwrapTSQueryCast(*arg),
                                     "## ts_any phrase part term", term_text);
              !r.ok()) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
          }
          terms_opts.terms.emplace(
            irs::ViewCast<irs::byte_type>(std::string_view{term_text}));
        }
        break;
      }
      case TSQueryOp::All:
        // ts_all rejected for the same reason min_match > 1 is rejected
        // for ts_any: a phrase position can match only one token.
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("## ts_all phrase part is not supported (a phrase position "
                  "can match only one token; use ts_any instead)"),
          ERR_HINT(kSyntaxHint));
      case TSQueryOp::Between: {
        // ts_between as a phrase part -> ByRangeOptions slot. Only the
        // VARCHAR variant is meaningful here: phrases live on the
        // analyzed text field, so numeric / boolean ranges (which would
        // target separate fields) make no sense at a phrase position.
        auto args = ParseRangeArgs(*f);
        if ((args.min &&
             args.min->type().id() != duckdb::LogicalTypeId::VARCHAR &&
             args.min->type().id() != duckdb::LogicalTypeId::BLOB) ||
            (args.max &&
             args.max->type().id() != duckdb::LogicalTypeId::VARCHAR &&
             args.max->type().id() != duckdb::LogicalTypeId::BLOB)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("## ts_between phrase part requires VARCHAR bounds"),
            ERR_HINT("Phrase parts live on the analyzed VARCHAR field; "
                     "numeric / BOOLEAN ranges target other fields."));
        }
        FillByRangeOptionsVarchar(
          args, options->push_back<irs::ByRangeOptions>(gap.min, gap.max));
        break;
      }
      default:
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("## part type not supported yet: ",
                  f ? f->function.GetName() : "<bare-const>"),
          ERR_HINT("Supported phrase parts: bare 'word', ts_starts_with, "
                   "ts_like, ts_levenshtein, ts_phrase, ts_any, ts_between."));
    }
  }
}

void FromTSQueryPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                          const SearchColumnInfo& column_info,
                          const duckdb::BoundFunctionExpression& func) {
  PhraseSeq seq;
  FlattenPhraseSeq(func, seq);
  EmitPhraseSeq(parent, ctx, column_info, seq);
}

}  // namespace sdb::connector

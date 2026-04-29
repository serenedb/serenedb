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

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/assert.h"
#include "catalog/mangling.h"
#include "functions/search.h"
#include "functions/string.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "tsq_common.hpp"

namespace sdb::connector {

TSQueryOp ClassifyTSQueryFunction(std::string_view name);

namespace {

// True iff `id` is one of the integral types we accept as a phrase-gap
// scalar (any signed integer narrower than or equal to BIGINT). Values
// in this set need a sign check at parse time.
bool IsPhraseGapIntegralTypeId(duckdb::LogicalTypeId id) {
  return id == duckdb::LogicalTypeId::TINYINT ||
         id == duckdb::LogicalTypeId::SMALLINT ||
         id == duckdb::LogicalTypeId::INTEGER ||
         id == duckdb::LogicalTypeId::BIGINT;
}

// Parses a constant Value into a PhraseGap. Accepts a non-negative
// integral scalar for an exact gap, or a 2-element LIST/ARRAY of
// non-negative integers for an interval gap (min <= max). The returned
// PhraseGap has offsets already +1-adjusted; callers can hand it
// directly to ByPhraseOptions::push_back(offs_min, offs_max). On error,
// returns a Result whose message is prefixed with `label` (e.g.
// "PHRASE", "##") so the call site is identifiable.
ResultOr<PhraseGap> ParsePhraseGap(const duckdb::Value& val,
                                   std::string_view label) {
  auto err = [&](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (val.IsNull()) {
    return err(label, " gap must be a non-null constant");
  }
  const auto id = val.type().id();
  if (IsPhraseGapIntegralTypeId(id)) {
    auto raw = val.GetValue<int64_t>();
    if (raw < 0) {
      return err(label, " gap must be >= 0, got ", raw);
    }
    return PhraseGap{.offs_min = static_cast<size_t>(raw) + 1,
                     .offs_max = static_cast<size_t>(raw) + 1};
  }
  if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::ARRAY) {
    const auto& children = id == duckdb::LogicalTypeId::ARRAY
                             ? duckdb::ArrayValue::GetChildren(val)
                             : duckdb::ListValue::GetChildren(val);
    if (children.size() != 2) {
      return err(label,
                 " interval gap must be a 2-element list [min, max], got ",
                 children.size(), " elements");
    }
    if (!IsPhraseGapIntegralTypeId(children[0].type().id()) ||
        !IsPhraseGapIntegralTypeId(children[1].type().id()) ||
        children[0].IsNull() || children[1].IsNull()) {
      return err(label, " interval gap elements must be non-negative integers");
    }
    auto lo = children[0].GetValue<int64_t>();
    auto hi = children[1].GetValue<int64_t>();
    if (lo < 0 || hi < 0 || lo > hi) {
      return err(label, " interval gap must satisfy 0 <= min <= max, got [", lo,
                 ", ", hi, "]");
    }
    return PhraseGap{.offs_min = static_cast<size_t>(lo) + 1,
                     .offs_max = static_cast<size_t>(hi) + 1};
  }
  return err(label, " gap has unsupported type ", val.type().ToString(),
             "; expected non-negative INTEGER or 2-element INTEGER[] for an "
             "interval gap");
}

}  // namespace

Result FromPhrase(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  // PHRASE is registered with at least one VARCHAR arg (plus variadic
  // ANY tail), so DuckDB's function resolver rejects empty calls at
  // bind time before we get here.
  SDB_ASSERT(!func.children.empty());

  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }

  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(filter)
                             : AddFilter<irs::ByPhrase>(filter);
  phrase.boost(ctx.boost);
  *phrase.mutable_field() = field_name;
  auto* opts = phrase.mutable_options();
  auto& analyzer = ctx.tokenizer;
  const irs::TermAttr* token = irs::get<irs::TermAttr>(analyzer);

  std::optional<PhraseGap> pending_gap;

  // A non-constant argument is an index-only restriction -- a future
  // full-scan PHRASE executor could handle it -- so we return Result
  // and let the caller roll back any partially-built phrase filter so
  // this predicate falls through to regular execution. All OTHER
  // violations below are gap-grammar errors: the PHRASE call is
  // malformed and no executor can satisfy it, so they throw with a
  // specific message rather than letting SearchStubFn surface its
  // generic "outside inverted index context" error.
  for (size_t i = 0; i < func.children.size(); ++i) {
    const auto* const_val = TryGetConstant(*func.children[i]);
    if (!const_val) {
      return {ERROR_BAD_PARAMETER, "PHRASE argument ", i,
              " must be a constant"};
    }
    if (const_val->type().id() == duckdb::LogicalTypeId::VARCHAR) {
      auto text = const_val->GetValue<std::string>();
      analyzer.reset(std::string_view{text});
      while (analyzer.next()) {
        if (pending_gap) {
          // First token of a new text pattern: apply pending gap.
          opts
            ->push_back<irs::ByTermOptions>(pending_gap->offs_min,
                                            pending_gap->offs_max)
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
    auto gap = ParsePhraseGap(*const_val, "PHRASE");
    if (!gap) {
      THROW_SQL_ERROR(
        ERR_CODE(ERROR_BAD_PARAMETER),
        ERR_MSG(gap.error().errorMessage(), " (argument ", i, ")"));
    }
    if (opts->empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                      ERR_MSG("PHRASE gap at argument ", i,
                              " must be preceded by a text pattern"));
    }
    if (pending_gap) {
      THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                      ERR_MSG("PHRASE has consecutive gaps at argument ", i));
    }
    pending_gap = *gap;
  }

  if (pending_gap) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_BAD_PARAMETER),
      ERR_MSG("PHRASE ends with a gap; a text pattern must follow each gap"));
  }
  if (opts->empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_BAD_PARAMETER),
      ERR_MSG("PHRASE text arguments produced no searchable terms"));
  }
  return {};
}

namespace {

Result EmitPhraseTokens(irs::ByPhraseOptions& options, const FilterContext& ctx,
                        const SearchColumnInfo& column_info,
                        std::string_view text, PhraseGap base_gap) {
  auto& analyzer = ctx.tokenizer;
  if (!analyzer.reset(text)) {
    return {ERROR_BAD_PARAMETER, "PHRASE failed to analyse '", text, "'"};
  }
  const auto* token = irs::get<irs::TermAttr>(analyzer);
  bool first = true;
  while (analyzer.next()) {
    const PhraseGap g = first ? base_gap : PhraseGap{1, 1};
    auto& part = options.push_back<irs::ByTermOptions>(g.offs_min, g.offs_max);
    part.term.assign(token->value);
    first = false;
  }
  if (first) {
    return {ERROR_BAD_PARAMETER, "PHRASE('", text,
            "') produced no tokens after analysis"};
  }
  return {};
}

}  // namespace

Result BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }
  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);
  return EmitPhraseTokens(*phrase.mutable_options(), ctx, column_info, text,
                          PhraseGap{});
}

// Expression-level wrapper for `##`: extracts the constant Value from
// `expr` (unwrapping any TSQUERY casts) and delegates to ParsePhraseGap.
ResultOr<PhraseGap> ParsePhraseSeqGap(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   "## gap must be a constant"};
  }
  return ParsePhraseGap(*val, "##");
}

namespace {

// True iff `expr`'s return_type indicates a gap operand (bare INTEGER
// or INTEGER[]) rather than a TSQUERY part. Used to distinguish the
// RHS of a `##` binary: if INTEGER/INTEGER[], treat as gap; else as
// next phrase part.
bool IsPhraseSeqGapType(const duckdb::Expression& expr) {
  const auto id = expr.return_type.id();
  return IsPhraseGapIntegralTypeId(id) || id == duckdb::LogicalTypeId::LIST ||
         id == duckdb::LogicalTypeId::ARRAY;
}

// True iff `expr` is a ## FunctionExpression.
bool IsPhraseSeqNode(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (unwrapped.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  return f.function.name == kTSQueryPhraseSeq;
}

}  // namespace

// Attaches `next` (a part OR a gap-bearing sub-expression) to `seq`,
// using the `pending` gap if any, defaulting to "adjacent" otherwise.
Result AttachPart(PhraseSeq& seq, const duckdb::Expression& next) {
  if (IsPhraseSeqNode(next)) {
    PhraseSeq sub;
    if (auto r = FlattenPhraseSeq(next, sub); !r.ok()) {
      return r;
    }
    if (sub.parts.empty()) {
      return {ERROR_BAD_PARAMETER, "## produced no parts"};
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
    return {};
  }
  if (!seq.parts.empty()) {
    seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
    seq.pending.reset();
  }
  seq.parts.push_back(&next);
  return {};
}

Result FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (!IsPhraseSeqNode(unwrapped)) {
    if (IsPhraseSeqGapType(unwrapped)) {
      return {ERROR_BAD_PARAMETER,
              "## gap must appear between two phrase parts"};
    }
    seq.parts.push_back(&unwrapped);
    return {};
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  if (f.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "## expects 2 arguments (lhs ## rhs), got ",
            f.children.size()};
  }
  if (auto r = FlattenPhraseSeq(*f.children[0], seq); !r.ok()) {
    return r;
  }
  const auto& right = *f.children[1];
  if (IsPhraseSeqGapType(right)) {
    if (seq.pending) {
      return {ERROR_BAD_PARAMETER, "## gap must be followed by a phrase part"};
    }
    auto gap = ParsePhraseSeqGap(right);
    if (!gap) {
      return std::move(gap.error());
    }
    seq.pending = *gap;
    return {};
  }
  return AttachPart(seq, right);
}

// Emits the flattened phrase sequence as an irs::ByPhrase under `parent`.
Result EmitPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const PhraseSeq& seq) {
  if (seq.parts.empty()) {
    return {ERROR_BAD_PARAMETER, "## phrase has no parts"};
  }
  if (seq.pending) {
    return {ERROR_BAD_PARAMETER,
            "## trailing gap must be followed by a phrase part"};
  }
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "## field is not VARCHAR"};
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "## field should have Positions and Frequency features enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
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
    if (part_expr_ref.expression_class ==
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      const auto& val =
        part_expr_ref.Cast<duckdb::BoundConstantExpression>().value;
      if (val.IsNull() || val.type().id() != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_BAD_PARAMETER, "## part must be a VARCHAR constant"};
      }
      bare_text = val.GetValue<std::string>();
      leaf_op = TSQueryOp::Term;
    } else if (part_expr_ref.expression_class ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      f = &part_expr_ref.Cast<duckdb::BoundFunctionExpression>();
      leaf_op = ClassifyTSQueryFunction(f->function.name);
    } else {
      return {ERROR_NOT_IMPLEMENTED, "## part expression class: ",
              static_cast<int>(part_expr_ref.expression_class)};
    }

    auto get_text_arg = [&](std::string& out) -> Result {
      if (!f) {
        out = bare_text;
        return {};
      }
      if (f->children.size() != 1) {
        return {ERROR_BAD_PARAMETER, "## ", f->function.name,
                " phrase part expects 1 argument, got ", f->children.size()};
      }
      return GetVarcharArg(*f->children[0], "## phrase part text", out);
    };

    switch (leaf_op) {
      case TSQueryOp::Term: {
        std::string text;
        if (auto r = get_text_arg(text); !r.ok()) {
          return r;
        }
        options->push_back<irs::ByTermOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Prefix: {
        std::string text;
        if (auto r = get_text_arg(text); !r.ok()) {
          return r;
        }
        options->push_back<irs::ByPrefixOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Like: {
        std::string text;
        if (auto r = get_text_arg(text); !r.ok()) {
          return r;
        }
        auto pattern = LikeEscapePattern(text, '\\');
        options->push_back<irs::ByWildcardOptions>(gap.offs_min, gap.offs_max)
          .term.assign(
            irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
        break;
      }
      case TSQueryOp::Fuzzy: {
        auto args = ParseLevenshteinArgs(*f);
        if (!args) {
          return std::move(args.error());
        }
        FillByEditDistanceOptions(
          *args, options->push_back<irs::ByEditDistanceOptions>(gap.offs_min,
                                                                gap.offs_max));
        break;
      }
      case TSQueryOp::Phrase: {
        // Nested PHRASE('x y z') -> tokenise via column analyzer and
        // emit one term part per token. The FIRST token uses the
        // incoming gap; subsequent tokens are strictly adjacent. Shared
        // with BuildFtsPhrase via EmitPhraseTokens.
        if (f->children.empty() || f->children.size() > 2) {
          return {ERROR_BAD_PARAMETER,
                  "## PHRASE phrase part expects 1 or 2 arguments "
                  "(text[, slop]), got ",
                  f->children.size()};
        }
        std::string phrase_text;
        if (auto r =
              GetVarcharArg(*f->children[0], "## PHRASE text", phrase_text);
            !r.ok()) {
          return r;
        }
        if (auto r =
              EmitPhraseTokens(*options, ctx, column_info, phrase_text, gap);
            !r.ok()) {
          return r;
        }
        break;
      }
      case TSQueryOp::AnyOf: {
        // ANY_OF as a phrase part -> ByTermsOptions slot with the
        // listed terms as alternatives at this phrase position. Only
        // `ANY_OF([list])` and `ANY_OF([list], 1)` are accepted:
        // iresearch's phrase filter ignores min_match for a
        // ByTermsOptions slot (a single position holds at most one
        // token, so min_match > 1 is unsatisfiable).
        std::vector<const duckdb::Expression*> sub_args;
        std::vector<duckdb::unique_ptr<duckdb::Expression>> sub_synth;
        std::optional<size_t> sub_min_match;
        if (auto r = ExtractAnyAllOfArgs(*f, /*is_any=*/true, sub_args,
                                         sub_synth, sub_min_match);
            !r.ok()) {
          return r;
        }
        if (sub_min_match && *sub_min_match != 1) {
          throw duckdb::InvalidInputException(
            "## ANY_OF phrase part requires min_match=1 (got %d); a "
            "phrase position can match only one token",
            static_cast<int>(*sub_min_match));
        }
        auto& terms_opts =
          options->push_back<irs::ByTermsOptions>(gap.offs_min, gap.offs_max);
        terms_opts.min_match = 1;
        for (const auto* arg : sub_args) {
          std::string term_text;
          if (auto r = GetVarcharArg(UnwrapTSQueryCast(*arg),
                                     "## ANY_OF phrase part term", term_text);
              !r.ok()) {
            return r;
          }
          terms_opts.terms.emplace(
            irs::ViewCast<irs::byte_type>(std::string_view{term_text}));
        }
        break;
      }
      case TSQueryOp::AllOf:
        // ALL_OF rejected for the same reason min_match > 1 is rejected
        // for ANY_OF: a phrase position can match only one token.
        throw duckdb::InvalidInputException(
          "## ALL_OF phrase part is not supported (a phrase position "
          "can match only one token; use ANY_OF instead)");
      case TSQueryOp::Range: {
        // RANGE as a phrase part -> ByRangeOptions slot. Only the
        // VARCHAR variant is meaningful here: phrases live on the
        // analyzed text field, so numeric / boolean ranges (which would
        // target separate fields) make no sense at a phrase position.
        auto args = ParseRangeArgs(*f);
        if (!args) {
          return std::move(args.error());
        }
        if ((args->min_val &&
             args->min_val->type().id() != duckdb::LogicalTypeId::VARCHAR) ||
            (args->max_val &&
             args->max_val->type().id() != duckdb::LogicalTypeId::VARCHAR)) {
          throw duckdb::InvalidInputException(
            "## RANGE phrase part requires VARCHAR bounds");
        }
        FillByRangeOptionsVarchar(
          *args,
          options->push_back<irs::ByRangeOptions>(gap.offs_min, gap.offs_max));
        break;
      }
      default:
        return {ERROR_NOT_IMPLEMENTED, "## part type not supported yet: ",
                f ? f->function.name : "<bare-const>"};
    }
  }
  return {};
}

Result FromTSQueryPhraseSeq(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func) {
  PhraseSeq seq;
  if (auto r = FlattenPhraseSeq(func, seq); !r.ok()) {
    return r;
  }
  return EmitPhraseSeq(parent, ctx, column_info, seq);
}

}  // namespace sdb::connector

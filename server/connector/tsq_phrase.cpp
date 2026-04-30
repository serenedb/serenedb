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
#include "basics/result_or.h"
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
// directly to ByPhraseOptions::push_back(offs_min, offs_max). Returns
// Result rather than throwing because PHRASE callers need to append
// the argument index to the error message before propagating.
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

void FromPhrase(irs::BooleanFilter& filter, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  constexpr auto kSyntaxHint =
    "Example: PHRASE('quick brown fox') or PHRASE('a', 1, 'b'). VARCHAR "
    "arguments are tokens; INTEGER / INTEGER[] arguments are gaps in "
    "between (N or [min, max]).";
  // PHRASE is registered with at least one VARCHAR arg (plus variadic
  // ANY tail), so DuckDB's function resolver rejects empty calls at
  // bind time before we get here.
  SDB_ASSERT(!func.children.empty());

  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("PHRASE field is not VARCHAR"),
                    ERR_HINT(kSyntaxHint));
  }

  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("PHRASE field should have Positions and Frequency features "
              "enabled"),
      ERR_HINT("Recreate the inverted index with both `Positions` and "
               "`Frequency` features attached to the column."));
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

  for (size_t i = 0; i < func.children.size(); ++i) {
    const auto* const_val = TryGetConstant(*func.children[i]);
    if (!const_val) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("PHRASE argument ", i, " must be a constant"),
                      ERR_HINT(kSyntaxHint));
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
    if (opts->empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("PHRASE gap at argument ", i,
                              " must be preceded by a text pattern"),
                      ERR_HINT(kSyntaxHint));
    }
    if (pending_gap) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("PHRASE has consecutive gaps at argument ", i),
                      ERR_HINT(kSyntaxHint));
    }
    auto gap = ParsePhraseGap(*const_val, "PHRASE");
    if (!gap) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(gap.error().errorMessage(), " (argument ", i, ")"),
        ERR_HINT(kSyntaxHint));
    }
    pending_gap = *gap;
  }

  if (pending_gap) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("PHRASE ends with a gap; a text pattern must follow each gap"),
      ERR_HINT(kSyntaxHint));
  }
  if (opts->empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("PHRASE text arguments produced no searchable terms"),
      ERR_HINT("All PHRASE text arguments tokenised to nothing (e.g. "
               "all-stopword input). Provide at least one searchable term."));
  }
}

namespace {

void EmitPhraseTokens(irs::ByPhraseOptions& options, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text, PhraseGap base_gap) {
  auto& analyzer = ctx.tokenizer;
  if (!analyzer.reset(text)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("PHRASE failed to analyse '", text, "'"),
      ERR_HINT("The column's analyzer rejected the input text."));
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
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("PHRASE('", text, "') produced no tokens after analysis"),
      ERR_HINT("All tokens were stripped (e.g. all-stopword input). Provide "
               "at least one searchable term."));
  }
}

}  // namespace

void BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view text) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("PHRASE field is not VARCHAR"));
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("PHRASE field should have Positions and Frequency features "
              "enabled"),
      ERR_HINT("Recreate the inverted index with both `Positions` and "
               "`Frequency` features attached to the column."));
  }
  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);
  EmitPhraseTokens(*phrase.mutable_options(), ctx, column_info, text,
                   PhraseGap{});
}

// Expression-level wrapper for `##`: extracts the constant Value from
// `expr` (unwrapping any TSQUERY casts) and delegates to ParsePhraseGap.
PhraseGap ParsePhraseSeqGap(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("## gap must be a constant"),
      ERR_HINT("Use a literal INTEGER (e.g. PHRASE('a') ## 1 ## TERM('b')) "
               "or a 2-element INTEGER[] interval."));
  }
  auto gap = ParsePhraseGap(*val, "##");
  if (!gap) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(gap.error().errorMessage()));
  }
  return *gap;
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
        ERR_HINT("Example: PHRASE('a') ## 1 ## TERM('b'). A gap (INTEGER "
                 "or INTEGER[]) is only valid between TSQUERY parts."));
    }
    seq.parts.push_back(&unwrapped);
    return;
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  if (f.children.size() != 2) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("## expects 2 arguments (lhs ## rhs), got ", f.children.size()),
      ERR_HINT("Example: PHRASE('a') ## TERM('b') (adjacent), or with a gap: "
               "PHRASE('a') ## 1 ## TERM('b')."));
  }
  FlattenPhraseSeq(*f.children[0], seq);
  const auto& right = *f.children[1];
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
  constexpr auto kSyntaxHint =
    "Example: PHRASE('hello') ## 1 ## TERM('world'). Each '##' separates "
    "TSQUERY parts with optional INTEGER / INTEGER[] gaps in between.";
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
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("## field is not VARCHAR"),
                    ERR_HINT(kSyntaxHint));
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
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("## part must be a VARCHAR constant"),
                        ERR_HINT(kSyntaxHint));
      }
      bare_text = val.GetValue<std::string>();
      leaf_op = TSQueryOp::Term;
    } else if (part_expr_ref.expression_class ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      f = &part_expr_ref.Cast<duckdb::BoundFunctionExpression>();
      leaf_op = ClassifyTSQueryFunction(f->function.name);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("## part expression class: ",
                static_cast<int>(part_expr_ref.expression_class)),
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
          ERR_MSG("## ", f->function.name,
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
        options->push_back<irs::ByTermOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Prefix: {
        auto text = get_text_arg();
        options->push_back<irs::ByPrefixOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      }
      case TSQueryOp::Like: {
        auto text = get_text_arg();
        auto pattern = LikeEscapePattern(text, '\\');
        options->push_back<irs::ByWildcardOptions>(gap.offs_min, gap.offs_max)
          .term.assign(
            irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
        break;
      }
      case TSQueryOp::Fuzzy: {
        auto args = ParseLevenshteinArgs(*f);
        FillByEditDistanceOptions(
          args, options->push_back<irs::ByEditDistanceOptions>(gap.offs_min,
                                                               gap.offs_max));
        break;
      }
      case TSQueryOp::Phrase: {
        // Nested PHRASE('x y z') -> tokenise via column analyzer and
        // emit one term part per token. The FIRST token uses the
        // incoming gap; subsequent tokens are strictly adjacent. Shared
        // with BuildFtsPhrase via EmitPhraseTokens.
        if (f->children.empty() || f->children.size() > 2) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("## PHRASE phrase part expects 1 or 2 arguments "
                    "(text[, slop]), got ",
                    f->children.size()),
            ERR_HINT(kSyntaxHint));
        }
        std::string phrase_text;
        if (auto r =
              GetVarcharArg(*f->children[0], "## PHRASE text", phrase_text);
            !r.ok()) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
        }
        EmitPhraseTokens(*options, ctx, column_info, phrase_text, gap);
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
        ExtractAnyAllOfArgs(*f, /*is_any=*/true, sub_args, sub_synth,
                            sub_min_match);
        if (sub_min_match && *sub_min_match != 1) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("## ANY_OF phrase part requires min_match=1 (got ",
                    *sub_min_match, "); a phrase position can match only "
                    "one token"),
            ERR_HINT("Drop the min_match argument or set it to 1."));
        }
        auto& terms_opts =
          options->push_back<irs::ByTermsOptions>(gap.offs_min, gap.offs_max);
        terms_opts.min_match = 1;
        for (const auto* arg : sub_args) {
          std::string term_text;
          if (auto r = GetVarcharArg(UnwrapTSQueryCast(*arg),
                                     "## ANY_OF phrase part term", term_text);
              !r.ok()) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
          }
          terms_opts.terms.emplace(
            irs::ViewCast<irs::byte_type>(std::string_view{term_text}));
        }
        break;
      }
      case TSQueryOp::AllOf:
        // ALL_OF rejected for the same reason min_match > 1 is rejected
        // for ANY_OF: a phrase position can match only one token.
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("## ALL_OF phrase part is not supported (a phrase position "
                  "can match only one token; use ANY_OF instead)"),
          ERR_HINT(kSyntaxHint));
      case TSQueryOp::Range: {
        // RANGE as a phrase part -> ByRangeOptions slot. Only the
        // VARCHAR variant is meaningful here: phrases live on the
        // analyzed text field, so numeric / boolean ranges (which would
        // target separate fields) make no sense at a phrase position.
        auto args = ParseRangeArgs(*f);
        if ((args.min_val &&
             args.min_val->type().id() != duckdb::LogicalTypeId::VARCHAR) ||
            (args.max_val &&
             args.max_val->type().id() != duckdb::LogicalTypeId::VARCHAR)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("## RANGE phrase part requires VARCHAR bounds"),
            ERR_HINT("Phrase parts live on the analyzed VARCHAR field; "
                     "numeric / BOOLEAN ranges target other fields."));
        }
        FillByRangeOptionsVarchar(
          args,
          options->push_back<irs::ByRangeOptions>(gap.offs_min, gap.offs_max));
        break;
      }
      default:
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("## part type not supported yet: ",
                  f ? f->function.name : "<bare-const>"),
          ERR_HINT("Supported phrase parts: TERM, PREFIX, LIKE, LEVENSHTEIN, "
                   "PHRASE, ANY_OF, RANGE."));
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

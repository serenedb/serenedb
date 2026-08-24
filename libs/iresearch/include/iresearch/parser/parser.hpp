////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/type_id.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace sdb {

enum class Conjunction {
  Or,
  And,
};
enum class Modifier {
  None,
  Required,
  Not,
};

struct ParserContext {
  irs::field_id default_field_id{irs::field_limits::invalid()};
  std::string_view default_field_name;
  irs::MixedBooleanFilter* current_root;
  irs::analysis::Analyzer* tokenizer;
  std::string error_message;
  Modifier last_mod{Modifier::None};
  bool strict_field = false;

  // What is being built while the grammar reads its parts.
  irs::ByPhrase* phrase{nullptr};
  irs::ByNGramSimilarity* ngram{nullptr};
  std::vector<std::string_view> fn_terms;
  std::string_view fn_other;
  size_t offs_min{0};
  size_t offs_max{0};

  ParserContext(irs::MixedBooleanFilter& root, irs::field_id field_id,
                irs::analysis::Analyzer& tokenizer)
    : default_field_id(field_id), current_root{&root}, tokenizer{&tokenizer} {}

  void AddClause(Conjunction conj) {
    if (conj != Conjunction::And && last_mod == Modifier::None) {
      return;
    }

    auto& opt = current_root->GetOptional();
    auto& req = current_root->GetRequired();

    auto current = opt.PopBack();
    if (!current) {
      return;
    }

    irs::Filter::ptr prev;
    if (conj == Conjunction::And) {
      prev = opt.PopBack();
    }

    if (prev) {
      req.add(std::move(prev));
    }

    if (last_mod == Modifier::Not) {
      req.add<irs::Not>().filter<irs::Or>().add(std::move(current));
    } else {
      req.add(std::move(current));
    }
  }

  // What the query spells is not what the index holds: the analyzer stands
  // between them, here as everywhere else. A word it splits is asked for the
  // way Lucene asks -- any of its parts, under the default operator.
  irs::FilterWithBoost& AddTerm(std::string_view value) {
    const auto text = Unescape(value);
    tokenizer->reset(irs::ViewCast<char>(irs::bytes_view{text}));
    auto token = irs::get<irs::TermAttr>(*tokenizer);

    irs::bstring first;
    size_t count = 0;
    irs::MixedBooleanFilter* several = nullptr;
    while (tokenizer->next()) {
      if (count == 0) {
        first = token->value;
      } else {
        if (count == 1) {
          several = &current_root->GetOptional().add<irs::MixedBooleanFilter>();
          AddTermTo(several->GetOptional(), first);
        }
        AddTermTo(several->GetOptional(), token->value);
      }
      ++count;
    }
    if (several != nullptr) {
      return *several;
    }
    auto& f = current_root->GetOptional().add<irs::ByTerm>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->term = std::move(first);
    return f;
  }

  // A pattern of its own rather than a wildcard: `.` and `*` mean what a
  // regular expression means by them, which is not what `%` and `_` mean.
  irs::ByRegexp& AddRegex(std::string_view value) {
    auto& f = current_root->GetOptional().add<irs::ByRegexp>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->pattern = irs::ViewCast<irs::byte_type>(value);
    return f;
  }

  // A named field is read where the default field is: there is nothing here
  // that maps a name to a field of the index, so the name is only checked.
  bool CheckField(std::string_view name) {
    if (strict_field && name != default_field_name) {
      error_message =
        "field-prefix in strict-field mode must match the default field";
      return false;
    }
    return true;
  }

  // A phrase is built part by part as the grammar reads them, so nothing
  // takes a string apart here: what a part is, and where it sits, are
  // answers the parser already has.
  void BeginPhrase() {
    auto& f = current_root->GetOptional().add<irs::ByPhrase>();
    *f.mutable_field_id() = default_field_id;
    phrase = &f;
    offs_min = 0;
    offs_max = 0;
  }

  irs::ByPhrase& EndPhrase() {
    SDB_ASSERT(phrase != nullptr);
    auto& f = *phrase;
    phrase = nullptr;
    return f;
  }

  // How far the part after it may sit from the part before, which the parts
  // of a phrase that says nothing leave at one.
  void SetGap(int min, int max) {
    offs_min = static_cast<size_t>(min);
    offs_max = static_cast<size_t>(max);
  }

  void SetSlop(irs::FilterWithBoost* f, int slop) {
    sdb::basics::downCast<irs::ByPhrase>(*f).mutable_options()->set_slop(
      static_cast<irs::PosAttr::value_t>(slop));
  }

  void SetMinMatch(irs::FilterWithBoost* f, int count) {
    sdb::basics::downCast<irs::MixedBooleanFilter>(*f)
      .GetOptional()
      .min_match_count(static_cast<size_t>(count));
  }

  // A word of a phrase is whatever the analyzer makes of it: one term, or
  // several, and several of them sit one after another.
  void AddPhraseTerm(std::string_view word) {
    const auto text = Unescape(word);
    tokenizer->reset(irs::ViewCast<char>(irs::bytes_view{text}));
    auto token = irs::get<irs::TermAttr>(*tokenizer);
    while (tokenizer->next()) {
      Emplace<irs::ByTermOptions>().term = token->value;
    }
  }

  void AddPhrasePrefix(std::string_view word) {
    word.remove_suffix(1);
    Emplace<irs::ByPrefixOptions>().term = Analyze(word);
  }

  void AddPhraseWildcard(std::string_view word) {
    // Only the literal head goes through the analyzer -- it would eat the
    // pattern characters.
    const auto wildcard = FindPattern(word);
    auto pattern = Analyze(word.substr(0, wildcard));
    pattern.append(Pattern(word.substr(wildcard)));
    Emplace<irs::ByWildcardOptions>().term = std::move(pattern);
  }

  // Where a pattern begins: the first `*` or `?` a backslash did not protect.
  static size_t FindPattern(std::string_view word) noexcept {
    for (size_t i = 0; i != word.size(); ++i) {
      if (word[i] == '\\') {
        ++i;
      } else if (word[i] == '*' || word[i] == '?') {
        return i;
      }
    }
    return word.size();
  }

  void AddPhraseFuzzy(std::string_view word, int distance) {
    auto& part = Emplace<irs::ByEditDistanceOptions>();
    part.term = Analyze(word);
    part.max_distance = static_cast<uint8_t>(distance);
  }

  // An n-gram similarity, built the same way: the grammar hands over the
  // threshold, then the terms.
  void BeginNGram(float threshold) {
    auto& f = current_root->GetOptional().add<irs::ByNGramSimilarity>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->threshold = threshold;
    ngram = &f;
  }

  void AddNGram(std::string_view word) {
    SDB_ASSERT(ngram != nullptr);
    ngram->mutable_options()->ngrams.emplace_back(Analyze(word));
  }

  irs::ByNGramSimilarity& EndNGram() {
    SDB_ASSERT(ngram != nullptr);
    auto& f = *ngram;
    ngram = nullptr;
    return f;
  }

  irs::ByPrefix& AddPrefix(std::string_view value) {
    auto& f = current_root->GetOptional().add<irs::ByPrefix>();
    *f.mutable_field_id() = default_field_id;
    SDB_ASSERT(!value.empty() && value.back() == '*');
    value.remove_suffix(1);
    f.mutable_options()->term = Analyze(value);
    return f;
  }

  irs::ByWildcard& AddWildcard(std::string_view value) {
    auto& f = current_root->GetOptional().add<irs::ByWildcard>();
    *f.mutable_field_id() = default_field_id;
    // Only the literal head goes through the analyzer -- it would eat the
    // pattern characters.
    const auto wildcard = FindPattern(value);
    auto pattern = Analyze(value.substr(0, wildcard));
    pattern.append(Pattern(value.substr(wildcard)));
    *f.mutable_options() = irs::ByWildcardOptions{std::move(pattern)};
    return f;
  }

  irs::ByRange& AddRange(std::string_view min_val, std::string_view max_val,
                         bool inc_min, bool inc_max) {
    auto& f = current_root->GetOptional().add<irs::ByRange>();
    *f.mutable_field_id() = default_field_id;
    auto& range = f.mutable_options()->range;
    if (min_val == "*") {
      range.min_type = irs::BoundType::Unbounded;
    } else {
      range.min = Analyze(min_val);
      range.min_type =
        inc_min ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (max_val == "*") {
      range.max_type = irs::BoundType::Unbounded;
    } else {
      range.max = Analyze(max_val);
      range.max_type =
        inc_max ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    return f;
  }

  // Lucene spells a wildcard `*` and `?`; the filters spell it `%` and `_`,
  // with a backslash making either literal. That is the same boundary the
  // field name and the analyzer are resolved at, so it is crossed here --
  // otherwise a pattern reads as a term that happens to contain a star, and
  // matches nothing.
  static irs::bstring Pattern(std::string_view word) {
    irs::bstring out;
    out.reserve(word.size());
    for (size_t i = 0; i != word.size(); ++i) {
      const auto c = word[i];
      switch (c) {
        case '\\':
          // What the query escaped stays literal, and `*` and `?` need no
          // escaping once they are no longer the pattern characters.
          if (i + 1 != word.size()) {
            const auto next = word[++i];
            if (next != '*' && next != '?') {
              out += static_cast<irs::byte_type>('\\');
            }
            out += static_cast<irs::byte_type>(next);
          }
          break;
        case '*':
          out += static_cast<irs::byte_type>(irs::WildcardMatch::kAnyStr);
          break;
        case '?':
          out += static_cast<irs::byte_type>(irs::WildcardMatch::kAnyChr);
          break;
        case irs::WildcardMatch::kAnyStr:
        case irs::WildcardMatch::kAnyChr:
          // Literal here, a pattern character there.
          out += static_cast<irs::byte_type>(irs::WildcardMatch::kEscape);
          out += static_cast<irs::byte_type>(c);
          break;
        default:
          out += static_cast<irs::byte_type>(c);
      }
    }
    return out;
  }

  // How far apart two parts of an ordered match may sit when nothing bounds
  // them. Large rather than unbounded: the matcher compares against it, so
  // it only has to be past any position a document can hold.
  static constexpr size_t kAnyGap = 1U << 24U;

  template<typename Boolean>
  void AddTermTo(Boolean& root, std::string_view word) {
    AddTermTo(root, Analyze(word));
  }

  template<typename Boolean>
  void AddTermTo(Boolean& root, irs::bytes_view word) {
    auto& f = root.template add<irs::ByTerm>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->term = word;
  }

  void RequirePair(std::string_view name) {
    if (fn_terms.size() != 2) {
      THROW_SQL_ERROR(ERR_MSG("`", name,
                              "` is supported over two terms, where what it "
                              "bounds is a distance rather than a total"));
    }
  }

  irs::ByPhrase& FnPhrase(size_t offs_min, size_t offs_max) {
    BeginPhrase();
    bool first = true;
    for (const auto term : fn_terms) {
      if (!first) {
        SetGap(static_cast<int>(offs_min), static_cast<int>(offs_max));
      }
      first = false;
      AddPhraseTerm(term);
    }
    return EndPhrase();
  }

  // What a backslash protected is the character itself, and `\uXXXX` is the
  // character it names. Lucene's `discardEscapeChar`: the query says how to
  // spell a term, the index holds the term itself.
  static uint32_t Hex(char c) noexcept {
    if (c >= '0' && c <= '9') {
      return static_cast<uint32_t>(c - '0');
    }
    if (c >= 'a' && c <= 'f') {
      return static_cast<uint32_t>(c - 'a') + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return static_cast<uint32_t>(c - 'A') + 10;
    }
    return 16;  // not a digit
  }

  static irs::bstring Unescape(std::string_view word) {
    irs::bstring out;
    out.reserve(word.size());
    for (size_t i = 0; i != word.size(); ++i) {
      if (word[i] != '\\' || i + 1 == word.size()) {
        out += static_cast<irs::byte_type>(word[i]);
        continue;
      }
      const auto next = word[++i];
      if (next != 'u' || i + 4 >= word.size()) {
        out += static_cast<irs::byte_type>(next);
        continue;
      }
      uint32_t code = 0;
      size_t digits = 0;
      for (; digits != 4; ++digits) {
        const auto digit = Hex(word[i + 1 + digits]);
        if (digit > 15) {
          break;
        }
        code = code * 16 + digit;
      }
      if (digits != 4) {
        out += static_cast<irs::byte_type>(next);
        continue;
      }
      i += 4;
      irs::byte_type utf8[irs::utf8_utils::kMaxCharSize];
      out.append(utf8, irs::utf8_utils::FromChar32(code, utf8));
    }
    return out;
  }

  irs::bstring Analyze(std::string_view word) {
    const auto text = Unescape(word);
    tokenizer->reset(irs::ViewCast<char>(irs::bytes_view{text}));
    auto token = irs::get<irs::TermAttr>(*tokenizer);
    if (tokenizer->next()) {
      return irs::bstring{token->value};
    }
    return {};
  }

  // Where a part goes: next to the one before it, or as far off as a gap
  // asked. Zero is no gap rather than a gap of zero, which is what the
  // one-argument `push_back` means.
  template<typename Part>
  Part& Emplace() {
    SDB_ASSERT(phrase != nullptr);
    auto* options = phrase->mutable_options();
    auto& part = offs_min == 0 && offs_max == 0
                   ? options->push_back<Part>()
                   : options->push_back<Part>(offs_min, offs_max);
    offs_min = 0;
    offs_max = 0;
    return part;
  }

  // Lucene's older spelling of an edit distance: a similarity between zero
  // and one, which it turns into edits by the length of the term.
  // `FuzzyQuery.floatToEdits`.
  // A term or a phrase may give a boost and a fuzziness in either order, and
  // both orders mean the same thing. Reading the boost first leaves a plain
  // term already built, and a fuzzy term is a different filter -- so the term
  // is taken back out and asked for again as the one it should have been. A
  // phrase only needs its slop set.
  irs::FilterWithBoost& ApplyFuzzy(irs::FilterWithBoost* built, bool has_value,
                                   float value) {
    if (built->type() == irs::Type<irs::ByPhrase>::id()) {
      if (has_value) {
        SetSlop(built, static_cast<int>(value));
      }
      return *built;
    }
    if (built->type() != irs::Type<irs::ByTerm>::id()) {
      THROW_SQL_ERROR(
        ERR_MSG("a fuzziness applies to a term or a phrase, not to this"));
    }
    auto& term = sdb::basics::downCast<irs::ByTerm>(*built);
    auto text = term.options().term;
    const auto boost = term.Boost();

    auto& optional = current_root->GetOptional();
    optional.PopBack();

    auto& f = has_value ? FuzzyFromSimilarity(std::move(text), value)
                        : AddFuzzyTerm(std::move(text), 2);
    f.boost(boost);
    return f;
  }

  irs::ByEditDistance& AddFuzzySimilarity(std::string_view value,
                                          float similarity) {
    return FuzzyFromSimilarity(Analyze(value), similarity);
  }

  irs::ByEditDistance& FuzzyFromSimilarity(irs::bstring value,
                                           float similarity) {
    constexpr int kMaxEdits = 2;
    int distance = 0;
    if (similarity >= 1.F) {
      distance = std::min(static_cast<int>(similarity), kMaxEdits);
    } else if (similarity > 0.F) {
      const auto length = static_cast<int>(irs::utf8_utils::Length(value));
      distance = std::min(
        static_cast<int>((1.F - similarity) * static_cast<float>(length)),
        kMaxEdits);
    }
    return AddFuzzyTerm(std::move(value), distance);
  }

  irs::ByEditDistance& AddFuzzyTerm(irs::bstring value, int distance) {
    auto& f = current_root->GetOptional().add<irs::ByEditDistance>();
    *f.mutable_field_id() = default_field_id;
    f.mutable_options()->term = std::move(value);
    f.mutable_options()->max_distance = static_cast<uint8_t>(distance);
    return f;
  }

  // Every document, which is what a query that names no term at all means.
  irs::All& AddAll() {
    auto& f = current_root->GetOptional().add<irs::All>();
    return f;
  }

  // A field with any value: Lucene's `field:*`. Nothing here answers it, so
  // it is read and refused rather than quietly meaning something else.
  irs::FilterWithBoost& AddFieldExists() {
    THROW_SQL_ERROR(
      ERR_MSG("field existence queries (`field:*`) are not supported"));
  }

  // The terms an interval function is applied to. What such a function means
  // for a document -- which documents hold a match at all -- is a question
  // this engine can answer even without intervals to compose; where the
  // answer depends on where the matches lie relative to each other, it
  // cannot, and that function is refused instead.
  void BeginFn() {
    fn_terms.clear();
    fn_other = {};
  }

  void AddFnTerm(std::string_view word) { fn_terms.emplace_back(word); }

  // A source that is not a term. What such a function means for a document
  // depends on where its matches lie, and this engine composes documents
  // rather than positions -- so the refusal says what it was given.
  void AddFnOther(std::string_view kind) { fn_other = kind; }

  void RequireTerms(std::string_view name) const {
    if (!fn_other.empty()) {
      THROW_SQL_ERROR(ERR_MSG("`", name, "` over ", fn_other,
                              " is not supported: it would ask where the "
                              "matches lie, and only terms can be answered "
                              "for here"));
    }
  }

  // One of them is enough.
  irs::FilterWithBoost& EndFnAny() {
    RequireTerms("fn:or");
    auto& f = current_root->GetOptional().add<irs::MixedBooleanFilter>();
    for (const auto term : fn_terms) {
      AddTermTo(f.GetOptional(), term);
    }
    return f;
  }

  // All of them, wherever they lie.
  irs::FilterWithBoost& EndFnAll() {
    RequireTerms("fn:unordered");
    auto& f = current_root->GetOptional().add<irs::MixedBooleanFilter>();
    for (const auto term : fn_terms) {
      AddTermTo(f.GetRequired(), term);
    }
    return f;
  }

  irs::FilterWithBoost& EndFnAtLeast(int count) {
    RequireTerms("fn:atLeast");
    auto& f = current_root->GetOptional().add<irs::MixedBooleanFilter>();
    for (const auto term : fn_terms) {
      AddTermTo(f.GetOptional(), term);
    }
    f.GetOptional().min_match_count(static_cast<size_t>(count));
    return f;
  }

  // In this order, however far apart: a phrase whose parts may sit anywhere
  // after the one before them.
  irs::FilterWithBoost& EndFnOrdered() {
    RequireTerms("fn:ordered");
    return FnPhrase(1, kAnyGap);
  }

  // `maxgaps` bounds the words between, `maxwidth` the span end to end. Over
  // one pair either is a distance; over more they bound a total across the
  // whole match, and a phrase says only how far each part sits from the last.
  irs::FilterWithBoost& EndFnMaxGaps(int gaps) {
    RequireTerms("fn:maxgaps");
    RequirePair("fn:maxgaps");
    return FnPhrase(1, static_cast<size_t>(gaps) + 1);
  }

  irs::FilterWithBoost& EndFnMaxWidth(int width) {
    RequireTerms("fn:maxwidth");
    RequirePair("fn:maxwidth");
    if (width < 2) {
      THROW_SQL_ERROR(ERR_MSG("`fn:maxwidth` needs a width of at least two"));
    }
    return FnPhrase(1, static_cast<size_t>(width) - 1);
  }

  // An interval function whose answer depends on where matches lie relative
  // to one another, which needs intervals this engine does not have.
  void Unsupported(std::string_view name) {
    THROW_SQL_ERROR(ERR_MSG("`", name, "` is not supported"));
  }

  irs::ByEditDistance& AddFuzzy(std::string_view value, int distance) {
    return AddFuzzyTerm(Analyze(value), distance);
  }
};

// Returns false on a parse error; the message is in ctx.error_message.
bool ParseQuery(ParserContext& ctx, std::string_view input);

}  // namespace sdb

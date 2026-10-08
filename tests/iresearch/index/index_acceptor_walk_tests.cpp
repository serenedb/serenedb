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

#include <re2/re2.h>
#include <simdutf.h>

#include <algorithm>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/detail/pattern_cache.hpp>
#include <iresearch/search/detail/term_acceptor.hpp>
#include <iresearch/search/filters/wildcard_filter.hpp>
#include <iresearch/utils/conjunction_acceptor.hpp>
#include <iresearch/utils/containers/small_vector.hpp>
#include <iresearch/utils/levenshtein_acceptor.hpp>
#include <iresearch/utils/levenshtein_utils.hpp>
#include <iresearch/utils/regexp_acceptor.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/index_tests.hpp"

using namespace std::literals;

class AcceptorWalkIndexTestCase : public tests::IndexTestBase {
 protected:
  template<typename Acceptor>
  static std::vector<std::pair<irs::bstring, irs::byte_type>> BruteForce(
    const Acceptor& acceptor, const irs::TermReader& field) {
    std::vector<std::pair<irs::bstring, irs::byte_type>> accepted;
    auto terms = field.iterator();
    EXPECT_NE(nullptr, terms);
    while (terms->next()) {
      const auto term = terms->value();
      if constexpr (Acceptor::kMayBeUnknown) {
        typename Acceptor::PayloadType payload{};
        bool matches = false;
        if constexpr (Acceptor::kHasPayload) {
          matches = acceptor.Matches(term, payload);
        } else {
          matches = acceptor.Matches(term);
        }
        if (matches) {
          accepted.emplace_back(term, payload);
        }
        continue;
      }
      auto state = acceptor.Start();
      bool alive = true;
      for (const auto label : term) {
        state = acceptor.Step(state, label);
        if (!Acceptor::Alive(state)) {
          alive = false;
          break;
        }
      }
      if (!alive) {
        continue;
      }
      typename Acceptor::PayloadType payload{};
      if (acceptor.Accept(state, payload)) {
        accepted.emplace_back(term, payload);
      }
    }
    return accepted;
  }

  static void AssertSameWalk(
    const std::vector<std::pair<irs::bstring, irs::byte_type>>& expected,
    irs::SeekTermIterator& walk, bool expect_payload) {
    const auto* payload = irs::get<irs::PayAttr>(walk);
    if (expect_payload) {
      ASSERT_NE(nullptr, payload);
    }
    for (const auto& [term, distance] : expected) {
      SCOPED_TRACE(testing::Message("Expected term: '")
                   << irs::ViewCast<char>(irs::bytes_view{term}) << "'");
      ASSERT_TRUE(walk.next());
      ASSERT_EQ(irs::bytes_view{term}, walk.value());
      if (expect_payload) {
        ASSERT_EQ(1, payload->value.size());
        ASSERT_EQ(distance, payload->value[0]);
      }
    }
    ASSERT_FALSE(walk.next());
  }

  template<typename Acceptor>
  void AssertWalk(const irs::IndexReader& reader, const Acceptor& acceptor,
                  bool expect_payload) {
    for (auto& segment : reader) {
      for (auto field_id : segment.field_ids()) {
        const auto* field = segment.field(field_id);
        ASSERT_NE(nullptr, field);
        SCOPED_TRACE(testing::Message("Field: ") << field_id);

        const auto expected = BruteForce(acceptor, *field);
        auto walk = field->iterator(acceptor);
        ASSERT_NE(nullptr, walk);
        AssertSameWalk(expected, *walk, expect_payload);
      }
    }
  }

  template<typename Acceptor>
  void AssertSourceMatchesWalk(const irs::IndexReader& reader,
                               const Acceptor& acceptor,
                               const irs::TermAcceptorSource& source) {
    const auto predicate = source.Predicate();
    ASSERT_NE(nullptr, predicate);
    for (auto& segment : reader) {
      for (auto field_id : segment.field_ids()) {
        const auto* field = segment.field(field_id);
        ASSERT_NE(nullptr, field);
        SCOPED_TRACE(testing::Message("Field: ") << field_id);
        std::vector<irs::bstring> walked;
        for (auto it = field->iterator(acceptor); it->next();) {
          walked.emplace_back(it->value());
        }
        std::vector<irs::bstring> sourced;
        for (auto it = source.Iterator(*field); it->next();) {
          sourced.emplace_back(it->value());
        }
        EXPECT_EQ(walked, sourced);
        auto walk = field->iterator(acceptor);
        auto seek = source.Iterator(*field);
        for (auto it = field->iterator(); it->next();) {
          const auto term = it->value();
          EXPECT_EQ(acceptor.Matches(term), predicate->Accepts(term));
          const auto expected = walk->seek_ge(term);
          EXPECT_EQ(expected, seek->seek_ge(term));
          if (expected != irs::SeekResult::End) {
            EXPECT_EQ(walk->value(), seek->value());
          }
        }
      }
    }
  }

  void AddEuroparl() {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.txt"), doc);
    add_segment(gen);
  }

  static constexpr irs::field_id kTermFieldId = 42;

  class TermListGenerator final : public tests::DocGeneratorBase {
   public:
    explicit TermListGenerator(std::span<const std::string_view> terms) noexcept
      : _terms{terms} {}

    const tests::Document* next() final {
      if (_next == _terms.size()) {
        return nullptr;
      }
      _doc.clear();
      auto field =
        std::make_shared<tests::StringField>("term", _terms[_next++]);
      field->id = kTermFieldId;
      _doc.insert(field);
      return &_doc;
    }

    void reset() final { _next = 0; }

   private:
    std::span<const std::string_view> _terms;
    tests::Document _doc;
    size_t _next{0};
  };

  void AddTerms(std::span<const std::string_view> terms) {
    TermListGenerator gen{terms};
    add_segment(gen);
  }

  void AssertRe2Perl(size_t part, size_t parts);

  static size_t RestrictedDamerau(std::span<const uint32_t> lhs,
                                  std::span<const uint32_t> rhs) {
    const size_t width = rhs.size() + 1;
    std::vector<size_t> d((lhs.size() + 1) * width);
    const auto at = [&](size_t i, size_t j) -> size_t& {
      return d[i * width + j];
    };
    for (size_t i = 0; i <= lhs.size(); ++i) {
      at(i, 0) = i;
    }
    for (size_t j = 0; j <= rhs.size(); ++j) {
      at(0, j) = j;
    }
    for (size_t i = 1; i <= lhs.size(); ++i) {
      for (size_t j = 1; j <= rhs.size(); ++j) {
        at(i, j) = std::min({at(i - 1, j) + 1, at(i, j - 1) + 1,
                             at(i - 1, j - 1) + (lhs[i - 1] != rhs[j - 1])});
        if (i > 1 && j > 1 && lhs[i - 1] == rhs[j - 2] &&
            lhs[i - 2] == rhs[j - 1]) {
          at(i, j) = std::min(at(i, j), at(i - 2, j - 2) + 1);
        }
      }
    }
    return at(lhs.size(), rhs.size());
  }

  void AssertEditDistanceOracle(const irs::IndexReader& reader,
                                const irs::ParametricDescription& description,
                                std::string_view target) {
    AssertEditDistanceOracle(reader, description, false, {}, target);
  }

  void AssertEditDistanceOracle(const irs::IndexReader& reader,
                                const irs::ParametricDescription& description,
                                bool transpositions, std::string_view prefix,
                                std::string_view target) {
    const auto fuzzy = std::make_shared<const irs::LevenshteinAcceptor>(
      description, irs::ViewCast<irs::byte_type>(prefix),
      irs::ViewCast<irs::byte_type>(target));
    AssertEditDistanceWalk(reader, *fuzzy, description, transpositions, prefix,
                           target);
    const auto dfa = irs::FuzzyConjunction::Make(fuzzy);
    ASSERT_NE(nullptr, dfa);
    AssertEditDistanceWalk(reader, *dfa, description, transpositions, prefix,
                           target);
    for (const size_t max_mem : {size_t{16384}, size_t{0}}) {
      SCOPED_TRACE(testing::Message("Budget: ") << max_mem);
      const irs::FuzzyConjunction budgeted{{}, fuzzy, max_mem};
      AssertEditDistanceWalk(reader, budgeted, description, transpositions,
                             prefix, target);
    }
  }

  template<typename Acceptor>
  void AssertEditDistanceWalk(const irs::IndexReader& reader,
                              const Acceptor& acceptor,
                              const irs::ParametricDescription& description,
                              bool transpositions, std::string_view prefix,
                              std::string_view target) {
    const auto target_bytes = irs::ViewCast<irs::byte_type>(target);
    const auto prefix_bytes = irs::ViewCast<irs::byte_type>(prefix);
    irs::containers::SmallVector<uint32_t, 16> target_chars;
    irs::utf8_utils::ToUTF32<false>(target_bytes,
                                    std::back_inserter(target_chars));

    for (auto& segment : reader) {
      for (auto field_id : segment.field_ids()) {
        const auto* field = segment.field(field_id);
        ASSERT_NE(nullptr, field);
        SCOPED_TRACE(testing::Message("Field: ") << field_id);

        auto expected_terms = field->iterator();
        ASSERT_NE(nullptr, expected_terms);
        auto actual_terms = field->iterator(acceptor);
        ASSERT_NE(nullptr, actual_terms);
        const auto* payload = irs::get<irs::PayAttr>(*actual_terms);
        ASSERT_NE(nullptr, payload);

        while (expected_terms->next()) {
          const auto expected_term = expected_terms->value();
          if (!expected_term.starts_with(prefix_bytes)) {
            continue;
          }

          irs::containers::SmallVector<uint32_t, 16> expected_chars;
          if (!irs::utf8_utils::ToUTF32<true>(
                expected_term.substr(prefix_bytes.size()),
                std::back_inserter(expected_chars))) {
            continue;
          }
          const auto edit_distance =
            transpositions
              ? RestrictedDamerau(
                  {expected_chars.data(), expected_chars.size()},
                  {target_chars.data(), target_chars.size()})
              : irs::EditDistance(expected_chars.data(), expected_chars.size(),
                                  target_chars.data(), target_chars.size());
          if (edit_distance > description.max_distance()) {
            continue;
          }
          if (!simdutf::validate_utf8(
                reinterpret_cast<const char*>(expected_term.data()),
                expected_term.size())) {
            continue;
          }

          SCOPED_TRACE(testing::Message("Expected term: '")
                       << irs::ViewCast<char>(expected_term) << "'");
          ASSERT_TRUE(actual_terms->next());
          ASSERT_EQ(expected_term, actual_terms->value());
          ASSERT_EQ(1, payload->value.size());
          ASSERT_EQ(edit_distance, payload->value[0]);
        }
        ASSERT_FALSE(actual_terms->next());
      }
    }
  }

  static bool IsUtf8(irs::bytes_view term) {
    return simdutf::validate_utf8(reinterpret_cast<const char*>(term.data()),
                                  term.size());
  }

  static std::string LikeRegex(std::string_view pattern) {
    static constexpr std::string_view kMeta = "\\[](){}.*+?|^$";
    std::string regex = "\\A";
    bool escaped = false;
    for (const auto c : pattern) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
        continue;
      } else if (c == '%') {
        regex += ".*";
        continue;
      } else if (c == '_') {
        regex += '.';
        continue;
      }
      if (kMeta.find(c) != std::string_view::npos) {
        regex += '\\';
      }
      regex += c;
    }
    regex += "\\z";
    return regex;
  }

  template<typename Acceptor, typename Covers, typename Oracle>
  void AssertOracle(const irs::IndexReader& reader, const Acceptor& acceptor,
                    Covers&& covers, Oracle&& oracle) {
    for (auto& segment : reader) {
      for (auto field_id : segment.field_ids()) {
        const auto* field = segment.field(field_id);
        ASSERT_NE(nullptr, field);
        SCOPED_TRACE(testing::Message("Field: ") << field_id);

        std::vector<irs::bstring> expected;
        auto terms = field->iterator();
        ASSERT_NE(nullptr, terms);
        while (terms->next()) {
          const auto term = terms->value();
          if (covers(term) && oracle(irs::ViewCast<char>(term))) {
            expected.emplace_back(term);
          }
        }
        std::vector<irs::bstring> walked;
        auto walk = field->iterator(acceptor);
        ASSERT_NE(nullptr, walk);
        while (walk->next()) {
          if (const auto term = walk->value(); covers(term)) {
            walked.emplace_back(term);
          }
        }
        ASSERT_EQ(expected, walked);
      }
    }
  }
};

TEST_P(AcceptorWalkIndexTestCase, levenshtein_walk_matches_scan) {
  const irs::ParametricDescription descriptions[]{
    irs::MakeParametricDescription(1, false),
    irs::MakeParametricDescription(2, false),
    irs::MakeParametricDescription(3, false),
  };

  constexpr std::string_view kTargets[]{
    "atlas", "bloom", "burden", "del", "survenius", "surbenus", ""};

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto& description : descriptions) {
    for (const auto target : kTargets) {
      SCOPED_TRACE(testing::Message("Target: '")
                   << target << testing::Message("', Edit distance: ")
                   << size_t(description.max_distance()));
      const auto fuzzy = std::make_shared<const irs::LevenshteinAcceptor>(
        description, irs::kEmptyStringView<irs::byte_type>,
        irs::ViewCast<irs::byte_type>(target));
      AssertWalk(*reader.GetImpl(), *fuzzy, true);
      const auto dfa = irs::FuzzyConjunction::Make(fuzzy);
      ASSERT_NE(nullptr, dfa);
      AssertWalk(*reader.GetImpl(), *dfa, true);
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, levenshtein_payload_is_edit_distance) {
  const auto description = irs::MakeParametricDescription(2, false);
  constexpr std::string_view kTarget = "burden";

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  irs::containers::SmallVector<uint32_t, 16> target_chars;
  irs::utf8_utils::ToUTF32<false>(irs::ViewCast<irs::byte_type>(kTarget),
                                  std::back_inserter(target_chars));

  const auto fuzzy = std::make_shared<const irs::LevenshteinAcceptor>(
    description, irs::kEmptyStringView<irs::byte_type>,
    irs::ViewCast<irs::byte_type>(kTarget));
  const auto dfa = irs::FuzzyConjunction::Make(fuzzy);
  ASSERT_NE(nullptr, dfa);

  size_t checked = 0;
  for (auto& segment : *reader.GetImpl()) {
    for (auto field_id : segment.field_ids()) {
      const auto* field = segment.field(field_id);
      ASSERT_NE(nullptr, field);
      auto walk = field->iterator(*dfa);
      ASSERT_NE(nullptr, walk);
      const auto* payload = irs::get<irs::PayAttr>(*walk);
      ASSERT_NE(nullptr, payload);
      while (walk->next()) {
        const auto term = walk->value();
        irs::containers::SmallVector<uint32_t, 16> chars;
        ASSERT_TRUE(
          irs::utf8_utils::ToUTF32<true>(term, std::back_inserter(chars)));
        SCOPED_TRACE(testing::Message("Term: '")
                     << irs::ViewCast<char>(term) << "'");
        ASSERT_EQ(1, payload->value.size());
        ASSERT_EQ(irs::EditDistance(chars.data(), chars.size(),
                                    target_chars.data(), target_chars.size()),
                  payload->value[0]);
        ++checked;
      }
    }
  }
  ASSERT_NE(0, checked);
}

TEST_P(AcceptorWalkIndexTestCase, levenshtein_walk_is_the_edit_distance_set) {
  const irs::ParametricDescription descriptions[]{
    irs::MakeParametricDescription(1, false),
    irs::MakeParametricDescription(2, false),
    irs::MakeParametricDescription(3, false),
  };

  constexpr std::string_view kTargets[]{
    "atlas", "bloom", "burden", "del", "survenius", "surbenus", ""};

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto& description : descriptions) {
    for (const auto target : kTargets) {
      SCOPED_TRACE(testing::Message("Target: '")
                   << target << testing::Message("', Edit distance: ")
                   << size_t(description.max_distance()));
      AssertEditDistanceOracle(*reader.GetImpl(), description, target);
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, regexp_walk_matches_scan) {
  constexpr std::string_view kPatterns[]{
    "bur.*", ".*tion", "b.rden", "atl(as|antic)", "a.{4}s", "zzzz.*", ".*", "",
  };

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto pattern : kPatterns) {
    SCOPED_TRACE(testing::Message("Pattern: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
  }
}

TEST_P(AcceptorWalkIndexTestCase, wildcard_walk_matches_scan) {
  constexpr std::string_view kPatterns[]{
    "bur%", "%tion", "b_rden", "%den%", "a____s", "zzzz%", "%", "",
  };

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto pattern : kPatterns) {
    SCOPED_TRACE(testing::Message("Pattern: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::RegexpAcceptor::WildcardTag{},
                                       irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
  }
}

TEST_P(AcceptorWalkIndexTestCase, regexp_walk_past_the_row_budget) {
  constexpr std::string_view kPatterns[]{
    "[ab]*a[ab]{15}",      ".*a.{12}",
    "(.*e){3}.*",          "bur.*|.*tion|a.{4}s",
    ".*\\bur.*|.*on\\b.*", "(?i).*\\Bst\\B.*",
  };
  constexpr size_t kBudgets[]{1, 4096};

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto pattern : kPatterns) {
    const irs::RegexpAcceptor unbounded{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(unbounded.ok());
    for (const auto budget : kBudgets) {
      SCOPED_TRACE(testing::Message("Pattern: '")
                   << pattern << "', budget: " << budget);
      const irs::RegexpAcceptor bounded{
        irs::ViewCast<irs::byte_type>(pattern), irs::RegexpSyntax::Perl,
        irs::RegexpAcceptor::kDefaultMaxMem, budget};
      ASSERT_TRUE(bounded.ok());
      AssertWalk(*reader.GetImpl(), bounded, false);
      for (auto& segment : *reader.GetImpl()) {
        for (auto field_id : segment.field_ids()) {
          const auto* field = segment.field(field_id);
          ASSERT_NE(nullptr, field);
          auto bounded_walk = field->iterator(bounded);
          auto unbounded_walk = field->iterator(unbounded);
          ASSERT_NE(nullptr, bounded_walk);
          ASSERT_NE(nullptr, unbounded_walk);
          while (unbounded_walk->next()) {
            ASSERT_TRUE(bounded_walk->next());
            ASSERT_EQ(unbounded_walk->value(), bounded_walk->value());
          }
          ASSERT_FALSE(bounded_walk->next());
        }
      }
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, walk_selects_multibyte_terms) {
  constexpr std::string_view kTerms[]{
    "burden",  "b\xC3\xBCrden",     "bxrden", "b\xC5\xB1rden",
    "b",       "b\xE4\xB8\xADrden", "burde",  "b\xF0\x9F\x98\x80rden",
    "burdens", "b\xC3\xBCrde",      "urden",  "b\xC3\xBCrdens",
  };
  constexpr std::string_view kWildcards[]{"b_rden", "b%rden", "%rden",
                                          "_rden",  "b_rde%", "%"};
  constexpr std::string_view kRegexps[]{"b.rden", "b.*rden", ".*rden",
                                        "b.rde.*"};

  AddTerms(kTerms);

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto pattern : kWildcards) {
    SCOPED_TRACE(testing::Message("Wildcard: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::RegexpAcceptor::WildcardTag{},
                                       irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
  }
  for (const auto pattern : kRegexps) {
    SCOPED_TRACE(testing::Message("Regexp: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
  }

  const irs::RegexpAcceptor single{
    irs::RegexpAcceptor::WildcardTag{},
    irs::ViewCast<irs::byte_type>(std::string_view{"b_rden"})};
  ASSERT_TRUE(single.ok());
  for (const auto term : kTerms) {
    SCOPED_TRACE(testing::Message("Term: '") << term << "'");
    const bool one_char_between =
      term.starts_with("b") && term.ends_with("rden") &&
      irs::utf8_utils::Length(irs::ViewCast<irs::byte_type>(term)) == 6;
    EXPECT_EQ(one_char_between,
              single.Matches(irs::ViewCast<irs::byte_type>(term)));
  }
}

TEST_P(AcceptorWalkIndexTestCase, walk_crosses_block_boundaries) {
  constexpr size_t kCount = 5000;
  std::vector<std::string> storage;
  storage.reserve(kCount);
  for (size_t i = 0; i != kCount; ++i) {
    std::string term = "blk00000";
    for (size_t n = i, pos = term.size(); n != 0; n /= 10) {
      term[--pos] = static_cast<char>('0' + (n % 10));
    }
    storage.emplace_back(std::move(term));
  }
  std::vector<std::string_view> terms{storage.begin(), storage.end()};

  constexpr std::string_view kWildcards[]{"blk00___", "blk0__00", "%99",
                                          "blk00000", "blk04999", "blk%9"};
  constexpr std::string_view kRegexps[]{"blk00[0-4].*", "blk.*99",
                                        "blk0.0.0.*"};

  AddTerms(terms);

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto pattern : kWildcards) {
    SCOPED_TRACE(testing::Message("Wildcard: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::RegexpAcceptor::WildcardTag{},
                                       irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
  }
  for (const auto pattern : kRegexps) {
    SCOPED_TRACE(testing::Message("Regexp: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
  }
}

TEST_P(AcceptorWalkIndexTestCase, conjunction_source_is_the_intersection) {
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  constexpr std::string_view kDriver = "bur%";
  constexpr std::string_view kResidual = "%n";

  for (auto& segment : *reader.GetImpl()) {
    for (auto field_id : segment.field_ids()) {
      const auto* field = segment.field(field_id);
      ASSERT_NE(nullptr, field);
      SCOPED_TRACE(testing::Message("Field: ") << field_id);

      const irs::RegexpAcceptor driver_acceptor{
        irs::RegexpAcceptor::WildcardTag{},
        irs::ViewCast<irs::byte_type>(kDriver)};
      const irs::RegexpAcceptor residual_acceptor{
        irs::RegexpAcceptor::WildcardTag{},
        irs::ViewCast<irs::byte_type>(kResidual)};
      ASSERT_TRUE(driver_acceptor.ok());
      ASSERT_TRUE(residual_acceptor.ok());

      std::vector<std::pair<irs::bstring, irs::byte_type>> expected;
      for (const auto& [term, payload] : BruteForce(driver_acceptor, *field)) {
        if (residual_acceptor.Matches(term)) {
          expected.emplace_back(term, payload);
        }
      }

      auto source = irs::MakeConjunctionSource(
        irs::MakePatternSource(irs::ViewCast<irs::byte_type>(kDriver),
                               irs::PatternKind::Wildcard),
        irs::TermBounds{},
        irs::CreateByWildcard(field_id,
                              irs::ViewCast<irs::byte_type>(kResidual)));
      ASSERT_TRUE(source->ok());

      auto walk = source->Iterator(*field);
      ASSERT_NE(nullptr, walk);
      AssertSameWalk(expected, *walk, false);

      auto predicate = source->Predicate();
      ASSERT_NE(nullptr, predicate);
      for (const auto& [term, _] : expected) {
        EXPECT_TRUE(predicate->Accepts(term));
      }
      EXPECT_FALSE(
        predicate->Accepts(irs::ViewCast<irs::byte_type>("nonesuch"sv)));
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, conjunction_source_honours_its_bounds) {
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  constexpr std::string_view kPrefix = "bur";
  constexpr std::string_view kDriver = "bur%";
  constexpr std::string_view kResidual = "%n";

  const auto prefix = irs::ViewCast<irs::byte_type>(kPrefix);
  const irs::TermBounds bounds{.lower = irs::bstring{prefix},
                               .upper = irs::UpperBoundOf(prefix)};
  ASSERT_EQ(irs::ViewCast<irs::byte_type>("bus"sv),
            irs::bytes_view{bounds.upper});

  for (auto& segment : *reader.GetImpl()) {
    for (auto field_id : segment.field_ids()) {
      const auto* field = segment.field(field_id);
      ASSERT_NE(nullptr, field);
      SCOPED_TRACE(testing::Message("Field: ") << field_id);

      const irs::RegexpAcceptor driver_acceptor{
        irs::RegexpAcceptor::WildcardTag{},
        irs::ViewCast<irs::byte_type>(kDriver)};
      const irs::RegexpAcceptor residual_acceptor{
        irs::RegexpAcceptor::WildcardTag{},
        irs::ViewCast<irs::byte_type>(kResidual)};
      ASSERT_TRUE(driver_acceptor.ok());
      ASSERT_TRUE(residual_acceptor.ok());

      std::vector<std::pair<irs::bstring, irs::byte_type>> expected;
      for (const auto& [term, payload] : BruteForce(driver_acceptor, *field)) {
        if (residual_acceptor.Matches(term)) {
          expected.emplace_back(term, payload);
        }
      }

      {
        auto source = irs::MakeConjunctionSource(
          nullptr, bounds,
          irs::CreateByWildcard(field_id,
                                irs::ViewCast<irs::byte_type>(kResidual)));
        ASSERT_TRUE(source->ok());
        auto walk = source->Iterator(*field);
        ASSERT_NE(nullptr, walk);
        AssertSameWalk(expected, *walk, false);
      }

      {
        auto source = irs::MakeConjunctionSource(
          irs::MakePatternSource(irs::ViewCast<irs::byte_type>(kDriver),
                                 irs::PatternKind::Wildcard),
          bounds,
          irs::CreateByWildcard(field_id,
                                irs::ViewCast<irs::byte_type>(kResidual)));
        ASSERT_TRUE(source->ok());
        auto walk = source->Iterator(*field);
        ASSERT_NE(nullptr, walk);
        AssertSameWalk(expected, *walk, false);
      }
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, levenshtein_transpositions_and_prefix) {
  struct Case {
    std::string_view prefix;
    std::string_view target;
  };
  constexpr Case kCases[]{
    {"", "atlas"},  {"", "atlsa"},     {"", "bloom"}, {"b", "urden"},
    {"b", "rudne"}, {"bu", "rden"},    {"", "del"},   {"", "bruden"},
    {"a", ""},      {"sur", "venius"},
  };

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  for (const auto distance : {uint8_t{1}, uint8_t{2}}) {
    for (const bool transpositions : {false, true}) {
      const auto description =
        irs::MakeParametricDescription(distance, transpositions);
      for (const auto& [prefix, target] : kCases) {
        SCOPED_TRACE(testing::Message("Prefix: '")
                     << prefix << "', target: '" << target
                     << "', distance: " << size_t{distance}
                     << ", transpositions: " << transpositions);
        const irs::LevenshteinAcceptor acceptor{
          description, irs::ViewCast<irs::byte_type>(prefix),
          irs::ViewCast<irs::byte_type>(target)};
        AssertWalk(*reader.GetImpl(), acceptor, true);
        AssertEditDistanceOracle(*reader.GetImpl(), description, transpositions,
                                 prefix, target);
      }
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, levenshtein_multibyte_targets) {
  constexpr std::string_view kChars[]{
    "a",
    "\xC3\xA9",
    "\xD0\xB2",
    "\xD0\xB1",
    "\xD1\x8B",
    "\xE4\xB8\xAD",
    "\xE4\xB8\xAB",
    "\xE4\xBA\xAC",
    "\xF0\x9F\x98\x80",
    "\xF0\x9F\x98\x81",
    "\xF0\x9F\x99\x82",
  };
  std::vector<std::string> terms;
  for (const auto a : kChars) {
    terms.emplace_back(a);
    for (const auto b : kChars) {
      terms.emplace_back(std::string{a}.append(b));
      for (const auto c : kChars) {
        terms.emplace_back(std::string{a}.append(b).append(c));
      }
    }
  }
  const std::vector<std::string_view> views{terms.begin(), terms.end()};
  AddTerms(views);

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  struct Case {
    std::string_view prefix;
    std::string_view target;
  };
  constexpr Case kCases[]{
    {"", "\xD0\xB2"},
    {"",
     "\xD0\xB2\xD0\xB1"
     "a"},
    {"", "\xE4\xB8\xAD\xE4\xBA\xAC"},
    {"",
     "\xF0\x9F\x98\x80"
     "a\xC3\xA9"},
    {"", "a\xE4\xB8\xAB\xF0\x9F\x99\x82\xD1\x8B"},
    {"\xD0\xB2", "\xE4\xB8\xAD"},
    {"\xE4\xB8\xAD", "\xD0\xB1\xF0\x9F\x98\x81"},
  };
  for (const auto distance : {uint8_t{1}, uint8_t{2}}) {
    for (const bool transpositions : {false, true}) {
      const auto description =
        irs::MakeParametricDescription(distance, transpositions);
      for (const auto& [prefix, target] : kCases) {
        SCOPED_TRACE(testing::Message("Prefix: '")
                     << prefix << "', target: '" << target
                     << "', distance: " << size_t{distance}
                     << ", transpositions: " << transpositions);
        AssertEditDistanceOracle(*reader.GetImpl(), description, transpositions,
                                 prefix, target);
      }
    }
  }
}

TEST_P(AcceptorWalkIndexTestCase, fuzzy_source_walks_the_parametric_language) {
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  const auto narrow = irs::MakeParametricDescription(1, false);
  const auto small = std::make_shared<const irs::LevenshteinAcceptor>(
    narrow, irs::kEmptyStringView<irs::byte_type>,
    irs::ViewCast<irs::byte_type>("burden"sv));
  EXPECT_NE(nullptr, irs::FuzzyConjunction::Make(small));
  EXPECT_EQ(nullptr, irs::FuzzyConjunction::Make(small, 4096));
  const auto prefixed = std::make_shared<const irs::LevenshteinAcceptor>(
    narrow, irs::ViewCast<irs::byte_type>("bu"sv),
    irs::ViewCast<irs::byte_type>("rden"sv));

  const auto wide = irs::MakeParametricDescription(4, false);
  const auto large = std::make_shared<const irs::LevenshteinAcceptor>(
    wide, irs::kEmptyStringView<irs::byte_type>,
    irs::ViewCast<irs::byte_type>("parliamentarians"sv));
  EXPECT_EQ(nullptr, irs::FuzzyConjunction::Make(large));

  for (const auto& fuzzy : {small, prefixed, large}) {
    const auto source = irs::MakeFuzzySource(fuzzy);
    ASSERT_NE(nullptr, source);
    size_t total = 0;
    for (auto& segment : *reader.GetImpl()) {
      for (auto field_id : segment.field_ids()) {
        const auto* field = segment.field(field_id);
        ASSERT_NE(nullptr, field);
        std::vector<std::pair<irs::bstring, irs::byte_type>> walked;
        auto walk = field->iterator(*fuzzy);
        const auto* walk_payload = irs::get<irs::PayAttr>(*walk);
        ASSERT_NE(nullptr, walk_payload);
        while (walk->next()) {
          walked.emplace_back(walk->value(), walk_payload->value[0]);
        }
        std::vector<std::pair<irs::bstring, irs::byte_type>> sourced;
        auto it = source->Iterator(*field);
        const auto* payload = irs::get<irs::PayAttr>(*it);
        ASSERT_NE(nullptr, payload);
        while (it->next()) {
          sourced.emplace_back(it->value(), payload->value[0]);
        }
        EXPECT_EQ(walked, sourced);
        total += walked.size();
      }
    }
    EXPECT_NE(0, total);
  }
}

constexpr std::string_view kRe2Terms[]{
  "burden",
  "b\xC3\xBCrden",
  "bxrden",
  "b\xE4\xB8\xADrden",
  "b\xF0\x9F\x98\x80rden",
  "b\xE0\x80\x80rden",
  "b\xED\xA0\x80rden",
  "b\xF4\x90\x80\x80rden",
  "b\xFFrden",
  "b\x80rden",
  "b\xC3rden",
  "BURDEN",
  "Burden",
  "atlas",
  "atlantic",
  "Atlas",
  "gray",
  "grey",
  "gr\xC3\xA9y",
  "a\nb",
  "axb",
  "x",
  "zz",
  "abab",
  "tion",
  "Tion",
  "nation",
  "a1b2",
  "\xC3\xA9t\xC3\xA9",
  "den%x",
  "%x",
  "the siemens financial services",
  "siemens",
  "siemensland",
  "the siemens ag",
  "The Siemens AG",
  "access point",
  "accessories inc",
  "the access group",
  "senior data engineer",
  "bigdata engineer",
  "data engineering",
  "Data Engineer, GCP",
  "foobar",
  "foo bar",
  "a\nb",
  "ab",
  "abb",
  "ac",
  "abac",
  "ABab",
  "access",
  "accessory",
  "f0e1d2c3b4a5968778695a4b3c2d1e0f-abcd-0001",
  "f0e1d2c3b4a5968778695a4b3c2d1e0f-abce-0002",
  "abcd000000000000000000000000000000000000",
  "0000000000000000000000000000000000000abcd",
  "0000000000000000000000000000000000000abc",
};

constexpr std::string_view kRe2Perl[]{
  "bur.*",
  ".*tion",
  "b.rden",
  "atl(as|antic)",
  "a.{4}s",
  "(?i)bur.*",
  "(?i:t)ion",
  "b[^a-z]rden",
  "\\pL+",
  "x|y|zz",
  "(ab)*",
  "",
  "b\\w+n",
  "gr[ae]y",
  "gr.y",
  "a.b",
  "(?s)a.b",
  "(?i)^(the\\s+)?siemens\\b.*",
  "^(the\\s+)?access\\b.*|^(the\\s+)?siemens financial services\\b.*",
  ".*\\bdata engineer\\b.*",
  "(?i).*\\bdata engineer\\b.*|.*\\bgcp\\b.*",
  "foo\\Bbar",
  "foo\\B.*",
  "a$b",
  "(?m)a$\\n^b",
  "\\bbur\\w*",
  "(?:ab){2}|ac",
  "(?i:ab)ab|abac",
  "(?:a|x)b|(?:a|x)x",
  "(?i)^(the\\s+)?siemens\\b.*|^(the\\s+)?access.?\\b.*|^(the\\s+)?"
  "accessories\\b.*|^(the\\s+)?siemens financial services\\b.*",
  ".*\\bdata\\b.*|.*\\bgcp\\b.*|.*\\bgroup",
  ".*abcd.*",
  ".*5a4b3c.*",
  ".*ent.*",
  ".*the.*",
  ".*tion.*",
};

void AcceptorWalkIndexTestCase::AssertRe2Perl(size_t part, size_t parts) {
  AddTerms(kRe2Terms);
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  RE2::Options perl;
  perl.set_log_errors(false);
  for (size_t i = part; i < std::size(kRe2Perl); i += parts) {
    const auto pattern = kRe2Perl[i];
    SCOPED_TRACE(testing::Message("Regexp: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    const RE2 re{pattern, perl};
    ASSERT_TRUE(re.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
    AssertOracle(
      *reader.GetImpl(), acceptor, IsUtf8,
      [&](std::string_view term) { return RE2::FullMatch(term, re); });
    const auto source = irs::MakePatternSource(
      irs::ViewCast<irs::byte_type>(pattern), irs::PatternKind::RegexpPerl);
    ASSERT_NE(nullptr, source);
    AssertSourceMatchesWalk(*reader.GetImpl(), acceptor, *source);
  }
}

TEST_P(AcceptorWalkIndexTestCase, walks_match_re2_perl0) {
  AssertRe2Perl(0, 4);
}

TEST_P(AcceptorWalkIndexTestCase, walks_match_re2_perl1) {
  AssertRe2Perl(1, 4);
}

TEST_P(AcceptorWalkIndexTestCase, walks_match_re2_perl2) {
  AssertRe2Perl(2, 4);
}

TEST_P(AcceptorWalkIndexTestCase, walks_match_re2_perl3) {
  AssertRe2Perl(3, 4);
}

TEST_P(AcceptorWalkIndexTestCase, walks_match_re2) {
  AddTerms(kRe2Terms);
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  RE2::Options posix;
  posix.set_log_errors(false);
  posix.set_posix_syntax(true);
  posix.set_one_line(true);
  constexpr std::string_view kPosix[]{
    "[[:alpha:]]+", "gr(a|e)y", "b.rden", "a[a-z]*e", "bur.*", "(ab)*",
  };
  for (const auto pattern : kPosix) {
    SCOPED_TRACE(testing::Message("POSIX: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern),
                                       irs::RegexpSyntax::PosixEre};
    ASSERT_TRUE(acceptor.ok());
    const RE2 re{pattern, posix};
    ASSERT_TRUE(re.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
    AssertOracle(
      *reader.GetImpl(), acceptor, IsUtf8,
      [&](std::string_view term) { return RE2::FullMatch(term, re); });
  }

  RE2::Options like;
  like.set_log_errors(false);
  like.set_dot_nl(true);
  constexpr std::string_view kWildcards[]{
    "bur%", "%tion",  "b_rden", "%den%", "a____s", "%",    "",       "b%r%n",
    "\\%x", "b_rde%", "gr_y",   "_t_",   "%\\%x",  "b_%n", "%abcd%",
  };
  for (const auto pattern : kWildcards) {
    SCOPED_TRACE(testing::Message("Wildcard: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::RegexpAcceptor::WildcardTag{},
                                       irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    const RE2 re{LikeRegex(pattern), like};
    ASSERT_TRUE(re.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
    AssertOracle(
      *reader.GetImpl(), acceptor, [](irs::bytes_view) { return true; },
      [&](std::string_view term) { return RE2::PartialMatch(term, re); });
  }
}

TEST_P(AcceptorWalkIndexTestCase, union_walk_is_the_union_of_its_parts) {
  constexpr std::string_view kTerms[]{
    "burden",
    "b\xC3\xBCrden",
    "bxrden",
    "b\xE4\xB8\xADrden",
    "b\xE0\x80\x80rden",
    "b\xED\xA0\x80rden",
    "b\xF4\x90\x80\x80rden",
    "b\xFFrden",
    "b\x80rden",
    "b\xC3rden",
    "BURDEN",
    "atlas",
    "atlantic",
    "atl\xFF",
    "gray",
    "grey",
    "nation",
    "Nation",
    "x",
    "zz",
    "ab",
    "a\nb",
    "access point",
    "the siemens ag",
    "siemensland",
    "den\xFF",
  };

  AddTerms(kTerms);
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  using Kind = irs::RegexpAcceptor::PartKind;
  using Part = irs::RegexpAcceptor::Part;
  const auto part = [](Kind kind, std::string_view pattern) {
    return Part{kind, irs::ViewCast<irs::byte_type>(pattern)};
  };
  const auto alone = [](const Part& part, irs::bytes_view term) {
    switch (part.kind) {
      case Kind::Term:
        return term == part.pattern;
      case Kind::Prefix:
        return term.starts_with(part.pattern);
      case Kind::Wildcard:
        return irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{},
                                   part.pattern}
          .Matches(term);
      case Kind::Perl:
        return irs::RegexpAcceptor{part.pattern}.Matches(term);
      case Kind::PosixEre:
        return irs::RegexpAcceptor{part.pattern, irs::RegexpSyntax::PosixEre}
          .Matches(term);
    }
    return false;
  };

  const std::vector<std::vector<Part>> unions{
    {part(Kind::Prefix, "b"), part(Kind::Wildcard, "%tion")},
    {part(Kind::Term, "zz"), part(Kind::Perl, "b.rden"),
     part(Kind::Prefix, "atl")},
    {part(Kind::Wildcard, "b_rden"), part(Kind::Perl, "(?i)bur.*")},
    {part(Kind::Prefix, "b"), part(Kind::Perl, "x|y|zz")},
    {part(Kind::Term, "x"), part(Kind::Term, "zz"), part(Kind::Term, "grey")},
    {part(Kind::PosixEre, "gr(a|e)y"), part(Kind::Wildcard, "%den%")},
    {part(Kind::Perl, "(?i)^(the\\s+)?siemens\\b.*"),
     part(Kind::Prefix, "access")},
    {part(Kind::Wildcard, "%"), part(Kind::Perl, "x")},
    {part(Kind::Term, ""), part(Kind::Perl, "ab"), part(Kind::Perl, "a.b")},
    {part(Kind::Perl, "atl(as|antic)"), part(Kind::Perl, "(?i)nation")},
    {part(Kind::Prefix, "atl"), part(Kind::Wildcard, "%den"),
     part(Kind::Term, "zz"), part(Kind::Perl, ".*(ion|ing)")},
    {part(Kind::Term, "x"), part(Kind::Wildcard, "%rden"),
     part(Kind::Wildcard, "%\xC3\xBCrden")},
    {part(Kind::Prefix, "ab"), part(Kind::Term, "abc"), part(Kind::Prefix, "a"),
     part(Kind::Wildcard, "%tion")},
    {part(Kind::Term, "then"), part(Kind::Term, "the"),
     part(Kind::Term, "there"), part(Kind::Term, "th"),
     part(Kind::Perl, ".*ing")},
    {part(Kind::Prefix, "pre"), part(Kind::Term, "president"),
     part(Kind::Prefix, "pro"), part(Kind::Term, "pr"),
     part(Kind::Wildcard, "%ment")},
    {part(Kind::Term, "parliamentary"), part(Kind::Prefix, "international"),
     part(Kind::Term, "parliament"), part(Kind::Wildcard, "%ness")},
  };
  for (const auto& parts : unions) {
    const auto key = irs::UnionKey(parts);
    SCOPED_TRACE(testing::Message("Union: '")
                 << irs::DescribeUnion(key) << "'");
    const irs::RegexpAcceptor acceptor{std::span{parts}};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
    AssertOracle(
      *reader.GetImpl(), acceptor, [](irs::bytes_view) { return true; },
      [&](std::string_view term) {
        return std::any_of(parts.begin(), parts.end(), [&](const Part& p) {
          return alone(p, irs::ViewCast<irs::byte_type>(term));
        });
      });
    const auto source = irs::MakePatternSource(key, irs::PatternKind::Union);
    ASSERT_NE(nullptr, source);
    AssertSourceMatchesWalk(*reader.GetImpl(), acceptor, *source);
  }
}

TEST_P(AcceptorWalkIndexTestCase, conjunction_walk_is_the_intersection) {
  constexpr std::string_view kTerms[]{
    "burden",
    "b\xC3\xBCrden",
    "bxrden",
    "b\xE0\x80\x80rden",
    "b\xFFrden",
    "atlas",
    "atlantic",
    "atl\xFF",
    "gray",
    "grey",
    "nation",
    "Nation",
    "station",
    "x",
    "zz",
    "the siemens ag",
    "siemens ag",
    "siemensland ag",
    "siemens",
    "siemen",
    "simens",
    "sieemens",
    "siemenz",
    "siem\xC3\xA9ns",
    "siem\xC3\xA9n",
    "si\xE0\x81\xA5mens",
    "siem\xF0\x80\x81\xA5ns",
    "sie\xC0\xADmens",
    "s\xFFiemens",
    "siemen\x80s",
    "siemen\xC3",
    "\xD0\xBC\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0",
    "\xD0\xBC\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB0",
    "\xD0\xBC\xD0\xB0\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0",
    "\xD0\xBC\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0\xD0\xB5",
    "\xD0\xBC\xD0\xBE\xD1\x81\xE0\xB4\xBA\xD0\xB2\xD0\xB0",
    "sameness",
    "sanction",
    "abcd",
    "ab",
    "a\nb",
  };

  AddTerms(kTerms);
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  using Kind = irs::RegexpAcceptor::PartKind;
  const auto make = [](Kind kind, std::string_view pattern) {
    const auto bytes = irs::ViewCast<irs::byte_type>(pattern);
    if (kind == Kind::Wildcard) {
      return std::make_shared<const irs::RegexpAcceptor>(
        irs::RegexpAcceptor::WildcardTag{}, bytes);
    }
    return std::make_shared<const irs::RegexpAcceptor>(bytes);
  };

  struct Case {
    std::vector<std::pair<Kind, std::string_view>> patterns;
    std::string_view fuzzy;
    std::string_view prefix;
  };
  const Case cases[]{
    {{{Kind::Perl, ".*den"}, {Kind::Perl, "b.*"}}, {}},
    {{{Kind::Wildcard, "%tion"}, {Kind::Perl, "(?i)n.*"}}, {}},
    {{{Kind::Perl, "atl.*"}, {Kind::Wildcard, "%s"}}, {}},
    {{{Kind::Perl, "(?i)^(the\\s+)?siemens\\b.*"}, {Kind::Wildcard, "%ag"}},
     {}},
    {{{Kind::Perl, ".*a.*"}, {Kind::Perl, ".{2,4}"}}, {}},
    {{{Kind::Wildcard, "b_rden"}, {Kind::Perl, "b.rden"}}, {}},
    {{{Kind::Perl, "x|zz"}, {Kind::Perl, "zz|y"}}, {}},
    {{{Kind::Perl, "b.*"}, {Kind::Perl, "a.*"}}, {}},
    {{{Kind::Wildcard, "%a%"},
      {Kind::Wildcard, "%e%"},
      {Kind::Wildcard, "s%n_"}},
     {}},
    {{{Kind::Wildcard, "%e%"},
      {Kind::Wildcard, "%n%"},
      {Kind::Perl, "[a-z ]+"},
      {Kind::Wildcard, "%s"}},
     {}},
    {{{Kind::Wildcard, "%e%"}}, "siemens"},
    {{{Kind::Wildcard, "s%"}, {Kind::Perl, ".*m.*"}}, "siemens"},
    {{{Kind::Wildcard, "%a%"}, {Kind::Wildcard, "%e%"}, {Kind::Wildcard, "s%"}},
     "sanctions"},
    {{{Kind::Perl, "b.*"}}, "burden"},
    {{{Kind::Perl, "(?s).*"}}, "b\xC3\xBCrden"},
    {{{Kind::Wildcard, "%n%"}}, "emens", "si"},
    {{{Kind::Perl, "(?s).*"}}, "siemens"},
    {{{Kind::Perl, "(?s).*"}}, "siem\xC3\xA9ns"},
    {{{Kind::Wildcard, "%\xD0\xB0"}},
     "\xD0\xBC\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0"},
    {{{Kind::Perl, "(?s).*"}},
     "\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0",
     "\xD0\xBC\xD0\xBE"},
  };
  const auto description = irs::MakeParametricDescription(1, false);
  for (const auto& [parts, target, prefix] : cases) {
    std::vector<std::shared_ptr<const irs::RegexpAcceptor>> patterns;
    testing::Message trace;
    for (const auto& [kind, text] : parts) {
      trace << "'" << text << "' & ";
      patterns.emplace_back(make(kind, text));
      ASSERT_TRUE(patterns.back()->ok());
    }
    trace << "'" << prefix << "|" << target << "~'";
    SCOPED_TRACE(trace);
    const auto matches = [&](irs::bytes_view term) {
      return std::all_of(
        patterns.begin(), patterns.end(),
        [&](const auto& pattern) { return pattern->Matches(term); });
    };
    const auto covers = [](irs::bytes_view) { return true; };
    if (target.empty()) {
      for (const size_t max_mem :
           {irs::RegexpAcceptor::kDefaultMaxDfaMem, size_t{4096}, size_t{0}}) {
        SCOPED_TRACE(testing::Message("Budget: ") << max_mem);
        const irs::RegexpConjunction conjunction{patterns, max_mem};
        AssertWalk(*reader.GetImpl(), conjunction, false);
        AssertOracle(*reader.GetImpl(), conjunction, covers,
                     [&](std::string_view term) {
                       return matches(irs::ViewCast<irs::byte_type>(term));
                     });
      }
      const irs::RegexpConjunction conjunction{patterns};
      const auto source = irs::MakeJointSource(patterns, nullptr);
      ASSERT_NE(nullptr, source);
      AssertSourceMatchesWalk(*reader.GetImpl(), conjunction, *source);
      continue;
    }
    auto fuzzy = std::make_shared<const irs::LevenshteinAcceptor>(
      description, irs::ViewCast<irs::byte_type>(prefix),
      irs::ViewCast<irs::byte_type>(target));
    for (const size_t max_mem :
         {irs::RegexpAcceptor::kDefaultMaxDfaMem, size_t{16384}, size_t{0}}) {
      SCOPED_TRACE(testing::Message("Budget: ") << max_mem);
      const irs::FuzzyConjunction conjunction{patterns, fuzzy, max_mem};
      AssertWalk(*reader.GetImpl(), conjunction, false);
      AssertOracle(*reader.GetImpl(), conjunction, covers,
                   [&](std::string_view term) {
                     const auto bytes = irs::ViewCast<irs::byte_type>(term);
                     return matches(bytes) && fuzzy->Matches(bytes);
                   });
    }
    const irs::FuzzyConjunction conjunction{patterns, fuzzy};
    const auto source = irs::MakeJointSource(patterns, fuzzy);
    ASSERT_NE(nullptr, source);
    AssertSourceMatchesWalk(*reader.GetImpl(), conjunction, *source);
  }
}

INSTANTIATE_TEST_SUITE_P(acceptor_walk_index_test, AcceptorWalkIndexTestCase,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         AcceptorWalkIndexTestCase::to_string);

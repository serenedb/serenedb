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
#include <iresearch/search/detail/term_acceptor.hpp>
#include <iresearch/search/filters/wildcard_filter.hpp>
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
        if (acceptor.Matches(term)) {
          accepted.emplace_back(term, irs::byte_type{0});
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
    const auto target_bytes = irs::ViewCast<irs::byte_type>(target);
    const auto prefix_bytes = irs::ViewCast<irs::byte_type>(prefix);
    irs::containers::SmallVector<uint32_t, 16> target_chars;
    irs::utf8_utils::ToUTF32<false>(target_bytes,
                                    std::back_inserter(target_chars));

    const irs::LevenshteinAcceptor acceptor{description, prefix_bytes,
                                            target_bytes};

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

  template<typename Covers, typename Oracle>
  void AssertOracle(const irs::IndexReader& reader,
                    const irs::RegexpAcceptor& acceptor, Covers&& covers,
                    Oracle&& oracle) {
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
      const irs::LevenshteinAcceptor acceptor{
        description, irs::kEmptyStringView<irs::byte_type>,
        irs::ViewCast<irs::byte_type>(target)};
      AssertWalk(*reader.GetImpl(), acceptor, true);
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

  const irs::LevenshteinAcceptor acceptor{
    description, irs::kEmptyStringView<irs::byte_type>,
    irs::ViewCast<irs::byte_type>(kTarget)};

  size_t checked = 0;
  for (auto& segment : *reader.GetImpl()) {
    for (auto field_id : segment.field_ids()) {
      const auto* field = segment.field(field_id);
      ASSERT_NE(nullptr, field);
      auto walk = field->iterator(acceptor);
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

TEST_P(AcceptorWalkIndexTestCase, walks_match_re2) {
  constexpr std::string_view kTerms[]{
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
  };

  AddTerms(kTerms);
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  RE2::Options perl;
  perl.set_log_errors(false);
  constexpr std::string_view kPerl[]{
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
  };
  for (const auto pattern : kPerl) {
    SCOPED_TRACE(testing::Message("Regexp: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    const RE2 re{pattern, perl};
    ASSERT_TRUE(re.ok());
    AssertWalk(*reader.GetImpl(), acceptor, false);
    AssertOracle(
      *reader.GetImpl(), acceptor, IsUtf8,
      [&](std::string_view term) { return RE2::FullMatch(term, re); });
  }

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
    "bur%",  "%tion", "b_rden", "%den%", "a____s", "%",     "",
    "b%r%n", "\\%x",  "b_rde%", "gr_y",  "_t_",    "%\\%x", "b_%n",
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

INSTANTIATE_TEST_SUITE_P(acceptor_walk_index_test, AcceptorWalkIndexTestCase,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         AcceptorWalkIndexTestCase::to_string);

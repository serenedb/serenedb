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

// An acceptor drives the dictionary walk from inside it: a sub-block whose
// prefix the acceptor cannot extend is skipped without being loaded. That is a
// pruning optimization, so the property to test is that it prunes nothing it
// should have kept -- the walk must yield EXACTLY the terms a whole-dictionary
// scan plus a per-term test yields, in dictionary order, with the same payload.
//
// It is the only test that says the pruning is sound rather than merely fast.

#include <simdutf.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/small_vector.h"
#include "index/index_tests.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/term_acceptor.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/utf8_utils.hpp"

using namespace std::literals;

class AcceptorWalkIndexTestCase : public tests::IndexTestBase {
 protected:
  // The terms an acceptor accepts when every term is offered to it directly --
  // the walk has no say in this, so it is the oracle the walk is compared to.
  template<typename Acceptor>
  static std::vector<std::pair<irs::bstring, irs::byte_type>> BruteForce(
    const Acceptor& acceptor, const irs::TermReader& field) {
    std::vector<std::pair<irs::bstring, irs::byte_type>> accepted;
    auto terms = field.iterator();
    EXPECT_NE(nullptr, terms);
    while (terms->next()) {
      const auto term = terms->value();
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

  // `walk` is what the field hands out for this acceptor; it must reproduce
  // `expected` term for term and stop where the oracle stops.
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
    // Nothing beyond the oracle: an over-eager walk would show up here.
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

  // europarl is pure ASCII, so nothing in it makes a walk step through the
  // middle of a UTF-8 sequence or seek out of one. This is a dictionary that
  // does.
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

  // The terms within `description`'s bound of `target`, computed with
  // `EditDistance` -- an oracle that shares nothing with the acceptor, so it
  // pins the language and not merely the pruning. This is what the FST-era
  // `index_levenshtein_tests` asserted, over the same corpus, the same three
  // descriptions and the same seven targets.
  void AssertEditDistanceOracle(const irs::IndexReader& reader,
                                const irs::ParametricDescription& description,
                                std::string_view target) {
    const auto target_bytes = irs::ViewCast<irs::byte_type>(target);
    sdb::containers::SmallVector<uint32_t, 16> target_chars;
    irs::utf8_utils::ToUTF32<false>(target_bytes,
                                    std::back_inserter(target_chars));

    const irs::LevenshteinAcceptor acceptor{
      description, irs::kEmptyStringView<irs::byte_type>, target_bytes};

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

          sdb::containers::SmallVector<uint32_t, 16> expected_chars;
          if (!irs::utf8_utils::ToUTF32<true>(
                expected_term, std::back_inserter(expected_chars))) {
            continue;
          }
          const auto edit_distance =
            irs::EditDistance(expected_chars.data(), expected_chars.size(),
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
        // Nothing beyond the oracle: the walk's language is the oracle's, not
        // merely a subset of it.
        ASSERT_FALSE(actual_terms->next());
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
      AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/true);
    }
  }
}

// The distance the walk reports has to be the real edit distance, not just
// some accepted byte: that is what similarity scoring ranks by.
TEST_P(AcceptorWalkIndexTestCase, levenshtein_payload_is_edit_distance) {
  const auto description = irs::MakeParametricDescription(2, false);
  constexpr std::string_view kTarget = "burden";

  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  sdb::containers::SmallVector<uint32_t, 16> target_chars;
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
        // A key the walk accepts is valid UTF-8 by construction of the
        // acceptor's byte model, so it decodes.
        sdb::containers::SmallVector<uint32_t, 16> chars;
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

// `BruteForce` drives the acceptor itself, so it proves the pruning sound and
// nothing about the language. This one replaces the oracle with an
// independently computed edit distance, over every description and every
// target the deleted `index_levenshtein_tests` covered.
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
  // A prefix, an anchored suffix, a bounded repeat, an alternation, a pattern
  // that accepts nothing, a pattern that accepts everything (where pruning
  // must never fire) and the empty pattern (where it must fire everywhere but
  // the empty key) -- the shapes whose pruning differs.
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
    AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/false);
  }
}

// The wildcard dialect takes every literal byte as itself, so it selects terms
// a regexp source string could not even express.
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
    AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/false);
  }
}

// A `_` or a `.` in the middle of a key is where a walk has to leave a
// sub-block whose prefix ends inside a multi-byte character and seek to the
// next one, which europarl -- pure ASCII -- never asks it to do.
TEST_P(AcceptorWalkIndexTestCase, walk_selects_multibyte_terms) {
  constexpr std::string_view kTerms[]{
    "burden",  "b\xC3\xBCrden",          // ü, 2 bytes
    "bxrden",  "b\xC5\xB1rden",          // ű, 2 bytes
    "b",       "b\xE4\xB8\xADrden",      // 中, 3 bytes
    "burde",   "b\xF0\x9F\x98\x80rden",  // 😀, 4 bytes
    "burdens", "b\xC3\xBCrde",          "urden", "b\xC3\xBCrdens",
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
    AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/false);
  }
  for (const auto pattern : kRegexps) {
    SCOPED_TRACE(testing::Message("Regexp: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/false);
  }

  // `b_rden` selects one code point between 'b' and "rden", of any byte
  // length, and nothing else -- an independent statement of the same thing.
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

// The walk's whole point is skipping sub-blocks it cannot extend, so the case
// that matters is a match on either side of a block edge. A few thousand keys
// sharing one prefix is what forces the dictionary to split into blocks at
// all; europarl crosses them only incidentally and never on purpose.
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

  // Selecting one key per hundred, one key per ten and a scattered suffix
  // puts matches inside blocks, on their first key and on their last.
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
    AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/false);
  }
  for (const auto pattern : kRegexps) {
    SCOPED_TRACE(testing::Message("Regexp: '") << pattern << "'");
    const irs::RegexpAcceptor acceptor{irs::ViewCast<irs::byte_type>(pattern)};
    ASSERT_TRUE(acceptor.ok());
    AssertWalk(*reader.GetImpl(), acceptor, /*expect_payload=*/false);
  }
}

// The fusion rule turns a conjunction of same-field term predicates into one
// walk: a driver whose language is a superset, with the rest as a residual
// test. The driver is trusted to be exact, so the risk is the pair selecting
// more or less than the conjunction does.
TEST_P(AcceptorWalkIndexTestCase, conjunction_source_is_the_intersection) {
  AddEuroparl();

  auto reader = open_reader();
  ASSERT_NE(nullptr, reader);

  // Both operands are wildcards over the same field: "starts with bur" AND
  // "ends with n".
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

      // The conjunction, computed without any of the machinery under test.
      std::vector<std::pair<irs::bstring, irs::byte_type>> expected;
      for (const auto& [term, payload] : BruteForce(driver_acceptor, *field)) {
        if (residual_acceptor.Matches(term)) {
          expected.emplace_back(term, payload);
        }
      }

      auto source = irs::MakeConjunctionSource(
        irs::MakePatternSource(
          irs::bstring{irs::ViewCast<irs::byte_type>(kDriver)},
          irs::PatternKind::Wildcard),
        irs::TermBounds{},
        irs::CreateByWildcard(field_id,
                              irs::ViewCast<irs::byte_type>(kResidual)));
      ASSERT_TRUE(source->ok());

      auto walk = source->Iterator(*field);
      ASSERT_NE(nullptr, walk);
      AssertSameWalk(expected, *walk, /*expect_payload=*/false);

      // The whole-term test the same source hands out has to agree with its
      // walk, since a filter may use either.
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

// The production rule bounds the walk by the driver's literal prefix --
// `[prefix, UpperBoundOf(prefix))` -- and an upper bound one key too small
// drops terms with nothing to report it. Both the bounds-only shape (no driver
// acceptor at all) and the driver-plus-bounds shape have to select exactly the
// conjunction.
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

      // Bounds only: the range is the whole driver, the residual is the test.
      {
        auto source = irs::MakeConjunctionSource(
          nullptr, bounds,
          irs::CreateByWildcard(field_id,
                                irs::ViewCast<irs::byte_type>(kResidual)));
        ASSERT_TRUE(source->ok());
        auto walk = source->Iterator(*field);
        ASSERT_NE(nullptr, walk);
        AssertSameWalk(expected, *walk, /*expect_payload=*/false);
      }

      // Driver plus the same bounds: the bounds must not cut the driver short.
      {
        auto source = irs::MakeConjunctionSource(
          irs::MakePatternSource(
            irs::bstring{irs::ViewCast<irs::byte_type>(kDriver)},
            irs::PatternKind::Wildcard),
          bounds,
          irs::CreateByWildcard(field_id,
                                irs::ViewCast<irs::byte_type>(kResidual)));
        ASSERT_TRUE(source->ok());
        auto walk = source->Iterator(*field);
        ASSERT_NE(nullptr, walk);
        AssertSameWalk(expected, *walk, /*expect_payload=*/false);
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
  acceptor_walk_index_test, AcceptorWalkIndexTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MemoryDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5simd"})),
  AcceptorWalkIndexTestCase::to_string);

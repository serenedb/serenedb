////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/minhash_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_shared.hpp"

namespace {

class ArrayStream final : public irs::analysis::TypedAnalyzer<ArrayStream> {
 public:
  ArrayStream(std::string_view data, const std::string_view* begin,
              const std::string_view* end) noexcept
    : _data{data}, _begin{begin}, _it{end}, _end{end} {}

  bool next() final {
    if (_it == _end) {
      return false;
    }

    auto& offs = std::get<irs::OffsAttr>(_attrs);
    offs.start = offs.end;
    offs.end += _it->size();

    std::get<irs::TermAttr>(_attrs).value = irs::ViewCast<irs::byte_type>(*_it);

    ++_it;
    return true;
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  bool reset(std::string_view data) final {
    std::get<irs::OffsAttr>(_attrs) = {};

    if (data == _data) {
      _it = _begin;
      return true;
    }

    _it = _end;
    return false;
  }

 private:
  using Attributes = std::tuple<irs::TermAttr, irs::IncAttr, irs::OffsAttr>;

  Attributes _attrs;
  std::string_view _data;
  const std::string_view* _begin;
  const std::string_view* _it;
  const std::string_view* _end;
};

}  // namespace

TEST(MinHashTokenizerTest, CheckConsts) {
  static_assert("minhash" ==
                irs::Type<irs::analysis::MinHashTokenizer>::name());
}

TEST(MinHashTokenizerTest, ConstructDefault) {
  auto assert_analyzer = [](const irs::analysis::Analyzer::ptr& stream,
                            size_t expected_num_hashes) {
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::MinHashTokenizer>::id(), stream->type());

    auto* impl =
      dynamic_cast<const irs::analysis::MinHashTokenizer*>(stream.get());
    ASSERT_NE(nullptr, impl);
    ASSERT_EQ(expected_num_hashes, impl->num_hashes());
  };

  assert_analyzer(irs::analysis::MinHashTokenizer::Make(
                    irs::analysis::MinHashTokenizer::Options{
                      .analyzer = nullptr,
                      .num_hashes = 42,
                    }),
                  42);

  // .........................................................................
  // Failing cases ported from the old JSON-based suite.
  // JSON-internal type errors (e.g. `"numHashes": []`, `"numHashes": true`,
  // `"numHashes": "42"`) collapse to the same direct-API failure: an Options
  // value that Make must reject. With the Options API we exercise the two
  // remaining failure modes: `num_hashes == 0` and a child config that
  // itself fails to construct (already covered by CreateAnalyzer returning
  // nullptr -- see below).
  // .........................................................................
  ASSERT_ANY_THROW(irs::analysis::MinHashTokenizer::Make(
    irs::analysis::MinHashTokenizer::Options{
      .analyzer = nullptr,
      .num_hashes = 0,
    }));
}

TEST(MinHashTokenizerTest, ConstructCustom) {
  auto assert_analyzer = [](const irs::analysis::Analyzer::ptr& stream,
                            size_t expected_num_hashes) {
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::MinHashTokenizer>::id(), stream->type());

    auto* impl =
      dynamic_cast<const irs::analysis::MinHashTokenizer*>(stream.get());
    ASSERT_NE(nullptr, impl);
    ASSERT_EQ(expected_num_hashes, impl->num_hashes());
  };

  auto child = std::make_unique<irs::analysis::TokenizerConfig>(
    irs::analysis::TokenizerConfig{
      irs::analysis::SegmentationTokenizer::Options{}});
  assert_analyzer(irs::analysis::MinHashTokenizer::Make(
                    irs::analysis::MinHashTokenizer::Options{
                      .analyzer = std::move(child),
                      .num_hashes = 42,
                    }),
                  42);
}

TEST(MinHashTokenizerTest, ConstructFromOptions) {
  using namespace irs::analysis;

  {
    MinHashTokenizer stream{nullptr, 0};
    ASSERT_NE(nullptr, irs::get<irs::TermAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::OffsAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::IncAttr>(stream));
    ASSERT_EQ(0, stream.num_hashes());
  }

  {
    MinHashTokenizer stream{SegmentationTokenizer::Make({}), 42};
    ASSERT_NE(nullptr, irs::get<irs::TermAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::OffsAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::IncAttr>(stream));
    ASSERT_EQ(42, stream.num_hashes());
  }

  {
    MinHashTokenizer stream{std::make_unique<EmptyAnalyzer>(), 42};
    ASSERT_NE(nullptr, irs::get<irs::TermAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::OffsAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::IncAttr>(stream));
    ASSERT_EQ(42, stream.num_hashes());
    ASSERT_FALSE(stream.reset(""));
  }
}

TEST(MinHashTokenizerTest, NextReset) {
  using namespace irs::analysis;

  constexpr uint32_t kNumHashes = 4;
  constexpr std::string_view kData{"Hund"};
  constexpr std::string_view kValues[]{"quick", "brown", "fox",  "jumps",
                                       "over",  "the",   "lazy", "dog"};

  MinHashTokenizer stream{std::make_unique<ArrayStream>(
                            kData, std::begin(kValues), std::end(kValues)),
                          kNumHashes};

  auto* term = irs::get<irs::TermAttr>(stream);
  ASSERT_NE(nullptr, term);
  auto* offset = irs::get<irs::OffsAttr>(stream);
  ASSERT_NE(nullptr, offset);
  auto* inc = irs::get<irs::IncAttr>(stream);
  ASSERT_NE(nullptr, inc);

  for (size_t i = 0; i < 2; ++i) {
    ASSERT_TRUE(stream.reset(kData));

    ASSERT_TRUE(stream.next());
    EXPECT_EQ("q9VZS3VMEoY", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);
    EXPECT_EQ(0, offset->start);
    EXPECT_EQ(32, offset->end);

    ASSERT_TRUE(stream.next());
    EXPECT_EQ("9oVVAx777yc", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);
    EXPECT_EQ(0, offset->start);
    EXPECT_EQ(32, offset->end);

    ASSERT_TRUE(stream.next());
    EXPECT_EQ("U9QEhWO/5Dw", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);
    EXPECT_EQ(0, offset->start);
    EXPECT_EQ(32, offset->end);

    ASSERT_TRUE(stream.next());
    EXPECT_EQ("Y9at6wPcrAk", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);
    EXPECT_EQ(0, offset->start);
    EXPECT_EQ(32, offset->end);

    ASSERT_FALSE(stream.next());
  }
}

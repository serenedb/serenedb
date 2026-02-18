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

#include <vpack/common.h>
#include <vpack/parser.h>

#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/minhash_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
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

TEST(MinHashTokenizerTest, NormalizeDefault) {
  std::string out;
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": 42})"));
  const auto expected_out = vpack::Parser::fromJson(R"({"numHashes": 42})");
  ASSERT_EQ(expected_out->slice().toString(), out);

  // Failing cases
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": "42"})"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": null})"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": 0})"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": false})"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": []})"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({"numHashes": {}})"));
}

TEST(MinHashTokenizerTest, ConstructDefault) {
  auto assert_analyzer = [](const irs::analysis::Analyzer::ptr& stream,
                            size_t expected_num_hashes) {
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::MinHashTokenizer>::id(), stream->type());

    auto* impl =
      dynamic_cast<const irs::analysis::MinHashTokenizer*>(stream.get());
    ASSERT_NE(nullptr, impl);
    const auto& [analyzer, num_hashes] = impl->options();
    ASSERT_NE(nullptr, analyzer);
    ASSERT_EQ(irs::Type<irs::StringTokenizer>::id(), analyzer->type());
    ASSERT_EQ(expected_num_hashes, num_hashes);
  };

  assert_analyzer(irs::analysis::analyzers::Get(
                    "minhash", irs::Type<irs::text_format::Json>::get(),
                    R"({"numHashes": 42})"),
                  42);

  // Failing cases
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"numHashes": 0})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"numHashes": []})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"numHashes": {}})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"numHashes": true})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"numHashes": null})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"numHashes": "42"})"));
  ASSERT_EQ(nullptr,
            irs::analysis::analyzers::Get(
              "minhash", irs::Type<irs::text_format::Json>::get(), R"({})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"analyzer":{}})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"analyzer":""})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"analyzer":null})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"analyzer":[]})"));
  ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                       "minhash", irs::Type<irs::text_format::Json>::get(),
                       R"({"analyzer":42})"));
}

TEST(MinHashTokenizerTest, ConstructCustom) {
  auto assert_analyzer = [](const irs::analysis::Analyzer::ptr& stream,
                            size_t expected_num_hashes) {
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::MinHashTokenizer>::id(), stream->type());

    auto* impl =
      dynamic_cast<const irs::analysis::MinHashTokenizer*>(stream.get());
    ASSERT_NE(nullptr, impl);
    const auto& [analyzer, num_hashes] = impl->options();
    ASSERT_EQ(expected_num_hashes, num_hashes);
    ASSERT_NE(nullptr, analyzer);
    ASSERT_EQ(irs::Type<irs::analysis::SegmentationTokenizer>::id(),
              analyzer->type());
  };

  assert_analyzer(
    irs::analysis::analyzers::Get(
      "minhash", irs::Type<irs::text_format::Json>::get(),
      R"({ "analyzer":{"type":"segmentation"}, "numHashes": 42 })"),
    42);
}

TEST(MinHashTokenizerTest, NormalizeCustom) {
  std::string out;
  const auto expected_out = vpack::Parser::fromJson(
    R"({ "analyzer": {
             "type":"segmentation",
             "properties": {"break":"alpha","case":"lower"} },
             "numHashes": 42 })");

  out.clear();
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation"}, "numHashes": 42 })"));
  ASSERT_EQ(expected_out->slice().toString(), out);

  out.clear();
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":{}}, "numHashes": 42 })"));
  ASSERT_EQ(expected_out->slice().toString(), out);

  out.clear();
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":{"case":"lower"}}, "numHashes": 42 })"));
  ASSERT_EQ(expected_out->slice().toString(), out);

  out.clear();
  ASSERT_TRUE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":{"case":"upper"}}, "numHashes": 42 })"));
  ASSERT_NE(expected_out->slice().toString(), out);

  // Failing cases
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":{}, "numHashes": 0 })"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":false, "numHashes": 42 })"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":[], "numHashes": 42 })"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":false, "numHashes": 42 })"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":null, "numHashes": 42 })"));
  ASSERT_FALSE(irs::analysis::analyzers::Normalize(
    out, "minhash", irs::Type<irs::text_format::Json>::get(),
    R"({ "analyzer":{"type":"segmentation", "properties":42, "numHashes": 42 })"));
}

TEST(MinHashTokenizerTest, CheckOptions) {
  using namespace irs::analysis;

  MinHashTokenizer::Options opts;

  ASSERT_EQ(nullptr, opts.analyzer);
  ASSERT_EQ(1, opts.num_hashes);
}

TEST(MinHashTokenizerTest, ConstructFromOptions) {
  using namespace irs::analysis;

  {
    MinHashTokenizer stream{
      MinHashTokenizer::Options{.analyzer = nullptr, .num_hashes = 0}};
    ASSERT_NE(nullptr, irs::get<irs::TermAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::OffsAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::IncAttr>(stream));
    const auto& [analyzer, num_hashes] = stream.options();
    ASSERT_NE(nullptr, analyzer);
    ASSERT_EQ(0, num_hashes);
    ASSERT_EQ(irs::Type<irs::StringTokenizer>::id(), analyzer->type());
  }

  {
    MinHashTokenizer stream{MinHashTokenizer::Options{
      .analyzer = SegmentationTokenizer::make({}), .num_hashes = 42}};
    ASSERT_NE(nullptr, irs::get<irs::TermAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::OffsAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::IncAttr>(stream));
    const auto& [analyzer, num_hashes] = stream.options();
    ASSERT_NE(nullptr, analyzer);
    ASSERT_EQ(42, num_hashes);
    ASSERT_EQ(irs::Type<SegmentationTokenizer>::id(), analyzer->type());
  }

  {
    MinHashTokenizer stream{MinHashTokenizer::Options{
      .analyzer = std::make_unique<EmptyAnalyzer>(), .num_hashes = 42}};
    ASSERT_NE(nullptr, irs::get<irs::TermAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::OffsAttr>(stream));
    ASSERT_NE(nullptr, irs::get<irs::IncAttr>(stream));
    const auto& [analyzer, num_hashes] = stream.options();
    ASSERT_NE(nullptr, analyzer);
    ASSERT_EQ(42, num_hashes);
    ASSERT_EQ(irs::Type<EmptyAnalyzer>::id(), analyzer->type());
    ASSERT_FALSE(stream.reset(""));
  }
}

TEST(MinHashTokenizerTest, NextReset) {
  using namespace irs::analysis;

  constexpr uint32_t kNumHashes = 4;
  constexpr std::string_view kData{"Hund"};
  constexpr std::string_view kValues[]{"quick", "brown", "fox",  "jumps",
                                       "over",  "the",   "lazy", "dog"};

  MinHashTokenizer stream{{.analyzer = std::make_unique<ArrayStream>(
                             kData, std::begin(kValues), std::end(kValues)),
                           .num_hashes = kNumHashes}};

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

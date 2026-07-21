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

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/minhash_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

class ArrayStream final : public irs::analysis::TypedTokenizer<ArrayStream> {
 public:
  ArrayStream(std::string_view data, const std::string_view* begin,
              const std::string_view* end) noexcept
    : _data{data}, _begin{begin}, _end{end} {}

  irs::TokenTraits Traits() const noexcept final {
    return {.dense_pos = false};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view value, irs::TokenEmitter& sink) {
    if (value != _data) {
      return false;
    }
    uint32_t pos = 0;
    uint32_t offs = 0;
    for (const auto* it = _begin; it != _end; ++it) {
      const auto start = offs;
      offs += static_cast<uint32_t>(it->size());
      sink.EmitInterned<Layout>(irs::ViewCast<irs::byte_type>(*it), ++pos,
                                start, offs);
    }
    return true;
  }

 private:
  std::string_view _data;
  const std::string_view* _begin;
  const std::string_view* _end;
};

}  // namespace

TEST(MinHashTokenizerTest, CheckConsts) {
  static_assert("minhash" ==
                irs::Type<irs::analysis::MinHashTokenizer>::name());
}

TEST(MinHashTokenizerTest, ConstructDefault) {
  auto assert_analyzer = [](const irs::analysis::Tokenizer::ptr& stream,
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
  // itself fails to construct (already covered by CreateTokenizer returning
  // nullptr -- see below).
  // .........................................................................
  ASSERT_ANY_THROW(irs::analysis::MinHashTokenizer::Make(
    irs::analysis::MinHashTokenizer::Options{
      .analyzer = nullptr,
      .num_hashes = 0,
    }));
}

TEST(MinHashTokenizerTest, ConstructCustom) {
  auto assert_analyzer = [](const irs::analysis::Tokenizer::ptr& stream,
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
    ASSERT_FALSE(stream.Traits().offsets);
    ASSERT_EQ(0, stream.num_hashes());
  }

  {
    MinHashTokenizer stream{SegmentationTokenizer::Make({}), 42};
    ASSERT_FALSE(stream.Traits().offsets);
    ASSERT_EQ(42, stream.num_hashes());
  }

  {
    MinHashTokenizer stream{std::make_unique<EmptyTokenizer>(), 42};
    ASSERT_FALSE(stream.Traits().offsets);
    ASSERT_EQ(42, stream.num_hashes());
    ASSERT_FALSE(tests::Analyze(stream, "").has_value());
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

  for (size_t i = 0; i < 2; ++i) {
    const auto tokens = tests::Analyze(stream, kData);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(4U, tokens->size());

    EXPECT_EQ("q9VZS3VMEoY", (*tokens)[0].term);
    ASSERT_EQ(1, (*tokens)[0].pos);
    EXPECT_EQ(0, (*tokens)[0].offs_end);

    EXPECT_EQ("9oVVAx777yc", (*tokens)[1].term);
    ASSERT_EQ(1, (*tokens)[1].pos);
    EXPECT_EQ(0, (*tokens)[1].offs_end);

    EXPECT_EQ("U9QEhWO/5Dw", (*tokens)[2].term);
    ASSERT_EQ(1, (*tokens)[2].pos);
    EXPECT_EQ(0, (*tokens)[2].offs_end);

    EXPECT_EQ("Y9at6wPcrAk", (*tokens)[3].term);
    ASSERT_EQ(1, (*tokens)[3].pos);
    EXPECT_EQ(0, (*tokens)[3].offs_end);
  }

  ASSERT_FALSE(tests::Analyze(stream, "Katze").has_value());
}

TEST(MinHashTokenizerTest, NativeFillsMatchPull) {
  using namespace irs::analysis;

  const std::vector<std::string> values = {
    "quick brown fox jumps over the lazy dog", "quick", "", "the the the the",
    "Aiacos Kottos Gyges Briareos Minos Rhadamanthys"};

  for (const uint32_t num_hashes : {1u, 4u, 100u}) {
    SCOPED_TRACE(num_hashes);
    MinHashTokenizer single_stream{SegmentationTokenizer::Make({}), num_hashes};
    MinHashTokenizer column_stream{SegmentationTokenizer::Make({}), num_hashes};

    for (const auto& value : values) {
      SCOPED_TRACE(value);
      const auto single = tests::Analyze(single_stream, value);

      duckdb::string_t column_value{value.data(),
                                    static_cast<uint32_t>(value.size())};
      irs::doc_id_t doc = 42;
      tests::OneBatchSink sink{irs::TokenLayout::TermsPos};
      column_stream.Fill(std::span{&column_value, 1}, std::span{&doc, 1},
                         sink.writer, sink.layout);
      ASSERT_FALSE(sink.flushed());
      auto& batch = sink.writer.buf;
      const auto runs = sink.writer.Runs();
      EXPECT_FALSE(batch.dense_pos);
      ASSERT_EQ(1U, runs.size());
      ASSERT_EQ(doc, runs[0].doc);

      const size_t expected_count = single ? single->size() : 0;
      ASSERT_EQ(expected_count, runs[0].ntokens);
      ASSERT_EQ(expected_count, batch.count);
      for (size_t i = 0; i < expected_count; ++i) {
        SCOPED_TRACE(i);
        const auto& t = batch.terms[i];
        ASSERT_EQ((*single)[i].term, (std::string{t.GetData(), t.GetSize()}));
        ASSERT_EQ((*single)[i].pos, batch.pos[i]);
      }
    }
  }
}

TEST(MinHashTokenizerTest, ColumnFillMatchesPull) {
  using namespace irs::analysis;

  const std::vector<std::string> raw = {
    "quick brown fox jumps over the lazy dog", "", "quick",
    "Aiacos Kottos Gyges Briareos Minos Rhadamanthys", "the the the the"};
  std::vector<duckdb::string_t> values;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < raw.size(); ++i) {
    values.emplace_back(raw[i].data(), static_cast<uint32_t>(raw[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(100 + i));
  }

  for (const uint32_t num_hashes : {1u, 4u}) {
    SCOPED_TRACE(num_hashes);
    MinHashTokenizer single_stream{SegmentationTokenizer::Make({}), num_hashes};
    MinHashTokenizer fill_stream{SegmentationTokenizer::Make({}), num_hashes};

    tests::OneBatchSink sink{irs::TokenLayout::TermsPos};
    fill_stream.Fill(values, docs, sink.writer, sink.layout);
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    const auto runs = sink.writer.Runs();
    ASSERT_EQ(raw.size(), runs.size());

    uint32_t token_idx = 0;
    for (size_t v = 0; v < raw.size(); ++v) {
      SCOPED_TRACE(raw[v]);
      ASSERT_EQ(docs[v], runs[v].doc);
      const auto single =
        tests::Analyze(single_stream, raw[v], irs::TokenLayout::TermsPos);
      const size_t expected_count = single ? single->size() : 0;
      ASSERT_EQ(expected_count, runs[v].ntokens);
      for (size_t j = 0; j < expected_count; ++j) {
        const auto& t = batch.terms[token_idx];
        ASSERT_EQ((*single)[j].term, (std::string{t.GetData(), t.GetSize()}));
        ASSERT_EQ((*single)[j].pos, batch.pos[token_idx]);
        ++token_idx;
      }
    }
    ASSERT_EQ(batch.count, token_idx);
  }
}

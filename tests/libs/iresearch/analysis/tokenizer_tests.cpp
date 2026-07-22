////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <span>

#include "iresearch/analysis/batch/numeric_terms.hpp"
#include "iresearch/analysis/batch/token_sinks.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

using namespace irs;
using namespace std::string_view_literals;

TEST(string_token_stream_tests, next_end) {
  const std::string str("QBVnCx4NCizekHA");
  StringTokenizer ts;

  {
    const auto tokens = tests::Analyze(ts, str);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    ASSERT_EQ(str, (*tokens)[0].term);
    ASSERT_EQ(0, (*tokens)[0].offs_start);
    ASSERT_EQ(str.size(), (*tokens)[0].offs_end);
  }

  {
    const auto tokens = tests::Analyze(ts, str);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    ASSERT_EQ(str, (*tokens)[0].term);
    ASSERT_EQ(0, (*tokens)[0].offs_start);
    ASSERT_EQ(str.size(), (*tokens)[0].offs_end);
  }
}

template<typename T>
std::vector<irs::bstring> NumericTerms(T value) {
  std::vector<irs::bstring> terms;
  irs::numeric_utils::ForEachNumericTerm(
    value, [&](irs::bytes_view term) { terms.emplace_back(term); });
  return terms;
}

template<typename T>
void AssertNumericTerms(T value,
                        const std::vector<std::string_view>& expected) {
  SCOPED_TRACE(testing::Message() << "value=" << value);
  const auto terms = NumericTerms(value);
  ASSERT_EQ(irs::NumericTermCount<T>(), terms.size());
  ASSERT_EQ(expected.size(), terms.size());
  for (size_t i = 0; i < terms.size(); ++i) {
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(expected[i]), terms[i]);
  }
  irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
  ASSERT_EQ(terms.front(), irs::numeric_utils::EncodeNumericTerm(buf, value));
}

TEST(numeric_terms_test, golden_terms) {
  AssertNumericTerms(int64_t{0}, {"\x60\x80\x00\x00\x00\x00\x00\x00\x00"sv,
                                  "\x70\x80\x00\x00\x00\x00\x00"sv,
                                  "\x80\x80\x00\x00\x00"sv, "\x90\x80\x00"sv});
  AssertNumericTerms(int64_t{42}, {"\x60\x80\x00\x00\x00\x00\x00\x00\x2A"sv,
                                   "\x70\x80\x00\x00\x00\x00\x00"sv,
                                   "\x80\x80\x00\x00\x00"sv, "\x90\x80\x00"sv});
  AssertNumericTerms(int64_t{-1}, {"\x60\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"sv,
                                   "\x70\x7F\xFF\xFF\xFF\xFF\xFF"sv,
                                   "\x80\x7F\xFF\xFF\xFF"sv, "\x90\x7F\xFF"sv});
  AssertNumericTerms(int32_t{7}, {"\x00\x80\x00\x00\x07"sv, "\x10\x80\x00"sv});
  AssertNumericTerms(
    double_t{2.5},
    {"\xA0\xC0\x04\x00\x00\x00\x00\x00\x00"sv, "\xB0\xC0\x04\x00\x00\x00\x00"sv,
     "\xC0\xC0\x04\x00\x00"sv, "\xD0\xC0\x04"sv});
#ifndef FLOAT_T_IS_DOUBLE_T
  AssertNumericTerms(float_t{1.5f},
                     {"\x20\xBF\xC0\x00\x00"sv, "\x30\xBF\xC0"sv});
#endif
}

template<typename T>
void AssertNumericBlockMatchesPerValue(std::span<const T> values) {
  auto batch = std::make_unique<irs::TokenBatch>();
  irs::AppendNumericTermsBlock(*batch, values);
  ASSERT_EQ(values.size() * irs::NumericTermCount<T>(), batch->count);
  uint32_t i = 0;
  for (const auto v : values) {
    for (const auto& expected : NumericTerms(v)) {
      const auto& t = batch->terms[i++];
      const irs::bytes_view term{
        reinterpret_cast<const irs::byte_type*>(t.GetData()),
        static_cast<size_t>(t.GetSize())};
      ASSERT_EQ(expected, term);
    }
  }
}

TEST(numeric_terms_test, block_matches_per_value) {
  constexpr int64_t kI64[] = {0,
                              1,
                              -1,
                              4242424242,
                              -9876543210,
                              std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max()};
  AssertNumericBlockMatchesPerValue(std::span<const int64_t>{kI64});
  constexpr int32_t kI32[] = {0,
                              1,
                              -1,
                              42,
                              -123456,
                              std::numeric_limits<int32_t>::min(),
                              std::numeric_limits<int32_t>::max()};
  AssertNumericBlockMatchesPerValue(std::span<const int32_t>{kI32});
  constexpr double_t kF64[] = {0.,
                               1.,
                               -1.,
                               3.141592653589793,
                               std::numeric_limits<double_t>::max(),
                               std::numeric_limits<double_t>::lowest(),
                               std::numeric_limits<double_t>::infinity(),
                               -std::numeric_limits<double_t>::infinity()};
  AssertNumericBlockMatchesPerValue(std::span<const double_t>{kF64});
#ifndef FLOAT_T_IS_DOUBLE_T
  constexpr float_t kF32[] = {0.f,
                              1.f,
                              -1.f,
                              3.14f,
                              std::numeric_limits<float_t>::max(),
                              std::numeric_limits<float_t>::lowest()};
  AssertNumericBlockMatchesPerValue(std::span<const float_t>{kF32});
#endif
}

TEST(string_token_stream_tests, native_batch_fill) {
  irs::StringTokenizer stream;
  const std::string long_value(64, 'x');

  for (const auto v :
       {std::string_view{"short"}, std::string_view{long_value}}) {
    tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
    ASSERT_TRUE(stream.Fill(v, sink.writer, sink.layout));
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    ASSERT_EQ(1, batch.count);
    ASSERT_TRUE(batch.dense_pos);
    const auto& t = batch.terms[0];
    ASSERT_EQ(v, std::string_view(t.GetData(), t.GetSize()));
    ASSERT_EQ(0, batch.offs_start[0]);
    ASSERT_EQ(v.size(), batch.offs_end[0]);
  }
}

TEST(string_token_stream_tests, native_column_fill) {
  irs::StringTokenizer stream;
  ASSERT_TRUE(stream.Traits().unique);
  ASSERT_TRUE(stream.Traits().keyword);

  const std::string long_value(64, 'x');
  const duckdb::string_t values[] = {
    duckdb::string_t{"short", 5},
    duckdb::string_t{long_value.data(),
                     static_cast<uint32_t>(long_value.size())}};
  const irs::doc_id_t docs[] = {1, 5};

  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  stream.Fill(std::span<const duckdb::string_t>{values},
              std::span<const irs::doc_id_t>{docs}, sink.writer, sink.layout);
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  const auto runs = sink.writer.Runs();
  ASSERT_EQ(2, runs.size());
  for (uint32_t i = 0; i < 2; ++i) {
    ASSERT_EQ(docs[i], runs[i].doc);
    ASSERT_EQ(1, runs[i].ntokens);
  }
  ASSERT_EQ(2, batch.count);
  ASSERT_TRUE(batch.dense_pos);
  for (uint32_t i = 0; i < 2; ++i) {
    const auto& t = batch.terms[i];
    ASSERT_EQ(std::string_view(values[i].GetData(), values[i].GetSize()),
              std::string_view(t.GetData(), t.GetSize()));
    ASSERT_EQ(0, batch.offs_start[i]);
    ASSERT_EQ(values[i].GetSize(), batch.offs_end[i]);
  }
}

TEST(string_token_stream_tests, column_suspension) {
  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  constexpr size_t kTotal = kCap + 7;

  std::vector<std::string> storage(kTotal);
  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs(kTotal);
  for (size_t i = 0; i < kTotal; ++i) {
    storage[i] = "v" + std::to_string(i);
    vals.emplace_back(storage[i].data(),
                      static_cast<uint32_t>(storage[i].size()));
    docs[i] = static_cast<irs::doc_id_t>(i + 1);
  }

  irs::StringTokenizer stream;
  size_t flushes = 0;
  size_t consumed = 0;
  const auto on_flush = [&](irs::TokenBatch& batch,
                            std::span<const irs::DocRun> runs) {
    ASSERT_EQ(batch.count, runs.size());
    for (uint32_t i = 0; i < batch.count; ++i) {
      ASSERT_EQ(consumed + i + 1, runs[i].doc);
      ASSERT_EQ(1, runs[i].ntokens);
    }
    if (batch.count == kCap) {
      ++flushes;
    } else {
      ASSERT_EQ(7, batch.count);
    }
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      ASSERT_EQ(storage[consumed + i],
                std::string_view(t.GetData(), t.GetSize()));
    }
    consumed += batch.count;
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, on_flush};
  stream.Fill(vals, docs, sink.writer, sink.layout);
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
  ASSERT_EQ(kTotal, consumed);
}

namespace {

class BulkMockTokenizer final
  : public irs::analysis::TypedTokenizer<BulkMockTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "bulk_mock_tokenizer";
  }

  irs::TokenTraits Traits() const noexcept final {
    return {.dense_pos = false};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view value, irs::TokenEmitter& sink) {
    size_t total = 0;
    const size_t want = value.size();
    while (total < want) {
      const auto slots = sink.Next(want - total);
      EXPECT_GT(slots.size(), 0);
      EXPECT_LE(slots.size(), want - total);
      for (auto& slot : slots) {
        slot = duckdb::string_t{"x", 1};
      }
      total += slots.size();
    }
    return true;
  }
};

}  // namespace

TEST(token_sink_tests, next_bulk_and_flag_preservation) {
  constexpr auto kCap = irs::TokenBatch::kCapacity;
  size_t flushes = 0;
  const auto on_flush = [&](irs::TokenBatch& batch,
                            std::span<const irs::DocRun> /*runs*/) {
    ++flushes;
    ASSERT_EQ(kCap, batch.count);
    ASSERT_FALSE(batch.dense_pos);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, on_flush};
  auto& w = sink.writer;
  auto& batch = w.buf;

  BulkMockTokenizer stream;
  const std::string bulk_value(kCap + 100, 'v');
  ASSERT_TRUE(stream.Fill(bulk_value, w, sink.layout));
  ASSERT_EQ(1, flushes);
  ASSERT_EQ(100, batch.count);
  ASSERT_FALSE(batch.dense_pos);

  const std::string exact_refill(kCap - 100 + 1, 'v');
  ASSERT_TRUE(stream.Fill(exact_refill, w, sink.layout));
  ASSERT_EQ(2, flushes);
  ASSERT_EQ(1, batch.count);
  ASSERT_FALSE(batch.dense_pos);
  w.Discard();
}

TEST(token_sink_tests, intern) {
  tests::OneBatchSink one_batch{irs::TokenLayout::Terms};
  auto& sink = one_batch.writer;

  std::string small = "short";
  std::string exact(duckdb::string_t::INLINE_LENGTH, 'a');
  std::string big(duckdb::string_t::INLINE_LENGTH + 1, 'b');
  std::string huge(100, 'c');

  const auto view = [](const std::string& s) {
    return irs::bytes_view{reinterpret_cast<const irs::byte_type*>(s.data()),
                           s.size()};
  };
  const auto t_small = sink.Intern(view(small));
  const auto t_exact = sink.Intern(view(exact));
  const auto t_big = sink.Intern(view(big));
  const auto t_huge = sink.Intern(view(huge));

  const std::string small_copy = small;
  const std::string exact_copy = exact;
  const std::string big_copy = big;
  const std::string huge_copy = huge;
  small.assign(small.size(), '!');
  exact.assign(exact.size(), '!');
  big.assign(big.size(), '!');
  huge.assign(huge.size(), '!');

  ASSERT_EQ(small_copy, std::string_view(t_small.GetData(), t_small.GetSize()));
  ASSERT_EQ(exact_copy, std::string_view(t_exact.GetData(), t_exact.GetSize()));
  ASSERT_EQ(big_copy, std::string_view(t_big.GetData(), t_big.GetSize()));
  ASSERT_EQ(huge_copy, std::string_view(t_huge.GetData(), t_huge.GetSize()));
}

TEST(token_sink_tests, value_run_sink) {
  constexpr auto kCap = irs::TokenBatch::kCapacity;
  std::vector<std::vector<irs::DocRun>> flushed_runs;
  const auto on_flush = [&](irs::TokenBatch& /*batch*/,
                            std::span<const irs::DocRun> runs) {
    flushed_runs.emplace_back(runs.begin(), runs.end());
  };
  tests::FnTokenSink root{irs::TokenLayout::Terms, on_flush};
  auto& w = root.writer;
  auto& batch = w.buf;

  w.BeginValue(7);
  for (int i = 0; i < 3; ++i) {
    batch.terms[w.Next()] = duckdb::string_t{"a", 1};
  }
  w.EndValue();
  ASSERT_TRUE(flushed_runs.empty());
  ASSERT_EQ(1, w.Runs().size());
  ASSERT_EQ(7, w.Runs()[0].doc);
  ASSERT_EQ(3, w.Runs()[0].ntokens);

  w.BeginValue(8);
  for (size_t i = 0; i < kCap + 5; ++i) {
    batch.terms[w.Next()] = duckdb::string_t{"b", 1};
  }
  w.EndValue();
  ASSERT_EQ(1, flushed_runs.size());
  ASSERT_EQ(3, flushed_runs[0].size());
  ASSERT_EQ(7, flushed_runs[0][0].doc);
  ASSERT_EQ(8, flushed_runs[0][1].doc);
  ASSERT_EQ(kCap - 3, flushed_runs[0][1].ntokens);
  ASSERT_EQ(irs::DocRun::kOpenValue, flushed_runs[0][2].doc);
  ASSERT_EQ(1, w.Runs().size());
  ASSERT_EQ(8, w.Runs()[0].doc);
  ASSERT_EQ(8, w.Runs()[0].ntokens);

  w.BeginValue(8);
  while (batch.count < kCap) {
    batch.terms[w.Next()] = duckdb::string_t{"c", 1};
  }
  w.EndValue();
  w.BeginValue(9);
  batch.terms[w.Next()] = duckdb::string_t{"d", 1};
  w.EndValue();
  ASSERT_EQ(2, flushed_runs.size());
  for (const auto& run : flushed_runs[1]) {
    ASSERT_NE(9, run.doc);
    ASSERT_NE(irs::DocRun::kOpenValue, run.doc);
  }
  ASSERT_EQ(1, w.Runs().size());
  ASSERT_EQ(9, w.Runs()[0].doc);
  ASSERT_EQ(1, w.Runs()[0].ntokens);

  w.BeginValue(10);
  w.EndValue();
  ASSERT_EQ(2, w.Runs().size());
  ASSERT_EQ(10, w.Runs()[1].doc);
  ASSERT_EQ(0, w.Runs()[1].ntokens);
  w.Discard();
}

TEST(token_sink_tests, term_vector_sink) {
  irs::analysis::DelimitedTokenizer stream{","};
  std::string csv;
  for (size_t i = 0; i < 1500; ++i) {
    csv += "t" + std::to_string(i) + ",";
  }
  csv.pop_back();

  std::vector<irs::bstring> out;
  irs::TermVectorSink sink{out};
  ASSERT_TRUE(stream.Fill(csv, sink.writer, irs::TokenLayout::Terms));
  sink.writer.Finish();

  ASSERT_EQ(1500, out.size());
  ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"t0"}), out[0]);
  ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"t1499"}),
            out[1499]);
}

TEST(token_sink_tests, list_vector_sink) {
  irs::analysis::DelimitedTokenizer stream{","};
  std::string csv;
  for (size_t i = 0; i < 1500; ++i) {
    csv += "v" + std::to_string(i) + ",";
  }
  csv.pop_back();

  duckdb::Vector list{duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
                      2};
  irs::ListVectorSink sink{list, 0};
  ASSERT_TRUE(stream.Fill(csv, sink.writer, irs::TokenLayout::Terms));
  sink.writer.Finish();
  ASSERT_EQ(1500, sink.offset());

  irs::ListVectorSink sink2{list, sink.offset()};
  ASSERT_TRUE(stream.Fill("a,b", sink2.writer, irs::TokenLayout::Terms));
  sink2.writer.Finish();
  ASSERT_EQ(1502, sink2.offset());

  duckdb::ListVector::SetListSize(list, sink2.offset());
  auto& child = duckdb::ListVector::GetEntry(list);
  const auto* data = duckdb::FlatVector::GetData<duckdb::string_t>(child);
  ASSERT_EQ("v0", std::string_view(data[0].GetData(), data[0].GetSize()));
  ASSERT_EQ("v1499",
            std::string_view(data[1499].GetData(), data[1499].GetSize()));
  ASSERT_EQ("a", std::string_view(data[1500].GetData(), data[1500].GetSize()));
  ASSERT_EQ("b", std::string_view(data[1501].GetData(), data[1501].GetSize()));
}

namespace {

class OneTokenMockTokenizer final
  : public irs::analysis::TypedTokenizer<OneTokenMockTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "one_token_mock_tokenizer";
  }

  irs::TokenTraits Traits() const noexcept final {
    return {.unique = true};
  }

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view value, irs::TokenEmitter& sink) {
    if (value == "!") {
      sink.buf.unique = false;
      return false;
    }
    const auto size = static_cast<uint32_t>(value.size());
    sink.Emit<Layout>(duckdb::string_t{value.data(), size}, 0, size);
    return true;
  }
};

}  // namespace

TEST(token_sink_tests, unique_hint) {
  OneTokenMockTokenizer stream;
  ASSERT_TRUE(stream.Traits().unique);
  ASSERT_TRUE(irs::StringTokenizer{}.Traits().unique);
  ASSERT_FALSE(irs::analysis::EmptyTokenizer{}.Traits().unique);

  const std::string a{"aaa"};
  const std::string b{"bbb"};
  const std::string bad{"!"};

  {
    const duckdb::string_t vals[] = {{a.data(), 3}, {b.data(), 3}};
    const irs::doc_id_t docs[] = {1, 2};
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    stream.Fill(vals, docs, sink.writer, sink.layout);
    ASSERT_FALSE(sink.flushed());
    ASSERT_TRUE(sink.writer.buf.unique);
    ASSERT_EQ(sink.writer.buf.count, sink.writer.Runs().size());
  }

  {
    const duckdb::string_t vals[] = {
      {a.data(), 3}, {bad.data(), 1}, {b.data(), 3}};
    const irs::doc_id_t docs[] = {1, 2, 3};
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    stream.Fill(vals, docs, sink.writer, sink.layout);
    ASSERT_FALSE(sink.writer.buf.unique);
    ASSERT_EQ(3, sink.writer.Runs().size());
    ASSERT_EQ(2, sink.writer.buf.count);
  }

  {
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    ASSERT_TRUE(stream.Fill(a, sink.writer, sink.layout));
    ASSERT_FALSE(sink.writer.buf.unique);
    ASSERT_TRUE(sink.writer.Runs().empty());
  }

  {
    irs::analysis::EmptyTokenizer empty;
    const duckdb::string_t vals[] = {{a.data(), 3}};
    const irs::doc_id_t docs[] = {1};
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    empty.Fill(vals, docs, sink.writer, sink.layout);
    ASSERT_FALSE(sink.writer.buf.unique);
  }
}

TEST(token_sink_tests, run_capacity_forced_cycle) {
  constexpr auto kCap = irs::TokenBatch::kCapacity;
  std::vector<std::vector<irs::DocRun>> flushed;
  std::vector<uint32_t> flushed_counts;
  const auto on_flush = [&](irs::TokenBatch& batch,
                            std::span<const irs::DocRun> runs) {
    flushed.emplace_back(runs.begin(), runs.end());
    flushed_counts.push_back(batch.count);
  };
  tests::FnTokenSink root{irs::TokenLayout::Terms, on_flush};
  auto& w = root.writer;

  for (uint32_t d = 1; d <= kCap; ++d) {
    w.BeginValue(d);
    if ((d % 2) == 0) {
      w.buf.terms[w.Next()] = duckdb::string_t{"x", 1};
    }
    w.EndValue();
  }
  ASSERT_EQ(1, flushed.size());
  ASSERT_EQ(kCap, flushed[0].size());
  ASSERT_EQ(kCap / 2, flushed_counts[0]);
  for (uint32_t d = 1; d <= kCap; ++d) {
    ASSERT_EQ(d, flushed[0][d - 1].doc);
    ASSERT_EQ((d % 2) == 0 ? 1 : 0, flushed[0][d - 1].ntokens);
  }
  ASSERT_EQ(0, w.Runs().size());
  ASSERT_EQ(0, w.buf.count);

  w.BeginValue(kCap + 1);
  w.buf.terms[w.Next()] = duckdb::string_t{"y", 1};
  w.EndValue();
  w.Finish();
  ASSERT_EQ(2, flushed.size());
  ASSERT_EQ(1, flushed[1].size());
  ASSERT_EQ(kCap + 1, flushed[1][0].doc);
  ASSERT_EQ(1, flushed[1][0].ntokens);
}

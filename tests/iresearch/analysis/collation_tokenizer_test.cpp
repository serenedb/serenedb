////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include <collation_collator.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <span>
#include <string>
#include <text_locale.hpp>
#include <vector>

#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

inline irs::analysis::Tokenizer::ptr MakeCollation(
  std::string_view locale_name) {
  return irs::analysis::CollationTokenizer::Make(
    irs::analysis::CollationTokenizer::Options{
      .locale = duckdb::text::Locale::FromName(locale_name),
    });
}

irs::bstring ReferenceKey(std::string_view locale_name, std::string_view data) {
  std::string collation;
  duckdb::text::Locale::FromName(locale_name).GetCollation(collation);
  const duckdb::collation::Collator collator{collation};
  duckdb::collation::CollationBuffer buffer;
  collator.GetSortKey(data.data(), data.size(), buffer);
  return irs::bstring{buffer.key.data(), buffer.key.size() - 1};
}

irs::bstring BlockTerm(irs::analysis::Tokenizer& stream,
                       std::string_view data) {
  irs::bstring out;
  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch, irs::DocRuns) {
    ++flushes;
    EXPECT_EQ(1, batch.count);
    EXPECT_EQ(0, batch.offs_start[0]);
    EXPECT_EQ(data.size(), batch.offs_end[0]);
    const auto& t = batch.terms[0];
    out.assign(reinterpret_cast<const irs::byte_type*>(t.GetData()),
               t.GetSize());
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
  EXPECT_TRUE(stream.Fill(tests::ToStringT(data), irs::doc_limits::min(),
                          sink.writer, {sink.layout}));
  sink.writer.Finish();
  EXPECT_EQ(1, flushes);
  return out;
}

}  // namespace

TEST(collation_token_stream_test, consts) {
  static_assert("collate_tokens" ==
                irs::Type<irs::analysis::CollationTokenizer>::name());
}

TEST(collation_token_stream_test, empty_analyzer) {
  ASSERT_THROW(irs::analysis::CollationTokenizer{{}}, std::exception);
}

std::vector<irs::bstring> ColumnTerms(irs::analysis::Tokenizer& stream,
                                      const std::vector<std::string>& values) {
  std::vector<duckdb::string_t> vals;
  vals.reserve(values.size());
  for (const auto& v : values) {
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
  }
  std::vector<irs::bstring> out(values.size());
  const auto collect = [&](irs::TokenBatch& batch,
                           std::span<const irs::DocRun> runs) {
    uint32_t tok = 0;
    for (const auto& run : runs) {
      EXPECT_EQ(1, run.ntokens);
      const auto& t = batch.terms[tok++];
      out[run.doc - 1].assign(
        reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, collect};
  tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();
  return out;
}

TEST(collation_token_stream_test, ascii_block_matches_unicode_path) {
  auto stream = MakeCollation("en");
  const std::vector<std::string> ascii{"Running12", "fox", "Z z a"};
  const std::vector<std::string> mixed{"Running12", "å b z a", "fox",
                                       "\xD0\xBC\xD0\xB8\xD1\x80"};
  for (const auto& values : {ascii, mixed}) {
    const auto column = ColumnTerms(*stream, values);
    for (size_t i = 0; i < values.size(); ++i) {
      SCOPED_TRACE(values[i]);
      ASSERT_EQ(BlockTerm(*stream, values[i]), column[i]);
    }
  }
  ASSERT_EQ(ColumnTerms(*stream, {"Running12"})[0],
            ColumnTerms(*stream, {"Running12", "å"})[0]);
}

TEST(collation_token_stream_test, construct_from_str) {
  for (auto locale_name :
       {"ru.koi8.r", "en-US", "en-US.utf-8", "de_DE_phonebook", "C",
        "de_DE.utf-8@phonebook", "de_DE.UTF-8@collation=phonebook"}) {
    auto stream = MakeCollation(locale_name);
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  ASSERT_ANY_THROW(irs::analysis::CollationTokenizer::Make(
    irs::analysis::CollationTokenizer::Options{}));
}

TEST(collation_token_stream_test, check_collation) {
  {
    auto stream = MakeCollation("en");
    ASSERT_NE(nullptr, stream);
    constexpr std::string_view kData{"å b z a"};
    ASSERT_EQ(ReferenceKey("en", kData), BlockTerm(*stream, kData));
  }
  {
    auto stream = MakeCollation("sv");
    ASSERT_NE(nullptr, stream);
    constexpr std::string_view kData{"a å b z"};
    ASSERT_EQ(ReferenceKey("sv", kData), BlockTerm(*stream, kData));
    ASSERT_NE(ReferenceKey("en", kData), BlockTerm(*stream, kData));
  }
  {
    auto sv = MakeCollation("sv");
    auto en = MakeCollation("en");
    ASSERT_LT(BlockTerm(*sv, "z"), BlockTerm(*sv, "å"));
    ASSERT_LT(BlockTerm(*en, "å"), BlockTerm(*en, "z"));
  }
}

TEST(collation_token_stream_test, locales_resolve_to_collations) {
  const std::pair<std::string_view, std::string_view> kExpected[] = {
    {"en", ""},
    {"en_US.UTF-8", ""},
    {"de_DE", ""},
    {"sv_SE", "sv"},
    {"zh", "zh"},
    {"zh_TW", "zh_tw"},
    {"zh_Hant_TW", "zh_tw"},
    {"zh_Hans_CN", "zh_cn"},
    {"sr_BA", "sr_ba"},
  };
  for (const auto& [name, expected] : kExpected) {
    SCOPED_TRACE(name);
    std::string collation;
    ASSERT_TRUE(duckdb::text::Locale::FromName(name).GetCollation(collation));
    ASSERT_EQ(expected, collation);
  }
}

TEST(collation_token_stream_test, unsupported_collation_types) {
  constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
  for (auto name : {"de__phonebook", "de_phonebook", "de@collation=phonebook",
                    "de@collation=pinyan", "es__traditional", "sr_Latn"}) {
    SCOPED_TRACE(name);
    std::string collation;
    ASSERT_FALSE(duckdb::text::Locale::FromName(name).GetCollation(collation));
    auto stream = MakeCollation(name);
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(ReferenceKey("", kData), BlockTerm(*stream, kData));
  }
}

TEST(collation_token_stream_test, check_tokens_utf8) {
  auto stream = MakeCollation("en");
  ASSERT_NE(nullptr, stream);
  for (std::string_view data :
       {std::string_view{}, std::string_view{""}, std::string_view{"quick"},
        std::string_view{"foo"},
        std::string_view{"the quick Brown fox jumps over the lazy dog"}}) {
    SCOPED_TRACE(data);
    ASSERT_EQ(ReferenceKey("en-EN.UTF-8", data), BlockTerm(*stream, data));
  }
}

TEST(collation_token_stream_test, check_tokens) {
  auto stream = MakeCollation("de_DE");
  ASSERT_NE(nullptr, stream);
  const std::string unicode_data = "\xE2\x82\xAC";
  ASSERT_EQ(ReferenceKey("de-DE", unicode_data),
            BlockTerm(*stream, unicode_data));
}

TEST(collation_token_stream_test, native_fills_match_pull) {
  auto analyzer = MakeCollation("de");
  ASSERT_NE(nullptr, analyzer);
  auto& stream = *analyzer;

  ASSERT_TRUE(stream.Traits().unique);
  ASSERT_FALSE(stream.Traits().keyword);
  ASSERT_EQ(duckdb::LogicalTypeId::BLOB, stream.Traits().output);

  const std::vector<std::string> values = {
    "\xc3\x84pfel", "Apfel", "Zebra",
    "a-collated-value-considerably-longer-than-inline-storage"};

  std::vector<irs::bstring> expected;
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    const auto tokens = tests::Analyze(stream, v);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    ASSERT_EQ(1, (*tokens)[0].pos);
    ASSERT_EQ(0, (*tokens)[0].offs_start);
    ASSERT_EQ(v.size(), (*tokens)[0].offs_end);
    expected.emplace_back(
      reinterpret_cast<const irs::byte_type*>((*tokens)[0].term.data()),
      (*tokens)[0].term.size());
  }

  for (size_t i = 0; i < values.size(); ++i) {
    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns) {
      ++flushes;
      ASSERT_EQ(1, batch.count);
      const auto& t = batch.terms[0];
      ASSERT_EQ(
        expected[i],
        irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                     t.GetSize()));
      ASSERT_EQ(0, batch.offs_start[0]);
      ASSERT_EQ(values[i].size(), batch.offs_end[0]);
    };
    tests::FnTokenSink sink{irs::TokenLayout::TermsPosOffs, check};
    ASSERT_TRUE(stream.Fill(values[i], irs::doc_limits::min(), sink.writer,
                            {sink.layout}));
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }

  {
    irs::ValueAnalyzer analyzer;
    irs::ValueTokens tokens;
    ASSERT_TRUE(analyzer.Analyze(stream, values[0], tokens));
    ASSERT_EQ(1, tokens.terms().size());
    ASSERT_EQ(expected[0], irs::AsBytesView(tokens.terms()[0]));
  }

  {
    std::vector<duckdb::string_t> vals;
    for (size_t i = 0; i < values.size(); ++i) {
      vals.emplace_back(values[i].data(),
                        static_cast<uint32_t>(values[i].size()));
    }
    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
      ++flushes;
      ASSERT_EQ(values.size(), runs.size());
      for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(i + 1, runs[i].doc);
        ASSERT_EQ(1, runs[i].ntokens);
      }
      ASSERT_EQ(values.size(), batch.count);
      for (size_t i = 0; i < values.size(); ++i) {
        const auto& t = batch.terms[i];
        ASSERT_EQ(
          expected[i],
          irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                       t.GetSize()));
      }
    };
    tests::FnTokenSink sink{irs::TokenLayout::Terms, check};
    tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }
}

TEST(collation_token_stream_test, column_suspension) {
  auto analyzer = MakeCollation("de");
  ASSERT_NE(nullptr, analyzer);
  auto& stream = *analyzer;

  const std::vector<std::string> inputs = {
    "\xc3\x84pfel", "a-collated-value-considerably-longer-than-inline-storage"};
  std::vector<irs::bstring> collated;
  for (const auto& v : inputs) {
    const duckdb::string_t one{v.data(), static_cast<uint32_t>(v.size())};
    const irs::doc_id_t doc = 1;
    size_t flushes = 0;
    const auto check = [&](irs::TokenBatch& batch, irs::DocRuns) {
      ++flushes;
      ASSERT_EQ(1, batch.count);
      const auto& t = batch.terms[0];
      collated.emplace_back(
        reinterpret_cast<const irs::byte_type*>(t.GetData()), t.GetSize());
    };
    tests::FnTokenSink sink{irs::TokenLayout::Terms, check};
    tests::FillColumn(stream, {&one, 1}, doc, sink.writer, sink.layout);
    sink.writer.Finish();
    ASSERT_EQ(1, flushes);
  }

  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  constexpr size_t kTotal = kCap + 3;
  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < kTotal; ++i) {
    const auto& v = inputs[i % inputs.size()];
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
  }

  size_t consumed = 0;
  size_t flushes = 0;
  const auto verify = [&](irs::TokenBatch& batch,
                          std::span<const irs::DocRun> runs) {
    ASSERT_EQ(batch.count, runs.size());
    for (uint32_t i = 0; i < batch.count; ++i) {
      ASSERT_EQ(consumed + i + 1, runs[i].doc);
      ASSERT_EQ(1, runs[i].ntokens);
    }
    if (batch.count == kCap) {
      ++flushes;
    } else {
      ASSERT_EQ(3, batch.count);
    }
    for (uint32_t i = 0; i < batch.count; ++i, ++consumed) {
      const auto& t = batch.terms[i];
      ASSERT_EQ(
        collated[consumed % inputs.size()],
        irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                     t.GetSize()));
    }
  };
  tests::FnTokenSink sink{irs::TokenLayout::Terms, verify};
  tests::FillColumn(stream, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
  ASSERT_EQ(kTotal, consumed);
}

TEST(collation_token_stream_test, memory_usage_accounts_scratch) {
  auto stream = MakeCollation("en");
  ASSERT_NE(nullptr, stream);
  EXPECT_EQ(0, stream->MemoryUsage());
  ASSERT_FALSE(BlockTerm(*stream, "quick brown fox").empty());
  EXPECT_GT(stream->MemoryUsage(), 0);
}

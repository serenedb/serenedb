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

#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/sortkey.h>

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/batch/token_sinks.hpp"
#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/collation_tokenizer_encoder.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

inline irs::analysis::Tokenizer::ptr MakeCollation(
  std::string_view locale_name) {
  return irs::analysis::CollationTokenizer::Make(
    irs::analysis::CollationTokenizer::Options{
      .locale = icu::Locale::createFromName(locale_name.data()),
    });
}

irs::bstring BlockTerm(irs::analysis::Tokenizer& stream,
                       std::string_view data) {
  tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
  EXPECT_TRUE(stream.Fill(data, sink.writer, sink.layout));
  EXPECT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  EXPECT_EQ(1, batch.count);
  EXPECT_EQ(0, batch.offs_start[0]);
  EXPECT_EQ(data.size(), batch.offs_end[0]);
  const auto& t = batch.terms[0];
  return irs::bstring{reinterpret_cast<const irs::byte_type*>(t.GetData()),
                      t.GetSize()};
}

std::vector<uint8_t> Encode(uint8_t b) {
  std::vector<uint8_t> res;
  if (0x80 & b) {
    res.push_back(0xc0 | (b >> 3));
    res.push_back(0x80 | (b & 0x7));
  } else {
    res.push_back(b);
  }
  return res;
}

class CollationEncoder {
 public:
  void Encode(irs::bytes_view src) {
    _buffer.clear();
    _buffer.reserve(src.size() * 2);
    for (auto b : src) {
      auto enc = ::Encode(b);
      _buffer.insert(_buffer.end(), enc.begin(), enc.end());
    }
  }

  irs::bytes_view GetByteArray() noexcept {
    return {_buffer.data(), _buffer.size()};
  }

 private:
  std::vector<uint8_t> _buffer;
};

}  // namespace

TEST(collation_token_stream_test, consts) {
  static_assert("collation" ==
                irs::Type<irs::analysis::CollationTokenizer>::name());
}

TEST(collation_token_stream_test, empty_analyzer) {
  irs::analysis::CollationTokenizer stream{{}};
  ASSERT_FALSE(tests::Analyze(stream, "quick").has_value());
}

TEST(collation_token_stream_test, test_byte_encoder) {
  uint8_t target{0x0};
  ASSERT_EQ(256, irs::kRecalcMap.size());
  do {
    --target;
    const auto expected = Encode(target);
    const auto actual = irs::kRecalcMap[target];
    ASSERT_EQ(expected.size(), actual.second);
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(expected[i], irs::kBytesRecalcMap[actual.first + i]);
    }
  } while (target != 0);
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

  // .........................................................................
  // invalid -- JSON-parser cases ("{}", "[]", "1", `{"locale":1}`) collapse to
  // a single direct-API case: missing locale yields a default Options whose
  // `locale` is bogus, which `Make` must reject.
  // .........................................................................
  ASSERT_ANY_THROW(irs::analysis::CollationTokenizer::Make(
    irs::analysis::CollationTokenizer::Options{}));
}

TEST(collation_token_stream_test, check_collation) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view kLocaleName = R"(en)";
  const icu::Locale icu_locale =
    icu::Locale::createFromName(kLocaleName.data());

  CollationEncoder encoded_key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    icu::CollationKey key;
    coll->getCollationKey(icu::UnicodeString::fromUTF8(icu::StringPiece{
                            data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encoded_key.Encode({p, static_cast<size_t>(size - 1)});
    return encoded_key.GetByteArray();
  };

  {
    auto stream = MakeCollation("en");
    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"å b z a"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  {
    auto stream = MakeCollation("sv");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"a å b z"};
      ASSERT_NE(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }
}

// collation defined by keywords
TEST(collation_token_stream_test, check_collation_with_variant1) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view kLocaleName = R"(de@collation=phonebook)";
  const icu::Locale icu_locale =
    icu::Locale::createFromName(kLocaleName.data());

  CollationEncoder encoded_key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    icu::CollationKey key;
    coll->getCollationKey(icu::UnicodeString::fromUTF8(icu::StringPiece{
                            data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encoded_key.Encode({p, static_cast<size_t>(size - 1)});
    return encoded_key.GetByteArray();
  };

  // locale defined as object
  // different variants, should be NOT EQUAL
  {
    auto stream = MakeCollation("de__pinyin");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_NE(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = MakeCollation("de_pinyan");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_NE(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = MakeCollation("de@pinyan");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_NE(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = MakeCollation("de@collation=pinyan");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_NE(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = MakeCollation("de__phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = MakeCollation("de_phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = MakeCollation("de@collation=phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  {
    auto stream = MakeCollation("de@collation=phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as object
  // same variants, but collation is ignored, because input as old string.
  // should be NOT EQUAL
  {
    auto stream = MakeCollation("de_phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }
}

TEST(collation_token_stream_test, check_collation_with_variant2) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view kLocaleName = "de_phonebook";
  const icu::Locale icu_locale =
    icu::Locale::createFromName(kLocaleName.data());

  CollationEncoder encoded_key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    icu::CollationKey key;
    coll->getCollationKey(icu::UnicodeString::fromUTF8(icu::StringPiece{
                            data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encoded_key.Encode({p, static_cast<size_t>(size - 1)});
    return encoded_key.GetByteArray();
  };

  // locale defined as object
  // different variants, should be NOT EQUAL
  {
    auto stream = MakeCollation("de__pinyan");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_NE(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = MakeCollation("de__phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = MakeCollation("de@collation=phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  // locale defined as string
  // same variants, should be EQUAL
  {
    auto stream = MakeCollation("de_phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }

  {
    auto stream = MakeCollation("de@collation=phonebook");

    ASSERT_NE(nullptr, stream);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }
}

TEST(collation_token_stream_test, check_tokens_utf8) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view kLocaleName = "en-EN.UTF-8";

  const auto icu_locale = icu::Locale::createFromName(kLocaleName.data());

  CollationEncoder encoded_key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    icu::CollationKey key;
    coll->getCollationKey(icu::UnicodeString::fromUTF8(icu::StringPiece{
                            data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encoded_key.Encode({p, static_cast<size_t>(size - 1)});
    return encoded_key.GetByteArray();
  };

  {
    auto stream = MakeCollation("en");

    ASSERT_NE(nullptr, stream);

    {
      const std::string_view data{};
      ASSERT_EQ(irs::bstring{get_collation_key(data)},
                BlockTerm(*stream, data));
    }

    {
      const std::string_view data{""};
      ASSERT_EQ(irs::bstring{get_collation_key(data)},
                BlockTerm(*stream, data));
    }

    {
      constexpr std::string_view kData{"quick"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }

    {
      constexpr std::string_view kData{"foo"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }

    {
      constexpr std::string_view kData{
        "the quick Brown fox jumps over the lazy dog"};
      ASSERT_EQ(irs::bstring{get_collation_key(kData)},
                BlockTerm(*stream, kData));
    }
  }
}

TEST(collation_token_stream_test, check_tokens) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view kLocaleName = "de-DE";

  const auto icu_locale = icu::Locale::createFromName(kLocaleName.data());

  CollationEncoder encoded_key;
  std::unique_ptr<icu::Collator> coll{
    icu::Collator::createInstance(icu_locale, err)};

  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    icu::CollationKey key;
    err = UErrorCode::U_ZERO_ERROR;
    coll->getCollationKey(icu::UnicodeString::fromUTF8(icu::StringPiece{
                            data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encoded_key.Encode({p, static_cast<size_t>(size - 1)});
    return encoded_key.GetByteArray();
  };

  {
    auto stream = MakeCollation("de_DE");

    ASSERT_NE(nullptr, stream);

    {
      std::string unicode_data = "\xE2\x82\xAC";

      ASSERT_EQ(irs::bstring{get_collation_key(unicode_data)},
                BlockTerm(*stream, unicode_data));
    }
  }
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
    tests::OneBatchSink sink{irs::TokenLayout::TermsPosOffs};
    ASSERT_TRUE(stream.Fill(values[i], sink.writer, sink.layout));
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    ASSERT_EQ(1, batch.count);
    ASSERT_TRUE(sink.writer.dense_pos);
    const auto& t = batch.terms[0];
    ASSERT_EQ(expected[i],
              irs::bstring(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                           t.GetSize()));
    ASSERT_EQ(0, batch.offs_start[0]);
    ASSERT_EQ(values[i].size(), batch.offs_end[0]);
  }

  {
    std::vector<irs::bstring> out;
    irs::TermVectorSink vec_sink{out};
    ASSERT_TRUE(
      stream.Fill(values[0], vec_sink.writer, irs::TokenLayout::Terms));
    vec_sink.writer.Finish();
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(expected[0], out[0]);
  }

  {
    std::vector<duckdb::string_t> vals;
    std::vector<irs::doc_id_t> docs;
    for (size_t i = 0; i < values.size(); ++i) {
      vals.emplace_back(values[i].data(),
                        static_cast<uint32_t>(values[i].size()));
      docs.push_back(static_cast<irs::doc_id_t>(i + 1));
    }
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    stream.Fill(vals, docs, sink.writer, sink.layout);
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    const auto runs = sink.writer.Runs();
    ASSERT_EQ(values.size(), runs.size());
    for (size_t i = 0; i < values.size(); ++i) {
      ASSERT_EQ(docs[i], runs[i].doc);
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
    tests::OneBatchSink sink{irs::TokenLayout::Terms};
    stream.Fill({&one, 1}, {&doc, 1}, sink.writer, sink.layout);
    ASSERT_FALSE(sink.flushed());
    auto& batch = sink.writer.buf;
    ASSERT_EQ(1, batch.count);
    const auto& t = batch.terms[0];
    collated.emplace_back(reinterpret_cast<const irs::byte_type*>(t.GetData()),
                          t.GetSize());
  }

  constexpr size_t kCap = irs::TokenBatch::kCapacity;
  constexpr size_t kTotal = kCap + 3;
  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs(kTotal);
  for (size_t i = 0; i < kTotal; ++i) {
    const auto& v = inputs[i % inputs.size()];
    vals.emplace_back(v.data(), static_cast<uint32_t>(v.size()));
    docs[i] = static_cast<irs::doc_id_t>(i + 1);
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
  stream.Fill(vals, docs, sink.writer, sink.layout);
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
  ASSERT_EQ(kTotal, consumed);
}

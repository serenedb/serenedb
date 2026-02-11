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
#include <vpack/common.h>
#include <vpack/parser.h>

#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer_encoder.hpp>

#include "tests_shared.hpp"

namespace {

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
  ASSERT_FALSE(stream.next());
}

TEST(collation_token_stream_test, test_byte_encoder) {
  uint8_t target{0x0};
  ASSERT_EQ(256, kRecalcMap.size());
  do {
    --target;
    const auto expected = Encode(target);
    const auto actual = kRecalcMap[target];
    ASSERT_EQ(expected.size(), actual.second);
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(expected[i], kBytesRecalcMap[actual.first + i]);
    }
  } while (target != 0);
}

TEST(collation_token_stream_test, construct_from_str) {
  // json
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "ru.koi8.r"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "en-US"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "en-US.utf-8"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de_DE_phonebook"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "C"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de_DE.utf-8@phonebook"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de_DE.UTF-8@collation=phonebook"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::Type<irs::analysis::CollationTokenizer>::id(),
              stream->type());
  }

  // invalid
  {
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "collation", irs::Type<irs::text_format::Json>::get(),
                         std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "collation", irs::Type<irs::text_format::Json>::get(), "1"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "collation", irs::Type<irs::text_format::Json>::get(), "[]"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::Get(
                "collation", irs::Type<irs::text_format::Json>::get(), "{}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::Get(
                         "collation", irs::Type<irs::text_format::Json>::get(),
                         "{\"locale\":1}"));
  }
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
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "en"})");
    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"å b z a"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "sv"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"a å b z"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_NE(get_collation_key(kData), term->value);

      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
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
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de__pinyin"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_NE(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de_pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_NE(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de@pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_NE(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de@collation=pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_NE(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de__phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de_phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, but collation is ignored, because input as old string.
  // should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de_phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
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
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de__pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_NE(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de__phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale": "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de_phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({"locale" : "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view kData{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
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
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({ "locale" : "en" })");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      const std::string_view data{};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      const std::string_view data{""};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr std::string_view kData{"quick"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr std::string_view kData{"foo"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr std::string_view kData{
        "the quick Brown fox jumps over the lazy dog"};
      ASSERT_TRUE(stream->reset(kData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(kData.size(), offset->end);
      ASSERT_EQ(get_collation_key(kData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
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
    auto stream = irs::analysis::analyzers::Get(
      "collation", irs::Type<irs::text_format::Json>::get(),
      R"({ "locale" : "de_DE" })");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::OffsAttr>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::TermAttr>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::IncAttr>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      std::string unicode_data = "\xE2\x82\xAC";

      ASSERT_TRUE(stream->reset(unicode_data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(unicode_data.size(), offset->end);
      ASSERT_EQ(get_collation_key(unicode_data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, normalize) {
  {
    std::string config = R"({ "locale" : "de_DE_phonebook" })";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "collation", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(R"({ "locale" : "de_DE_PHONEBOOK" })")
                ->toString(),
              actual);
  }

  {
    std::string config = R"({ "locale" : "de_DE.utf-8" })";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "collation", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(vpack::Parser::fromJson(R"({ "locale" : "de_DE"})")->toString(),
              actual);
  }

  {
    std::string config = R"({ "locale" : "de_DE@collation=phonebook" })";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      actual, "collation", irs::Type<irs::text_format::Json>::get(), config));
    ASSERT_EQ(
      vpack::Parser::fromJson(R"({ "locale" : "de_DE@collation=phonebook" })")
        ->toString(),
      actual);
  }

  {
    std::string config = R"({ "locale" : "de_DE@phonebook" })";
    auto in_vpack = vpack::Parser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::Normalize(
      out_str, "collation", irs::Type<irs::text_format::VPack>::get(), in_str));
    vpack::Slice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(
      vpack::Parser::fromJson(R"({ "locale" : "de_DE_PHONEBOOK"})")->toString(),
      out_slice.toString());
  }
}

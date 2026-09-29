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

#include <gtest/gtest.h>

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/utils/serialization.hpp>
#include <iresearch/utils/serializer.hpp>
#include <string>
#include <variant>

#include "catalog/entry/tokenizer.h"

namespace sdb::catalog {
namespace {

using irs::analysis::NGramTokenizer;
using NGramMode = NGramTokenizer::NGramMode;

struct PrevNGramOptions {
  size_t min_gram{0};
  size_t max_gram{0};
  bool preserve_original{true};
  NGramTokenizer::InputType stream_bytes_type{
    NGramTokenizer::InputType::Binary};
  irs::bstring start_marker;
  irs::bstring end_marker;
};

struct NextNGramOptions {
  size_t min_gram{0};
  size_t max_gram{0};
  bool preserve_original{true};
  NGramTokenizer::InputType stream_bytes_type{
    NGramTokenizer::InputType::Binary};
  irs::bstring start_marker;
  irs::bstring end_marker;
  NGramMode ngram_mode{NGramMode::All};
  bool lowercase{false};
};

template<typename NGramOptions>
struct ConfigWith {
  std::variant<irs::KeywordTokenizer::Options,
               irs::analysis::StemmingTokenizer::Options,
               irs::analysis::DelimitedTokenizer::Options,
               irs::analysis::MultiDelimitedTokenizer::Options,
               irs::analysis::PatternTokenizer::Options,
               irs::analysis::PathHierarchyTokenizer::Options, NGramOptions>
    config;
};

template<typename Config>
std::string PackAs(const Config& config) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream, duckdb::VersionStorageOptions()};
  irs::utils::WriteTuple(serializer, config);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

std::string UnpackError(const std::string& bytes) {
  try {
    UnpackTokenizerConfig("dict", bytes);
  } catch (const std::exception& e) {
    return e.what();
  }
  return {};
}

TEST(TokenizerEntry, OptionMissingFromAnOlderReleaseReadsItsDefault) {
  const auto bytes = PackAs(ConfigWith<PrevNGramOptions>{PrevNGramOptions{
    .min_gram = 2,
    .max_gram = 4,
    .preserve_original = false,
    .start_marker = irs::bstring{irs::byte_type{'^'}},
  }});
  const auto config = UnpackTokenizerConfig("dict", bytes);
  const auto& options = std::get<NGramTokenizer::Options>(config.config);
  EXPECT_EQ(options.min_gram, 2u);
  EXPECT_EQ(options.max_gram, 4u);
  EXPECT_FALSE(options.preserve_original);
  EXPECT_EQ(options.start_marker, irs::bstring{irs::byte_type{'^'}});
  EXPECT_EQ(options.ngram_mode, NGramMode::All);
}

TEST(TokenizerEntry, NewerOptionAtItsDefaultIsReadAsIfAbsent) {
  const auto bytes = PackAs(ConfigWith<NextNGramOptions>{NextNGramOptions{
    .min_gram = 2,
    .max_gram = 3,
    .ngram_mode = NGramMode::Prefix,
  }});
  EXPECT_EQ(bytes, PackTokenizerConfig({NGramTokenizer::Options{
                     .min_gram = 2,
                     .max_gram = 3,
                     .ngram_mode = NGramMode::Prefix,
                   }}));
  const auto config = UnpackTokenizerConfig("dict", bytes);
  EXPECT_EQ(std::get<NGramTokenizer::Options>(config.config).ngram_mode,
            NGramMode::Prefix);
}

TEST(TokenizerEntry, NewerOptionInUseIsRefused) {
  const auto message =
    UnpackError(PackAs(ConfigWith<NextNGramOptions>{NextNGramOptions{
      .min_gram = 2,
      .max_gram = 3,
      .lowercase = true,
    }}));
  EXPECT_NE(message.find("cannot read text search dictionary \"dict\""),
            std::string::npos)
    << message;
}

TEST(TokenizerEntry, TrailingBytesAreRefused) {
  auto bytes = PackTokenizerConfig(
    {NGramTokenizer::Options{.min_gram = 2, .max_gram = 3}});
  bytes.push_back('\0');
  EXPECT_FALSE(UnpackError(bytes).empty());
}

}  // namespace
}  // namespace sdb::catalog

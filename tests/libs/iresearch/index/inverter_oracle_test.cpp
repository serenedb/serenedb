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

// Byte-identity oracle: writes a deterministic index through the LEGACY
// ingestion API only (Field + Tokenizer), so the identical source builds and
// runs against both the V1 (FieldsData) and V2 (FieldsInverter) trees.
// Run with SDB_ORACLE_DIR=<path> in each build, then byte-compare the
// directories.

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/store/fs_directory.hpp"
#include "tests_shared.hpp"

namespace {

using namespace irs;

struct ReplayToken {
  std::string term;
  uint32_t inc;
  uint32_t offs_start;
  uint32_t offs_end;
};

class ReplayStream final : public analysis::TypedTokenizer<ReplayStream> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "ReplayStream";
  }

  void Reset(const std::vector<ReplayToken>* tokens) noexcept {
    _tokens = tokens;
  }

  irs::TokenTraits Traits() const noexcept final {
    return {.dense_pos = false};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view, TokenEmitter& sink) {
    uint32_t pos = 0;
    for (const auto& t : *_tokens) {
      pos += t.inc;
      sink.EmitInterned<Layout>(
        bytes_view{reinterpret_cast<const byte_type*>(t.term.data()),
                   t.term.size()},
        pos, t.offs_start, t.offs_end);
    }
    return true;
  }

 private:
  const std::vector<ReplayToken>* _tokens = nullptr;
};

struct OracleField {
  field_id Id() const noexcept { return id; }
  IndexFeatures GetIndexFeatures() const noexcept { return features; }
  analysis::Tokenizer& GetTokens() const noexcept { return *stream; }
  std::string_view Value() const noexcept { return {}; }

  field_id id;
  IndexFeatures features;
  ReplayStream* stream;
};

std::vector<ReplayToken> MakeDocTokens(uint32_t seed, size_t count) {
  std::vector<ReplayToken> tokens;
  tokens.reserve(count);
  uint32_t offs = 0;
  uint64_t state = seed * 2654435761u + 1;
  for (size_t i = 0; i < count; ++i) {
    state = state * 6364136223846793005ull + 1442695040888963407ull;
    const auto rank = static_cast<uint32_t>((state >> 33) % 5000);
    std::string term = "t" + std::to_string(rank * rank % 99991);
    const auto len = static_cast<uint32_t>(term.size());
    const uint32_t inc = (i != 0 && (state >> 21) % 16 == 0) ? 0 : 1;
    tokens.push_back({std::move(term), inc, offs, offs + len});
    offs += len + 1;
  }
  return tokens;
}

TEST(InverterOracleTest, WriteDeterministicIndex) {
  const char* out_dir = std::getenv("SDB_ORACLE_DIR");
  if (out_dir == nullptr) {
    GTEST_SKIP() << "SDB_ORACLE_DIR not set";
  }
  std::filesystem::remove_all(out_dir);
  std::filesystem::create_directories(out_dir);

  auto codec = formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  FSDirectory dir{out_dir};
  auto writer = IndexWriter::Make(dir, codec, kOmCreate,
                                  irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  constexpr field_id kText = 1;
  constexpr field_id kKeyword = 2;
  constexpr field_id kFreqOnly = 3;
  constexpr auto kTextFeatures = IndexFeatures::Freq | IndexFeatures::Pos |
                                 IndexFeatures::Offs | IndexFeatures::Norm;
  constexpr auto kKeywordFeatures = IndexFeatures::Freq | IndexFeatures::Pos;
  constexpr auto kFreqOnlyFeatures = IndexFeatures::Freq;

  ReplayStream text_stream;
  ReplayStream keyword_stream;
  ReplayStream freq_stream;

  constexpr size_t kDocs = 500;
  {
    auto trx = writer->GetBatch();
    for (uint32_t d = 0; d < kDocs; ++d) {
      auto doc = trx.Insert();

      const auto text_tokens = MakeDocTokens(d, 40 + d % 90);
      text_stream.Reset(&text_tokens);
      ASSERT_TRUE(::tests::InsertField(
        doc, OracleField{kText, kTextFeatures, &text_stream}));

      // Second value for the same field on every third doc: multi-value
      // position/offset continuation.
      std::vector<ReplayToken> extra;
      if (d % 3 == 0) {
        extra = MakeDocTokens(d + 100000, 7);
        text_stream.Reset(&extra);
        ASSERT_TRUE(::tests::InsertField(
          doc, OracleField{kText, kTextFeatures, &text_stream}));
      }

      const std::vector<ReplayToken> kw = {
        {"cat" + std::to_string(d % 17), 1, 0, 0}};
      keyword_stream.Reset(&kw);
      ASSERT_TRUE(::tests::InsertField(
        doc, OracleField{kKeyword, kKeywordFeatures, &keyword_stream}));

      const std::vector<ReplayToken> fo = {
        {"bucket" + std::to_string(d % 5), 1, 0, 0},
        {"bucket" + std::to_string((d + 1) % 5), 1, 0, 0}};
      freq_stream.Reset(&fo);
      ASSERT_TRUE(::tests::InsertField(
        doc, OracleField{kFreqOnly, kFreqOnlyFeatures, &freq_stream}));
    }
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());
}

}  // namespace

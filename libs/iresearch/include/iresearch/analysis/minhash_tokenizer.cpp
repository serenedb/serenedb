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

#include "minhash_tokenizer.hpp"

#include <absl/strings/escaping.h>

#include <duckdb/common/vector/flat_vector.hpp>

#include "basics/wyhash.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr uint64_t kHashSeed = 0xdeadbeef;

class SignatureConsumer final : public TokenConsumer {
 public:
  explicit SignatureConsumer(MinHash& minhash) noexcept : _minhash{&minhash} {}

  void Consume(TokenBatch& batch, std::span<const DocRun>) final {
    const auto count = batch.count;
    for (uint32_t i = 0; i < count; ++i) {
      const auto& term = batch.terms[i];
      _minhash->Insert(
        sdb::basics::WyHash(term.GetData(), term.GetSize(), kHashSeed));
    }
  }

 private:
  MinHash* _minhash;
};

}  // namespace

struct MinHashTokenizer::FillScratchT {
  explicit FillScratchT(MinHash& minhash)
    : consumer{minhash}, writer{consumer} {}

  SignatureConsumer consumer;
  TokenWriter writer;
};

void MinHashTokenizer::FillScratchDeleterT::operator()(
  FillScratchT* p) const noexcept {
  delete p;
}

Tokenizer::ptr MinHashTokenizer::Make(Options opts) {
  if (opts.num_hashes == 0) {
    THROW_SQL_ERROR(ERR_MSG("minhash: num_hashes must be positive"));
  }
  Tokenizer::ptr sub;
  if (opts.analyzer) {
    sub = CreateTokenizer(std::move(*opts.analyzer));
  }
  // If `analyzer` is absent the ctor falls back to StringTokenizer.
  return std::make_unique<MinHashTokenizer>(std::move(sub), opts.num_hashes);
}

MinHashTokenizer::MinHashTokenizer(analysis::Tokenizer::ptr analyzer,
                                   uint32_t num_hashes)
  : _analyzer{std::move(analyzer)},
    _num_hashes{num_hashes},
    _minhash{_num_hashes} {
  if (!_analyzer) {
    _analyzer = std::make_unique<StringTokenizer>();
  }
}

template<TokenLayout Layout>
bool MinHashTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (!_scratch) {
    _scratch.reset(new FillScratchT(_minhash));
  }
  _minhash.Clear();
  if (!_analyzer->Fill(value, _scratch->writer, TokenLayout::Terms)) {
    return false;
  }
  _scratch->writer.Finish();
  EmitSignature<Layout>(sink);
  return true;
}

template<TokenLayout Layout>
void MinHashTokenizer::EmitSignature(TokenEmitter& sink) {
  auto& buf = sink.buf;
  auto it = _minhash.begin();
  size_t remaining = static_cast<size_t>(_minhash.end() - it);
  while (remaining != 0) {
    const auto slots = sink.Next(remaining);
    const auto first = static_cast<uint32_t>(slots.data() - buf.terms);
    for (size_t j = 0; j < slots.size(); ++j) {
      const auto value = absl::little_endian::FromHost(*it++);
      char b64[11];
      [[maybe_unused]] const size_t length = absl::Base64EscapeInternal(
        reinterpret_cast<const uint8_t*>(&value), sizeof value, b64, sizeof b64,
        absl::kBase64Chars, false);
      SDB_ASSERT(length == sizeof b64);
      slots[j] = duckdb::string_t{b64, static_cast<uint32_t>(sizeof b64)};
      if constexpr (Layout != TokenLayout::Terms) {
        buf.pos[first + j] = 1;
      }
    }
    remaining -= slots.size();
  }
}

template class TypedTokenizer<MinHashTokenizer>;

}  // namespace irs::analysis

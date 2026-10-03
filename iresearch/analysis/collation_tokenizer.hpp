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

#pragma once

#include <collation_collator.hpp>
#include <tuple>
#include <vector>

#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/utils/locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class CollationTokenizer final : public TypedTokenizer<CollationTokenizer>,
                                 public TypedTokenStage<CollationTokenizer>,
                                 private util::Noncopyable {
 public:
  struct Options {
    using Owner = CollationTokenizer;
    duckdb::text::Locale locale;
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "collate_tokens";
  }

  explicit CollationTokenizer(const Options& options);

  TokenTraits Traits() const noexcept final {
    return {
      .output = duckdb::LogicalTypeId::BLOB,
      .unique = true,
      .offsets = true,
    };
  }

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = true};
  }

  std::tuple<bool> PrepareBatch(BlockTraits traits) const noexcept {
    return {traits.ascii};
  }

  size_t MemoryUsage() const noexcept final {
    return _buffer.text.capacity() * sizeof(uint32_t) +
           _buffer.elements.capacity() * sizeof(uint64_t) +
           _buffer.key.capacity() + _valid.capacity();
  }

  template<TokenLayout Layout, bool Ascii, typename Sink>
  bool DoFill(duckdb::string_t value, Sink& sink);

 private:
  duckdb::collation::Collator _collator;
  duckdb::collation::CollationBuffer _buffer;
  std::vector<uint8_t> _valid;
};

}  // namespace irs::analysis

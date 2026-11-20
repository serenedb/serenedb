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

#include <fsst.h>

#include <array>
#include <cstdint>
#include <duckdb/common/serializer/write_stream.hpp>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/index/term_dict_block_format.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

struct FsstEncoderDelete {
  void operator()(duckdb_fsst_encoder_t* encoder) const noexcept;
};

using FsstEncoder = std::unique_ptr<duckdb_fsst_encoder_t, FsstEncoderDelete>;

inline constexpr uint32_t kNoBase = std::numeric_limits<uint32_t>::max();

using Misfits = std::array<uint32_t, kLenSplits>;

struct VarColumns {
  std::vector<uint32_t> lcps;
  std::vector<uint32_t> lens;
  bstring suffixes;
  std::vector<uint16_t> restarts;
  Misfits misfits{};
  uint32_t base = kNoBase;
  uint32_t max_lcp = 0;
  uint32_t max_len = 0;
};

struct VarEntry {
  bytes_view suffix;
  size_t lcp;
  uint32_t head_skip;
  bool restart;
};

struct BlockBuilder {
  bstring keys;
  std::vector<uint32_t> ends;
  VarColumns columns;
  uint64_t size = 0;
  uint64_t deferred = 0;
  uint32_t deferred_len = 0;
  uint32_t coded = 0;
  uint32_t prefix = 0;
  uint32_t head_skip = 0;
  BlockKind kind = BlockKind::Var;
};

class BlockEncoder {
 public:
  void StartField(const BasicTermReader& reader, uint32_t width) noexcept;
  std::string TakeFsstTable() noexcept;
  void EndField() noexcept;

  bool Empty() const noexcept { return _block.ends.empty(); }
  uint32_t Count() const noexcept {
    return static_cast<uint32_t>(_block.ends.size());
  }
  bytes_view Key(uint32_t entry) const noexcept;
  bool Coded() const noexcept { return _block.kind == BlockKind::VarFsst; }
  void SetPages(uint32_t pages) noexcept { _pages = pages; }

  bool Append(bytes_view prefix, bytes_view term, uint64_t records, bool codes);
  uint64_t Seal(uint64_t records);
  void Serialize(duckdb::WriteStream& out, bytes_view records) const;
  void Reset() noexcept;

 private:
  struct Fsst {
    FsstEncoder encoder;
    std::string table;
    const BasicTermReader* reader = nullptr;
    bool trained = false;
    bool used = false;
  };

  struct Scratch {
    bstring coded;
    VarColumns encoded;
    std::vector<VarEntry> entries;
    std::vector<size_t> lengths;
    std::vector<unsigned char*> starts;
    std::vector<size_t> coded_lengths;
    std::vector<unsigned char*> coded_starts;
  };

  duckdb_fsst_encoder_t* Encoder();
  void TrainFsst();
  bytes_view EncodeSuffix(bytes_view suffix);
  void EncodeEntries(VarColumns& columns, uint32_t from, uint32_t to,
                     const VarEntry* last);
  void EncodeDeferred();
  bool AppendFixedKey(bytes_view key, uint64_t records);
  bool AppendVarKey(bytes_view key, uint64_t records, bool codes);
  bool EncodeBlock(const VarEntry& entry, uint64_t records);
  bool AppendCodedKey(const VarEntry& entry, uint64_t records);
  uint64_t Budget() const noexcept;

  BlockBuilder _block;
  Fsst _fsst;
  Scratch _scratch;
  uint32_t _width = 0;
  uint32_t _pages = 1;
};

}  // namespace irs::term_dict

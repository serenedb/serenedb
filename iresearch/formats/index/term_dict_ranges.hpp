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

#include <array>
#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::term_dict {

class RangeWriter final {
 public:
  static uint64_t Bound(uint64_t docs) noexcept;

  bool Active() const noexcept { return _pw != nullptr; }
  DocTee Tee() noexcept;
  void Begin(PostingsWriter& pw, uint64_t docs, MemoryBudget* budget);
  void Add(bytes_view term, const PostingMeta& meta);
  void Finish(uint64_t terms);
  void Write(bstring& region, size_t prefix,
             std::vector<uint64_t>& counts) const;
  void Clear() noexcept;

 private:
  struct Piece {
    bstring key;
    TrackedVector<doc_id_t> docs;
    PostingMeta meta;
    std::array<byte_type, PostingMeta::kInlineBytes> inline_bytes{};
    uint64_t docs_count = 0;
    uint32_t first = 0;
    bool join = false;
  };

  struct Level {
    bstring starts;
    std::vector<uint32_t> firsts;
    std::vector<uint32_t> ends;
    bstring records;
    Piece group;
    Piece held;
    uint32_t count = 0;
    uint32_t children = 0;
    bool holding = false;
  };

  struct Child {
    bytes_view key;
    const PostingMeta& meta;
    std::span<const doc_id_t> docs;
    uint64_t docs_count;
    uint32_t index;
  };

  static void Keep(Piece& piece, const PostingMeta& meta) noexcept;
  void Push(size_t level, const Child& child);
  void Close(size_t level);
  void Release(size_t level);

  std::array<Level, kMaxRangeLevels> _levels;
  PostingsWriter* _pw = nullptr;
  RecordOutput _record;
  uint64_t _heavy = 0;
  size_t _height = 0;
};

}  // namespace irs::term_dict

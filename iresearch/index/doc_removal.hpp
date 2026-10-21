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

#include <cstdint>
#include <limits>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct SubReader;

struct KeyRange {
  int64_t begin;
  int64_t end;
};

void AppendKeyRanges(std::vector<KeyRange>& ranges,
                     std::span<const int64_t> keys);

void NormalizeKeyRanges(std::vector<KeyRange>& ranges);

struct DocRemoval {
  static constexpr uint32_t kNoSegment = std::numeric_limits<uint32_t>::max();

  struct Row {
    uint32_t segment;
    doc_id_t doc;
    int64_t key;
  };

  static DocRemoval Build(field_id key_column, std::span<const Row> rows,
                          std::vector<KeyRange> ranges = {});

  size_t Find(uint32_t segment) const noexcept;

  std::span<const doc_id_t> Docs(size_t group) const noexcept {
    return {docs.data() + offsets[group], docs.data() + offsets[group + 1]};
  }

  std::span<const int64_t> Keys(size_t group) const noexcept {
    return {keys.data() + offsets[group], keys.data() + offsets[group + 1]};
  }

  field_id key_column = field_limits::invalid();
  std::vector<uint32_t> segments;
  std::vector<uint32_t> offsets;
  std::vector<doc_id_t> docs;
  std::vector<int64_t> keys;
  std::vector<KeyRange> ranges;
};

uint32_t SegmentNumber(std::string_view name) noexcept;

class DocRemovalResolver {
 public:
  void Prepare(std::span<const DocRemoval* const> removals,
               std::span<const SubReader* const> live);

  std::span<const doc_id_t> PositionalDocs(const DocRemoval& removal,
                                           const SubReader& segment) const;

  bool Keyed(const DocRemoval& removal) const noexcept {
    return _keyed.contains(&removal);
  }

  std::vector<DocRange> KeyedDocs(const SubReader& segment) const;

 private:
  field_id _key_column = field_limits::invalid();
  std::vector<KeyRange> _ranges;
  irs::containers::FlatHashSet<const DocRemoval*> _keyed;
};

}  // namespace irs

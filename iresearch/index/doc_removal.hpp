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

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct SubReader;

struct DocRemoval {
  static constexpr uint64_t kNoSegment = std::numeric_limits<uint64_t>::max();

  struct Row {
    uint64_t segment;
    doc_id_t doc;
    int64_t key;
  };

  static DocRemoval Build(field_id key_column, std::vector<Row> rows);

  size_t Find(uint64_t segment) const noexcept;

  std::span<const doc_id_t> Docs(size_t group) const noexcept {
    return {docs.data() + offsets[group], docs.data() + offsets[group + 1]};
  }

  std::span<const int64_t> Keys(size_t group) const noexcept {
    return {keys.data() + offsets[group], keys.data() + offsets[group + 1]};
  }

  field_id key_column = field_limits::invalid();
  std::vector<uint64_t> segments;
  std::vector<uint32_t> offsets;
  std::vector<doc_id_t> docs;
  std::vector<int64_t> keys;
};

class DocRemovalResolver {
 public:
  void Prepare(std::span<const DocRemoval* const> removals,
               std::span<const SubReader* const> live);

  std::span<const doc_id_t> Docs(const DocRemoval& removal,
                                 const SubReader& segment);

  std::span<const doc_id_t> PositionalDocs(const DocRemoval& removal,
                                           const SubReader& segment) const;

 private:
  struct Hit {
    int64_t key;
    doc_id_t doc;

    bool operator<(const Hit& rhs) const noexcept {
      return key < rhs.key || (key == rhs.key && doc < rhs.doc);
    }
  };

  struct Lookup {
    field_id column;
    std::vector<int64_t> keys;
    absl::flat_hash_map<std::string, std::vector<Hit>> hits;
  };

  Lookup* LookupOf(field_id column);
  std::span<const Hit> Resolve(Lookup& lookup, const SubReader& segment);
  static void Scan(const Lookup& lookup, const SubReader& segment,
                   std::vector<Hit>& hits);

  absl::flat_hash_map<const DocRemoval*, std::vector<int64_t>> _fallback;
  std::vector<Lookup> _lookups;
  std::vector<doc_id_t> _scratch;
};

}  // namespace irs

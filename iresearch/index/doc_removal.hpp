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

#include <absl/base/internal/endian.h>

#include <bit>
#include <cstdint>
#include <memory>
#include <vector>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DirectoryReader;
struct DocMapsSlot;

struct DocMap {
  uint64_t source = 0;
  uint64_t target = 0;
  std::shared_ptr<const DocumentMask> mask;
  doc_id_t rank_begin = 0;
  doc_id_t rank_end = 0;
  doc_id_t base = doc_limits::min();
  std::shared_ptr<const DocMapsSlot> next;
};

struct DocMapsSlot {
  std::vector<DocMap> maps;
};

struct DocRemoval {
  struct Group {
    uint64_t segment = 0;
    DocumentMask docs;
    std::shared_ptr<const DocMapsSlot> maps;
  };

  std::vector<Group> groups;
};

struct TermRemoval {
  static bytes_view Term(int64_t key,
                         byte_type (&buf)[sizeof(int64_t)]) noexcept {
    absl::big_endian::Store64(
      buf, std::bit_cast<uint64_t>(key) ^ (uint64_t{1} << 63));
    return {buf, sizeof(int64_t)};
  }

  static int64_t Key(bytes_view term) noexcept {
    SDB_ASSERT(term.size() == sizeof(int64_t));
    return std::bit_cast<int64_t>(absl::big_endian::Load64(term.data()) ^
                                  (uint64_t{1} << 63));
  }

  std::vector<int64_t> terms;
};

class DocRemovalBuilder {
 public:
  void Add(uint32_t index, doc_id_t doc);
  void AddRange(uint32_t index, doc_id_t first, doc_id_t last);

  bool Empty() const noexcept { return _groups.empty(); }

  std::shared_ptr<const DocRemoval> Finish(const DirectoryReader& reader) &&;

 private:
  DocumentMaskBuilder& Group(uint32_t index);

  std::vector<std::pair<uint32_t, DocumentMaskBuilder>> _groups;
  size_t _last = 0;
};

bool MapDocs(const DocMap& map, const DocumentMask& docs,
             DocumentMaskBuilder& out);

}  // namespace irs

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

#include <absl/algorithm/container.h>

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DirectoryReader;

struct DocMap {
  uint64_t source = 0;
  uint64_t target = 0;
  std::shared_ptr<const DocumentMask> mask;
  doc_id_t rank_begin = 0;
  doc_id_t rank_end = 0;
  doc_id_t base = doc_limits::min();
};

struct DocRemoval {
  struct Group {
    uint64_t segment = 0;
    DocumentMask docs;
  };

  std::vector<Group> groups;
};

class DocRemovalBuilder {
 public:
  void Add(uint32_t index, doc_id_t doc);
  void AddRange(uint32_t index, doc_id_t first, doc_id_t last);
  DocumentMaskBuilder& Group(uint32_t index);

  bool Empty() const noexcept {
    return absl::c_all_of(
      _groups, [](const auto& group) { return group.second.Empty(); });
  }

  std::shared_ptr<const DocRemoval> Finish(const DirectoryReader& reader) &&;

 private:
  std::vector<std::pair<uint32_t, DocumentMaskBuilder>> _groups;
  size_t _last = 0;
};

bool MapDocs(const DocMap& map, const DocumentMask& docs,
             DocumentMaskBuilder& out);

struct MappedRow {
  size_t index = 0;
  uint64_t row = 0;
};

void MapRows(const DocMap& map, std::span<const uint64_t> rows,
             std::vector<MappedRow>& out);

}  // namespace irs

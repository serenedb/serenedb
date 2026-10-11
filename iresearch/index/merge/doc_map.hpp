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

#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct DocRemap {
  explicit DocRemap(IResourceManager& rm) noexcept : id_map{{rm}} {}

  DocumentMask::Iterator mask;
  doc_id_t base_id = doc_limits::invalid();
  ManagedVector<doc_id_t> id_map;

  bool IsMasked(doc_id_t src) const noexcept { return mask.Contains(src); }

  doc_id_t Remap(doc_id_t src) const noexcept {
    if (!id_map.empty()) {
      SDB_ASSERT(src < id_map.size());
      return id_map[src];
    }
    return base_id + (src - doc_limits::min());
  }
};

struct MergeInput {
  MergeInput(const SubReader* reader, IResourceManager& rm) noexcept
    : reader{reader}, remap{rm} {
    SDB_ASSERT(this->reader);
  }

  MergeInput(const SubReader& reader, IResourceManager& rm) noexcept
    : MergeInput{&reader, rm} {}

  const SubReader* reader;
  DocRemap remap;
};

using MergeDocMap = ManagedVector<MergeInput>;

}  // namespace irs

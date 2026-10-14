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

#include <absl/base/call_once.h>
#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/formats/column/merge.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

class MergeOrder;

class InputNormProvider final : public NormProvider {
 public:
  struct Input {
    const MergeSource* source;
    doc_id_t first;
    doc_id_t end;
    ManagedVector<doc_id_t> docs;
  };

  InputNormProvider(std::span<const MergeSource> sources,
                    IResourceManager& memory,
                    const MergeOrder* order = nullptr);

  void AddField(field_id column, field_id field, uint64_t sum,
                uint64_t non_zero);

  NormReader::ptr norms(field_id column) const final;

 private:
  struct Field {
    field_id field;
    score_t avg;
  };

  void MapDocs() const;

  mutable std::vector<Input> _inputs;
  const MergeOrder* _order;
  mutable ManagedVector<doc_id_t> _sources;
  mutable absl::once_flag _mapped;
  absl::flat_hash_map<field_id, Field> _fields;
};

}  // namespace irs

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

#include "basics/containers/node_hash_map.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/norm.hpp"

namespace irs {

class ColumnCollector {
 public:
  const PayAttr* AddColumn(const ColumnReader* field) {
    auto it = GetIterator(field, false);
    if (!it) {
      return nullptr;
    }
    return irs::get<PayAttr>(*it->it);
  }

  const Norm* AddNorm(const ColumnReader* field) {
    auto it = GetIterator(field, true);
    if (!it) {
      return nullptr;
    }
    SDB_ASSERT(it->norm.payload);
    return &it->norm.value;
  }

  void Clear() noexcept { _columns.clear(); }

  void Collect(doc_id_t doc) {
    for (auto& [_, adapter] : _columns) {
      if (doc != adapter.it->seek(doc)) [[unlikely]] {
        adapter.norm.value.value = 1U;
      } else {
        adapter.norm.Read();
      }
    }
  }

 private:
  struct NormData {
    const PayAttr* payload = nullptr;
    Norm value;

    void Read() {
      if (payload) {
        value.value = Norm::Read(payload->value);
      }
    }
  };

  struct ColumnIterator {
    ResettableDocIterator::ptr it;
    NormData norm;

    bool Init(const ColumnReader& reader, bool with_norms);
  };

  ColumnIterator* GetIterator(const ColumnReader* field, bool with_norms);

  // TODO(gnusi): remove norms and make it flat map
  mutable sdb::containers::NodeHashMap<field_id, ColumnIterator> _columns;
};

}  // namespace irs

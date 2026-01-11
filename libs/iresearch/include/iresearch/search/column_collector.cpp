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

#include "iresearch/search/column_collector.hpp"

namespace irs {

auto ColumnCollector::GetIterator(const ColumnReader* field, bool with_norms)
  -> ColumnIterator* {
  if (!field) {
    return nullptr;
  }
  auto& it = _columns.try_emplace(field->id()).first->second;
  if (!it.it && !it.Init(*field, with_norms)) {
    return nullptr;
  }
  return &it;
}

bool ColumnCollector::ColumnIterator::Init(const ColumnReader& reader,
                                           bool with_norms) {
  SDB_ASSERT(!it);
  it = reader.iterator(ColumnHint::Normal);
  if (!it) [[unlikely]] {
    return false;
  }

  if (with_norms) {
    norm.payload = irs::get<PayAttr>(*it);
    if (!norm.payload) [[unlikely]] {
      return false;
    }

    auto header = NormHeader::Read(reader.payload());
    if (!header) [[unlikely]] {
      return false;
    }
    norm.value.num_bytes = static_cast<byte_type>(header->NumBytes());
  }

  return true;
}

}  // namespace irs

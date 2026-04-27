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

#include "connector/search_pk_lookup.h"

namespace sdb::connector {

bool OpenSegmentPkIterator(const irs::SubReader& segment,
                           SegmentPkIterator& out) {
  out.Reset();
  const auto* pk_col = segment.column(kPkFieldName);
  if (!pk_col) {
    return false;
  }
  out.iter = pk_col->iterator(irs::ColumnHint::Normal);
  if (!out.iter) {
    return false;
  }
  out.value = irs::get<irs::PayAttr>(*out.iter);
  if (!out.value) {
    out.iter.reset();
    return false;
  }
  return true;
}

}  // namespace sdb::connector

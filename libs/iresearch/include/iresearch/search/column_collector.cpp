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

#include "iresearch/formats/formats.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {

const uint32_t* ColumnArgsFetcher::AddNorms(const ColumnReader* field) {
  if (!field) {
    return nullptr;
  }
  auto& it = _columns.try_emplace(field->id()).first->second;
  if (!it.reader) {
    it.reader = field->norms();
    if (!it.reader) [[unlikely]] {
      return nullptr;
    }
    it.norms.resize(kPostingBlock);  // TODO(gnusi): fix
  }
  return it.norms.data();
}

}  // namespace irs

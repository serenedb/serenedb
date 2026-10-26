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

#include "iresearch/formats/term_reader.hpp"

#include <absl/algorithm/container.h>

#include "iresearch/utils/assert.hpp"

namespace irs {

void TermReader::LookupBatch(std::span<const bytes_view> terms,
                             std::span<PostingMeta> out) const {
  SDB_ASSERT(terms.size() == out.size());
  absl::c_transform(terms, out.begin(),
                    [&](bytes_view term) { return Lookup(term); });
}

void TermReader::VisitRange(bytes_view from, bytes_view to,
                            RangeVisitor visit) const {
  auto terms = iterator();
  if (!terms || terms->seek_ge(from) == SeekResult::End) {
    return;
  }
  do {
    const bytes_view term = terms->value();
    if (!to.empty() && term >= to) {
      return;
    }
    visit(term, terms->cookie());
  } while (terms->next());
}

}  // namespace irs

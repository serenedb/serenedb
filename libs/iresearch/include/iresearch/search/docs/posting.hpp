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

#include <algorithm>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/posting_batch.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename InputType, typename Table>
class Posting : public Root,
                public search::PostingBatch<InputType, Table, false> {
  using Base = search::PostingBatch<InputType, Table, false>;

  using Base::_last;
  using Base::_left_in_list;
  using Base::kBlock;
  using Base::ReadDocs;
  using Base::SkipFreqs;

 public:
  using Base::kTable;

  explicit Posting(Table table) noexcept : _table{table} {}

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool has_score_bounds, bool has_freq) {
    SDB_ASSERT(meta.docs_count > 1, "a single document has its own root");
    this->OpenInput(meta, doc_in, has_score_bounds);
    this->SetFreqLen(has_freq);
    this->ArmWalk(meta, layout, has_score_bounds);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t emitted = 0;
    while (_left_in_list != 0 && emitted + kBlock <= capacity) {
      if constexpr (kTable) {
        const auto from = _last + doc_limits::min();
        if (const auto live = _table.Live(from);
            live != from && !this->Step(live)) {
          break;
        }
      }
      const auto len = std::min(_left_in_list, kBlock);
      auto* const dest = out + emitted;
      ReadDocs(dest, len);
      SkipFreqs(len);
      emitted += len;
    }
    return emitted;
  }

 private:
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::docs

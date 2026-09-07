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

#include <utility>
#include <vector>

#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/hnsw_query.hpp"

namespace irs::docs {

namespace {

class HnswHits : public Root {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, search::DeadRuns* table)
    : _hits{std::move(hits)}, _table{table} {}

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    uint32_t n = 0;
    while (_pos != _hits.size() && n != capacity) {
      const auto doc = _hits[_pos++].doc;
      if (_table != nullptr && _table->Live(doc) != doc) {
        continue;
      }
      out[n++] = doc;
    }
    return n;
  }

 private:
  std::vector<ScoreDoc> _hits;
  search::DeadRuns* _table;
  size_t _pos = 0;
};

}  // namespace

Root::ptr Make(const HnswQuery& query, const Context& ctx) {
  return memory::make_managed<HnswHits>(query.RunSearch(), ctx.table);
}

}  // namespace irs::docs

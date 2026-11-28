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

#include "iresearch/search/docs/family.hpp"
#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/query_memory.hpp"

namespace irs::docs {
namespace {

class HnswHits : public Root {
 public:
  explicit HnswHits(memory::ArenaVector<ScoreDoc>&& hits)
    : _hits{std::move(hits)} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    uint32_t n = 0;
    for (; _pos != _hits.size(); ++_pos) {
      const auto doc = _hits[_pos].doc;
      if (doc >= max) {
        break;
      }
      if (doc < min) {
        continue;
      }
      out[n++] = doc;
    }
    return n;
  }

 private:
  memory::ArenaVector<ScoreDoc> _hits;
  size_t _pos = 0;
};

}  // namespace

Root::ptr Family::Make(const HnswQuery& query, const Context& ctx) {
  return ctx.memory.Make<HnswHits>(query.RunSearch(ctx.memory));
}

}  // namespace irs::docs

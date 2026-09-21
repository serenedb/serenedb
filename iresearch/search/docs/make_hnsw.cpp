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

#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/utils/containers/fixed.hpp"

namespace irs::docs {
namespace {

class HnswHits : public Root {
 public:
  explicit HnswHits(std::vector<ScoreDoc>&& hits)
    : _hits{hits.size(),
            [&](ScoreDoc& slot, size_t i) noexcept { slot = hits[i]; }} {}

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
  containers::Fixed<ScoreDoc> _hits;
  size_t _pos = 0;
};

}  // namespace

Root::ptr Make(const HnswQuery& query, const Context&) {
  return memory::make_managed<HnswHits>(query.RunSearch());
}

}  // namespace irs::docs

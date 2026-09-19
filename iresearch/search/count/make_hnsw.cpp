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

#include "iresearch/search/count/make.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"

namespace irs::count {
namespace {

class HnswCount : public Root {
 public:
  explicit HnswCount(uint64_t count) noexcept : _count{count} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    SDB_ASSERT(min == doc_limits::min() && doc_limits::eof(max));
    return _count;
  }

 private:
  uint64_t _count;
};

}  // namespace

Root::ptr Make(const HnswQuery& query, const Context& ctx) {
  HnswRefuseFilter(ctx.table);
  return memory::make_managed<HnswCount>(query.RunSearch().size());
}

}  // namespace irs::count

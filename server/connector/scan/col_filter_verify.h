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

#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/search/detail/table_filter.hpp>
#include <memory>
#include <span>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

class ColFilterVerify : public irs::detail::TableFilter {
 public:
  void Begin(const irs::SubReader& seg,
             std::span<const irs::ColFilterSpec> active,
             duckdb::ClientContext& context, irs::ColFilterStateCache& states);

  bool Empty() const noexcept {
    return _chain.Empty() && _score_filter == nullptr;
  }

  irs::doc_id_t Live(irs::doc_id_t doc) final {
    if (_chain.Empty()) {
      return doc;
    }
    const auto dead = _chain.DeadUntil(doc - irs::doc_limits::min());
    return dead == 0
             ? doc
             : irs::doc_limits::min() + static_cast<irs::doc_id_t>(dead);
  }

  uint32_t Narrow(irs::doc_id_t* docs, irs::score_t* scores, uint32_t n) final;

  uint32_t Narrow(irs::doc_id_t base, uint64_t* mask, irs::score_t* scores,
                  uint32_t words) final;

  uint64_t CountAndClear(irs::doc_id_t base, uint64_t* mask,
                         uint32_t words) final;

  void Rewind() {
    if (_ctx) {
      _chain.Rewind(*_ctx);
    }
  }

 private:
  std::unique_ptr<irs::ReadContext> _ctx;
  irs::ColFilterChain _chain;
  const duckdb::TableFilter* _score_filter = nullptr;
  duckdb::TableFilterState* _score_state = nullptr;
};

}  // namespace sdb::connector

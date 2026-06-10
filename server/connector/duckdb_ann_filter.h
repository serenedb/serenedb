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

#include <faiss/impl/IDSelector.h>

#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>

namespace sdb::connector {

// Non-iresearch WHERE conjuncts remain as LogicalFilter above the scan; only
// iresearch-claimable conjuncts are evaluated here per ANN candidate.
class TextScanFilter final : public faiss::IDSelector {
 public:
  explicit TextScanFilter(const irs::Filter::Query& query);

  void Reset(const irs::SubReader& segment);
  bool is_member(faiss::idx_t id) const final;

 private:
  const irs::Filter::Query& _query;
  mutable irs::DocIterator::ptr _it;
};

}  // namespace sdb::connector

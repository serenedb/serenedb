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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/docs/all.hpp"
#include "iresearch/search/docs/family.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/filters/all_filter.hpp"

namespace irs::docs {

Root::ptr Family::MakeAll(const SubReader& segment, const Context&,
                          ScoreMergeType, score_t) {
  return memory::make_managed<All>(static_cast<doc_id_t>(segment.docs_count()));
}

}  // namespace irs::docs

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

#include "iresearch/search/filters/docs_mask_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/queries/docs_mask_query.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs {
namespace {

uint32_t MaskedCount(const SubReader& segment) noexcept {
  const auto& meta = segment.Meta();
  const auto* set = segment.docs_mask();
  const auto scattered =
    set != nullptr ? static_cast<uint32_t>(set->Count()) : uint32_t{0};
  return scattered + UncommittedCount(meta);
}

}  // namespace

QueryBuilder::ptr DocsMaskFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto masked = MaskedCount(segment);
  if (masked == 0) {
    return QueryBuilder::Empty();
  }
  return memory::make_tracked<DocsMaskQuery>(ctx.memory, segment, masked);
}

Filter::ptr WithDocsMask(Filter::ptr base) {
  auto masked = std::make_unique<BooleanFilter>();
  masked->Add(std::move(base), Occur::Must);
  masked->Add(std::make_unique<DocsMaskFilter>(), Occur::MustNot);
  return masked;
}

}  // namespace irs

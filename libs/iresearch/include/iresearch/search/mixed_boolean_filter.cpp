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

#include "mixed_boolean_filter.hpp"

#include "iresearch/search/boolean_query.hpp"

namespace irs {

PrepareCollector::ptr MixedBooleanFilter::MakeCollector(
  const Scorer* scorer) const {
  if (_and->empty()) {
    return _or->MakeCollector(scorer);
  }
  if (_or->empty()) {
    return _and->MakeCollector(scorer);
  }
  auto compound = std::make_unique<CompoundCollector>(scorer);
  compound->Add(_and->MakeCollector(scorer));
  for (const auto& opt_filter : *_or) {
    compound->Add(opt_filter->MakeCollector(scorer));
  }
  return compound;
}

QueryBuilder::ptr MixedBooleanFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  if (_and->empty()) {
    return _or->PrepareSegment(segment, ctx);
  }
  if (_or->empty()) {
    return _and->PrepareSegment(segment, ctx);
  }

  auto* compound = dynamic_cast<CompoundCollector*>(ctx.collector);
  SDB_ASSERT(compound != nullptr);

  PrepareContext req_ctx = ctx;
  req_ctx.collector = &compound->Child(0);
  auto req = _and->PrepareSegment(segment, req_ctx);

  std::vector<QueryBuilder::ptr> opt;
  opt.reserve(_or->size());
  const auto opt_ctx = ctx.Boost(_or->Boost());
  size_t idx = 1;
  for (const auto& opt_filter : *_or) {
    PrepareContext child = opt_ctx;
    child.collector = &compound->Child(idx);
    opt.emplace_back(opt_filter->PrepareSegment(segment, child));
    ++idx;
  }

  return memory::make_tracked<BoostQuery>(ctx.memory, segment, std::move(req),
                                          std::move(opt));
}

bool MixedBooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<MixedBooleanFilter>(rhs);
  return *_and == *typed_rhs._and && *_or == *typed_rhs._or;
}

}  // namespace irs

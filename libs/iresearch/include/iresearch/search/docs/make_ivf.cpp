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

#include "iresearch/search/common/vector_of.hpp"
#include "iresearch/search/docs/make.hpp"

namespace irs::docs {

Root::ptr Make(const RangeVectorQuery& query, const Context& ctx) {
  auto inner = search::InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  return ResolveBool(query.Inclusive(), [&]<bool Inclusive>() -> Root::ptr {
    if (ctx.table != nullptr) {
      return search::MakeVectorDocs<FilteredWalk, Root::ptr,
                                    search::RadiusGate<Inclusive>,
                                    lead::TwoPhaseDocs>(
        query, query.Threshold(), std::move(inner), ctx.table);
    }
    return search::MakeVectorDocs<
      PlainWalk, Root::ptr, search::RadiusGate<Inclusive>, lead::TwoPhaseDocs>(
      query, query.Threshold(), std::move(inner), utils::Empty{});
  });
}

}  // namespace irs::docs

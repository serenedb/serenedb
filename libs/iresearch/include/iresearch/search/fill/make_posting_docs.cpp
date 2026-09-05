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
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"

namespace irs::fill {

Node::ptr MakePostingDocs(const search::PostingClause& posting,
                          const SubReader&) {
  SDB_ASSERT(posting.state.cookie.docs_count != 0);
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  const auto& doc = *search::DocOf(own);
  return search::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    return memory::make_managed<Impl<search::PostingFill<Input>>>(
      posting.state.cookie, doc, search::BoundsOf(own), search::FreqOf(own));
  });
}

}  // namespace irs::fill

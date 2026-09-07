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

#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/single_posting_docs.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs::fill {

Node::ptr MakeSinglePostingDocs(const search::PostingClause& posting) {
  SDB_ASSERT(posting.state.cookie.docs_count == 1);
  return memory::make_managed<Impl<SingleDocs>>(doc_limits::min() +
                                                posting.state.cookie.doc_delta);
}

}  // namespace irs::fill

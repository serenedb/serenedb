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

#include <cstddef>
#include <cstdint>

#include "basics/memory.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/resolve.hpp"

namespace irs::search {

class LazyBitset;

}  // namespace irs::search
namespace irs::count {

struct TermCounts : memory::Managed {
  using ptr = memory::managed_ptr<TermCounts>;

  virtual uint64_t Count(const PostingMeta& term) = 0;

  virtual bool Any(const PostingMeta& term) = 0;
};

TermCounts::ptr MakeTermCounts(search::LazyBitset& set, const TermReader& field,
                               size_t terms);

}  // namespace irs::count

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

#include <cstdint>

#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

class BitsetStorage;

}
namespace irs::fill {

struct Node : memory::Managed {
  using ptr = memory::managed_ptr<Node>;

  virtual doc_id_t FillOr(doc_id_t, doc_id_t, uint64_t* IRS_RESTRICT) {
    SDB_UNREACHABLE();
  }

  virtual doc_id_t FillAnd(doc_id_t, doc_id_t, uint64_t* IRS_RESTRICT) {
    SDB_UNREACHABLE();
  }

  virtual doc_id_t FillAndNot(doc_id_t, doc_id_t, uint64_t* IRS_RESTRICT) {
    SDB_UNREACHABLE();
  }

  virtual doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                        score_t* IRS_RESTRICT) {
    return FillOr(min, max, mask);
  }

  virtual search::BitsetStorage* Folded() noexcept { return nullptr; }
};

}  // namespace irs::fill

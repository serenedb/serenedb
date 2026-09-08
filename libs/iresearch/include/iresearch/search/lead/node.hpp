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
#include "iresearch/search/score_function.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

struct Node : memory::Managed {
  using ptr = memory::managed_ptr<Node>;

  virtual doc_id_t Advance() = 0;

  virtual doc_id_t Seek(doc_id_t target) = 0;

  virtual void FetchScoreArgs(uint32_t) {}

  virtual ScoreFunction PrepareScore() { return ScoreFunction::Default(); }
};

}  // namespace irs::lead

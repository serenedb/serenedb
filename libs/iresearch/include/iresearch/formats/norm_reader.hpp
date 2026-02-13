////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <span>

#include "basics/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct NormReader : public memory::Managed {
  using ptr = memory::managed_ptr<NormReader>;

  virtual ~NormReader() = default;

  virtual void Collect(std::span<doc_id_t> docs,
                       std::span<uint32_t> values) = 0;
  virtual bool Get(doc_id_t docs, uint32_t* value) = 0;
};

}  // namespace irs

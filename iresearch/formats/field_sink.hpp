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
#include <span>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs {

struct FieldSink {
  virtual ~FieldSink() = default;

  virtual void prepare(const FlushState& state, uint64_t keyed_dicts) = 0;
  virtual void write(DictRef dict, const BasicTermReader& reader) = 0;
  virtual void WriteSlot(field_id id, DictRef dict,
                         std::span<const doc_id_t> docs) = 0;
  virtual void end() = 0;
};

}  // namespace irs

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

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class IndexOutput;

struct TermPayloadWriter {
  virtual ~TermPayloadWriter() = default;

  virtual void WriteTermPayload(IndexOutput& out,
                                std::span<const doc_id_t> docs) = 0;

  virtual void Finish(IndexOutput& out) = 0;

  virtual uint32_t PendingLanes() const noexcept { return 0; }
};

struct BasicTermReader : public memory::Managed {
  virtual TermOnlyIterator::ptr iterator() const = 0;

  virtual field_id id() const = 0;

  virtual FieldProperties properties() const = 0;

  virtual bytes_view min() const = 0;
  virtual bytes_view max() const = 0;

  virtual TermPayloadWriter* PayloadWriter() const { return nullptr; }
};

}  // namespace irs

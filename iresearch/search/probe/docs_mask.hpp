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

#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::probe {

class DocsMask {
 public:
  DocsMask(const DocumentMask* mask, doc_id_t uncommitted) noexcept
    : _it{mask, uncommitted} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : _it{segment.MaskedDocs()} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) noexcept {
    return _it.Seek(target);
  }

 private:
  DocumentMask::Iterator _it;
};

}  // namespace irs::probe

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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

class AllDocs {
 public:
  explicit AllDocs(const SubReader& segment) noexcept
    : _last{static_cast<doc_id_t>(segment.docs_count())} {}

  doc_id_t Advance() noexcept {
    if (_doc >= _last) {
      return _doc = doc_limits::eof();
    }
    return ++_doc;
  }

  doc_id_t Seek(doc_id_t target) noexcept {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = target > _last ? doc_limits::eof() : target;
  }

 private:
  doc_id_t _last;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead

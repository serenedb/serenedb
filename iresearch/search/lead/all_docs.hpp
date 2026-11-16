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

#include <algorithm>
#include <cstdint>
#include <numeric>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

class AllDocs {
 public:
  explicit AllDocs(const SubReader& segment) noexcept
    : _last{static_cast<doc_id_t>(segment.docs_count())} {}

  doc_id_t Next() noexcept {
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

  uint32_t Gather(doc_id_t& doc, doc_id_t max, doc_id_t* out,
                  uint32_t cap) noexcept {
    SDB_ASSERT(doc == _doc && doc < max);
    const auto stop = std::min<uint64_t>(max, uint64_t{_last} + 1);
    const auto n = static_cast<uint32_t>(std::min<uint64_t>(cap, stop - doc));
    std::iota(out, out + n, doc);
    _doc = doc + n - 1;
    doc = Next();
    return n;
  }

  uint64_t Count(doc_id_t min, doc_id_t max) noexcept {
    const auto from = Seek(min);
    if (from >= max) {
      return 0;
    }
    const auto stop = std::min<uint64_t>(max, uint64_t{_last} + 1);
    Seek(static_cast<doc_id_t>(stop));
    return stop - from;
  }

 private:
  doc_id_t _last;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead

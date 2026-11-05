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
#include <array>
#include <cstdint>
#include <span>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Verifier>
class VerifiedBatch {
 public:
  static constexpr uint32_t kMaxSize = 128;

  template<typename Approx>
  doc_id_t Next(Approx& approx, Verifier& verifier) {
    if (_at + 1 < _count) {
      return _docs[++_at];
    }
    if (_exhausted) {
      return doc_limits::eof();
    }
    _size = std::min(_size * 2, kMaxSize);
    return Fill(approx.Next(), approx, verifier);
  }

  template<typename Approx>
  doc_id_t Seek(Approx& approx, Verifier& verifier, doc_id_t target) {
    while (_at < _count && _docs[_at] < target) {
      ++_at;
    }
    if (_at < _count) {
      return _docs[_at];
    }
    if (_exhausted) {
      return doc_limits::eof();
    }
    _size = std::max<uint32_t>(_size / 2, 1);
    return Fill(approx.Seek(target), approx, verifier);
  }

  bool Match(doc_id_t doc) const {
    SDB_ASSERT(_at < _count && _docs[_at] == doc);
    return _matched[_at];
  }

 private:
  template<typename Approx>
  doc_id_t Fill(doc_id_t first, Approx& approx, Verifier& verifier) {
    _at = 0;
    _count = 0;
    if (doc_limits::eof(first)) {
      _exhausted = true;
      return first;
    }
    _docs[_count++] = first;
    while (_count < _size) {
      const auto doc = approx.Next();
      if (doc_limits::eof(doc)) {
        _exhausted = true;
        break;
      }
      _docs[_count++] = doc;
    }
    verifier.Check(std::span<const doc_id_t>{_docs.data(), _count},
                   std::span<bool>{_matched.data(), _count});
    return first;
  }

  std::array<doc_id_t, kMaxSize> _docs{};
  std::array<bool, kMaxSize> _matched{};
  uint32_t _count = 0;
  uint32_t _at = 0;
  uint32_t _size = 4;
  bool _exhausted = false;
};

}  // namespace irs::detail

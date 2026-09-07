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

#include <tuple>
#include <utility>

#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<Type Head, probe::Type Tail>
class SparseConjunctionDocs {
 public:
  template<typename HeadArgs, typename TailArgs>
  SparseConjunctionDocs(std::piecewise_construct_t, HeadArgs&& head,
                        TailArgs&& tail)
    : _head{std::make_from_tuple<Head>(std::forward<HeadArgs>(head))},
      _tail{std::make_from_tuple<Tail>(std::forward<TailArgs>(tail))} {}

  SparseConjunctionDocs(SparseConjunctionDocs&&) = delete;
  SparseConjunctionDocs& operator=(SparseConjunctionDocs&&) = delete;

  doc_id_t Advance() { return Converge(_head.Advance()); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_head.Seek(target));
  }

 private:
  doc_id_t Converge(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      const auto probe = _tail.Probe(doc);
      if (probe == doc) {
        return _doc = doc;
      }
      doc = _head.Seek(probe);
    }
    return _doc = doc;
  }

  Head _head;
  Tail _tail;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead

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

#include <cstddef>
#include <utility>

#include "iresearch/search/lead/concept.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<Type Leaf>
class DisjointDisjunction {
 public:
  template<typename Init>
  DisjointDisjunction(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)},
      _heads{size,
             [this](Head& head, size_t i) noexcept {
               head = {.doc = doc_limits::invalid(), .leaf = &_leaves[i]};
             }},
      _size{size} {}

  DisjointDisjunction(DisjointDisjunction&&) = delete;
  DisjointDisjunction& operator=(DisjointDisjunction&&) = delete;

  doc_id_t Next() {
    if (doc_limits::eof(_doc)) [[unlikely]] {
      return _doc;
    }
    return _doc = Settle(_doc + 1);
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return _doc = Settle(target);
  }

 private:
  struct Head {
    doc_id_t doc;
    Leaf* leaf;
  };

  doc_id_t Settle(doc_id_t target) {
    while (_size != 0) {
      auto& top = _heads[0];
      if (top.doc >= target) {
        return top.doc;
      }
      top.doc =
        top.doc + 1 == target ? top.leaf->Next() : top.leaf->Seek(target);
      if (doc_limits::eof(top.doc)) {
        top = _heads[--_size];
      }
      SiftDown();
    }
    return doc_limits::eof();
  }

  void SiftDown() noexcept {
    const auto head = _heads[0];
    size_t at = 0;
    for (auto child = size_t{1}; child < _size; child = 2 * at + 1) {
      if (child + 1 < _size && _heads[child + 1].doc < _heads[child].doc) {
        ++child;
      }
      if (head.doc <= _heads[child].doc) {
        break;
      }
      _heads[at] = _heads[child];
      at = child;
    }
    _heads[at] = head;
  }

  containers::Fixed<Leaf> _leaves;
  containers::Fixed<Head> _heads;
  size_t _size;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead

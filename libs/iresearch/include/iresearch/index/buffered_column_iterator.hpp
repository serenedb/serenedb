////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/buffered_column.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

class BufferedColumnIterator : public ResettableDocIterator {
  static constexpr BufferedValue kEmpty{irs::doc_limits::eof(), 0, 0};

 public:
  BufferedColumnIterator(std::span<const BufferedValue> values,
                         bytes_view data) noexcept
    : _begin{values.data()},
      _next{_begin},
      _end{_begin + values.size()},
      _data{data} {
    if (_begin == _end) {
      _begin = &kEmpty;
      _next = &kEmpty;
      _end = &kEmpty;
    }
    std::get<CostAttr>(_attrs).reset(values.size());
  }

  void Reset(std::span<const BufferedValue> values, bytes_view data) noexcept {
    _begin = values.data();
    _next = _begin;
    _end = _begin + values.size();
    _data = data;
    if (_begin == _end) {
      _begin = &kEmpty;
      _next = &kEmpty;
      _end = &kEmpty;
    }
    std::get<CostAttr>(_attrs).reset(values.size());
    std::get<irs::PayAttr>(_attrs).value = {};
    std::get<irs::DocAttr>(_attrs).value = {};
  }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() noexcept final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;

    if (_next == _end) [[unlikely]] {
      doc_value = doc_limits::eof();
      return false;
    }

    auto& payload = std::get<irs::PayAttr>(_attrs);

    doc_value = _next->key;
    payload.value = {_data.data() + _next->begin, _next->size};

    ++_next;

    return true;
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    // Currently the iterator is only used for access during the segment
    // flushing. We intentionally allow iterator to seek backwards.
    // We expect a lot of dense ranges.
    const auto* curr = _next == _end ? _begin : _next;
    curr = (curr + target) - curr->key;

    if (curr < _begin || _end <= curr || curr->key != target) [[unlikely]] {
      curr = std::lower_bound(
        _begin, _end, target,
        [](const BufferedValue& value, doc_id_t target) noexcept {
          return value.key < target;
        });
    }

    _next = curr;
    next();
    return value();
  }

  void reset() final {
    _next = _begin;
    std::get<irs::DocAttr>(_attrs).value = {};
    std::get<irs::PayAttr>(_attrs).value = {};
  }

 private:
  using Attributes = std::tuple<DocAttr, CostAttr, irs::PayAttr>;

  Attributes _attrs;
  const BufferedValue* _begin;
  const BufferedValue* _next;
  const BufferedValue* _end;
  bytes_view _data;
};

}  // namespace irs

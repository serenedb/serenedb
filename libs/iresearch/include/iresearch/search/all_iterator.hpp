////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

class AllIterator : public DocIterator {
 public:
  AllIterator(const SubReader& reader, const byte_type* query_stats,
              const Scorers& order, uint64_t docs_count, score_t boost);

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t advance() noexcept final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    doc_value = doc_value < _max_doc ? doc_value + 1 : doc_limits::eof();
    return doc_value;
  }

  doc_id_t seek(doc_id_t target) noexcept final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    doc_value = target <= _max_doc ? target : doc_limits::eof();
    return doc_value;
  }

  uint32_t count() final {
    auto& doc_value = std::get<DocAttr>(_attrs).value;
    if (doc_limits::eof(doc_value)) {
      return 0;
    }
    const auto count = _max_doc - doc_value;
    doc_value = doc_limits::eof();
    return count;
  }

 private:
  using Attributes = std::tuple<DocAttr, CostAttr, ScoreAttr>;

  doc_id_t _max_doc;  // largest valid doc_id
  Attributes _attrs;
};

}  // namespace irs

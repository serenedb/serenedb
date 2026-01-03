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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/iterators.hpp"

namespace irs {

class Exclusion : public DocIterator {
 public:
  Exclusion(DocIterator::ptr&& incl, DocIterator::ptr&& excl) noexcept
    : _incl(std::move(incl)), _excl(std::move(excl)) {
    SDB_ASSERT(_incl);
    SDB_ASSERT(_excl);
    _incl_doc = irs::get<DocAttr>(*_incl);
    _excl_doc = irs::get<DocAttr>(*_excl);
    SDB_ASSERT(_incl_doc);
    SDB_ASSERT(_excl_doc);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _incl->GetMutable(type);
  }

  doc_id_t value() const final { return _incl_doc->value; }

  doc_id_t advance() final {
    const auto incl = _incl->advance();
    return converge(incl);
  }

  doc_id_t seek(doc_id_t target) final {
    if (const auto doc = value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto incl = _incl->seek(target);
    return converge(incl);
  }

  uint32_t count() final { return Count(*this); }

 private:
  doc_id_t converge(doc_id_t incl) {
    if (doc_limits::eof(incl)) [[unlikely]] {
      return incl;
    }
    auto excl = _excl_doc->value;
    if (excl < incl) {
      excl = _excl->seek(incl);
    }
    while (excl == incl) {
      incl = _incl->advance();
      if (doc_limits::eof(incl)) {
        return incl;
      }
      if (excl < incl) {
        excl = _excl->seek(incl);
      }
    }
    return incl;
  }

  DocIterator::ptr _incl;
  DocIterator::ptr _excl;
  const DocAttr* _incl_doc;
  const DocAttr* _excl_doc;
};

}  // namespace irs

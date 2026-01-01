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

  bool next() final {
    if (!_incl->next()) {
      return false;
    }

    return !doc_limits::eof(converge(_incl_doc->value));
  }

  doc_id_t seek(doc_id_t target) final {
    if (!doc_limits::valid(target)) {
      return _incl_doc->value;
    }

    if (doc_limits::eof(target = _incl->seek(target))) {
      return target;
    }

    return converge(target);
  }

 private:
  // moves iterator to next not excluded
  // document not less than "target"
  doc_id_t converge(doc_id_t target) {
    auto excl = _excl_doc->value;

    if (excl < target) {
      excl = _excl->seek(target);
    }

    for (; excl == target;) {
      if (!_incl->next()) {
        return _incl_doc->value;
      }

      target = _incl_doc->value;

      if (excl < target) {
        excl = _excl->seek(target);
      }
    }

    return target;
  }

  DocIterator::ptr _incl;
  DocIterator::ptr _excl;
  const DocAttr* _incl_doc;
  const DocAttr* _excl_doc;
};

}  // namespace irs

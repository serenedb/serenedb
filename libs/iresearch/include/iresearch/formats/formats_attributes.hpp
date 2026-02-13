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

#include "basics/down_cast.h"
#include "iresearch/formats/formats.hpp"

namespace irs {

struct TermMetaImpl : TermMeta {
  TermMetaImpl() noexcept
    : e_skip_start(0) {}  // GCC 4.9 does not initialize unions properly

  void clear() noexcept {
    TermMeta::clear();
    doc_start = pos_start = pay_start = 0;
    pos_end = address_limits::invalid();
  }

  uint64_t doc_start = 0;  // where this term's postings start in the .doc file
  uint64_t pos_start = 0;  // where this term's postings start in the .pos file
  // file pointer where the last (vInt encoded) pos delta is
  uint64_t pos_end = address_limits::invalid();
  // where this term's payloads/offsets start in the .pay file
  uint64_t pay_start = 0;
  union {
    doc_id_t e_single_doc;  // singleton document id delta
    uint64_t e_skip_start;  // pointer where skip data starts (after doc_start)
  };
};

struct CookieImpl final : SeekCookie {
  CookieImpl() = default;
  explicit CookieImpl(const TermMetaImpl& meta) noexcept : meta(meta) {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<TermMeta>::id()) [[likely]] {
      return &meta;
    }

    return nullptr;
  }

  bool IsEqual(const SeekCookie& rhs) const noexcept final {
    const auto& rhs_meta = sdb::basics::downCast<CookieImpl>(rhs).meta;
    return meta.doc_start == rhs_meta.doc_start &&
           meta.pos_start == rhs_meta.pos_start;
  }

  size_t Hash() const noexcept final {
    return absl::HashOf(meta.doc_start, meta.pos_start);
  }

  TermMetaImpl meta;
};

template<>
struct Type<TermMetaImpl> : Type<TermMeta> {};

}  // namespace irs

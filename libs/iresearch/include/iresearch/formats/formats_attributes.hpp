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
  TermMetaImpl() noexcept : e_skip_start{0} {}
  void clear() noexcept {
    TermMeta::clear();
    e_skip_start = doc_start = pos_start = pay_start = pos_offset = 0;
  }

  uint64_t doc_start = 0;  // where this term's postings start in the .doc file
  uint64_t pos_start = 0;  // where this term's postings start in the .pos file
  uint64_t pay_start = 0;  // where this term's postings start in the .pay file
  // how many positions do we need to skip in positions block
  size_t pos_offset = 0;
  union {
    uint64_t e_skip_start;  // pointer where skip data starts (after doc_start)
    doc_id_t e_single_doc;  // singleton document id delta
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

  TermMetaImpl meta;
};

template<>
struct Type<TermMetaImpl> : Type<TermMeta> {};

}  // namespace irs

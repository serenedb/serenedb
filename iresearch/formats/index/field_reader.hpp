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

#include <absl/algorithm/container.h>
#include <fsst.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_key.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_reader.hpp"
#include "iresearch/formats/index/term_dict_single.hpp"
#include "iresearch/formats/index/term_dict_store.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs::term_dict {

class FieldReader final {
 public:
  FieldReader() = default;

  FieldReader(const FieldReader&) = delete;
  FieldReader& operator=(const FieldReader&) = delete;

  void prepare(const ReaderState& state);

  std::span<const TermReader* const> fields(field_id id) const;
  std::span<const field_id> field_ids() const noexcept { return _ids; }
  size_t size() const noexcept { return _dicts.size(); }

  const PostingsReader& Postings() const noexcept { return _pr; }
  template<typename Self>
  auto&& Store(this Self&& self) noexcept {
    return std::forward<Self>(self)._store;
  }

 private:
  struct FieldDicts {
    uint32_t first = 0;
    uint32_t count = 0;
  };

  Separators _separators;
  std::vector<TermReaderImpl> _keyed;
  std::deque<SingleTermReader> _singles;
  std::vector<const TermReader*> _dicts;
  containers::FlatHashMap<field_id, FieldDicts> _by_id;
  std::vector<field_id> _ids;
  PostingsReader _pr;
  BlockStore _store;
};

inline TermPostings::ptr TermReaderImpl::PostingsOf(
  IndexFeatures features, const PostingMeta& meta,
  TermPostings::ptr reuse) const {
  return _owner->Postings().Postings(_meta.index_features, features, meta,
                                     _has_score_bounds, std::move(reuse));
}

inline const BlockStore& TermReaderImpl::Store() const noexcept {
  return _owner->Store();
}

inline uint64_t TermReaderImpl::BlockOffset(size_t block) const noexcept {
  return Store().BlockOffset(_first_block + block);
}

inline bytes_view TermReaderImpl::Block(size_t block,
                                        PinnedRange& pinned) const {
  const uint64_t offset = BlockOffset(block);
  const uint64_t size = BlockOffset(block + 1) - offset;
  if (_small) {
    return {_small, size};
  }
  if (const auto* base = Store().Base()) {
    return {base + offset, size};
  }
  return {PinBlock(block, pinned), size};
}

}  // namespace irs::term_dict

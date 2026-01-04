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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "postings.hpp"

#include "iresearch/utils/type_limits.hpp"

namespace irs {

void Postings::get_sorted_postings(
  std::vector<const Posting*>& postings) const {
  SDB_ASSERT(_terms.size() == _postings.size());

  postings.resize(_postings.size());

  for (auto* p = postings.data(); const auto& posting : _postings) {
    *p++ = &posting;
  }

  absl::c_sort(postings, [](const auto lhs, const auto rhs) {
    return MemcmpLess(lhs->term, rhs->term);
  });
}

Posting* Postings::emplace(bytes_view term) {
  auto& parent = _writer.parent();

  // maximum number to bytes needed for storage of term length and data
  const auto term_size = term.size();  // + vencode_size(term.size());

  if (writer_t::container::block_type::kSize < term_size) {
    // TODO: maybe move big terms it to a separate storage
    // reject terms that do not fit in a block
    return nullptr;
  }

  const auto slice_end = _writer.pool_offset() + term_size;
  const auto next_block_start =
    _writer.pool_offset() < parent.value_count()
      ? _writer.position().block_offset() +
          writer_t::container::block_type::kSize
      : writer_t::container::block_type::kSize * parent.block_count();

  // do not span slice over 2 blocks, start slice at the start of the next block
  if (slice_end > next_block_start) {
    _writer.seek(next_block_start);
  }

  SDB_ASSERT(size() < doc_limits::eof());  // not larger then the static flag
  SDB_ASSERT(_terms.size() == _postings.size());

  const hashed_bytes_view hashed_term{term};

  bool is_new = false;
  const auto it = _terms.lazy_emplace(
    hashed_term, [&, size = _terms.size()](const auto& ctor) {
      ctor(size, hashed_term.Hash());
      is_new = true;
    });
  if (!is_new) [[likely]] {
    return &_postings[it->ref];
  }
  // for new terms also write out their value
  try {
    auto* start = _writer.position().buffer();
    _writer.write(term.data(), term_size);
    SDB_ASSERT(start == (_writer.position() - term_size).buffer());
    return &_postings.emplace_back(start, term_size);
  } catch (...) {
    // we leave some garbage in block pool
    _terms.erase(it);
    throw;
  }
}

}  // namespace irs

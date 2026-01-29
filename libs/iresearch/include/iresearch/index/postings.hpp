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

#pragma once

#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/block_pool.hpp"
#include "iresearch/utils/hash_set_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

inline bool MemcmpLess(const byte_type* lhs, size_t lhs_size,
                       const byte_type* rhs, size_t rhs_size) noexcept {
  SDB_ASSERT(lhs && rhs);

  const size_t size = std::min(lhs_size, rhs_size);
  const auto res = ::memcmp(lhs, rhs, size);

  if (0 == res) {
    return lhs_size < rhs_size;
  }

  return res < 0;
}

inline bool MemcmpLess(bytes_view lhs, bytes_view rhs) noexcept {
  return MemcmpLess(lhs.data(), lhs.size(), rhs.data(), rhs.size());
}

using byte_block_pool =
  BlockPool<byte_type, 32768, ManagedTypedAllocator<byte_type>>;

struct Posting {
  explicit Posting(const byte_type* data, size_t size) noexcept
    : term{data, size} {}

  bytes_view term;
  uint64_t doc_code;
  // ...........................................................................
  // store pointers to data in the following way:
  // [0] - pointer to freq stream end
  // [1] - pointer to prox stream end
  // [2] - pointer to freq stream begin
  // [3] - pointer to prox stream begin
  // ...........................................................................
  size_t int_start;
  doc_id_t doc{doc_limits::invalid()};
  uint32_t freq;
  uint32_t pos;
  uint32_t offs{0};
  doc_id_t size{1};  // length of postings
};

class Postings : util::Noncopyable {
 public:
  using writer_t = byte_block_pool::inserter;

  explicit Postings(writer_t& writer)
    : _terms{0, ValueRefHash{}, TermEq{_postings}}, _writer(writer) {}

  void clear() noexcept {
    _terms.clear();
    _postings.clear();
  }

  /// @brief fill a provided vector with terms and corresponding postings in
  /// sorted order
  void get_sorted_postings(std::vector<const Posting*>& postings) const;

  /// @note on error returns nullptr
  /// @note returned poitern remains valid until the next call
  Posting* emplace(bytes_view term);

  bool empty() const noexcept { return _terms.empty(); }
  size_t size() const noexcept { return _terms.size(); }

 private:
  struct TermEq : ValueRefEq<size_t> {
    using is_transparent = void;
    using Self::operator();

    explicit TermEq(const std::vector<Posting>& data) noexcept : _data{&data} {}

    bool operator()(const Ref& lhs,
                    const hashed_bytes_view& rhs) const noexcept {
      SDB_ASSERT(lhs.ref < _data->size());
      return (*_data)[lhs.ref].term == rhs;
    }

    bool operator()(const hashed_bytes_view& lhs,
                    const Ref& rhs) const noexcept {
      return this->operator()(rhs, lhs);
    }

   private:
    const std::vector<Posting>* _data;
  };

  // TODO(mbkkt) Maybe just flat_hash_set<unique_ptr<posting>>?
  std::vector<Posting> _postings;
  flat_hash_set<TermEq> _terms;
  writer_t& _writer;
};

}  // namespace irs

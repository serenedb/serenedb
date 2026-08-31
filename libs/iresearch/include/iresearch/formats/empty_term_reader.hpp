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

#include "formats.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs {

// term_reader implementation with docs_count but without terms
class EmptyTermReader final : public irs::TermReader {
 public:
  constexpr explicit EmptyTermReader(uint64_t docs_count) noexcept
    : _docs_count{docs_count} {}

  SeekTermIterator::ptr iterator() const noexcept final {
    return SeekTermIterator::empty();
  }

  SeekTermIterator::ptr iterator(
    const automaton_table_matcher&) const noexcept final {
    return SeekTermIterator::empty();
  }

  PostingMeta Lookup(bytes_view) const noexcept final { return {}; }

  void ReadDocs(bytes_view, Acceptor acceptor) const noexcept final {}

  size_t BitUnion(CookieProvider, uint64_t*) const noexcept final { return 0; }

  DocIterator::ptr Iterator(IndexFeatures features,
                            std::span<const PostingCookie> cookies,
                            IteratorFieldOptions options, size_t min_match,
                            ScoreMergeType type) const final {
    return DocIterator::empty();
  }

  const FieldMeta& meta() const noexcept final { return FieldMeta::kEmpty; }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  // total number of terms
  size_t size() const noexcept final {
    return 0;  // no terms in reader
  }

  // total number of documents
  uint64_t docs_count() const noexcept final { return _docs_count; }

  bytes_view min() const noexcept final { return {}; }
  bytes_view max() const noexcept final { return {}; }

  bool HasScoreBounds() const noexcept final { return false; }

 private:
  uint64_t _docs_count;
};

}  // namespace irs

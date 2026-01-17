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

  SeekTermIterator::ptr iterator(SeekMode) const noexcept final {
    return SeekTermIterator::empty();
  }

  SeekTermIterator::ptr iterator(
    automaton_table_matcher&) const noexcept final {
    return SeekTermIterator::empty();
  }

  size_t BitUnion(const cookie_provider&, size_t*) const noexcept final {
    return 0;
  }

  size_t read_documents(bytes_view, std::span<doc_id_t>) const noexcept final {
    return 0;
  }

  TermMeta term(bytes_view) const noexcept final { return {}; }

  DocIterator::ptr Iterator(IndexFeatures features,
                            std::span<const SeekCookie* const> cookies,
                            const IteratorOptions& options, size_t min_match,
                            ScoreMergeType type,
                            size_t num_buckets) const final {
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

  // least significant term
  bytes_view(min)() const noexcept final { return {}; }

  // most significant term
  bytes_view(max)() const noexcept final { return {}; }

  bool has_scorer(byte_type) const noexcept final { return false; }

 private:
  uint64_t _docs_count;
};

}  // namespace irs

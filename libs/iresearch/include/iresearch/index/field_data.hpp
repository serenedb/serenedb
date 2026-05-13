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

#include <deque>
#include <vector>

#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/postings.hpp"
#include "iresearch/utils/block_pool.hpp"
#include "iresearch/utils/hash_set_utils.hpp"

namespace irs {

struct FieldWriter;
class Tokenizer;
class Format;
struct Directory;
struct FlushState;

namespace columnstore {

class Writer;
class NormColumnWriter;

}  // namespace columnstore

using int_block_pool = BlockPool<size_t, 8192, ManagedTypedAllocator<size_t>>;

class FieldData : util::Noncopyable {
 public:
  // `columnstore` (when non-null) is the per-segment Writer that owns
  // the segment's `<seg>.cs`. When the field has a Norm feature, FieldData
  // opens a NormColumnWriter on it -- per-doc norm values flow into the
  // columnstore. Reads during the same flush are served by
  // NormColumnWriter::Get; reads after commit go through
  // columnstore::Reader::NormColumn over the .cs file.
  // `norm_id` is the per-segment sequential id allocated by FieldsData
  // when the field has a Norm feature; pass field_limits::invalid()
  // otherwise.
  FieldData(std::string_view name, byte_block_pool::inserter& byte_writer,
            int_block_pool::inserter& int_writer, IndexFeatures index_features,
            columnstore::Writer* columnstore = nullptr,
            field_id norm_id = field_limits::invalid());

  doc_id_t doc() const noexcept { return _last_doc; }

  const FieldMeta& meta() const noexcept { return _meta; }

  bool empty() const noexcept { return !doc_limits::valid(_last_doc); }

  bool invert(Tokenizer& tokens, doc_id_t id);

  const FieldStats& stats() const noexcept { return _stats; }

  bool seen() const noexcept { return _seen; }
  void seen(bool value) noexcept { _seen = value; }

  IndexFeatures requested_features() const noexcept {
    return _requested_features;
  }

  void compute_features() const;

  bool has_features() const noexcept { return _norm_writer != nullptr; }

  // For postings writers reading the in-flight norm during the same flush.
  // Returns nullptr when the field has no norm. Lifetime is the FieldData.
  columnstore::NormColumnWriter* NormColumnWriter() const noexcept {
    return _norm_writer;
  }

 private:
  friend class TermIteratorImpl;
  friend class DocIteratorImpl;
  friend class SortingDocIteratorImpl;
  friend class FieldsData;

  using process_term_f = void (FieldData::*)(Posting&, doc_id_t,
                                             const OffsAttr*);

  void reset(doc_id_t doc_id);

  void new_term(Posting& p, doc_id_t did, const OffsAttr* offs);
  void add_term(Posting& p, doc_id_t did, const OffsAttr* offs);

  void new_term_random_access(Posting& p, doc_id_t did, const OffsAttr* offs);
  void add_term_random_access(Posting& p, doc_id_t did, const OffsAttr* offs);

  static constexpr process_term_f kTermProcessingTables[2][2] = {
    {&FieldData::add_term, &FieldData::new_term},
    {&FieldData::add_term_random_access, &FieldData::new_term_random_access}};

  bool prox_random_access() const noexcept {
    return kTermProcessingTables[1] == _proc_table;
  }

  // Norm column for this field (nullptr when the field has no Norm feature
  // or when no columnstore Writer was provided). Owned by the segment's
  // columnstore::Writer; we hold a raw pointer.
  columnstore::NormColumnWriter* _norm_writer = nullptr;
  mutable FieldMeta _meta;
  Postings _terms;
  byte_block_pool::inserter* _byte_writer;
  int_block_pool::inserter* _int_writer;
  const process_term_f* _proc_table;
  FieldStats _stats;
  IndexFeatures _requested_features{};
  doc_id_t _last_doc{doc_limits::invalid()};
  uint32_t _pos;
  uint32_t _last_pos;
  uint32_t _offs;
  uint32_t _last_start_offs;
  bool _seen{false};
};

class FieldsData : util::Noncopyable {
 private:
  struct FieldEq : ValueRefEq<FieldData*> {
    using is_transparent = void;
    using Self::operator();

    bool operator()(const Ref& lhs,
                    const hashed_string_view& rhs) const noexcept {
      return lhs.ref->meta().name == rhs;
    }

    bool operator()(const hashed_string_view& lhs,
                    const Ref& rhs) const noexcept {
      return this->operator()(rhs, lhs);
    }
  };

  using Fields = std::deque<FieldData, ManagedTypedAllocator<FieldData>>;
  using FieldsMap = flat_hash_set<FieldEq>;

 public:
  using postings_ref_t = std::vector<const Posting*>;

  FieldsData(IResourceManager& rm, IndexFeatures scorers_features);

  // Sets the per-segment columnstore Writer used to open NormColumnWriters
  // for fields with the Norm feature. Set once at SegmentWriter init; the
  // pointer must outlive every FieldData created via emplace.
  void SetColumnstore(columnstore::Writer* w) noexcept { _columnstore = w; }

  FieldData* emplace(const hashed_string_view& name,
                     IndexFeatures index_features);

  size_t memory_active() const noexcept;
  size_t memory_reserved() const noexcept;

  size_t size() const { return _fields.size(); }
  void flush(FieldWriter& fw, FlushState& state);
  void reset() noexcept;

 private:
  Fields _fields;
  FieldsMap _fields_map;
  postings_ref_t _sorted_postings;
  std::vector<const FieldData*> _sorted_fields;
  byte_block_pool _byte_pool;
  byte_block_pool::inserter _byte_writer;
  int_block_pool _int_pool;  // FIXME why don't to use std::vector<size_t>?
  int_block_pool::inserter _int_writer;
  IndexFeatures _scorers_features;
  // Optional per-segment columnstore Writer; when non-null,
  // FieldData::emplace forwards it to the FieldData ctor so fields with
  // Norm features get a NormColumnWriter on it.
  columnstore::Writer* _columnstore = nullptr;
};

}  // namespace irs

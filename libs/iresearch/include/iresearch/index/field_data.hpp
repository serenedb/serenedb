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
#include "iresearch/index/buffered_column.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/postings.hpp"
#include "iresearch/utils/block_pool.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/hash_set_utils.hpp"

namespace irs {

struct FieldWriter;
class Tokenizer;
class Format;
struct Directory;
class Comparer;
struct FlushState;

namespace analysis {
class Analyzer;
}  // namespace analysis

using int_block_pool = BlockPool<size_t, 8192, ManagedTypedAllocator<size_t>>;

// Represents a mapping between cached column data
// and a pointer to column identifier
class CachedColumn final : public ColumnReader {
 public:
  CachedColumn(field_id* id, ColumnInfo info, ColumnFinalizer finalizer,
               IResourceManager& rm) noexcept
    : _id{id}, _stream{info, rm}, _finalizer{std::move(finalizer)} {}

  BufferedColumn& Stream() noexcept { return _stream; }
  const BufferedColumn& Stream() const noexcept { return _stream; }

  void Flush(ColumnstoreWriter& writer, DocMapView docmap,
             BufferedColumn::BufferedValues& buffer) {
    auto finalizer = ColumnFinalizer{
      [this, payload_finalizer =
               _finalizer.ExtractPayloadFinalizer()](DataOutput& out) mutable {
        irs::BytesOutput writer{_payload};
        payload_finalizer(writer);
        if (_payload.size() > 4) {
          out.WriteBytes(_payload.data(), _payload.size());
          _payload = _payload.substr(sizeof(uint32_t));  // skip size
        }
      },
      [this, name_finalizer = _finalizer.ExtractNameFinalizer()]() mutable
        -> std::string_view {
        _name = name_finalizer();
        return _name;
      },
    };

    *_id = _stream.Flush(writer, std::move(finalizer), docmap, buffer);
  }

  field_id id() const noexcept final { return *_id; }
  std::string_view name() const noexcept final { return _name; }
  bytes_view payload() const noexcept final { return _payload; }

  ResettableDocIterator::ptr iterator(ColumnHint hint) const final;
  NormReader::ptr norms() const final;

  doc_id_t size() const final {
    SDB_ASSERT(_stream.Size() < doc_limits::eof());
    return static_cast<doc_id_t>(_stream.Size());
  }

 private:
  field_id* _id;
  std::string_view _name;
  bstring _payload;
  BufferedColumn _stream;
  ColumnFinalizer _finalizer;
};

class FieldData : util::Noncopyable {
 public:
  using CachedColumns =
    std::deque<CachedColumn, ManagedTypedAllocator<CachedColumn>>;

  FieldData(std::string_view name, const FeatureInfoProvider& feature_columns,
            CachedColumns& cached_columns, IndexFeatures cached_features,
            ColumnstoreWriter& columns, byte_block_pool::inserter& byte_writer,
            int_block_pool::inserter& int_writer, IndexFeatures index_features,
            bool random_access);

  doc_id_t doc() const noexcept { return _last_doc; }

  const FieldMeta& meta() const noexcept { return _meta; }

  // returns false if field contains indexed data
  bool empty() const noexcept { return !doc_limits::valid(_last_doc); }

  bool invert(Tokenizer& tokens, doc_id_t id);

  const FieldStats& stats() const noexcept { return _stats; }

  bool seen() const noexcept { return _seen; }
  void seen(bool value) noexcept { _seen = value; }

  IndexFeatures requested_features() const noexcept {
    return _requested_features;
  }

  void compute_features() const {
    for (auto& entry : _features) {
      SDB_ASSERT(entry.handler);
      entry.handler->write(_stats, doc(), *entry.writer);
    }
  }

  bool has_features() const noexcept { return !_features.empty(); }

 private:
  friend class TermIteratorImpl;
  friend class DocIteratorImpl;
  friend class SortingDocIteratorImpl;
  friend class FieldsData;

  struct FeatureInfo {
    FeatureInfo(FeatureWriter::ptr handler, ColumnOutput& writer)
      : handler{std::move(handler)}, writer{&writer} {}

    FeatureWriter::ptr handler;
    ColumnOutput* writer;
  };

  using process_term_f = void (FieldData::*)(Posting&, doc_id_t,
                                             const OffsAttr*);

  void reset(doc_id_t doc_id);

  void new_term(Posting& p, doc_id_t did, const OffsAttr* offs);
  void add_term(Posting& p, doc_id_t did, const OffsAttr* offs);

  void new_term_random_access(Posting& p, doc_id_t did, const OffsAttr* offs);
  void add_term_random_access(Posting& p, doc_id_t did, const OffsAttr* offs);

  static constexpr process_term_f kTermProcessingTables[2][2] = {
    // sequential access: [0] - new term, [1] - add term
    {&FieldData::add_term, &FieldData::new_term},
    // random access: [0] - new term, [1] - add term
    {&FieldData::add_term_random_access, &FieldData::new_term_random_access}};

  bool prox_random_access() const noexcept {
    return kTermProcessingTables[1] == _proc_table;
  }

  mutable std::vector<FeatureInfo> _features;
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

  explicit FieldsData(const FeatureInfoProvider& feature_info,
                      FieldData::CachedColumns& cached_columns,
                      IndexFeatures cached_features,
                      const Comparer* comparator);

  const Comparer* comparator() const noexcept { return _comparator; }

  FieldData* emplace(const hashed_string_view& name,
                     IndexFeatures index_features, ColumnstoreWriter& columns);

  //////////////////////////////////////////////////////////////////////////////
  /// @return approximate amount of memory actively in-use by this instance
  //////////////////////////////////////////////////////////////////////////////
  size_t memory_active() const noexcept;

  //////////////////////////////////////////////////////////////////////////////
  /// @return approximate amount of memory reserved by this instance
  //////////////////////////////////////////////////////////////////////////////
  size_t memory_reserved() const noexcept;

  size_t size() const { return _fields.size(); }
  void flush(FieldWriter& fw, FlushState& state);
  void reset() noexcept;

 private:
  const Comparer* _comparator;
  const FeatureInfoProvider* _feature_info;
  Fields _fields;                             // pointers remain valid
  FieldData::CachedColumns* _cached_columns;  // pointers remain valid
  FieldsMap _fields_map;
  postings_ref_t _sorted_postings;
  std::vector<const FieldData*> _sorted_fields;
  byte_block_pool _byte_pool;
  byte_block_pool::inserter _byte_writer;
  int_block_pool _int_pool;  // FIXME why don't to use std::vector<size_t>?
  int_block_pool::inserter _int_writer;
  IndexFeatures _cached_features;
};

}  // namespace irs

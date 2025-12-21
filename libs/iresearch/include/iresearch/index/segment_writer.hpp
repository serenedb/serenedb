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

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

#include "basics/containers/bitset.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/index/buffered_column.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_data.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class Comparer;
struct SegmentMeta;

// Defines how the inserting field should be processed
enum class Action {
  // Field should be indexed only
  // Field must satisfy 'Field' concept
  INDEX = 1,

  // Field should be stored only
  // Field must satisfy 'Attribute' concept
  STORE = 2,

  // Field should be stored in sorted order
  // Field must satisfy 'Attribute' concept
  StoreSorted = 4
};

ENABLE_BITMASK_ENUM(Action);

struct DocsMask final {
  ManagedBitset set;
  uint32_t count{0};
};

// Interface for an index writer over a directory
// an object that represents a single ongoing transaction
// non-thread safe
class SegmentWriter final : public ColumnProvider, util::Noncopyable {
 private:
  // Disallow using public constructor
  struct ConstructToken final {
    explicit ConstructToken() = default;
  };

 public:
  struct DocContext final {
    uint64_t tick{0};
    size_t query_id{writer_limits::kInvalidOffset};
  };

  static std::unique_ptr<SegmentWriter> make(
    Directory& dir, const SegmentWriterOptions& options);

  // begin document-write transaction
  // Return first doc_id_t in batch as per doc_limits
  // TODO(Dronplane): does it make sense to have single doc version?
  doc_id_t begin(DocContext ctx, doc_id_t batch_size = 1);

  void ResetNorms() noexcept {
    _doc.clear();  // clear norm fields
  }

  template<Action A, typename Field>
  bool insert(Field&& field) {
    // user should check return of begin() != eof()
    SDB_ASSERT(LastDocId() < doc_limits::eof());
    doc_id_t doc = LastDocId();
    return insert<A>(std::forward<Field>(field), doc);
  }

  template<Action A, typename Field>
  bool insert(Field&& field, doc_id_t doc) {
    if (!_valid) [[unlikely]] {
      return false;
    }
    SDB_ASSERT(doc <= LastDocId());
    if constexpr (Action::INDEX == A) {
      return index(std::forward<Field>(field), doc);
    } else if constexpr (Action::STORE == A) {
      return store(std::forward<Field>(field), doc);
    } else if constexpr (Action::StoreSorted == A) {
      return store_sorted(std::forward<Field>(field), doc);
    } else if constexpr ((Action::INDEX | Action::STORE) == A) {
      return index_and_store<false>(std::forward<Field>(field), doc);
    } else if constexpr ((Action::INDEX | Action::StoreSorted) == A) {
      return index_and_store<true>(std::forward<Field>(field), doc);
    } else {
      static_assert(false);
    }
  }

  // Commit document-write transaction
  void commit() {
    if (_valid) {
      finish();
    } else {
      rollback();
    }
  }

  // Return approximate amount of memory actively in-use by this instance
  size_t memory_active() const noexcept;

  // Return approximate amount of memory reserved by this instance
  size_t memory_reserved() const noexcept;

  // doc_id the document id as returned by begin(...)
  // Return success
  bool remove(doc_id_t doc_id) noexcept;

  // Rollback document-write transaction,
  // implicitly noexcept since we reserve memory in 'begin'
  void rollback() noexcept {
    // mark as removed since not fully inserted
    for (doc_id_t idx = 0; idx < _batch_size; ++idx) {
      // TODO(Dronplane): make remove also batch aware? But it is only for rollback so maybe ok as is
      remove(LastDocId() - idx);
    }
    _valid = false;
  }

  std::span<DocContext> docs_context() noexcept { return _docs_context; }

  [[nodiscard]] DocMap flush(IndexSegment& segment, DocsMask& docs_mask);

  const std::string& name() const noexcept { return _seg_name; }
  size_t buffered_docs() const noexcept { return _docs_context.size(); }
  bool initialized() const noexcept { return _initialized; }
  bool valid() const noexcept { return _valid; }
  void reset() noexcept;
  void reset(const SegmentMeta& meta);

  doc_id_t LastDocId() const noexcept {
    SDB_ASSERT(buffered_docs() <= doc_limits::eof());
    return doc_limits::min() + static_cast<doc_id_t>(buffered_docs()) - 1;
  }

  SegmentWriter(ConstructToken, Directory& dir,
                const SegmentWriterOptions& options) noexcept;

  const ColumnReader* column(field_id id) const final {
    const auto it = _column_ids.find(id);
    if (it != _column_ids.end()) {
      return it->second;
    }
    return nullptr;
  }

 private:
  struct StoredColumn : util::Noncopyable {
    struct HashEq {
      using is_transparent = void;

      size_t operator()(const hashed_string_view& str) const {
        return str.Hash();
      }

      size_t operator()(const StoredColumn& column) const {
        return column.name_hash;
      }

      bool operator()(const StoredColumn& column,
                      const hashed_string_view& str) const {
        return column.name == str;
      }

      bool operator()(const StoredColumn& l, const StoredColumn& r) const {
        return l.name == r.name;
      }
    };

    StoredColumn(const hashed_string_view& name, ColumnstoreWriter& columnstore,
                 IResourceManager& rm, const ColumnInfoProvider& column_info,
                 std::deque<CachedColumn, ManagedTypedAllocator<CachedColumn>>&
                   cached_columns,
                 bool cache);

    std::string name;
    size_t name_hash;
    ColumnOutput* writer{};
    CachedColumn* cached{};
    mutable field_id id{field_limits::invalid()};
  };

  // FIXME consider refactor this
  // we can't use flat_hash_set as stored_column stores 'this' in non-cached
  // case
  using stored_columns =
    absl::node_hash_set<StoredColumn, StoredColumn::HashEq,
                        StoredColumn::HashEq,
                        ManagedTypedAllocator<StoredColumn>>;

  struct SortedColumn : util::Noncopyable {
    explicit SortedColumn(const ColumnInfoProvider& column_info,
                          ColumnFinalizer finalizer,
                          IResourceManager& rm) noexcept
      : stream{column_info({}), rm},  // compression for sorted column
        finalizer{std::move(finalizer)} {}

    field_id id{field_limits::invalid()};
    irs::BufferedColumn stream;
    ColumnFinalizer finalizer;
  };

  bool index(const hashed_string_view& name, doc_id_t doc,
             IndexFeatures index_features, Tokenizer& tokens);

  template<typename Writer>
  bool store_sorted(const doc_id_t doc, Writer& writer) {
    SDB_ASSERT(doc < doc_limits::eof());

    if (!_fields.comparator()) [[unlikely]] {
      // can't store sorted field without a comparator
      _valid = false;
      return false;
    }

    auto& out = sorted_stream(doc);

    if (writer.Write(out)) [[likely]] {
      return true;
    }

    out.Reset();

    _valid = false;
    return false;
  }

  template<typename Writer>
  bool store(const hashed_string_view& name, const doc_id_t doc,
             Writer& writer) {
    SDB_ASSERT(doc < doc_limits::eof());

    auto& out = stream(name, doc);

    if (writer.Write(out)) [[likely]] {
      return true;
    }

    out.Reset();

    _valid = false;
    return false;
  }

  template<typename Field>
  bool store(Field&& field, doc_id_t doc) {
    const hashed_string_view field_name{
      static_cast<std::string_view>(field.Name())};

    return store(field_name, doc, field);
  }

  template<typename Field>
  bool store_sorted(Field&& field, doc_id_t doc) {
    return store_sorted(doc, field);
  }

  template<typename Field>
  bool index(Field&& field, doc_id_t doc) {
    const hashed_string_view field_name{
      static_cast<std::string_view>(field.Name())};

    auto& tokens = static_cast<Tokenizer&>(field.GetTokens());
    const IndexFeatures index_features = field.GetIndexFeatures();
    return index(field_name, doc, index_features, tokens);
  }

  template<bool Sorted, typename Field>
  bool index_and_store(Field&& field, doc_id_t doc) {
    const hashed_string_view field_name{
      static_cast<std::string_view>(field.Name())};

    auto& tokens = static_cast<Tokenizer&>(field.GetTokens());
    const IndexFeatures index_features = field.GetIndexFeatures();

    if (!index(field_name, doc, index_features, tokens)) [[unlikely]] {
      return false;  // indexing failed
    }

    if constexpr (Sorted) {
      return store_sorted(doc, field);
    }

    return store(field_name, doc, field);
  }

  // Returns stream for storing attributes in sorted order
  ColumnOutput& sorted_stream(const doc_id_t doc_id) {
    _sort.stream.Prepare(doc_id);
    return _sort.stream;
  }

  // Returns stream for storing attributes
  ColumnOutput& stream(const hashed_string_view& name, const doc_id_t doc);

  // Finishes document
  void finish() {
    for (const auto* field : _doc) {
      field->compute_features();
    }
  }

  // Flushes indexed fields to directory
  void FlushFields(FlushState& state);

  TrackingDirectory _dir;
  ScorersView _scorers;
  std::deque<CachedColumn, ManagedTypedAllocator<CachedColumn>>
    _cached_columns;  // pointers remain valid
  absl::flat_hash_map<field_id, CachedColumn*> _column_ids;
  SortedColumn _sort;
  ManagedVector<DocContext> _docs_context;
  // invalid/removed doc_ids (e.g. partially indexed due to indexing failure)
  DocsMask _docs_mask;
  FieldsData _fields;
  stored_columns _columns;
  std::vector<const FieldData*> _doc;  // document fields
  std::string _seg_name;
  FieldWriter::ptr _field_writer;
  const ColumnInfoProvider* _column_info;
  ColumnstoreWriter::ptr _col_writer;
  doc_id_t _batch_size = 0;
  bool _initialized = false;
  bool _valid = true;
};

}  // namespace irs

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

#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/inverter/fields_inverter.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

struct SegmentMeta;

struct DocsMask final {
  ManagedBitset set;
  uint32_t count{0};
};

class SegmentWriter final : public NormProvider, util::Noncopyable {
 private:
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

  // Begin a batch. Returns first valid doc_id in the batch.
  doc_id_t begin(DocContext ctx, doc_id_t batch_size = 1);

  bool InsertKeywordBlock(FieldInverter& slot,
                          std::span<const duckdb::string_t> values,
                          std::span<const doc_id_t> docs) {
    SDB_ASSERT(values.size() == docs.size());
    AssertDocRange(docs);
    return InvertChecked([&] { return slot.InvertKeywordBlock(values, docs); });
  }

  template<typename T>
  bool InsertKeywordBlock(FieldInverter& slot, std::span<const T> values,
                          doc_id_t first_doc) {
    SDB_ASSERT(values.empty() || !_valid ||
               (first_doc >= _batch_first_doc_id &&
                first_doc + values.size() - 1 <= LastDocId()));
    return InvertChecked(
      [&] { return slot.InvertKeywordBlock(values, first_doc); });
  }

  bool InsertConstantBlock(FieldInverter& slot, bytes_view term,
                           std::span<const doc_id_t> docs) {
    AssertDocRange(docs);
    return InvertChecked([&] { return slot.InvertConstantBlock(term, docs); });
  }

  template<typename... Args>
  bool InsertKeywordBlock(field_id id, IndexFeatures index_features,
                          Args&&... args) {
    auto* slot = Field(id, index_features);
    if (!slot) [[unlikely]] {
      _valid = false;
      return false;
    }
    return InsertKeywordBlock(*slot, std::forward<Args>(args)...);
  }

  bool InsertConstantBlock(field_id id, IndexFeatures index_features,
                           bytes_view term, std::span<const doc_id_t> docs) {
    auto* slot = Field(id, index_features);
    if (!slot) [[unlikely]] {
      _valid = false;
      return false;
    }
    return InsertConstantBlock(*slot, term, docs);
  }

  bool InsertNullBlock(field_id id, IndexFeatures index_features,
                       std::span<const doc_id_t> docs) {
    return InsertConstantBlock(id, index_features,
                               ViewCast<byte_type>(kNullTerm), docs);
  }

  // Block ingest: `runs` segments the batch's tokens by doc (whole
  // field-major columns in one call).
  bool InsertBlock(field_id id, IndexFeatures index_features, TokenBatch& batch,
                   std::span<const DocRun> runs) {
    auto* slot = Field(id, index_features);
    if (!slot) [[unlikely]] {
      _valid = false;
      return false;
    }
    return InsertBlock(*slot, batch, runs);
  }

  bool InsertBlock(FieldInverter& slot, std::span<const duckdb::string_t> terms,
                   std::span<const doc_id_t> docs, uint32_t tokens_per_doc) {
    AssertDocRange(docs);
    return InvertChecked(
      [&] { return slot.InvertBlock(terms, docs, tokens_per_doc); });
  }

  // Slot-direct flush for column-based writers: the slot comes from Field()
  // once per column, so repeated flushes skip the lookup and feature check.
  bool InsertBlock(FieldInverter& slot, TokenBatch& batch,
                   std::span<const DocRun> runs) {
    return InvertChecked([&] {
      AssertRunRange(runs);
      return slot.InvertBlock(batch, runs);
    });
  }

  // Per-column slot acquisition: resolves (creating on first sight) and
  // validates the requested features once; null on a feature mismatch.
  FieldInverter* Field(field_id id, IndexFeatures index_features) {
    auto* slot = _fields.Emplace(id, index_features);
    return IsSubsetOf(index_features, slot->RequestedFeatures()) ? slot
                                                                 : nullptr;
  }

  duckdb::Allocator& Allocator() const noexcept { return _fields.Allocator(); }

  void commit() {
    if (!_valid) {
      rollback();
    }
  }

  size_t memory_active() const noexcept;
  size_t memory_reserved() const noexcept;

  bool remove(doc_id_t doc_id) noexcept;

  void rollback() noexcept {
    const auto batch_last_doc_id = LastDocId();
    for (auto id = _batch_first_doc_id; id <= batch_last_doc_id; ++id) {
      remove(id);
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

  doc_id_t FirstBatchDocId() const noexcept {
    SDB_ASSERT(doc_limits::valid(_batch_first_doc_id));
    return _batch_first_doc_id;
  }

  SegmentWriter(ConstructToken, Directory& dir,
                const SegmentWriterOptions& options) noexcept;

  NormReader::ptr norms(field_id id) const final {
    if (_col_reader == nullptr) {
      return {};
    }
    const auto* col = _col_reader->NormColumn(id);
    if (col == nullptr) {
      return {};
    }
    return MakePersistedNormReader(*col);
  }
  ColWriter* GetColWriter() noexcept { return _col_writer.get(); }

  // Owning per-op encoding config (the snapshot's InvertedIndex): flush encodes
  // against the schema the rows were written under, not the live catalog.
  // Owning keeps a mid-write segment's index alive for the operation.
  void SetFieldOptions(
    std::shared_ptr<const IndexFieldOptions> options) noexcept;

  // Per-op override if set, else the construction-time fallback.
  const IndexFieldOptions* ActiveFieldOptions() const noexcept {
    return _field_options ? _field_options.get() : _fallback_field_options;
  }

 private:
  void FlushFields(FlushState& state,
                   std::span<const BasicTermReader* const> extra);

  // Reset body that keeps _field_options; reset() drops them, reset(meta)
  // keeps.
  void ResetState() noexcept;

  TrackingDirectory _dir;
  ScorerPtr _scorer;
  std::unique_ptr<ColReader> _col_reader;
  ManagedVector<DocContext> _docs_context;
  DocsMask _docs_mask;
  FieldsInverter _fields;
  std::string _seg_name;
  std::unique_ptr<burst_trie::FieldWriter> _field_writer;
  duckdb::DatabaseInstance& _db;
  // Non-owning fallback, owned by the IndexWriter; used when no override is
  // set.
  const IndexFieldOptions* _fallback_field_options = nullptr;
  // Owning per-op override; null falls back to _fallback_field_options.
  std::shared_ptr<const IndexFieldOptions> _field_options;
  std::unique_ptr<ColWriter> _col_writer;
  template<typename Invert>
  bool InvertChecked(Invert&& invert) {
    if (!_valid) [[unlikely]] {
      return false;
    }
    if (invert()) {
      return true;
    }
    _valid = false;
    return false;
  }

  void AssertDocRange([[maybe_unused]] std::span<const doc_id_t> docs) const {
    SDB_ASSERT(
      docs.empty() || !_valid ||
      (docs.front() >= _batch_first_doc_id && docs.back() <= LastDocId()));
  }

  void AssertRunRange([[maybe_unused]] std::span<const DocRun> runs) const {
    for ([[maybe_unused]] const auto& run : runs) {
      SDB_ASSERT(run.doc == DocRun::kOpenValue ||
                 (run.doc >= _batch_first_doc_id && run.doc <= LastDocId()));
    }
  }

  doc_id_t _batch_first_doc_id = doc_limits::eof();
  bool _initialized = false;
  bool _valid = true;
};

}  // namespace irs

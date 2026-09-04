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

  // WithField/WithTokens are the only ingest doors: the FieldInverter
  // Invert* entries are the vocabulary, run under the writer's failure
  // protocol (feature mismatch or inversion failure poisons the writer).
  // WithField runs `func(slot)` against the resolved slot; WithTokens
  // serves streamed fills against the pooled TokenSink.
  template<typename Func>
  bool WithField(field_id id, IndexFeatures index_features, Func&& func) {
    auto* slot = Field(id, index_features);
    if (!slot) [[unlikely]] {
      return false;
    }
    return WithSlot(*slot, std::forward<Func>(func));
  }

  // Streamed sibling of WithField: `func(slot, sink)` runs against the
  // resolved slot and the writer-pooled TokenSink wired to flush batches
  // into it under the same failure protocol (a failed flush poisons the
  // writer; later flushes no-op and the call reports false). Slot config
  // (Configure with the producer's traits) belongs at the top of `func`,
  // before the first emit. Per-value stored blobs, if any, go to `store`.
  template<typename Func>
  bool WithTokens(field_id id, IndexFeatures index_features, StoreSink* store,
                  Func&& func) {
    auto* slot = Field(id, index_features);
    if (!slot) [[unlikely]] {
      return false;
    }
    if (!_token_sink) {
      _token_sink = std::make_unique<TokenSink>(Allocator());
    }
    TokensTarget target{*this, *slot};
    _token_sink->Discard();
    _token_sink->Bind(target, store);
    try {
      func(*slot, *_token_sink);
      _token_sink->Finish();
    } catch (...) {
      _token_sink->Discard();
      throw;
    }
    return _valid;
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

  // Forwarded operator-cardinality reserve hint; observed history wins.
  void SeedTermsHistory(field_id id, uint32_t expected_terms) {
    _fields.SeedHistory(id, expected_terms);
  }

 private:
  // Per-column slot acquisition: resolves (creating on first sight) and
  // validates the requested features once; a feature mismatch poisons the
  // writer and returns null.
  FieldInverter* Field(field_id id, IndexFeatures index_features) {
    auto* slot = _fields.Emplace(id, index_features);
    if (!IsSubsetOf(index_features, slot->RequestedFeatures())) [[unlikely]] {
      _valid = false;
      return nullptr;
    }
    return slot;
  }

  template<typename Func>
  bool WithSlot(FieldInverter& slot, Func&& func) {
    if (!_valid) [[unlikely]] {
      return false;
    }
    if (!func(slot)) [[unlikely]] {
      _valid = false;
      return false;
    }
    SDB_ASSERT(!doc_limits::valid(slot.LastDoc()) ||
               slot.LastDoc() <= LastDocId());
    return true;
  }

  struct TokensTarget final : TokenConsumer {
    TokensTarget(SegmentWriter& writer, FieldInverter& slot) noexcept
      : writer{&writer}, slot{&slot} {}

    void Consume(TokenBatch& batch, DocRuns runs) final {
      writer->WithSlot(*slot, [&](FieldInverter& fld) {
        return fld.InvertBlock(batch, runs);
      });
    }

    SegmentWriter* writer;
    FieldInverter* slot;
  };

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
  std::unique_ptr<TokenSink> _token_sink;
  std::string _seg_name;
  std::unique_ptr<burst_trie::FieldWriter> _field_writer;
  duckdb::DatabaseInstance& _db;
  // Non-owning fallback, owned by the IndexWriter; used when no override is
  // set.
  const IndexFieldOptions* _fallback_field_options = nullptr;
  // Owning per-op override; null falls back to _fallback_field_options.
  std::shared_ptr<const IndexFieldOptions> _field_options;
  std::unique_ptr<ColWriter> _col_writer;
  doc_id_t _batch_first_doc_id = doc_limits::eof();
  bool _initialized = false;
  bool _valid = true;
};

}  // namespace irs

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
////////////////////////////////////////////////////////////////////////////////

#include "columnstore/legacy_compat.hpp"

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <deque>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/index/column_finalizer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/prev_doc.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

// All wrapper classes live in plain `irs` so unqualified
// `ColumnReader` / `ColumnstoreReader` / `ResettableDocIterator` resolve
// to the LEGACY types in `::irs::` rather than the new-cs ones in
// `::irs::columnstore::` (which are `final`). The public factories at
// the bottom of the file go back into `irs::columnstore::legacy`.
namespace irs {
namespace {

duckdb::DatabaseInstance& TestOnlyDatabase() {
  static std::unique_ptr<duckdb::DuckDB> kDB = []() {
    duckdb::DBConfig cfg;
    cfg.options.access_mode = duckdb::AccessMode::AUTOMATIC;
    return std::make_unique<duckdb::DuckDB>(":memory:", &cfg);
  }();
  return *kDB->instance;
}

// Per-column legacy-shaped header bytes (whatever the ColumnFinalizer's
// payload finalizer produced -- empty for STORE columns, a 22-byte
// NormHeader for norm columns) live in a small side file
// `<segment>.lh` written at LegacyWriter::commit() time and read at
// LegacyReader::prepare() time. `merge_writer.cpp` consumes this via
// `Norm::MakeWriter({hdrs.data(), hdrs.size()})` for norm consolidation;
// without it the merge would crash on a null FeatureWriter.
//
// Format on disk:
//   uint32_t num_entries
//   repeated num_entries times:
//     uint32_t field_id
//     uint32_t length
//     length bytes of header payload
//
// Storing the bytes in the directory rather than in a process-wide map
// keeps tests isolated from each other: the OS recycles MemoryDirectory
// addresses across tests, and a global address-keyed registry would let
// stale norm headers bleed from one test into the next and crash the
// merge driver further down the run.
constexpr std::string_view kLegacyHeaderExt = "lh";

std::string LegacyHeaderFileName(std::string_view segment) {
  std::string name;
  name.reserve(segment.size() + 1 + kLegacyHeaderExt.size());
  name.append(segment);
  name.push_back('.');
  name.append(kLegacyHeaderExt);
  return name;
}

void WriteLegacyHeaders(
  Directory& dir, std::string_view segment,
  const std::vector<std::pair<field_id, bstring>>& headers) {
  if (headers.empty()) {
    return;
  }
  auto out = dir.create(LegacyHeaderFileName(segment));
  if (!out) {
    return;
  }
  out->WriteU32(static_cast<uint32_t>(headers.size()));
  for (const auto& [fid, bytes] : headers) {
    out->WriteU32(static_cast<uint32_t>(fid));
    out->WriteU32(static_cast<uint32_t>(bytes.size()));
    if (!bytes.empty()) {
      out->WriteBytes(bytes.data(), bytes.size());
    }
  }
}

absl::flat_hash_map<field_id, bstring> ReadLegacyHeaders(
  const Directory& dir, std::string_view segment) {
  absl::flat_hash_map<field_id, bstring> result;
  bool exists = false;
  if (!dir.exists(exists, LegacyHeaderFileName(segment)) || !exists) {
    return result;
  }
  auto in = dir.open(LegacyHeaderFileName(segment), IOAdvice::RANDOM);
  if (!in) {
    return result;
  }
  auto read_u32 = [&]() {
    byte_type buf[sizeof(uint32_t)];
    in->ReadBytes(buf, sizeof(buf));
    const auto* p = buf;
    return irs::read<uint32_t>(p);
  };
  const auto count = read_u32();
  result.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    const auto fid = static_cast<field_id>(read_u32());
    const auto len = read_u32();
    bstring bytes;
    bytes.resize(len);
    if (len > 0) {
      in->ReadBytes(bytes.data(), len);
    }
    result.emplace(fid, std::move(bytes));
  }
  return result;
}

// ---------------------------------------------------------------------------
// Norm reader for legacy-compat columns. Norm bytes (1/2/4 BE-packed
// values per row) live inside our BLOB column; the 22-byte NormHeader
// captured from the column finalizer carries the encoding + sum/non-zero
// stats. Backs `LegacyColumnReader::norms()` so BM25 / TFIDF / Wand /
// LM-* / DFI / Indri scorers find a working norm reader on consolidated
// + freshly-written segments alike.
class LegacyNormReader : public NormReader {
 public:
  LegacyNormReader(const irs::columnstore::ColumnReader& reader, NormHeader hdr)
    : _cursor{reader.NewPointCursor()},
      _hdr{hdr},
      _vec{duckdb::LogicalType::BLOB, /*capacity=*/1} {}

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    for (size_t i = 0; i < docs.size(); ++i) {
      values[i] = GetOne(docs[i]);
    }
  }

  uint32_t Get(doc_id_t doc) noexcept final { return GetOne(doc); }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    for (scores_size_t i = 0; i != kPostingBlock; ++i) {
      values[i] = GetOne(docs[i]);
    }
  }

  score_t GetAvg() const noexcept final {
    if (_hdr.NonZeroCount() == 0) {
      return {};
    }
    return static_cast<double>(_hdr.Sum()) /
           static_cast<double>(_hdr.NonZeroCount());
  }

 private:
  uint32_t GetOne(doc_id_t doc) noexcept {
    SDB_ASSERT(doc >= doc_limits::min());
    const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
    _cursor.FetchRow(row, _vec, /*out_idx=*/0);
    auto* slots = duckdb::FlatVector::GetData<duckdb::string_t>(_vec);
    const auto& s = slots[0];
    if (s.GetSize() == 0) {
      return 0;
    }
    return Norm::Read(
      bytes_view{reinterpret_cast<const byte_type*>(s.GetData()),
                 static_cast<size_t>(s.GetSize())});
  }

  irs::columnstore::ColumnReader::PointReadCursor _cursor;
  NormHeader _hdr;
  duckdb::Vector _vec;
};

// ---------------------------------------------------------------------------
// Per-column write-side state. Buffers per-doc bytes, flushes one BLOB
// row to the new cs ColumnWriter per `Prepare(new_doc)` boundary.
// ---------------------------------------------------------------------------
struct LegacyColumn final : ColumnOutput {
  irs::columnstore::ColumnWriter* col = nullptr;
  field_id field = field_limits::invalid();
  std::string pending_bytes;
  doc_id_t pending_doc = doc_limits::invalid();
  bool any_emitted = false;
  // HNSW vector columns: when ColumnInfo.hnsw_info was set, the legacy
  // writer is fed raw float32 bytes per doc but the new cs needs them
  // as ARRAY<FLOAT, dim> rows. _hnsw_dim != 0 selects the ARRAY flush
  // path; _hnsw_dim == 0 keeps the plain BLOB path.
  uint32_t hnsw_dim = 0;
  std::deque<std::string> live_bytes;
  // The ColumnFinalizer's payload lambda may capture state that other
  // parts of the merge pipeline still hold raw pointers into (notably
  // `buffered_column->_hnsw_index`, which is a raw pointer into a
  // unique_ptr<HNSWIndexWriter> captured by the lambda at merge time).
  // Keep the finalizer alive for the column's lifetime so those raw
  // pointers don't dangle while writes are still happening; the new cs
  // doesn't actually invoke `Finalize()` (HNSW graph is serialised
  // via slot 102 in the .cs footer instead), the storage here is
  // purely lifetime-extension.
  ColumnFinalizer finalizer;

  void Reset() final {
    pending_bytes.clear();
    pending_doc = doc_limits::invalid();
  }

  void Prepare(doc_id_t doc) final {
    // Only reset the staging buffer when crossing a doc boundary.
    // FormatTestCase.columns_rw_same_col_empty_repeat (and any other
    // caller that re-pulls the same column for an already-prepared doc)
    // expects bytes already written for `doc` to survive a re-Prepare
    // for the same key -- the legacy ColumnstoreWriter exposed this
    // shape, treating duplicate Prepare(same_doc) as a no-op.
    if (doc_limits::valid(pending_doc) && pending_doc != doc) {
      Flush();
      pending_bytes.clear();
    }
    pending_doc = doc;
  }

  void WriteByte(byte_type b) final {
    pending_bytes.push_back(static_cast<char>(b));
  }
  void WriteBytes(const byte_type* b, size_t n) final {
    pending_bytes.append(reinterpret_cast<const char*>(b), n);
  }

  void FlushOnCommit() {
    if (doc_limits::valid(pending_doc)) {
      Flush();
    }
  }

 private:
  void Flush() {
    SDB_ASSERT(col != nullptr);
    SDB_ASSERT(doc_limits::valid(pending_doc));
    const uint64_t row = static_cast<uint64_t>(pending_doc) - doc_limits::min();
    if (hnsw_dim != 0) {
      // ARRAY<FLOAT, hnsw_dim>: parse pending_bytes as `hnsw_dim`
      // packed float32 values, materialise them into the parent
      // vector's child slot at `row`. The new cs ColumnWriter handles
      // the array-level validity + child storage; HNSWWriter (attached
      // separately on Writer::AttachHNSW) is co-fed by ColumnWriter.
      SDB_ASSERT(pending_bytes.size() == hnsw_dim * sizeof(float));
      const auto array_type =
        duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, hnsw_dim);
      duckdb::Vector staging_vec{array_type, /*capacity=*/1};
      duckdb::FlatVector::ValidityMutable(staging_vec).SetAllValid(1);
      auto& child = duckdb::ArrayVector::GetEntry(staging_vec);
      auto* fp = duckdb::FlatVector::GetDataMutable<float>(child);
      std::memcpy(fp, pending_bytes.data(), hnsw_dim * sizeof(float));
      duckdb::FlatVector::ValidityMutable(child).SetAllValid(hnsw_dim);
      col->Append(row, staging_vec, /*count=*/1);
    } else {
      auto& owned = live_bytes.emplace_back(std::move(pending_bytes));
      duckdb::Vector staging_vec{duckdb::LogicalType::BLOB, /*capacity=*/1};
      auto* slots =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(staging_vec);
      slots[0] =
        duckdb::string_t{owned.data(), static_cast<uint32_t>(owned.size())};
      duckdb::FlatVector::ValidityMutable(staging_vec).SetAllValid(1);
      col->Append(row, staging_vec, /*count=*/1);
    }
    any_emitted = true;
    pending_bytes.clear();
    pending_doc = doc_limits::invalid();
  }
};

class LegacyWriter final : public ColumnstoreWriter {
 public:
  void prepare(Directory& dir, const SegmentMeta& meta) final {
    // Tests reuse one ColumnstoreWriter across multiple segments and
    // expect field_ids to restart at 0 each prepare(). Reset per-segment
    // state here. Anything left over from a prior segment that wasn't
    // committed is silently dropped, mirroring legacy semantics.
    _columns.clear();
    _dir = &dir;
    _segment_name = meta.name;
    _writer = std::make_unique<irs::columnstore::Writer>(dir, meta.name,
                                                         TestOnlyDatabase());
  }

  ColumnT push_column(const ColumnInfo& info, ColumnFinalizer finalizer) final {
    SDB_ASSERT(_writer != nullptr);
    // Use the shared columnstore::Writer allocator so STORE column ids
    // don't collide with norm column ids (FieldsData also pulls from
    // this counter for new-format norm columns).
    const auto fid = _writer->AllocateColumnId();
    // Names are stored in the new cs footer per column. The legacy
    // pipeline supplies the name through the finalizer's
    // name_finalizer, called at column-finalize time. For simplicity
    // here we evaluate it eagerly at push_column; tests exercised so
    // far supply a callback that returns a captured string, so eager
    // call is safe. Empty name is preserved as-is (legacy "no-name"
    // columns -- sort and similar -- are surfaced as IsNull(name) by
    // segment_reader_impl, so we must NOT substitute a synthetic
    // string here).
    std::string name{finalizer.GetName()};
    auto col = std::make_unique<LegacyColumn>();
    col->field = fid;
    if (info.hnsw_info.has_value()) {
      // HNSW vector column: open as ARRAY<FLOAT, dim>, then attach the
      // faiss graph builder so the new cs HNSWReader is searchable
      // post-commit. The legacy writer feeds raw float32 bytes per
      // doc; LegacyColumn::Flush parses them into the array child.
      const auto dim = static_cast<uint32_t>(info.hnsw_info->d);
      col->hnsw_dim = dim;
      auto& cw = _writer->OpenColumn(
        fid, name, duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, dim));
      _writer->AttachHNSW(fid, *info.hnsw_info);
      col->col = &cw;
    } else {
      auto& cw = _writer->OpenColumn(fid, name, duckdb::LogicalType::BLOB);
      col->col = &cw;
    }
    // Keep the finalizer alive for the column's lifetime. Merge-time
    // callers (merge_writer.cpp) capture HNSWIndexWriter ownership in
    // the payload finalizer; dropping it here would dangle the raw
    // pointer that BufferedValues holds and crash the next Add().
    col->finalizer = std::move(finalizer);
    _columns.push_back(std::move(col));
    return ColumnT{fid, *_columns.back()};
  }

  bool commit(const FlushState& /*state*/) final {
    if (_writer == nullptr) {
      return false;
    }
    for (auto& c : _columns) {
      c->FlushOnCommit();
    }
    bool any_data = false;
    for (auto& c : _columns) {
      if (c->any_emitted) {
        any_data = true;
        break;
      }
    }
    if (!any_data) {
      _writer->Rollback();
      _writer.reset();
      return false;
    }
    // Capture each column's legacy header bytes via the finalizer's
    // payload writer. Norm columns produce a 26-byte
    // [u32 length=22][NormHeader] sequence here (NormHeader::Write
    // prefixes its 22-byte body with the size); STORE columns produce
    // nothing (the default finalizer is a no-op).
    //
    // The legacy ColumnstoreReader stripped the leading size during
    // payload() exposure -- merge code (`Norm::MakeWriter`) and
    // assert_format both expect the 22 raw bytes. Drop the prefix here
    // so LegacyColumnReader::payload() can hand out exactly what the
    // legacy contract delivered.
    std::vector<std::pair<field_id, bstring>> headers;
    for (auto& c : _columns) {
      if (!c->any_emitted) {
        continue;
      }
      bstring buf;
      BytesOutput out{buf};
      c->finalizer.Finalize(out);
      if (buf.size() > sizeof(uint32_t)) {
        bstring stripped{buf.begin() + sizeof(uint32_t), buf.end()};
        headers.emplace_back(c->field, std::move(stripped));
      }
    }
    if (!headers.empty() && _dir != nullptr) {
      WriteLegacyHeaders(*const_cast<Directory*>(_dir), _segment_name, headers);
    }
    _writer->Commit();
    // Don't reset _writer here -- SegmentWriter shares this Writer's
    // NormColumnWriters with its in-flight norm column readers and
    // reads through them during FlushFields, which runs *after*
    // commit() returns. The writer is replaced on the next
    // prepare(), or destroyed when LegacyWriter itself goes away.
    return true;
  }

  void rollback() noexcept final {
    if (_writer != nullptr) {
      try {
        _writer->Rollback();
      } catch (...) {
      }
      _writer.reset();
    }
    _columns.clear();
  }

  // Exposes the inner columnstore::Writer so SegmentWriter can drive
  // norm-column writes into the SAME .cs file LegacyWriter is using
  // (otherwise both would open <seg>.cs separately and the second
  // Commit would overwrite the first).
  irs::columnstore::Writer* GetUnderlyingWriter() noexcept final {
    return _writer.get();
  }

 private:
  std::unique_ptr<irs::columnstore::Writer> _writer;
  std::vector<std::unique_ptr<LegacyColumn>> _columns;
  std::string _segment_name;
  const Directory* _dir = nullptr;
};

// ---------------------------------------------------------------------------
// Read-side iterator. Walks rows 0..row_count-1 of the BLOB column and
// surfaces the bytes via PayAttr.
// ---------------------------------------------------------------------------
// Not `final`: memory::make_managed wraps in OnHeap<Base> which requires
// the base to be inheritable.
class LegacyIterator : public ResettableDocIterator {
 public:
  explicit LegacyIterator(const irs::columnstore::ColumnReader& reader)
    : _row_count{reader.RowCount()} {
    // Two reader shapes:
    //   - Primitive (BLOB): PointReadCursor on the column itself.
    //   - ARRAY<FLOAT, dim> (HNSW): primitive cursor on the child element
    //     column; per logical row read `dim` floats and re-pack as
    //     packed-float bytes so the merge driver's WriteBytes path keeps
    //     working unchanged.
    if (reader.Type().id() == duckdb::LogicalTypeId::ARRAY) {
      const auto* child = reader.Child();
      SDB_ASSERT(child != nullptr);
      _array_size = static_cast<size_t>(reader.ArraySize());
      _cursor = child->NewPointCursor();
      _value_vec.emplace(duckdb::LogicalType::FLOAT,
                         static_cast<duckdb::idx_t>(_array_size));
      _scratch.resize(_array_size * sizeof(float));
    } else {
      _cursor = reader.NewPointCursor();
      _value_vec.emplace(duckdb::LogicalType::BLOB, /*capacity=*/1);
    }
    // Empty columns: pre-position at eof so value() reports eof before
    // the first advance/next, matching the legacy iterator contract
    // (FormatTestCase.columns_rw probes this for the never-written
    // column).
    if (_row_count == 0) {
      _doc = doc_limits::eof();
    }
    // Pre-decode the per-row validity bitmap. PointReadCursor::FetchRow
    // only walks the DATA segment, never the validity segment, so we
    // can't infer "row was null-padded by ColumnWriter::Append's gap
    // logic" from FetchRow's output alone. Decode validity once up
    // front and consult the bitmap from advance/seek.
    if (_row_count == 0) {
      return;
    }
    _validity.assign(_row_count, true);
    const auto vrg_count = reader.ValidityGroupCount();
    if (vrg_count == 0) {
      // No validity emitted means "all rows valid" (skip_validity write
      // path). Leave the bitmap as-is (default true).
      _cost = CostAttr{static_cast<CostAttr::Type>(_row_count)};
      return;
    }
    duckdb::Vector vbuf{duckdb::LogicalType(duckdb::LogicalTypeId::VALIDITY),
                        static_cast<duckdb::idx_t>(_row_count)};
    duckdb::idx_t produced = 0;
    for (size_t vrg = 0; vrg < vrg_count; ++vrg) {
      auto seg = reader.OpenValiditySegment(vrg);
      const auto rg_rows =
        static_cast<duckdb::idx_t>(reader.ValidityGroupRowCount(vrg));
      duckdb::ColumnScanState st{nullptr};
      seg->InitializeScan(st);
      seg->Scan(st, rg_rows, vbuf, produced,
                duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      produced += rg_rows;
    }
    auto& mask = duckdb::FlatVector::Validity(vbuf);
    uint64_t valid = 0;
    for (uint64_t i = 0; i < _row_count; ++i) {
      _validity[i] = mask.RowIsValid(i);
      if (_validity[i]) {
        ++valid;
      }
    }
    _cost = CostAttr{static_cast<CostAttr::Type>(valid)};
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<PayAttr>::id()) {
      // Empty columns: legacy iterators do not expose PayAttr at all
      // (FormatTestCase.columns_rw asserts on this for the
      // never-written column).
      if (_row_count == 0) {
        return nullptr;
      }
      return &_pay;
    }
    if (type == irs::Type<CostAttr>::id()) {
      return &_cost;
    }
    if (type == irs::Type<PrevDocAttr>::id()) {
      // Lazy bind: first lookup wires the callback to read _prev_doc.
      if (!_prev_doc_attr) {
        _prev_doc_attr.reset(
          [](const void* ctx) {
            return static_cast<const LegacyIterator*>(ctx)->_prev_doc;
          },
          this);
      }
      return &_prev_doc_attr;
    }
    return nullptr;
  }

  doc_id_t advance() final {
    while (_next_row < _row_count) {
      const auto row = _next_row++;
      if (!_validity.empty() && !_validity[row]) {
        continue;
      }
      Fetch(row);
      _prev_doc = _doc;
      _doc = static_cast<doc_id_t>(row + doc_limits::min());
      return _doc;
    }
    _prev_doc = _doc;
    _doc = doc_limits::eof();
    _pay.value = {};
    return _doc;
  }

  doc_id_t seek(doc_id_t target) final {
    // Legacy ColumnIterator::seek is forward-only: seeking to a target
    // less than the current position must NOT rewind -- callers expect
    // it to behave as "advance until value() >= target". The columnstore
    // tests (FormatTestCase.columns_rw_bit_mask) explicitly probe this
    // by seeking backward after advancing past the end, expecting eof.
    if (doc_limits::eof(_doc)) {
      _pay.value = {};
      return _doc;
    }
    if (target == doc_limits::eof()) {
      _prev_doc = _doc;
      _doc = doc_limits::eof();
      _next_row = _row_count;
      _pay.value = {};
      return _doc;
    }
    const auto target_min = doc_limits::min();
    uint64_t row =
      target < target_min ? 0 : static_cast<uint64_t>(target) - target_min;
    // Forward-only: never rewind below the row after the last yielded
    // doc. _next_row was set to (last_yielded_row + 1) by the previous
    // advance/seek, so use it as the floor.
    if (row < _next_row) {
      // Special case: if the caller is re-asking for the current doc
      // (target == _doc), return it without re-fetching.
      if (doc_limits::valid(_doc) && target <= _doc) {
        return _doc;
      }
      row = _next_row;
    }
    while (row < _row_count) {
      if (_validity.empty() || _validity[row]) {
        Fetch(row);
        _prev_doc = _doc;
        _doc = static_cast<doc_id_t>(row + target_min);
        _next_row = row + 1;
        return _doc;
      }
      ++row;
    }
    _prev_doc = _doc;
    _doc = doc_limits::eof();
    _next_row = _row_count;
    _pay.value = {};
    return _doc;
  }

  void reset() final {
    _next_row = 0;
    _doc = doc_limits::invalid();
    _prev_doc = doc_limits::invalid();
    _pay.value = {};
  }

 private:
  void Fetch(uint64_t row) {
    if (_array_size != 0) {
      // ARRAY<FLOAT, dim>: the child column holds dim consecutive floats
      // per logical row at element offsets [row*dim .. row*dim+dim).
      // Pull each float into a flat FLOAT vector then memcpy into
      // _scratch -- that's the packed-float byte layout the legacy
      // payload contract expects.
      const auto base = row * static_cast<uint64_t>(_array_size);
      for (size_t i = 0; i < _array_size; ++i) {
        _cursor->FetchRow(base + i, *_value_vec, static_cast<duckdb::idx_t>(i));
      }
      const auto* fp = duckdb::FlatVector::GetData<float>(*_value_vec);
      std::memcpy(_scratch.data(), fp, _scratch.size());
      _pay.value = bytes_view{
        reinterpret_cast<const byte_type*>(_scratch.data()), _scratch.size()};
      return;
    }
    _cursor->FetchRow(row, *_value_vec, /*out_idx=*/0);
    auto* slots = duckdb::FlatVector::GetData<duckdb::string_t>(*_value_vec);
    const auto& s = slots[0];
    // Legacy semantic: mask-column rows + writer-never-emitted-bytes
    // rows must come back IsNull-true, which requires data() == nullptr
    // (see utils/string.hpp IsNull). A non-null pointer to a zero-byte
    // span would fail tests like FormatTestCase.columns_rw_dense_mask.
    if (s.GetSize() == 0) {
      _pay.value = {};
    } else {
      _pay.value = bytes_view{reinterpret_cast<const byte_type*>(s.GetData()),
                              static_cast<size_t>(s.GetSize())};
    }
  }

  std::optional<irs::columnstore::ColumnReader::PointReadCursor> _cursor;
  std::optional<duckdb::Vector> _value_vec;
  // For ARRAY<FLOAT, dim> columns: _array_size = dim, _scratch holds the
  // re-packed floats per Fetch. Empty/zero for primitive (BLOB) columns.
  size_t _array_size = 0;
  std::vector<byte_type> _scratch;
  uint64_t _row_count;
  uint64_t _next_row = 0;
  PayAttr _pay;
  // CostAttr exposed to filters that estimate iterator cardinality
  // (ColumnExistenceFilter, geo, etc). Set in the iterator ctor below
  // to the count of valid rows in the underlying column; matches the
  // legacy ColumnReader::size() semantics those filters relied on.
  CostAttr _cost{0};
  // Tracks the previously yielded valid doc_id for PrevDocAttr.
  // doc_limits::invalid() before the first advance.
  doc_id_t _prev_doc = doc_limits::invalid();
  mutable PrevDocAttr _prev_doc_attr;
  // _validity[row] == false means the row was null-padded by
  // ColumnWriter::Append's gap logic and shouldn't appear in the
  // legacy iteration. Empty when the column has no validity segment
  // (skip_validity write); in that case all rows are treated as valid.
  std::vector<bool> _validity;
};

class LegacyColumnReader final : public ColumnReader {
 public:
  LegacyColumnReader(const irs::columnstore::ColumnReader& reader,
                     const absl::flat_hash_map<field_id, bstring>& headers)
    : _reader{&reader} {
    // Look up legacy header bytes captured at write time. Empty for
    // STORE columns; 22-byte NormHeader for norm columns.
    auto it = headers.find(reader.Id());
    if (it != headers.end()) {
      _header = it->second;
    }
    // Pre-count valid rows. Legacy ColumnReader::size() returns the
    // emitted-doc count, NOT total row positions; sparse columns'
    // null-padded gaps don't contribute. Decoding validity here lets
    // size() be O(1) per call.
    const auto rows = reader.RowCount();
    const auto vrg_count = reader.ValidityGroupCount();
    if (vrg_count == 0) {
      _valid_count = static_cast<doc_id_t>(rows);
      return;
    }
    duckdb::Vector vbuf{duckdb::LogicalType(duckdb::LogicalTypeId::VALIDITY),
                        static_cast<duckdb::idx_t>(rows)};
    duckdb::idx_t produced = 0;
    for (size_t vrg = 0; vrg < vrg_count; ++vrg) {
      auto seg = reader.OpenValiditySegment(vrg);
      const auto rg_rows =
        static_cast<duckdb::idx_t>(reader.ValidityGroupRowCount(vrg));
      duckdb::ColumnScanState st{nullptr};
      seg->InitializeScan(st);
      seg->Scan(st, rg_rows, vbuf, produced,
                duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      produced += rg_rows;
    }
    auto& mask = duckdb::FlatVector::Validity(vbuf);
    uint64_t cnt = 0;
    for (uint64_t i = 0; i < rows; ++i) {
      if (mask.RowIsValid(i)) {
        ++cnt;
      }
    }
    _valid_count = static_cast<doc_id_t>(cnt);
  }

  field_id id() const final { return _reader->Id(); }
  std::string_view name() const final {
    // Legacy "no-name" columns must come back as IsNull-true (data() ==
    // nullptr); a zero-length view that points into the new cs
    // footer's owned name string would have data() != nullptr and fail
    // sorted_index_tests' IsNull assertion.
    auto n = _reader->Name();
    if (n.size() == 0) {
      return {};
    }
    return n;
  }
  bytes_view payload() const final {
    if (_header.empty()) {
      return {};
    }
    return bytes_view{_header.data(), _header.size()};
  }

  NormReader::ptr norms() const final {
    if (_header.empty()) {
      return {};
    }
    auto hdr = NormHeader::Read(payload());
    if (!hdr) {
      return {};
    }
    return memory::make_managed<LegacyNormReader>(*_reader, *hdr);
  }

  ResettableDocIterator::ptr iterator(ColumnHint /*hint*/) const final {
    // ColumnHint::PrevDoc is silently treated like Normal here -- the
    // adapter doesn't track previous-doc payloads, so queries that
    // depend on that behaviour (NestedFilter family) will return
    // wrong results, not crash. PrevDoc / Mask / Consolidation hints
    // were format-internal optimisations on the legacy impl; the
    // wrapper exposes a single iteration shape and lets callers down-
    // stream notice if they need the missing feature.
    return memory::make_managed<LegacyIterator>(*_reader);
  }

  doc_id_t size() const final { return _valid_count; }

 private:
  const irs::columnstore::ColumnReader* _reader;
  doc_id_t _valid_count = 0;
  bstring _header;
};

class LegacyReader final : public ColumnstoreReader {
 public:
  uint64_t CountMappedMemory() const final { return 0; }

  bool prepare(const Directory& dir, const SegmentMeta& meta,
               const Options& /*opts*/) final {
    _reader = std::make_unique<irs::columnstore::Reader>(dir, meta.name,
                                                         TestOnlyDatabase());
    auto cols = _reader->Columns();
    auto headers = ReadLegacyHeaders(dir, meta.name);
    _wrappers.reserve(cols.size());
    _by_id.clear();
    for (auto* c : cols) {
      _wrappers.push_back(std::make_unique<LegacyColumnReader>(*c, headers));
      _by_id[_wrappers.back()->id()] = _wrappers.size() - 1;
    }
    // segment_reader_impl asserts that the visit() callback yields
    // named columns in strictly-sorted order (see segment_reader_impl
    // ":Named columns are out of order"). The new cs preserves
    // insertion order; build a name-sorted index for visit() to walk.
    _sorted_indices.resize(_wrappers.size());
    std::iota(_sorted_indices.begin(), _sorted_indices.end(), size_t{0});
    std::ranges::sort(_sorted_indices, [&](size_t a, size_t b) {
      return _wrappers[a]->name() < _wrappers[b]->name();
    });
    return !_wrappers.empty();
  }

  bool visit(const column_visitor_f& visitor) const final {
    for (auto idx : _sorted_indices) {
      if (!visitor(*_wrappers[idx])) {
        return false;
      }
    }
    return true;
  }

  const ColumnReader* column(field_id field) const final {
    auto it = _by_id.find(field);
    return it == _by_id.end() ? nullptr : _wrappers[it->second].get();
  }

  size_t size() const final { return _wrappers.size(); }

 private:
  std::unique_ptr<irs::columnstore::Reader> _reader;
  std::vector<std::unique_ptr<LegacyColumnReader>> _wrappers;
  absl::flat_hash_map<field_id, size_t> _by_id;
  // Name-sorted indices into `_wrappers`, for visit() ordering.
  std::vector<size_t> _sorted_indices;
};

}  // namespace
}  // namespace irs
namespace irs::columnstore::legacy {

duckdb::DatabaseInstance& TestOnlyDatabase() {
  return ::irs::TestOnlyDatabase();
}

ColumnstoreWriter::ptr MakeWriter(bool /*consolidation*/) {
  return std::make_unique<::irs::LegacyWriter>();
}

ColumnstoreReader::ptr MakeReader() {
  return std::make_unique<::irs::LegacyReader>();
}

namespace {

ColumnstoreWriter::ptr MakeWriterAdapter(bool consolidation) {
  return MakeWriter(consolidation);
}

ColumnstoreReader::ptr MakeReaderAdapter() { return MakeReader(); }

duckdb::DatabaseInstance* DbProvider() { return &TestOnlyDatabase(); }

}  // namespace

void RegisterAsFormatFactories() {
  ::irs::SetLegacyColumnstoreFactories({.writer_factory = &MakeWriterAdapter,
                                        .reader_factory = &MakeReaderAdapter,
                                        .db_provider = &DbProvider});
}

}  // namespace irs::columnstore::legacy

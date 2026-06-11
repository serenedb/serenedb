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

#pragma once

// Test-only helpers that let legacy-shape iresearch tests work against
// the new cs (`irs::*`). The legacy ColumnstoreWriter /
// ColumnReader / ColumnOutput interfaces are gone from production; ported
// tests use BLOB-typed cs columns through this thin layer.

#include <atomic>
#include <duckdb/common/types/vector.hpp>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "basics/duckdb_engine.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::tests {

// Production code reserves norm ids on the catalog side and the writer
// asserts `norm_column_options` returned a valid id; iresearch gtests have
// no catalog, so this builds a fresh monotonic allocator per call.
inline NormColumnOptionsProvider MakeNormColumnOptionsProvider() {
  return [next = std::make_shared<std::atomic<field_id>>(0)](
           field_id /*id*/) -> NormColumnOptions {
    return {
      .id = next->fetch_add(1, std::memory_order_relaxed),
      .row_group_size = DEFAULT_ROW_GROUP_SIZE,
    };
  };
}

// Default IndexWriterOptions / IndexReaderOptions wired to the process-wide
// DuckDB DatabaseInstance held by sdb::DuckDBEngine. tests_main brings
// the engine up before RUN_ALL_TESTS and tears it down afterwards. Legacy
// tests that just called `IndexWriter::Make(dir, codec, mode)` would skip
// opening a cs writer because `opts.db == nullptr`. Tests ported off legacy
// STORE -> column(name) need a cs writer to keep the same write / read-back
// behaviour; using these defaults plumbs the engine instance in without
// every call site re-typing it.
inline IndexWriterOptions DefaultWriterOptions() {
  auto* db = &::sdb::DuckDBEngine::Instance().instance();
  IndexWriterOptions opts;
  opts.db = db;
  opts.reader_options.db = db;
  opts.norm_column_options = MakeNormColumnOptionsProvider();
  return opts;
}
inline IndexReaderOptions DefaultReaderOptions() {
  IndexReaderOptions opts;
  opts.db = &::sdb::DuckDBEngine::Instance().instance();
  return opts;
}

// Convenience: construct a new-cs Writer over `dir`/`segment_name`, wired
// to the process-wide DuckDBEngine instance. Matches the production
// constructor.
std::unique_ptr<ColWriter> MakeCsWriter(Directory& dir,
                                        std::string_view segment_name);

// Convenience: construct a new-cs Reader. Returns nullptr if the `.col`
// file does not exist (test still needs to react -- the production
// SegmentReader does the same check via `Has*` on the result).
std::unique_ptr<ColReader> MakeCsReader(const Directory& dir,
                                        std::string_view segment_name);

// Open a BLOB column on the given writer; convenience wrapper around
// Writer::OpenColumn(id, BLOB, ...). Most legacy tests stored bytes per
// doc -- BLOB matches that shape exactly.
ColumnWriter& OpenBlobColumn(ColWriter& w, field_id id);

// Append one BLOB row at `doc - doc_limits::min()`. Wraps the
// per-row-at-a-time append pattern legacy tests use (one Insert per
// doc). For tests that already have a typed Vector available, call
// ColumnWriter::Append directly.
void AppendBlob(ColumnWriter& cw, doc_id_t doc, bytes_view payload);

// Append a null entry at `doc`. Same caveat as AppendBlob.
void AppendNullBlob(ColumnWriter& cw, doc_id_t doc);

// DataOutput that appends bytes to an `irs::bstring`. Used by tests to
// capture a field's serialised STORE bytes for forwarding to a cs blob
// column. Replaces the legacy IndexOutput-into-stored-column shape.
class BstringDataOutput final : public DataOutput {
 public:
  explicit BstringDataOutput(bstring& buf) noexcept : _buf{&buf} {}

  void WriteByte(byte_type b) final { _buf->push_back(b); }
  void WriteBytes(const byte_type* b, size_t len) final {
    _buf->append(b, len);
  }

 private:
  bstring* _buf;
};

// Serialise `field.Write(out)` bytes and append them as a single BLOB row
// to the cs column under `cs[id]`. Opens the column on first use, reuses
// it on subsequent calls. The field type only needs a
// `bool Write(irs::DataOutput&) const` method.
template<typename Field>
void StoreFieldAt(ColWriter& cs, field_id id, doc_id_t doc,
                  const Field& field) {
  auto& cw = OpenBlobColumn(cs, id);
  bstring buf;
  BstringDataOutput out{buf};
  field.Write(out);
  AppendBlob(cw, doc, {buf.data(), buf.size()});
}

// Visit a cs BLOB column doc-by-doc, calling visitor(doc_id, payload) for
// each row that is not null. Returns false if the visitor short-circuits.
bool VisitBlobColumn(const ColReader& cs_reader, const ColumnReader& column,
                     const std::function<bool(doc_id_t, bytes_view)>& visitor);

struct ExpectedBlobRow {
  doc_id_t doc;
  bytes_view payload;
};

// Assert that `column` yields exactly the rows in `expected` (in order) and
// reports the same total `RowCount`. Rows in `expected` correspond to the
// non-null doc_ids the test wrote. Use `BlobPointReader` for per-doc lookups
// when the test only cares about specific rows.
void AssertBlobColumn(const ColReader& cs_reader, const ColumnReader& column,
                      std::span<const ExpectedBlobRow> expected);

// Thin wrapper used by ported scoring / filter tests. Constructs a
// `ColumnReader::BlobPointReader` from a `SubReader` +
// `ColumnReader` pair (the segment supplies the cs `Reader`). Exposes the
// legacy `Get(doc) -> bytes_view` shape callers were using.
class BlobPointReader {
 public:
  BlobPointReader(const SubReader& segment, const ColumnReader& column)
    : _impl(*segment.GetColReader(), column) {}

  BlobPointReader(const BlobPointReader&) = delete;
  BlobPointReader& operator=(const BlobPointReader&) = delete;

  bytes_view Get(doc_id_t doc) { return _impl.FetchDoc(doc); }
  bool IsNull(doc_id_t doc) { return _impl.IsNullDoc(doc); }

 private:
  ColumnReader::BlobPointReader _impl;
};

// Convenience: read a length-prefixed string (irs::WriteStr layout) from
// `column` at `doc` via a BlobPointReader. Accepts both the wrapper above
// and the underlying `ColumnReader::BlobPointReader`.
template<typename StringType>
StringType ReadStoredStr(ColumnReader::BlobPointReader& reader, doc_id_t doc) {
  return ToString<StringType>(reader.FetchDoc(doc).data());
}
template<typename StringType>
StringType ReadStoredStr(BlobPointReader& reader, doc_id_t doc) {
  return ToString<StringType>(reader.Get(doc).data());
}

}  // namespace irs::tests

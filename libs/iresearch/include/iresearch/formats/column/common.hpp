////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include "basics/bit_packing.hpp"
#include "basics/containers/monotonic_buffer.hpp"
#include "basics/math_utils.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "iresearch/formats/column/sparse_bitmap.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/encryption.hpp"

namespace irs::columnstore2 {

enum class Version : int32_t {
  Min = 0,
  Max = Min,
};

class Column final : public ColumnOutput {
 public:
  static constexpr size_t kBlockSize = SparseBitmapWriter::kBlockSize;
  static_assert(math::IsPower2(kBlockSize));

  struct Context {
    IndexOutput* data_out;
    Encryption::Stream* cipher;
    std::optional<HNSWInfo> hnsw_info;
    union {
      byte_type* u8buf;
      uint64_t* u64buf;
    };
    bool consolidation;
    SparseBitmapVersion version;
  };

  struct ColumnBlock {
    uint64_t addr;
    uint64_t avg;
    uint64_t data;
    uint64_t last_size;
#ifdef SDB_DEV
    uint64_t size;
#endif
    // TODO(mbkkt) implement streamvbyte (0124 and 1234) length compression
    uint32_t bits;
  };

  explicit Column(const Context& ctx, field_id id, ValueType value_type,
                  ColumnFinalizer&& finalizer,
                  IResourceManager& resource_manager);

  void WriteByte(byte_type b) final { _data.stream.WriteByte(b); }

  void WriteBytes(const byte_type* b, size_t size) final {
    _data.stream.WriteBytes(b, size);
  }

  void WriteU16(uint16_t n) final { _data.stream.WriteU16(n); }
  void WriteU32(uint32_t n) final { _data.stream.WriteU32(n); }
  void WriteU64(uint64_t n) final { _data.stream.WriteU64(n); }
  void WriteV32(uint32_t n) final { _data.stream.WriteV32(n); }
  void WriteV64(uint64_t n) final { _data.stream.WriteV64(n); }

  void Reset() final;

  void WithHNSW(std::unique_ptr<HNSWIndexWriter> hnsw_index) {
    _finalizer = ColumnFinalizer{
      [result_finalizer = _finalizer.ExtractPayloadFinalizer(),
       hnsw_index = std::move(hnsw_index)](DataOutput& out) mutable {
        hnsw_index->Serialize(out);
        result_finalizer(out);
      },
      _finalizer.ExtractNameFinalizer(),
    };
  }

 private:
  friend class Writer;

  class AddressTable {
   public:
    AddressTable(IResourceManager& resource_manager)
      : _buffer{resource_manager, packed::kBlockSize64} {}

    uint64_t back() const noexcept {
      SDB_ASSERT(_size > 0);
      return _buffer.Current()[-1];
    }

    void push_back(uint64_t offset) noexcept {
      SDB_ASSERT(_size < kBlockSize);
      _buffer.Construct(offset);
      ++_size;
    }

    void pop_back() noexcept {
      SDB_ASSERT(_size > 0);
      _buffer.Free(1);
      --_size;
    }

    uint32_t size() const noexcept { return _size; }

    bool empty() const noexcept { return _size == 0; }

    bool full() const noexcept { return _size == kBlockSize; }

    void reset() noexcept {
      _buffer.Clear();
      _size = 0;
    }

    auto blocks() noexcept { return _buffer.Blocks(); }

    auto available() const noexcept { return _buffer.Available(); }

   private:
    MonotonicBuffer<uint64_t, packed::kBlockSize64, kBlockSize> _buffer;
    uint32_t _size = 0;
  };

  void Prepare(doc_id_t key) final;

  bool empty() const noexcept { return _addr_table.empty() && !_docs_count; }

  void flush() {
    if (!_addr_table.empty()) {
      flush_block();
#ifdef SDB_DEV
      _sealed = true;
#endif
    }
  }

  void FinalizeName() {
    flush();
    _name = _finalizer.GetName();
  }

  void FinalizePayload(IndexOutput& index_out) {
    uint64_t start_offset = index_out.Position();
    _finalizer.Finalize(index_out);
    uint64_t end_offset = index_out.Position();
    if (start_offset == end_offset) {
      index_out.WriteU32(0);
    }
  }

  void finish(IndexOutput& index_out);

  std::string_view name() const noexcept { return _name; }

  void flush_block();

  Context _ctx;
  TypeInfo _compression;
  compression::Compressor::ptr _deflater;
  ColumnFinalizer _finalizer;
  ManagedVector<ColumnBlock> _blocks;  // at most 65536 blocks
  MemoryOutput _data;
  MemoryOutput _docs;
  SparseBitmapWriter _docs_writer{_docs.stream, _ctx.version};
  AddressTable _addr_table;
  bstring _payload;
  std::string_view _name;
  uint64_t _prev_avg{};
  doc_id_t _docs_count{};
  doc_id_t _prev{};  // last committed doc_id_t
  doc_id_t _pend{};  // last pushed doc_id_t
  field_id _id;
  bool _fixed_length{true};
  ValueType _value_type{ValueType::Str};
#ifdef SDB_DEV
  bool _sealed{false};
#endif
};

class Writer final : public ColumnstoreWriter {
 public:
  static constexpr std::string_view kDataFormatName =
    "iresearch_11_columnstore_data";
  static constexpr std::string_view kIndexFormatName =
    "iresearch_11_columnstore_index";
  static constexpr std::string_view kDataFormatExt = "csd";
  static constexpr std::string_view kIndexFormatExt = "csi";

  Writer(Version version, bool consolidation,
         IResourceManager& resource_manager);
  ~Writer() final;

  void prepare(Directory& dir, const SegmentMeta& meta) final;
  ColumnT push_column(const ColumnInfo& info, ColumnFinalizer finalizer) final;

  bool commit(const FlushState& state) final;
  void rollback() noexcept final;

 private:
  Directory* _dir = nullptr;
  std::string _data_filename;
  // pointers remain valid
  std::deque<Column, ManagedTypedAllocator<Column>> _columns;
  std::vector<Column*> _sorted_columns;
  IndexOutput::ptr _data_out;
  Encryption::Stream::ptr _data_cipher;
  byte_type* _buf;
  Version _ver;
  bool _consolidation;
};

enum class ColumnType : uint16_t {
  // Variable length data
  Sparse = 0,

  // No data
  Mask,

  // Fixed length data
  Fixed,

  // Fixed length data in adjacent blocks
  DenseFixed,
};

enum class ColumnProperty : uint16_t {
  // Regular column
  Normal = 0,

  // Encrytped data
  Encrypt = 1,

  // Annonymous column
  NoName = 2,

  // Support accessing previous document
  PrevDoc = 4,
};

ENABLE_BITMASK_ENUM(ColumnProperty);

struct ColumnHeader {
  // Bitmap index offset, 0 if not present.
  // 0 - not present, meaning dense column
  uint64_t docs_index{};

  // Column identifier
  field_id id{field_limits::invalid()};

  // Total number of docs in a column
  doc_id_t docs_count{};

  // Min document identifier
  doc_id_t min{doc_limits::invalid()};

  // Column properties
  ColumnProperty props{ColumnProperty::Normal};

  ColumnType type{ColumnType::Sparse};

  ValueType value_type{ValueType::Str};

  std::optional<HNSWInfo> hnsw_info;
};

class Reader final : public ColumnstoreReader {
 public:
  uint64_t CountMappedMemory() const final {
    return _data_in != nullptr ? _data_in->CountMappedMemory() : 0;
  }

  bool prepare(const Directory& dir, const SegmentMeta& meta,
               const Options& opts = Options{}) final;

  const ColumnHeader* header(field_id field) const;

  const ColumnReader* column(field_id field) const final {
    return field >= _columns.size()
             ? nullptr  // can't find column with the specified identifier
             : _columns[field];
  }

  bool visit(const column_visitor_f& visitor) const final;

  size_t size() const final { return _columns.size(); }

 private:
  using column_ptr = memory::managed_ptr<ColumnReader>;

  void prepare_data(const Directory& dir, std::string_view filename);

  void prepare_index(const Directory& dir, const SegmentMeta& meta,
                     std::string_view filename, std::string_view data_filename,
                     const Options& opts);

  std::vector<column_ptr> _sorted_columns;
  std::vector<const column_ptr::element_type*> _columns;
  Encryption::Stream::ptr _data_cipher;
  IndexInput::ptr _data_in;
};

ColumnstoreWriter::ptr MakeWriter(Version version, bool consolidation,
                                  IResourceManager& resource_manager);
ColumnstoreReader::ptr MakeReader();

}  // namespace irs::columnstore2

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
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/formats/column/column_reader.hpp"

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <duckdb/common/enums/memory_tag.hpp>
#include <duckdb/common/file_buffer.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_allocator.hpp>
#include <duckdb/storage/buffer/block_handle.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <utility>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/internal/overflow_string_io.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/store/data_input.hpp"

namespace irs {

std::unique_ptr<ColumnReader> MakeColumnReader(field_id id,
                                               PersistentColumnData&& node) {
  return MakeColumnReader(id, std::move(node), ColumnBlockSource{});
}

std::unique_ptr<ColumnReader> MakeColumnReader(
  field_id id, PersistentColumnData&& node, const ColumnBlockSource& source) {
  std::unique_ptr<ColumnReader> element_child;
  std::vector<std::unique_ptr<ColumnReader>> struct_children;
  uint64_t array_size = 0;

  switch (node.type.id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      SDB_ASSERT(node.child_columns.size() == 1);
      array_size = static_cast<uint64_t>(duckdb::ArrayType::GetSize(node.type));
      element_child = MakeColumnReader(
        field_limits::invalid(), std::move(node.child_columns.front()), source);
      node.pointers.clear();  // ARRAY carries no self data on disk.
    } break;
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      // MAP rides on the LIST path: PhysicalType::LIST with a
      // STRUCT<key, value> element.
      SDB_ASSERT(node.child_columns.size() == 1);
      element_child = MakeColumnReader(
        field_limits::invalid(), std::move(node.child_columns.front()), source);
    } break;
    case duckdb::LogicalTypeId::VARIANT: {
      std::vector<ColumnReader::VariantRgReader> variant_rgs;
      variant_rgs.reserve(node.variant_layouts.size());
      for (auto& variant_layout : node.variant_layouts) {
        auto& row_group = variant_rgs.emplace_back();
        row_group.row_count = variant_layout.row_count;
        row_group.shred_state = variant_layout.shred_state;
        row_group.unshredded =
          MakeColumnReader(field_limits::invalid(),
                           std::move(*variant_layout.unshredded), source);
        if (variant_layout.shred_state != VariantShredState::Unshredded) {
          row_group.shredded_node =
            MakeColumnReader(field_limits::invalid(),
                             std::move(*variant_layout.shredded_node), source);
        }
      }
      return std::make_unique<ColumnReader>(id, std::move(node.type),
                                            std::move(node.validity_pointers),
                                            std::move(variant_rgs), source);
    }
    case duckdb::LogicalTypeId::STRUCT: {
      struct_children.reserve(node.child_columns.size());
      for (auto& child : node.child_columns) {
        struct_children.push_back(
          MakeColumnReader(field_limits::invalid(), std::move(child), source));
      }
      node.pointers.clear();
      break;
    }
    default:
      break;  // primitive leaf: keep pointers, no children.
  }

  const bool fully_shredded = node.fully_shredded;
  return std::make_unique<ColumnReader>(
    id, std::move(node.type), std::move(node.pointers),
    std::move(node.validity_pointers), std::move(element_child),
    std::move(struct_children), array_size, fully_shredded, source);
}

namespace {

const duckdb::LogicalType kLengthsType{duckdb::LogicalTypeId::UBIGINT};
const duckdb::LogicalType kValidityType{duckdb::LogicalTypeId::VALIDITY};

bool AnyNonEmptyValidity(std::span<const duckdb::DataPointer> pointers) {
  return absl::c_any_of(pointers, [](const duckdb::DataPointer& p) {
    return p.compression_type != duckdb::CompressionType::COMPRESSION_EMPTY;
  });
}

// Wraps a slice of the mmap'd .col file as a duckdb FileBuffer without
// copying. Owns a share of the input whose MMapHandle keeps the mapping
// alive for as long as any BlockHandle (and thus any output Vector pinning
// it) references the slice. ~FileBuffer frees internal_buffer, so the slice
// pointers are detached in the destructor.
class MmapSegmentBuffer final : public duckdb::FileBuffer {
 public:
  MmapSegmentBuffer(duckdb::BlockAllocator& block_allocator,
                    const byte_type* data, duckdb::idx_t data_size,
                    std::shared_ptr<IndexInput> mapping)
    : duckdb::FileBuffer{block_allocator, duckdb::FileBufferType::TINY_BUFFER,
                         /*user_size=*/0, /*block_header_size=*/0},
      _mapping{std::move(mapping)} {
    buffer = reinterpret_cast<duckdb::data_ptr_t>(const_cast<byte_type*>(data));
    size = data_size;
    internal_buffer = buffer;
    internal_size = data_size;
  }

  ~MmapSegmentBuffer() final { Init(); }

 private:
  std::shared_ptr<IndexInput> _mapping;
};

// Codecs assume the alignment that buffer-manager blocks provide (roaring
// asserts RunContainerRLEPair alignment, etc.), so a segment may only be
// wrapped in place when its file offset was written aligned (the writer
// pads to 64 bytes; older files weren't padded and must be staged).
inline bool MmapWrapAligned(const byte_type* data) noexcept {
  return reinterpret_cast<uintptr_t>(data) % 64 == 0;
}

// The staging copy that zero-copy replaced doubled as readahead; without a
// hint the decode demand-pages the mapping 4KB at a time, which is much
// slower on a cold page cache. Ask the kernel to fetch the whole segment
// asynchronously; near-free when the pages are already resident.
void PrefetchMmapRange(const byte_type* data, size_t size) noexcept {
#ifdef __linux__
  static const uintptr_t kPageMask =
    ~(static_cast<uintptr_t>(sysconf(_SC_PAGESIZE)) - 1);
  const auto addr = reinterpret_cast<uintptr_t>(data);
  const auto aligned = addr & kPageMask;
  madvise(reinterpret_cast<void*>(aligned), size + (addr - aligned),
          MADV_WILLNEED);
#endif
}

// Ids >= MAXIMUM_BLOCK mark temporary blocks: ~BlockHandle early-outs and
// never talks to the BlockManager. Offset far above the buffer manager's
// own temporary_id counter to keep the ranges visibly distinct.
duckdb::block_id_t NextMmapBlockId() noexcept {
  static std::atomic<duckdb::block_id_t> next{MAXIMUM_BLOCK +
                                              (duckdb::block_id_t{1} << 40)};
  return next.fetch_add(1, std::memory_order_relaxed);
}

duckdb::shared_ptr<duckdb::BlockHandle> WrapMmapPointer(
  duckdb::DatabaseInstance& db, const duckdb::DataPointer& p,
  const std::shared_ptr<IndexInput>& in) {
  const auto byte_size = static_cast<duckdb::idx_t>(p.block_pointer.offset);
  if (byte_size == 0 || p.block_pointer.block_id ==
                          static_cast<duckdb::block_id_t>(INVALID_BLOCK)) {
    return nullptr;
  }
  const auto* direct =
    in->ReadStable(static_cast<uint64_t>(p.block_pointer.block_id), byte_size);
  if (!direct || !MmapWrapAligned(direct)) {
    return nullptr;
  }
  auto& bm = duckdb::BufferManager::GetBufferManager(db);
  // TINY_BUFFER blocks are never unloaded by the buffer pool and the
  // temporary block id keeps ~BlockHandle away from the BlockManager. The
  // reservation must match the loaded buffer's size: ~BlockMemory asserts
  // a positive charge for loaded buffers and returns it to the pool.
  return duckdb::make_shared_ptr<duckdb::BlockHandle>(
    bm.GetTemporaryBlockManager(), NextMmapBlockId(),
    duckdb::MemoryTag::EXTERNAL_FILE_CACHE,
    duckdb::make_uniq<MmapSegmentBuffer>(duckdb::BlockAllocator::Get(db),
                                         direct, byte_size, in),
    duckdb::DestroyBufferUpon::BLOCK, byte_size,
    duckdb::TempBufferPoolReservation{duckdb::MemoryTag::EXTERNAL_FILE_CACHE,
                                      bm.GetBufferPool(), byte_size});
}

// One prebuilt wrapper per pointer; null slots (constant pointers or a
// non-mmap input) fall back to lazy wrapping / staging in OpenSegmentImpl.
std::vector<duckdb::shared_ptr<duckdb::BlockHandle>> WrapMmapPointers(
  const ColumnBlockSource& source,
  std::span<const duckdb::DataPointer> pointers) {
  std::vector<duckdb::shared_ptr<duckdb::BlockHandle>> blocks(pointers.size());
  if (source) {
    for (size_t i = 0; i < pointers.size(); ++i) {
      blocks[i] = WrapMmapPointer(*source.db, pointers[i], source.in);
    }
  }
  return blocks;
}

RgWindow LocateInOffsets(uint64_t row_pos, std::span<const uint64_t> offsets,
                         RgWindow hint) noexcept {
  if (row_pos >= hint.end) {
    // Forward jump: hint.rg + 1 is the common sequential-forward step.
    const size_t next = hint.rg + 1;
    SDB_ASSERT(next + 1 < offsets.size());
    if (row_pos < offsets[next + 1]) {
      return {next, hint.end, offsets[next + 1]};
    }
    SDB_ASSERT(next + 2 < offsets.size());
    auto it =
      std::upper_bound(offsets.begin() + next + 2, offsets.end(), row_pos);
    const size_t rg = static_cast<size_t>(it - offsets.begin() - 1);
    return {rg, offsets[rg], offsets[rg + 1]};
  }
  if (row_pos < hint.begin) {
    // Backward jump: answer is strictly before hint.rg.
    SDB_ASSERT(hint.rg < offsets.size());
    auto it =
      std::upper_bound(offsets.begin(), offsets.begin() + hint.rg, row_pos);
    const size_t rg = static_cast<size_t>(it - offsets.begin() - 1);
    return {rg, offsets[rg], offsets[rg + 1]};
  }
  return hint;
}

}  // namespace

ColumnReader::ColumnReader(
  field_id id, duckdb::LogicalType type,
  std::vector<duckdb::DataPointer> data_pointers,
  std::vector<duckdb::DataPointer> validity_pointers,
  std::unique_ptr<ColumnReader> element_child,
  std::vector<std::unique_ptr<ColumnReader>> struct_children,
  uint64_t array_size, bool fully_shredded, const ColumnBlockSource& source)
  : _id{id},
    _type{std::move(type)},
    _data_pointers{std::move(data_pointers)},
    _validity_pointers{std::move(validity_pointers)},
    _has_validity{AnyNonEmptyValidity(_validity_pointers)},
    _fully_shredded{fully_shredded},
    _child{std::move(element_child)},
    _array_size{array_size},
    _struct_fields{std::move(struct_children)} {
  if (!_validity_pointers.empty()) {
    _validity_offsets.reserve(_validity_pointers.size() + 1);
    uint64_t vtotal = 0;
    for (const auto& p : _validity_pointers) {
      _validity_offsets.push_back(vtotal);
      vtotal += p.tuple_count;
    }
    _validity_offsets.push_back(vtotal);
  }

  switch (_type.id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      SDB_ASSERT(_child);
      SDB_ASSERT(_array_size > 0);
      SDB_ASSERT(_data_pointers.empty());
      SDB_ASSERT(_struct_fields.empty());
      SDB_ASSERT((_child->RowCount() % _array_size) == 0);
      _row_count = _child->RowCount() / _array_size;
      _data_offsets.push_back(0);  // sentinel only
    } break;
    case duckdb::LogicalTypeId::VARIANT:
    case duckdb::LogicalTypeId::STRUCT: {
      SDB_ASSERT(!_struct_fields.empty());
      SDB_ASSERT(_data_pointers.empty());
      SDB_ASSERT(!_child);
      for (const auto& f : _struct_fields) {
        SDB_ASSERT(f);
      }
      _row_count = _struct_fields.front()->RowCount();
      _data_offsets.push_back(0);  // sentinel only
    } break;
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP: {
      SDB_ASSERT(_child);
      SDB_ASSERT(_struct_fields.empty());
      uint64_t total = 0;
      _data_offsets.reserve(_data_pointers.size() + 1);
      for (const auto& p : _data_pointers) {
        _data_offsets.push_back(total);
        total += p.tuple_count;
      }
      _data_offsets.push_back(total);
      _row_count = total;
      _rg_element_starts.reserve(_data_pointers.size() + 1);
      _rg_element_starts.push_back(0);  // sentinel
      for (const auto& p : _data_pointers) {
        if (p.tuple_count == 0) {
          _rg_element_starts.push_back(_rg_element_starts.back());
        } else {
          _rg_element_starts.push_back(
            duckdb::NumericStats::Max(p.statistics).GetValue<uint64_t>());
        }
      }
    } break;
    default: {
      SDB_ASSERT(!_child);
      SDB_ASSERT(_struct_fields.empty());
      uint64_t total = 0;
      _data_offsets.reserve(_data_pointers.size() + 1);
      for (const auto& p : _data_pointers) {
        _data_offsets.push_back(total);
        total += p.tuple_count;
      }
      _data_offsets.push_back(total);
      _row_count = total;
    } break;
  }
  _data_blocks = WrapMmapPointers(source, _data_pointers);
  _validity_blocks = WrapMmapPointers(source, _validity_pointers);
}

ColumnReader::ColumnReader(field_id id, duckdb::LogicalType type,
                           std::vector<duckdb::DataPointer> validity_pointers,
                           std::vector<VariantRgReader> variant_rgs,
                           const ColumnBlockSource& source)
  : _id{id},
    _type{std::move(type)},
    _validity_pointers{std::move(validity_pointers)},
    _has_validity{AnyNonEmptyValidity(_validity_pointers)},
    _variant_rgs{std::move(variant_rgs)} {
  SDB_ASSERT(_type.id() == duckdb::LogicalTypeId::VARIANT);
  if (!_validity_pointers.empty()) {
    _validity_offsets.reserve(_validity_pointers.size() + 1);
    uint64_t vtotal = 0;
    for (const auto& p : _validity_pointers) {
      _validity_offsets.push_back(vtotal);
      vtotal += p.tuple_count;
    }
    _validity_offsets.push_back(vtotal);
  }
  _variant_rg_starts.reserve(_variant_rgs.size() + 1);
  uint64_t total = 0;
  for (const auto& rg : _variant_rgs) {
    SDB_ASSERT(rg.unshredded);
    SDB_ASSERT(rg.unshredded->RowCount() == rg.row_count);
    SDB_ASSERT(rg.shred_state == VariantShredState::Unshredded ||
               rg.shredded_node->RowCount() == rg.row_count);
    _variant_rg_starts.push_back(total);
    total += rg.row_count;
  }
  _variant_rg_starts.push_back(total);
  _row_count = total;
  _data_offsets.push_back(0);
  _validity_blocks = WrapMmapPointers(source, _validity_pointers);
}

RgWindow ColumnReader::Locate(uint64_t row_pos, RgWindow hint) const noexcept {
  SDB_ASSERT(_type.id() != duckdb::LogicalTypeId::ARRAY &&
               _type.id() != duckdb::LogicalTypeId::STRUCT &&
               _type.id() != duckdb::LogicalTypeId::VARIANT,
             "Locate has no meaning on parents with no top-level data");
  SDB_ASSERT(row_pos < _row_count);
  return LocateInOffsets(row_pos, _data_offsets, hint);
}

RgWindow ColumnReader::LocateValidity(uint64_t row_pos,
                                      RgWindow hint) const noexcept {
  SDB_ASSERT(row_pos < _row_count);
  return LocateInOffsets(row_pos, _validity_offsets, hint);
}

RgWindow ColumnReader::LocateVariantRg(uint64_t row,
                                       RgWindow hint) const noexcept {
  SDB_ASSERT(_type.id() == duckdb::LogicalTypeId::VARIANT);
  SDB_ASSERT(row < _row_count);
  return LocateInOffsets(row, _variant_rg_starts, hint);
}

namespace {

// Whether a codec's SCAN_ENTIRE_VECTOR output keeps `out` a FLAT_VECTOR and
// leaves any pre-existing validity mask intact. MaterializeNode scans the
// separate validity column into `out`'s flat mask before the data scan, so a
// codec that retypes `out` to a DICTIONARY/CONSTANT vector (dictionary, FSST,
// RLE, constant) would silently drop that mask. The flat-preserving codecs
// either decode in place (ALP/bitpacking/chimp/patas) or SetData while
// preserving the mask (uncompressed).
bool CodecPreservesValidityOnEntireScan(duckdb::CompressionType t) noexcept {
  switch (t) {
    case duckdb::CompressionType::COMPRESSION_UNCOMPRESSED:
    case duckdb::CompressionType::COMPRESSION_ALP:
    case duckdb::CompressionType::COMPRESSION_ALPRD:
    case duckdb::CompressionType::COMPRESSION_BITPACKING:
    case duckdb::CompressionType::COMPRESSION_CHIMP:
    case duckdb::CompressionType::COMPRESSION_PATAS:
      return true;
    default:
      return false;
  }
}

}  // namespace

void ColumnReader::RangeScan::Scan(uint64_t row_pos, duckdb::idx_t count,
                                   duckdb::Vector& out,
                                   duckdb::idx_t out_offset,
                                   bool may_use_entire) {
  while (count > 0) {
    if (row_pos < _window.begin || _window.end <= row_pos) {
      _window = _validity ? _reader->LocateValidity(row_pos, _window)
                          : _reader->Locate(row_pos, _window);
      _cursor =
        ScanCursor{_validity ? _reader->OpenValiditySegment(_window.rg, *_ctx)
                             : _reader->OpenSegment(_window.rg, *_ctx)};
    }
    _cursor.SeekTo(row_pos - _window.begin);
    const auto take = std::min<duckdb::idx_t>(count, _window.end - row_pos);
    const bool single_shot = (out_offset == 0 && take == count);
    // SCAN_ENTIRE_VECTOR is zero-copy/in-place; SCAN_FLAT_VECTOR memcpys the
    // whole segment slice. Use the fast path on the data side whenever the
    // codec keeps `out` flat -- when the column has no validity (nothing to
    // preserve) or the codec preserves a pre-scanned mask (see helper).
    const bool entire_safe =
      !_validity && (!_reader->HasValidity() ||
                     CodecPreservesValidityOnEntireScan(_cursor.Codec()));
    const auto scan_type = (may_use_entire && single_shot && entire_safe)
                             ? duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR
                             : duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
    _cursor.Scan(take, out, out_offset, scan_type);
    row_pos += take;
    count -= take;
    out_offset += take;
  }
}

duckdb::unique_ptr<duckdb::ColumnSegment> ColumnReader::OpenSegmentImpl(
  const duckdb::DataPointer& p, const duckdb::LogicalType& type,
  ReadContext& ctx,
  const duckdb::shared_ptr<duckdb::BlockHandle>& prebuilt) const {
  auto& db = ctx.Database();
  auto& cfg = duckdb::DBConfig::GetConfig(db);
  auto codec =
    cfg.TryGetCompressionFunction(p.compression_type, type.InternalType());
  SDB_ENSURE(codec, sdb::ERROR_INTERNAL,
             ".col reader: missing compression function for codec type ",
             static_cast<uint8_t>(p.compression_type));
  auto stats = p.statistics.Copy();
  const auto byte_size = static_cast<duckdb::idx_t>(p.block_pointer.offset);

  // segment_state intentionally dropped: only used by VisitBlockIds at
  // checkpoint time (we don't checkpoint). Scan resolves inline page
  // block_ids via segment.block->GetBlockManager() == the ReadContext.

  if (byte_size == 0 || p.block_pointer.block_id ==
                          static_cast<duckdb::block_id_t>(INVALID_BLOCK)) {
    return duckdb::make_uniq<duckdb::ColumnSegment>(
      db, /*block=*/nullptr, duckdb::ColumnSegmentType::PERSISTENT,
      static_cast<duckdb::idx_t>(p.tuple_count), *codec, std::move(stats),
      /*block_id=*/0, /*offset=*/0, byte_size,
      /*segment_state=*/nullptr);
  }

  auto& bm = duckdb::BufferManager::GetBufferManager(db);
  const uint64_t file_offset = p.block_pointer.block_id;
  duckdb::shared_ptr<duckdb::BlockHandle> handle;
  if (prebuilt) {
    // Zero-copy: the block was wrapped around the mmap'd .col file when the
    // reader was built (see WrapMmapPointer); reuse costs one shared_ptr
    // copy.
    if (const auto* direct = ctx.In().ReadStable(file_offset, byte_size)) {
      PrefetchMmapRange(direct, byte_size);
    }
    handle = prebuilt;
  } else if (const auto* direct = ctx.In().ReadStable(file_offset, byte_size);
             direct && MmapWrapAligned(direct)) {
    PrefetchMmapRange(direct, byte_size);
    // Reader built without a ColumnBlockSource but the input is mmap'd:
    // wrap lazily, anchored by a duplicate of the per-query input.
    handle = duckdb::make_shared_ptr<duckdb::BlockHandle>(
      bm.GetTemporaryBlockManager(), NextMmapBlockId(),
      duckdb::MemoryTag::EXTERNAL_FILE_CACHE,
      duckdb::make_uniq<MmapSegmentBuffer>(
        duckdb::BlockAllocator::Get(db), direct, byte_size,
        std::shared_ptr<IndexInput>{ctx.In().Dup()}),
      duckdb::DestroyBufferUpon::BLOCK, byte_size,
      duckdb::TempBufferPoolReservation{duckdb::MemoryTag::EXTERNAL_FILE_CACHE,
                                        bm.GetBufferPool(), byte_size});
  } else {
    // Non-mmap input: stage a copy through the buffer manager.
    handle = bm.RegisterTransientMemory(byte_size, ctx);
    auto buf = bm.Pin(handle);
    ctx.In().ReadData(file_offset, buf.GetDataMutable(), byte_size);
  }
  auto segment = duckdb::make_uniq<duckdb::ColumnSegment>(
    db, std::move(handle), duckdb::ColumnSegmentType::PERSISTENT,
    static_cast<duckdb::idx_t>(p.tuple_count), *codec, std::move(stats),
    /*block_id=*/0, /*offset=*/0, byte_size,
    /*segment_state=*/nullptr);
  if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    if (auto seg_state = segment->GetSegmentState()) {
      auto& str_state =
        seg_state->Cast<duckdb::UncompressedStringSegmentState>();
      str_state.overflow_reader =
        duckdb::make_uniq<IndexInputOverflowReader>(ctx.In());
    }
  }
  return segment;
}

duckdb::unique_ptr<duckdb::ColumnSegment> ColumnReader::OpenSegment(
  size_t rg, ReadContext& ctx) const {
  if (_type.id() == duckdb::LogicalTypeId::LIST ||
      _type.id() == duckdb::LogicalTypeId::MAP) {
    return OpenSegmentImpl(_data_pointers[rg], kLengthsType, ctx,
                           _data_blocks[rg]);
  }
  return OpenSegmentImpl(_data_pointers[rg], _type, ctx, _data_blocks[rg]);
}

duckdb::unique_ptr<duckdb::ColumnSegment> ColumnReader::OpenValiditySegment(
  size_t vrg, ReadContext& ctx) const {
  return OpenSegmentImpl(_validity_pointers[vrg], kValidityType, ctx,
                         _validity_blocks[vrg]);
}

void ColumnReader::ListOffsetState::Read(size_t rg, uint64_t in_rg,
                                         uint64_t& start, uint64_t& end) {
  SDB_ASSERT(_list_column->_type.id() == duckdb::LogicalTypeId::LIST ||
             _list_column->_type.id() == duckdb::LogicalTypeId::MAP);
  if (_rg != rg) {
    _cursor = ScanCursor{_list_column->OpenSegment(rg, *_ctx)};
    _rg = rg;
    _next_pos = 0;
    _prev_offset = _list_column->_rg_element_starts[rg];
  }
  SDB_ASSERT(in_rg >= _next_pos);
  auto* buf_data = duckdb::FlatVector::GetDataMutable<uint64_t>(_buf);
  while (_next_pos < in_rg) {
    _cursor.Scan(1, _buf, 0);
    _prev_offset = buf_data[0];
    ++_next_pos;
  }
  _cursor.Scan(1, _buf, 0);
  end = buf_data[0];
  start = _prev_offset;
  _prev_offset = end;
  ++_next_pos;
}

uint64_t ColumnReader::ListOffsetState::Read(size_t rg, uint64_t first_in_rg,
                                             duckdb::idx_t count,
                                             duckdb::Vector& out_buf) {
  SDB_ASSERT(_list_column->_type.id() == duckdb::LogicalTypeId::LIST ||
             _list_column->_type.id() == duckdb::LogicalTypeId::MAP);
  SDB_ASSERT(count > 0);
  if (_rg != rg) {
    _cursor = ScanCursor{_list_column->OpenSegment(rg, *_ctx)};
    _rg = rg;
    _next_pos = 0;
    _prev_offset = _list_column->_rg_element_starts[rg];
  }
  SDB_ASSERT(first_in_rg >= _next_pos);
  auto* buf_data = duckdb::FlatVector::GetDataMutable<uint64_t>(_buf);
  while (_next_pos < first_in_rg) {
    _cursor.Scan(1, _buf, 0);
    _prev_offset = buf_data[0];
    ++_next_pos;
  }
  const uint64_t first_start = _prev_offset;
  _cursor.Scan(count, out_buf, 0);
  const auto* out_data = duckdb::FlatVector::GetData<uint64_t>(out_buf);
  _prev_offset = out_data[count - 1];
  _next_pos += count;
  return first_start;
}

void ColumnReader::PointReader::Reset(const ColReader& col_reader,
                                      const ColumnReader& reader) {
  _reader = &reader;
  _ctx.Reset(col_reader);
  _segment.reset();
  _validity_segment.reset();
  _fetch_state = duckdb::ColumnFetchState{};
  _validity_fetch_state = duckdb::ColumnFetchState{};
  _cached_rg = static_cast<size_t>(-1);
  _cached_vrg = static_cast<size_t>(-1);
}

bool ColumnReader::PointReader::FetchValidity(uint64_t row, duckdb::Vector& out,
                                              duckdb::idx_t out_offset) {
  SDB_ASSERT(_reader, "PointReader not bound; call Reset first");
  if (row >= _reader->RowCount()) {
    out.BufferMutable().GetValidityMask().SetInvalid(out_offset);
    return false;
  }
  out.BufferMutable().GetValidityMask().SetValid(out_offset);
  if (!_reader->HasValidity()) {
    return true;
  }
  const auto vwindow = _reader->LocateValidity(row, {});
  if (vwindow.rg != _cached_vrg) {
    _validity_segment = _reader->OpenValiditySegment(vwindow.rg, _ctx);
    _validity_fetch_state = duckdb::ColumnFetchState{};
    _cached_vrg = vwindow.rg;
  }
  const uint64_t in_vrg = row - vwindow.begin;
  _validity_segment->FetchRow(_validity_fetch_state, in_vrg, out, out_offset);
  return out.Buffer().GetValidityMask().RowIsValid(out_offset);
}

void ColumnReader::PointReader::FetchData(uint64_t row, duckdb::Vector& out,
                                          duckdb::idx_t out_offset) {
  SDB_ASSERT(_reader, "PointReader not bound; call Reset first");
  const auto window = _reader->Locate(row);
  if (window.rg != _cached_rg) {
    _segment = _reader->OpenSegment(window.rg, _ctx);
    _fetch_state = duckdb::ColumnFetchState{};
    _cached_rg = window.rg;
  }
  const uint64_t in_rg = row - window.begin;
  _segment->FetchRow(_fetch_state, in_rg, out, out_offset);
}

bool ColumnReader::PointReader::FetchRow(uint64_t row, duckdb::Vector& out,
                                         duckdb::idx_t out_offset) {
  if (!FetchValidity(row, out, out_offset)) {
    return false;
  }
  FetchData(row, out, out_offset);
  return true;
}

bytes_view ColumnReader::BlobPointReader::FetchRow(uint64_t row) {
  if (!PointReader::FetchRow(row, _buf, 0)) {
    return {};
  }
  const auto& s = duckdb::FlatVector::GetData<duckdb::string_t>(_buf)[0];
  return {reinterpret_cast<const byte_type*>(s.GetData()),
          static_cast<size_t>(s.GetSize())};
}

bool ColumnReader::BlobPointReader::IsNullRow(uint64_t row) {
  return !FetchValidity(row, _buf, 0);
}

}  // namespace irs

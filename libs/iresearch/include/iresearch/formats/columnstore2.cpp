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

#include "columnstore2.hpp"

#include <absl/cleanup/cleanup.h>

#include "basics/down_cast.h"
#include "basics/memory.hpp"
#include "basics/number_utils.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/search/all_iterator.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/store/store_avg_utils.hpp"
#include "iresearch/utils/bitpack.hpp"
#include "iresearch/utils/compression.hpp"
#include "iresearch/utils/directory_utils.hpp"

namespace irs::columnstore2 {
namespace {

class ColumnBase;
using ColumnBasePtr = memory::managed_ptr<ColumnBase>;
using ColumnIndex = std::vector<SparseBitmapWriter::Block>;

constexpr size_t kWriterBufSize = Column::kBlockSize * sizeof(uint64_t);

constexpr SparseBitmapVersion ToSparseBitmapVersion(
  const ColumnInfo& info) noexcept {
  static_assert(SparseBitmapVersion::PrevDoc == SparseBitmapVersion{1});

  return SparseBitmapVersion{info.track_prev_doc};
}

constexpr SparseBitmapVersion ToSparseBitmapVersion(
  ColumnProperty prop) noexcept {
  static_assert(SparseBitmapVersion::PrevDoc == SparseBitmapVersion{1});

  return SparseBitmapVersion{ColumnProperty::PrevDoc ==
                             (prop & ColumnProperty::PrevDoc)};
}

std::string DataFileName(std::string_view prefix) {
  return FileName(prefix, Writer::kDataFormatExt);
}

std::string IndexFileName(std::string_view prefix) {
  return FileName(prefix, Writer::kIndexFormatExt);
}

void WriteHeader(IndexOutput& out, const ColumnHeader& hdr) {
  out.WriteU64(hdr.docs_index);
  SDB_ASSERT(hdr.id < std::numeric_limits<uint32_t>::max());
  out.WriteU32(static_cast<uint32_t>(hdr.id & 0xFFFFFFFF));
  out.WriteU32(hdr.min);
  out.WriteU32(hdr.docs_count);
  // TODO(mbkkt) maybe WriteU8 and change type and props to u8?
  out.WriteU16(std::to_underlying(hdr.type));
  out.WriteU16(std::to_underlying(hdr.props));
  out.WriteByte(std::to_underlying(hdr.value_type));

  // TODO(codeworse): add HNSW serialization
  out.WriteByte(hdr.hnsw_info.has_value());
  if (hdr.hnsw_info) {
    out.WriteU32(hdr.hnsw_info->max_doc);
    out.WriteU32(hdr.hnsw_info->d);
    out.WriteU32(hdr.hnsw_info->m);
    out.WriteU32(std::to_underlying(hdr.hnsw_info->metric));
  }
}

ColumnHeader ReadHeader(IndexInput& in) {
  ColumnHeader hdr;
  hdr.docs_index = in.ReadI64();
  hdr.id = in.ReadI32();
  hdr.min = in.ReadI32();
  hdr.docs_count = in.ReadI32();
  hdr.type = static_cast<ColumnType>(in.ReadI16());
  hdr.props = static_cast<ColumnProperty>(in.ReadI16());
  hdr.value_type = irs::ValueType{in.ReadByte()};

  uint8_t has_hnsw = in.ReadByte();
  if (has_hnsw) {
    HNSWInfo hnsw_info;
    hnsw_info.max_doc = in.ReadI32();
    hnsw_info.d = in.ReadI32();
    hnsw_info.m = in.ReadI32();
    hnsw_info.metric = static_cast<faiss::MetricType>(in.ReadI32());
    hdr.hnsw_info = std::move(hnsw_info);
  }
  return hdr;
}

bool IsEncrypted(const ColumnHeader& hdr) noexcept {
  return ColumnProperty::Encrypt == (hdr.props & ColumnProperty::Encrypt);
}

void WriteBitmapIndex(IndexOutput& out,
                      std::span<const SparseBitmapWriter::Block> blocks) {
  const uint32_t count = static_cast<uint32_t>(blocks.size());

  if (count > 2) {
    out.WriteU32(count);
    for (auto& block : blocks) {
      out.WriteU32(block.index);
      out.WriteU32(block.offset);
    }
  } else {
    out.WriteU32(0);
  }
}

ColumnIndex ReadBitmapIndex(IndexInput& in) {
  const uint32_t count = in.ReadI32();

  if (count > std::numeric_limits<uint16_t>::max()) {
    throw IndexError("Invalid number of blocks in column index");
  }

  if (count > 2) {
    ColumnIndex blocks(count);

    in.ReadBytes(reinterpret_cast<byte_type*>(blocks.data()),
                 count * sizeof(SparseBitmapWriter::Block));

    if constexpr (std::endian::native != std::endian::big) {
      for (auto& block : blocks) {
        block.index = absl::big_endian::ToHost32(block.index);
        block.offset = absl::big_endian::ToHost32(block.offset);
      }
    }

    return blocks;
  }

  return {};
}

void WriteBlocksSparse(IndexOutput& out,
                       std::span<const Column::ColumnBlock> blocks) {
  for (auto& block : blocks) {
    out.WriteU64(block.addr);
    out.WriteU64(block.avg);
    out.WriteByte(static_cast<byte_type>(block.bits));
    out.WriteU64(block.data);
    out.WriteU64(block.last_size);
  }
}

void WriteBlocksDense(IndexOutput& out,
                      std::span<const Column::ColumnBlock> blocks) {
  for (auto& block : blocks) {
    out.WriteU64(block.data);
  }
}

// Iterator over a specified contiguous range of documents
template<typename PayloadReaderImpl>
class RangeColumnIterator : public ResettableDocIterator,
                            private PayloadReaderImpl {
 private:
  using PayloadReader = PayloadReaderImpl;

  // FIXME(gnusi):
  //  * don't expose payload for noop_value_reader?
  //  * don't expose prev_doc if not requested?
  using Attributes =
    std::tuple<DocAttr, CostAttr, ScoreAttr, PrevDocAttr, PayAttr>;

 public:
  template<typename... Args>
  RangeColumnIterator(const ColumnHeader& header, bool track_prev,
                      Args&&... args)
    : PayloadReader{std::forward<Args>(args)...},
      _min_base{header.min},
      _min_doc{_min_base},
      _max_doc{_min_base + header.docs_count - 1} {
    SDB_ASSERT(_min_doc <= _max_doc);
    SDB_ASSERT(!doc_limits::eof(_max_doc));
    std::get<CostAttr>(_attrs).reset(header.docs_count);
    if (track_prev) {
      std::get<PrevDocAttr>(_attrs).reset(
        [](const void* ctx) noexcept {
          auto* self = static_cast<const RangeColumnIterator*>(ctx);
          const auto value = self->value();
          const auto max_doc = self->_max_doc;

          if (self->_min_base < value && value <= max_doc) [[likely]] {
            return value - 1;
          }

          if (value > max_doc) {
            return max_doc;
          }

          return doc_limits::invalid();
        },
        this);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  doc_id_t seek(doc_id_t doc) final {
    if (_min_doc <= doc && doc <= _max_doc) [[likely]] {
      std::get<DocAttr>(_attrs).value = doc;
      _min_doc = doc + 1;
      std::get<PayAttr>(_attrs).value = this->payload(doc - _min_base);
      return doc;
    }

    if (!doc_limits::valid(value())) {
      std::get<DocAttr>(_attrs).value = _min_doc++;
      std::get<PayAttr>(_attrs).value = this->payload(value() - _min_base);
      return value();
    }

    if (value() < doc) {
      _max_doc = doc_limits::invalid();
      _min_doc = doc_limits::eof();
      std::get<DocAttr>(_attrs).value = doc_limits::eof();
      std::get<PayAttr>(_attrs).value = {};
      return doc_limits::eof();
    }

    return value();
  }

  bool next() final {
    if (_min_doc <= _max_doc) {
      std::get<PayAttr>(_attrs).value = this->payload(_min_doc - _min_base);
      std::get<DocAttr>(_attrs).value = _min_doc++;
      return true;
    }
    std::get<PayAttr>(_attrs).value = {};
    std::get<DocAttr>(_attrs).value = doc_limits::eof();
    return false;
  }

  void reset() noexcept final {
    _min_doc = _min_base;
    _max_doc = _min_doc +
               static_cast<doc_id_t>(std::get<CostAttr>(_attrs).estimate() - 1);
    std::get<DocAttr>(_attrs).value = doc_limits::invalid();
  }

 private:
  doc_id_t _min_base;
  doc_id_t _min_doc;
  doc_id_t _max_doc;
  Attributes _attrs;
};

// Iterator over a specified bitmap of documents
template<typename PayloadReaderImpl>
class BitmapColumnIterator : public ResettableDocIterator,
                             private PayloadReaderImpl {
 private:
  using PayloadReader = PayloadReaderImpl;

  using Attributes =
    std::tuple<AttributePtr<DocAttr>, CostAttr, AttributePtr<ScoreAttr>,
               AttributePtr<PrevDocAttr>, PayAttr>;

 public:
  template<typename... Args>
  BitmapColumnIterator(IndexInput::ptr&& bitmap_in,
                       const SparseBitmapIterator::Options& opts,
                       CostAttr::Type cost, Args&&... args)
    : PayloadReader{std::forward<Args>(args)...},
      _bitmap{std::move(bitmap_in), opts, cost} {
    std::get<irs::CostAttr>(_attrs).reset(cost);
    std::get<AttributePtr<DocAttr>>(_attrs) =
      irs::GetMutable<DocAttr>(&_bitmap);
    std::get<AttributePtr<ScoreAttr>>(_attrs) =
      irs::GetMutable<ScoreAttr>(&_bitmap);
    std::get<AttributePtr<PrevDocAttr>>(_attrs) =
      irs::GetMutable<PrevDocAttr>(&_bitmap);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  doc_id_t seek(doc_id_t doc) final {
    SDB_ASSERT(doc_limits::valid(doc) || doc_limits::valid(value()));
    doc = _bitmap.seek(doc);

    if (!doc_limits::eof(doc)) {
      std::get<PayAttr>(_attrs).value = this->payload(_bitmap.index());
      return doc;
    }

    std::get<PayAttr>(_attrs).value = {};
    return doc_limits::eof();
  }

  bool next() final {
    if (_bitmap.next()) {
      std::get<PayAttr>(_attrs).value = this->payload(_bitmap.index());
      return true;
    }
    std::get<PayAttr>(_attrs).value = {};
    return false;
  }

  void reset() final { _bitmap.reset(); }

 private:
  SparseBitmapIterator _bitmap;
  Attributes _attrs;
};

class ColumnBase : public ColumnReader, private util::Noncopyable {
 public:
  ColumnBase(std::optional<std::string>&& name, IResourceManager& rm_cache,
             bstring&& payload, ColumnHeader&& hdr, ColumnIndex&& index,
             const IndexInput& stream, Encryption::Stream* cipher)
    : _resource_manager_cached{rm_cache},
      _stream{&stream},
      _cipher{cipher},
      _hdr{std::move(hdr)},
      _index{std::move(index)},
      _payload{std::move(payload)},
      _name{std::move(name)} {
    SDB_ASSERT(!IsEncrypted(_hdr) || _cipher);
  }

  ~ColumnBase() override {
    // TODO(mbkkt) Maybe move to the derived classes?
    // This will allow to batch Decrease calls
    if (const auto released = _column_data.size(); released != 0) {
      _column_data = {};  // force memory release
      _resource_manager_cached.Decrease(released);
    }
  }

  std::string_view name() const final {
    return _name.has_value() ? _name.value() : std::string_view{};
  }

  field_id id() const noexcept final { return _hdr.id; }

  bytes_view payload() const final { return _payload; }

  doc_id_t size() const noexcept final { return _hdr.docs_count; }

  const ColumnHeader& Header() const noexcept { return _hdr; }

  bool TrackPrevDoc(ColumnHint hint) const noexcept {
    return ColumnHint::PrevDoc == (hint & ColumnHint::PrevDoc);
  }

  SparseBitmapIterator::Options BitmapIteratorOptions(
    ColumnHint hint) const noexcept {
    return {.version = ToSparseBitmapVersion(Header().props),
            .track_prev_doc = TrackPrevDoc(hint),
            .use_block_index = true,
            .blocks = _index};
  }

  const IndexInput& Stream() const noexcept {
    SDB_ASSERT(_stream);
    return *_stream;
  }

  void WithHNSW(faiss::HNSW&& hnsw) {
    SDB_ASSERT(_hdr.hnsw_info);
    _hnsw_index = std::make_unique<HNSWIndexReader>(std::move(hnsw), *this,
                                                    *_hdr.hnsw_info);
  }

  void Search(HNSWSearchContext& context) const final {
    if (_hnsw_index) {
      _hnsw_index->Search(context);
    }
  }

  virtual void MakeBuffered(IndexInput&,
                            std::span<memory::managed_ptr<ColumnReader>>) {}

 protected:
  template<typename Factory>
  ResettableDocIterator::ptr MakeIterator(Factory&& f, ColumnHint hint) const;

  ColumnHeader& MutableHeader() { return _hdr; }
  void ResetStream(const IndexInput* stream) { _stream = stream; }

  bool AllocateBufferedMemory(size_t size, size_t mappings) noexcept try {
    SDB_ASSERT(size != 0);
    _resource_manager_cached.Increase(size + mappings);
    // should be only one alllocation
    SDB_ASSERT(_column_data.empty());
    _column_data.resize(size);
    return true;
  } catch (...) {
    auto column_name = name();
    if (IsNull(column_name)) {
      column_name = "<anonymous>";
    }
    SDB_INFO("xxxxx", sdb::Logger::IRESEARCH,
             "Failed to allocate memory for buffered column id ", Header().id,
             " name: ", column_name, " of size ", (size + mappings),
             ". This can happen if no columns cache was configured or the "
             "column data size exceeds the columns cache size.");
    return false;
  }

  size_t CalculateBitmapSize(size_t file_len,
                             std::span<memory::managed_ptr<ColumnReader>>
                               next_sorted_columns) const noexcept {
    if (!Header().docs_index) {
      return 0;
    }
    for (const auto& c : next_sorted_columns) {
      auto column = static_cast<ColumnBase*>(c.get());
      SDB_ASSERT(column != nullptr);
      if (column->Header().docs_index) {
        file_len = column->Header().docs_index;
        break;
      }
    }
    SDB_ASSERT(Header().docs_index < file_len);
    return file_len - Header().docs_index;
  }

  void StoreBitmapIndex(size_t bitmap_size, size_t buffer_offset,
                        RemappedBytesViewInput::mapping* mapping,
                        ColumnHeader& hdr, IndexInput& in) {
    SDB_ASSERT(bitmap_size);
    SDB_ASSERT(hdr.docs_index);
    in.ReadBytes(hdr.docs_index, _column_data.data() + buffer_offset,
                 bitmap_size);
    SDB_ASSERT(mapping || !IsEncrypted(hdr));
    if (IsEncrypted(hdr) && mapping) {
      mapping->emplace_back(hdr.docs_index, buffer_offset);
    } else {
      hdr.docs_index = buffer_offset;
    }
  }

  // TODO(mbkkt) remove unnecessary memset to zero!
  std::vector<byte_type> _column_data;
  IndexInput::ptr _buffered_input;
  IResourceManager& _resource_manager_cached;

 private:
  template<typename ValueReader>
  ResettableDocIterator::ptr MakeIterator(ValueReader&& f, IndexInput::ptr&& in,
                                          ColumnHint hint) const;

  const IndexInput* _stream;
  Encryption::Stream* _cipher;
  ColumnHeader _hdr;
  ColumnIndex _index;
  bstring _payload;
  std::optional<std::string> _name;
  std::unique_ptr<HNSWIndexReader> _hnsw_index;
};

template<typename ValueReader>
ResettableDocIterator::ptr ColumnBase::MakeIterator(ValueReader&& rdr,
                                                    IndexInput::ptr&& index_in,
                                                    ColumnHint hint) const {
  if (!index_in) {
    using IteratorType = RangeColumnIterator<ValueReader>;

    return memory::make_managed<IteratorType>(Header(), TrackPrevDoc(hint),
                                              std::move(rdr));
  } else {
    index_in->Seek(Header().docs_index);

    using IteratorType = BitmapColumnIterator<ValueReader>;

    return memory::make_managed<IteratorType>(
      std::move(index_in), BitmapIteratorOptions(hint), Header().docs_count,
      std::move(rdr));
  }
}

template<typename Factory>
ResettableDocIterator::ptr ColumnBase::MakeIterator(Factory&& f,
                                                    ColumnHint hint) const {
  SDB_ASSERT(Header().docs_count);

  IndexInput::ptr value_in = Stream().Reopen();

  if (!value_in) {
    // implementation returned wrong pointer
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Failed to reopen input");

    throw IoError{"failed to reopen input"};
  }

  IndexInput::ptr index_in = nullptr;

  if (0 != Header().docs_index) {
    index_in = value_in->Dup();

    if (!index_in) {
      // implementation returned wrong pointer
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Failed to duplicate input");

      throw IoError{"failed to duplicate input"};
    }
  }

  if (IsEncrypted(Header())) {
    SDB_ASSERT(_cipher);
    return MakeIterator(f(std::move(value_in), *_cipher), std::move(index_in),
                        hint);
  } else {
    const byte_type* data =
      value_in->ReadBuffer(0, value_in->Length(), BufferHint::PERSISTENT);

    if (data) {
      // direct buffer access
      return MakeIterator(f(data), std::move(index_in), hint);
    }

    return MakeIterator(f(std::move(value_in)), std::move(index_in), hint);
  }
}

struct NoopValueReader {
  constexpr bytes_view payload(doc_id_t) const noexcept { return {}; }
};

class ValueDirectReader {
 protected:
  explicit ValueDirectReader(const byte_type* data) noexcept : _data{data} {
    SDB_ASSERT(data);
  }

  bytes_view value(uint64_t offset, size_t length) noexcept {
    return {_data + offset, length};
  }

  const byte_type* _data;
};

template<bool Resize>
class ValueReader {
 protected:
  ValueReader(IndexInput::ptr data_in, size_t size)
    : buf_(size, 0), data_in_{std::move(data_in)} {}

  bytes_view value(uint64_t offset, size_t length) {
    if constexpr (Resize) {
      buf_.resize(length);
    }

    auto* buf = buf_.data();

    [[maybe_unused]] const size_t read =
      data_in_->ReadBytes(offset, buf, length);
    SDB_ASSERT(read == length);

    return {buf, length};
  }

  bstring buf_;
  IndexInput::ptr data_in_;
};

template<bool Resize>
class EncryptedValueReader {
 protected:
  EncryptedValueReader(IndexInput::ptr&& data_in, Encryption::Stream* cipher,
                       size_t size)
    : buf_(size, 0), data_in_{std::move(data_in)}, _cipher{cipher} {}

  bytes_view value(uint64_t offset, size_t length) {
    if constexpr (Resize) {
      buf_.resize(length);
    }

    auto* buf = buf_.data();

    [[maybe_unused]] const size_t read =
      data_in_->ReadBytes(offset, buf, length);
    SDB_ASSERT(read == length);

    [[maybe_unused]] const bool ok = _cipher->Decrypt(offset, buf, length);
    SDB_ASSERT(ok);

    return {buf, length};
  }

  bstring buf_;
  IndexInput::ptr data_in_;
  Encryption::Stream* _cipher;
};

ResettableDocIterator::ptr MakeMaskIterator(const ColumnBase& column,
                                            ColumnHint hint) {
  const auto& header = column.Header();

  if (0 == header.docs_count) {
    // only mask column can be empty
    return ResettableDocIterator::empty();
  }

  if (0 == header.docs_index) {
    return memory::make_managed<RangeColumnIterator<NoopValueReader>>(
      header, column.TrackPrevDoc(hint));
  }

  auto dup = column.Stream().Reopen();

  if (!dup) {
    // implementation returned wrong pointer
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Failed to reopen input");

    throw IoError{"failed to reopen input"};
  }

  dup->Seek(header.docs_index);

  return memory::make_managed<SparseBitmapIterator>(
    std::move(dup), column.BitmapIteratorOptions(hint), header.docs_count);
}

struct MaskColumn : public ColumnBase {
  static ColumnBasePtr Read(std::optional<std::string>&& name,
                            IResourceManager& rm_r, IResourceManager& rm_c,
                            bstring&& payload, ColumnHeader&& hdr,
                            ColumnIndex&& index, IndexInput& /*index_in*/,
                            const IndexInput& data_in,
                            compression::Decompressor::ptr&& /*inflater*/,
                            Encryption::Stream* cipher) {
    return memory::make_tracked<MaskColumn>(rm_r, std::move(name), rm_c,
                                            std::move(payload), std::move(hdr),
                                            std::move(index), data_in, cipher);
  }

  MaskColumn(std::optional<std::string>&& name, IResourceManager& rm_c,
             bstring&& payload, ColumnHeader&& hdr, ColumnIndex&& index,
             const IndexInput& data_in, Encryption::Stream* cipher)
    : ColumnBase{std::move(name),
                 rm_c,
                 std::move(payload),
                 std::move(hdr),
                 std::move(index),
                 data_in,
                 cipher} {
    SDB_ASSERT(ColumnType::Mask == Header().type);
  }

  ResettableDocIterator::ptr iterator(ColumnHint hint) const final;
};

ResettableDocIterator::ptr MaskColumn::iterator(ColumnHint hint) const {
  return MakeMaskIterator(*this, hint);
}

class DenseFixedLengthColumn : public ColumnBase {
 public:
  static ColumnBasePtr Read(std::optional<std::string>&& name,
                            IResourceManager& rm_r, IResourceManager& rm_c,
                            bstring&& payload, ColumnHeader&& hdr,
                            ColumnIndex&& index, IndexInput& index_in,
                            const IndexInput& data_in,
                            compression::Decompressor::ptr&& inflater,
                            Encryption::Stream* cipher);

  DenseFixedLengthColumn(std::optional<std::string>&& name,
                         IResourceManager& rm_c, bstring&& payload,
                         ColumnHeader&& hdr, ColumnIndex&& index,
                         const IndexInput& data_in,
                         compression::Decompressor::ptr&& inflater,
                         Encryption::Stream* cipher, uint64_t data,
                         uint64_t len)
    : ColumnBase{std::move(name),
                 rm_c,
                 std::move(payload),
                 std::move(hdr),
                 std::move(index),
                 data_in,
                 cipher},
      _inflater{std::move(inflater)},
      _data{data},
      _len{len} {
    SDB_ASSERT(Header().docs_count);
    SDB_ASSERT(ColumnType::DenseFixed == Header().type);
  }

  ~DenseFixedLengthColumn() override {
    if (IsEncrypted(Header()) && !_column_data.empty()) {
      _buffered_input.reset();  // force memory release
      _resource_manager_cached.Decrease(
        sizeof(RemappedBytesViewInput::mapping_value) * 2);
    }
  }

  ResettableDocIterator::ptr iterator(ColumnHint hint) const final;

  void MakeBuffered(
    IndexInput& in,
    std::span<memory::managed_ptr<ColumnReader>> next_sorted_columns) final {
    auto& hdr = MutableHeader();
    const auto data_size = _len * hdr.docs_count;
    const auto bitmap_size =
      CalculateBitmapSize(in.Length(), next_sorted_columns);
    const auto total_size = data_size + bitmap_size;
    size_t mapping_size{0};
    const bool encrypted = IsEncrypted(hdr);
    if (encrypted) {
      // We don't want to store actual number to not increase column size,
      // so it's approximated number of mappings
      mapping_size = sizeof(RemappedBytesViewInput::mapping_value) * 2;
    }
    if (!AllocateBufferedMemory(total_size, mapping_size)) {
      return;
    }
    in.ReadBytes(_data, _column_data.data(), data_size);
    RemappedBytesViewInput::mapping mapping;
    if (bitmap_size) {
      StoreBitmapIndex(bitmap_size, data_size, &mapping, hdr, in);
    }
    if (encrypted) {
      mapping.emplace_back(_data, 0);
      _buffered_input = std::make_unique<RemappedBytesViewInput>(
        bytes_view{_column_data.data(), _column_data.size()},
        std::move(mapping));
    } else {
      _buffered_input = std::make_unique<BytesViewInput>(
        bytes_view{_column_data.data(), _column_data.size()});
      _data = 0;
    }
    ResetStream(_buffered_input.get());
  }

 private:
  template<typename ValueReader>
  class PayloadReader : private ValueReader {
   public:
    template<typename... Args>
    PayloadReader(uint64_t data, uint64_t len, Args&&... args)
      : ValueReader{std::forward<Args>(args)...}, _data{data}, _len{len} {}

    bytes_view payload(doc_id_t i) {
      const auto offset = _data + _len * i;

      return ValueReader::value(offset, _len);
    }

   private:
    uint64_t _data;  // where data starts
    uint64_t _len;   // data entry length
  };

  compression::Decompressor::ptr _inflater;
  uint64_t _data;
  uint64_t _len;
};

ColumnBasePtr DenseFixedLengthColumn::Read(
  std::optional<std::string>&& name, IResourceManager& rm_r,
  IResourceManager& rm_c, bstring&& payload, ColumnHeader&& hdr,
  ColumnIndex&& index, IndexInput& index_in, const IndexInput& data_in,
  compression::Decompressor::ptr&& inflater, Encryption::Stream* cipher) {
  const uint64_t len = index_in.ReadI64();
  const uint64_t data = index_in.ReadI64();
  return memory::make_tracked<DenseFixedLengthColumn>(
    rm_r, std::move(name), rm_c, std::move(payload), std::move(hdr),
    std::move(index), data_in, std::move(inflater), cipher, data, len);
}

ResettableDocIterator::ptr DenseFixedLengthColumn::iterator(
  ColumnHint hint) const {
  if (ColumnHint::Mask == (ColumnHint::Mask & hint)) {
    return MakeMaskIterator(*this, hint);
  }

  struct Factory {
    PayloadReader<EncryptedValueReader<false>> operator()(
      IndexInput::ptr&& stream, Encryption::Stream& cipher) const {
      return {ctx->_data, ctx->_len, std::move(stream), &cipher, ctx->_len};
    }

    PayloadReader<ValueReader<false>> operator()(
      IndexInput::ptr&& stream) const {
      return {ctx->_data, ctx->_len, std::move(stream), ctx->_len};
    }

    PayloadReader<ValueDirectReader> operator()(const byte_type* data) const {
      return {ctx->_data, ctx->_len, data};
    }

    const DenseFixedLengthColumn* ctx;
  };

  return MakeIterator(Factory{this}, hint);
}

class FixedLengthColumn : public ColumnBase {
 public:
  using Blocks = ManagedVector<uint64_t>;

  static ColumnBasePtr Read(std::optional<std::string>&& name,
                            IResourceManager& rm_r, IResourceManager& rm_c,
                            bstring&& payload, ColumnHeader&& hdr,
                            ColumnIndex&& index, IndexInput& index_in,
                            const IndexInput& data_in,
                            compression::Decompressor::ptr&& inflater,
                            Encryption::Stream* cipher) {
    const uint64_t len = index_in.ReadI64();
    auto blocks = ReadBlocksDense(hdr, index_in, rm_r);
    return memory::make_tracked<FixedLengthColumn>(
      rm_r, std::move(name), rm_c, std::move(payload), std::move(hdr),
      std::move(index), data_in, std::move(inflater), cipher, std::move(blocks),
      len);
  }

  FixedLengthColumn(std::optional<std::string>&& name,
                    IResourceManager& resource_manager_cache, bstring&& payload,
                    ColumnHeader&& hdr, ColumnIndex&& index,
                    const IndexInput& data_in,
                    compression::Decompressor::ptr&& inflater,
                    Encryption::Stream* cipher, Blocks&& blocks, uint64_t len)
    : ColumnBase{std::move(name), resource_manager_cache, std::move(payload),
                 std::move(hdr),  std::move(index),       data_in,
                 cipher},
      _blocks{blocks},
      _inflater{std::move(inflater)},
      _len{len} {
    SDB_ASSERT(Header().docs_count);
    SDB_ASSERT(ColumnType::Fixed == Header().type);
  }

  ~FixedLengthColumn() override {
    if (IsEncrypted(Header()) && !_column_data.empty()) {
      _buffered_input.reset();  // force memory release
      _resource_manager_cached.Decrease(
        sizeof(RemappedBytesViewInput::mapping_value) * _blocks.size());
    }
  }

  ResettableDocIterator::ptr iterator(ColumnHint hint) const final;

  void MakeBuffered(
    IndexInput& in,
    std::span<memory::managed_ptr<ColumnReader>> next_sorted_columns) final {
    auto& hdr = MutableHeader();
    if (!IsEncrypted(hdr)) {
      if (MakeBufferedData<false>(_len, hdr, in, _blocks, _column_data,
                                  next_sorted_columns, nullptr)) {
        _buffered_input = std::make_unique<BytesViewInput>(
          bytes_view{_column_data.data(), _column_data.size()});
      }
    } else {
      RemappedBytesViewInput::mapping mapping;
      if (MakeBufferedData<true>(_len, hdr, in, _blocks, _column_data,
                                 next_sorted_columns, &mapping)) {
        _buffered_input = std::make_unique<RemappedBytesViewInput>(
          bytes_view{_column_data.data(), _column_data.size()},
          std::move(mapping));
      }
    }
    if (_buffered_input) {
      ResetStream(_buffered_input.get());
    }
  }

 private:
  using ColumnBlock = uint64_t;

  template<typename ValueReader>
  class PayloadReader : private ValueReader {
   public:
    template<typename... Args>
    PayloadReader(const ColumnBlock* blocks, uint64_t len, Args&&... args)
      : ValueReader{std::forward<Args>(args)...}, _blocks{blocks}, _len{len} {}

    bytes_view payload(doc_id_t i) {
      const auto block_idx = i / Column::kBlockSize;
      const auto value_idx = i % Column::kBlockSize;

      const auto offset = _blocks[block_idx] + _len * value_idx;

      return ValueReader::value(offset, _len);
    }

   private:
    const ColumnBlock* _blocks;
    uint64_t _len;
  };

  template<bool Encrypted>
  bool MakeBufferedData(
    uint64_t len, ColumnHeader& hdr, IndexInput& in, Blocks& blocks,
    std::vector<byte_type>& column_data,
    std::span<memory::managed_ptr<ColumnReader>> next_sorted_columns,
    RemappedBytesViewInput::mapping* mapping) {
    SDB_ASSERT(!blocks.empty());
    const auto last_block_full = hdr.docs_count % Column::kBlockSize == 0;
    auto last_offset = blocks.back();
    std::vector<std::tuple<size_t, size_t, size_t>> blocks_offsets;
    size_t blocks_data_size{0};
    size_t block_index{0};
    blocks_offsets.reserve(blocks.size());
    size_t mapping_size{0};
    if constexpr (Encrypted) {
      // We don't want to store actual number to not increase column size,
      // so it's approximated number of mappings
      mapping_size =
        sizeof(RemappedBytesViewInput::mapping_value) * blocks.size();
    }
    for (auto& block : blocks) {
      size_t length = (block != last_offset || last_block_full)
                        ? Column::kBlockSize
                        : hdr.docs_count % Column::kBlockSize;
      length = length * len;
      blocks_offsets.emplace_back(block_index++, blocks_data_size, length);
      blocks_data_size += length;
    }
    const auto bitmap_index_size =
      CalculateBitmapSize(in.Length(), next_sorted_columns);
    if (!AllocateBufferedMemory(bitmap_index_size + blocks_data_size,
                                mapping_size)) {
      return false;
    }
    absl::c_sort(blocks_offsets, [&](const auto& lhs, const auto& rhs) {
      return blocks[std::get<0>(lhs)] < blocks[std::get<0>(rhs)];
    });
    for (const auto& offset : blocks_offsets) {
      auto& block = blocks[std::get<0>(offset)];
      in.ReadBytes(block, column_data.data() + std::get<1>(offset),
                   std::get<2>(offset));
      if constexpr (Encrypted) {
        SDB_ASSERT(mapping);
        mapping->emplace_back(block, std::get<1>(offset));
      } else {
        block = std::get<1>(offset);
      }
    }
    if (bitmap_index_size) {
      StoreBitmapIndex(bitmap_index_size, blocks_data_size, mapping, hdr, in);
    }
    return true;
  }

  static Blocks ReadBlocksDense(const ColumnHeader& hdr, IndexInput& in,
                                IResourceManager& resource_manager) {
    const auto blocks_count =
      math::DivCeil32(hdr.docs_count, Column::kBlockSize);
    Blocks blocks(blocks_count, {resource_manager});

    in.ReadBytes(reinterpret_cast<byte_type*>(blocks.data()),
                 sizeof(uint64_t) * blocks.size());

    if constexpr (std::endian::native != std::endian::big) {
      // FIXME simd?
      for (auto& block : blocks) {
        block = absl::big_endian::ToHost64(block);
      }
    }

    return blocks;
  }

  Blocks _blocks;
  compression::Decompressor::ptr _inflater;
  uint64_t _len;
};

ResettableDocIterator::ptr FixedLengthColumn::iterator(ColumnHint hint) const {
  if (ColumnHint::Mask == (ColumnHint::Mask & hint)) {
    return MakeMaskIterator(*this, hint);
  }

  struct Factory {
    PayloadReader<EncryptedValueReader<false>> operator()(
      IndexInput::ptr&& stream, Encryption::Stream& cipher) const {
      return {ctx->_blocks.data(), ctx->_len, std::move(stream), &cipher,
              ctx->_len};
    }

    PayloadReader<ValueReader<false>> operator()(
      IndexInput::ptr&& stream) const {
      return {ctx->_blocks.data(), ctx->_len, std::move(stream), ctx->_len};
    }

    PayloadReader<ValueDirectReader> operator()(const byte_type* data) const {
      return {ctx->_blocks.data(), ctx->_len, data};
    }

    const FixedLengthColumn* ctx;
  };

  return MakeIterator(Factory{this}, hint);
}

class SparseColumn : public ColumnBase {
 public:
  struct ColumnBlock : Column::ColumnBlock {
    doc_id_t last;
  };

  static ColumnBasePtr Read(std::optional<std::string>&& name,
                            IResourceManager& rm_r, IResourceManager& rm_c,
                            bstring&& payload, ColumnHeader&& hdr,
                            ColumnIndex&& index, IndexInput& index_in,
                            const IndexInput& data_in,
                            compression::Decompressor::ptr&& inflater,
                            Encryption::Stream* cipher) {
    auto blocks = ReadBlocksSparse(hdr, index_in, rm_r);
    return memory::make_tracked<SparseColumn>(
      rm_r, std::move(name), rm_c, std::move(payload), std::move(hdr),
      std::move(index), data_in, std::move(inflater), cipher,
      std::move(blocks));
  }

  SparseColumn(std::optional<std::string>&& name,
               IResourceManager& resource_manager, bstring&& payload,
               ColumnHeader&& hdr, ColumnIndex&& index,
               const IndexInput& data_in,
               compression::Decompressor::ptr&& inflater,
               Encryption::Stream* cipher, ManagedVector<ColumnBlock>&& blocks)
    : ColumnBase{std::move(name), resource_manager, std::move(payload),
                 std::move(hdr),  std::move(index), data_in,
                 cipher},
      _blocks{std::move(blocks)},
      _inflater{std::move(inflater)} {
    SDB_ASSERT(Header().docs_count);
    SDB_ASSERT(ColumnType::Sparse == Header().type);
  }

  ~SparseColumn() override {
    if (IsEncrypted(Header()) && !_column_data.empty()) {
      _buffered_input.reset();  // force memory release
      _resource_manager_cached.Decrease(
        sizeof(RemappedBytesViewInput::mapping_value) * _blocks.size() * 2);
    }
  }

  ResettableDocIterator::ptr iterator(ColumnHint hint) const final;

  void MakeBuffered(
    IndexInput& in,
    std::span<memory::managed_ptr<ColumnReader>> next_sorted_columns) final {
    auto& hdr = MutableHeader();
    if (!IsEncrypted(hdr)) {
      if (MakeBufferedData<false>(hdr, in, _blocks, _column_data,
                                  next_sorted_columns, nullptr)) {
        _buffered_input = std::make_unique<BytesViewInput>(
          bytes_view{_column_data.data(), _column_data.size()});
      }
    } else {
      RemappedBytesViewInput::mapping mapping;
      if (MakeBufferedData<true>(hdr, in, _blocks, _column_data,
                                 next_sorted_columns, &mapping)) {
        _buffered_input = std::make_unique<RemappedBytesViewInput>(
          bytes_view{_column_data.data(), _column_data.size()},
          std::move(mapping));
      }
    }
    if (_buffered_input) {
      ResetStream(_buffered_input.get());
    }
  }

 private:
  static ManagedVector<ColumnBlock> ReadBlocksSparse(
    const ColumnHeader& hdr, IndexInput& in,
    IResourceManager& resource_manager);

  template<typename ValueReader>
  class PayloadReader : private ValueReader {
   public:
    template<typename... Args>
    PayloadReader(const ColumnBlock* blocks, Args&&... args)
      : ValueReader{std::forward<Args>(args)...}, _blocks{blocks} {}

    bytes_view payload(doc_id_t i);

   private:
    const ColumnBlock* _blocks;
  };

  template<bool Encrypted>
  bool MakeBufferedData(
    ColumnHeader& hdr, IndexInput& in, ManagedVector<ColumnBlock>& blocks,
    std::vector<byte_type>& column_data,
    std::span<memory::managed_ptr<ColumnReader>> next_sorted_columns,
    RemappedBytesViewInput::mapping* mapping) {
    // idx adr/block offset length source
    std::vector<std::tuple<size_t, bool, size_t, size_t, size_t>> chunks;
    size_t chunks_size{0};
    size_t block_idx{0};
    std::vector<byte_type> addr_buffer;
    chunks.reserve(blocks.size());  // minimum, we may even need more chunks
    size_t mapping_size{0};
    if constexpr (Encrypted) {
      // We don't want to store actual number to not increase column size,
      // so it's approximated number of mappings
      mapping_size =
        sizeof(RemappedBytesViewInput::mapping_value) * blocks.size() * 2;
    }
    for (auto& block : blocks) {
      size_t length{0};
      if (bitpack::kAllEqual == block.bits) {
        length = block.avg * block.last;
      } else {
        // addr table size
        const size_t addr_length = packed::BytesRequired64(
          math::Ceil64((block.last + 1), packed::kBlockSize64), block.bits);
        chunks.emplace_back(block_idx, true, chunks_size, addr_length,
                            block.addr);
        chunks_size += addr_length;
        // block data size is calculated as end of the data for the last
        // document in block
        const size_t block_size = block.bits * sizeof(uint64_t);
        const auto value_index = block.last % packed::kBlockSize64;
        addr_buffer.resize(block_size);
        in.ReadBytes(block.addr + addr_length - block_size, addr_buffer.data(),
                     block_size);
        const uint64_t start_delta = sdb::ZigZagDecode64(packed::FastpackAt(
          reinterpret_cast<const uint64_t*>(addr_buffer.data()), value_index,
          block.bits));
        length = block.avg * block.last + start_delta;
      }
      length += block.last_size;
      // ALL_EQUAL could also be an empty block but still need this chunk to
      // properly calculate offsets
      SDB_ASSERT(length || bitpack::kAllEqual == block.bits);
      chunks.emplace_back(block_idx++, false, chunks_size, length, block.data);
      chunks_size += length;
    }
    const auto bitmap_size =
      CalculateBitmapSize(in.Length(), next_sorted_columns);
    if (!AllocateBufferedMemory(bitmap_size + chunks_size, mapping_size)) {
      return false;
    }

    absl::c_sort(chunks, [](const auto& lhs, const auto& rhs) {
      return std::get<4>(lhs) < std::get<4>(rhs);
    });

    for (const auto& chunk : chunks) {
      auto& block = blocks[std::get<0>(chunk)];
      if (bitpack::kAllEqual == block.bits || !std::get<1>(chunk)) {
        SDB_ASSERT(block.data == std::get<4>(chunk));
        in.ReadBytes(block.data, column_data.data() + std::get<2>(chunk),
                     std::get<3>(chunk));
        if constexpr (Encrypted) {
          SDB_ASSERT(mapping);
          mapping->emplace_back(block.data, std::get<2>(chunk));
        } else {
          block.data = std::get<2>(chunk);
          if (bitpack::kAllEqual == block.bits) {
            block.addr = std::get<2>(chunk);
          }
        }
      } else {
        SDB_ASSERT(std::get<1>(chunk));
        SDB_ASSERT(block.addr == std::get<4>(chunk));
        in.ReadBytes(block.addr, column_data.data() + std::get<2>(chunk),
                     std::get<3>(chunk));
        if constexpr (Encrypted) {
          SDB_ASSERT(mapping);
          mapping->emplace_back(block.addr, std::get<2>(chunk));
        } else {
          block.addr = std::get<2>(chunk);
        }
      }
    }
    if (bitmap_size) {
      StoreBitmapIndex(bitmap_size, chunks_size, mapping, hdr, in);
    }
    return true;
  }

  ManagedVector<ColumnBlock> _blocks;
  compression::Decompressor::ptr _inflater;
};

template<typename ValueReader>
bytes_view SparseColumn::PayloadReader<ValueReader>::payload(doc_id_t i) {
  const auto& block = _blocks[i / Column::kBlockSize];
  const size_t index = i % Column::kBlockSize;

  if (bitpack::kAllEqual == block.bits) {
    const size_t addr = block.data + block.avg * index;

    size_t length = block.avg;
    if (block.last == index) [[unlikely]] {
      length = block.last_size;
    }

    return ValueReader::value(addr, length);
  }

  const size_t block_size = block.bits * sizeof(uint64_t);
  const size_t block_index = index / packed::kBlockSize64;
  size_t value_index = index % packed::kBlockSize64;

  const size_t addr_offset = block.addr + block_index * block_size;

  const byte_type* addr_buf;
  if constexpr (std::is_same_v<ValueReader, ValueDirectReader>) {
    addr_buf = this->_data + addr_offset;
  } else {
    this->buf_.resize(block_size);
    this->data_in_->ReadBytes(addr_offset, this->buf_.data(), block_size);
    addr_buf = this->buf_.c_str();
  }
  const uint64_t start_delta = sdb::ZigZagDecode64(packed::FastpackAt(
    reinterpret_cast<const uint64_t*>(addr_buf), value_index, block.bits));
  const uint64_t start = block.avg * index + start_delta;

  size_t length = block.last_size;
  if (block.last != index) [[likely]] {
    if (++value_index == 64) [[unlikely]] {
      value_index = 0;

      if constexpr (std::is_same_v<ValueReader, ValueDirectReader>) {
        addr_buf += block_size;
      } else {
        this->buf_.resize(block_size);
        this->data_in_->ReadBytes(this->buf_.data(), block_size);
        addr_buf = this->buf_.c_str();
      }
    }

    const uint64_t end_delta = sdb::ZigZagDecode64(packed::FastpackAt(
      reinterpret_cast<const uint64_t*>(addr_buf), value_index, block.bits));
    length = end_delta - start_delta + block.avg;
  }

  const auto offset = block.data + start;

  return ValueReader::value(offset, length);
}

std::vector<SparseColumn::ColumnBlock,
            ManagedTypedAllocator<SparseColumn::ColumnBlock>>
SparseColumn::ReadBlocksSparse(const ColumnHeader& hdr, IndexInput& in,
                               IResourceManager& resource_manager) {
  const auto blocks_count = math::DivCeil32(hdr.docs_count, Column::kBlockSize);
  std::vector<SparseColumn::ColumnBlock,
              ManagedTypedAllocator<SparseColumn::ColumnBlock>>
    blocks{blocks_count, {resource_manager}};

  // FIXME optimize
  for (auto& block : blocks) {
    block.addr = in.ReadI64();
    block.avg = in.ReadI64();
    block.bits = in.ReadByte();
    block.data = in.ReadI64();
    block.last_size = in.ReadI64();
    block.last = Column::kBlockSize - 1;
  }
  blocks.back().last = uint16_t(hdr.docs_count % Column::kBlockSize - 1U);

  return blocks;
}

ResettableDocIterator::ptr SparseColumn::iterator(ColumnHint hint) const {
  if (ColumnHint::Mask == (ColumnHint::Mask & hint)) {
    return MakeMaskIterator(*this, hint);
  }

  struct Factory {
    PayloadReader<EncryptedValueReader<true>> operator()(
      IndexInput::ptr&& stream, Encryption::Stream& cipher) const {
      return {ctx->_blocks.data(), std::move(stream), &cipher, size_t{0}};
    }

    PayloadReader<ValueReader<true>> operator()(
      IndexInput::ptr&& stream) const {
      return {ctx->_blocks.data(), std::move(stream), size_t{0}};
    }

    PayloadReader<ValueDirectReader> operator()(const byte_type* data) const {
      return {ctx->_blocks.data(), data};
    }

    const SparseColumn* ctx;
  };

  return MakeIterator(Factory{this}, hint);
}

using ColumnFactoryF = ColumnBasePtr (*)(
  std::optional<std::string>&&, IResourceManager&, IResourceManager&, bstring&&,
  ColumnHeader&&, ColumnIndex&&, IndexInput&, const IndexInput&,
  compression::Decompressor::ptr&&, Encryption::Stream*);

constexpr ColumnFactoryF kFactories[]{&SparseColumn::Read, &MaskColumn::Read,
                                      &FixedLengthColumn::Read,
                                      &DenseFixedLengthColumn::Read};

bool Less(std::string_view lhs, std::string_view rhs) noexcept {
  if (IsNull(rhs)) {
    return false;
  }

  if (IsNull(lhs)) {
    return true;
  }

  return lhs < rhs;
}

}  // namespace

void Column::Prepare(doc_id_t key) {
  SDB_ASSERT(doc_limits::invalid() < key);
  SDB_ASSERT(key < doc_limits::eof());
#ifdef SDB_DEV
  SDB_ASSERT(!_sealed);
#endif
  if (key > _pend) [[likely]] {
    if (_addr_table.full()) {
      flush_block();
    }

    _prev = _pend;
    _pend = key;
    _docs_writer.push_back(key);
    _addr_table.push_back(_data.stream.Position());
  }
}

void Column::Reset() {
  if (_addr_table.empty()) {
    return;
  }

  [[maybe_unused]] const bool res = _docs_writer.erase(_pend);
  SDB_ASSERT(res);
  _data.stream.Truncate(_addr_table.back());
  _addr_table.pop_back();
  _pend = _prev;
}

void Column::flush_block() {
  SDB_ASSERT(!_addr_table.empty());
  SDB_ASSERT(_ctx.data_out);
  _data.stream.Flush();

  auto& data_out = *_ctx.data_out;
  auto& block = _blocks.emplace_back();
  block.addr = data_out.Position();
  const auto data_len = _data.file.Length();
  const auto last_offset = _addr_table.back();
  block.last_size = data_len - last_offset;

  const uint32_t used_docs_count = _addr_table.size();
  const uint64_t pack_docs_count =
    math::Ceil64(used_docs_count, packed::kBlockSize64);

  auto addr_blocks = _addr_table.blocks();
  const auto real_last_block_size = addr_blocks.back().size;
  addr_blocks.back().size -= _addr_table.available();
  const auto used_last_block_size = addr_blocks.back().size;
  const auto pack_last_block_size =
    math::Ceil64(used_last_block_size, packed::kBlockSize64);
  absl::Cleanup rollback_addr_blocks = [&] {
    addr_blocks.back().size = real_last_block_size;
  };

  const auto first_offset = addr_blocks[0].data[0];

  uint64_t or_value = 0;
  if (data_len) {
    std::fill(addr_blocks.back().data + used_last_block_size,
              addr_blocks.back().data + pack_last_block_size, 0);

    const uint64_t avg =
      std::lround(static_cast<double>(last_offset - first_offset) /
                  std::max<double>(1, used_docs_count - 1));
    auto avg_base = first_offset;
    for (const auto& addr_block : addr_blocks) {
      for (size_t i = 0; i != addr_block.size; ++i) {
        auto& offset = addr_block.data[i];
        offset = sdb::ZigZagEncode64(offset - avg_base);
        or_value |= offset;
        avg_base += avg;
      }
    }
    block.avg = avg;

    addr_blocks.back().size = pack_last_block_size;
  } else {
    block.avg = 0;
  }

  if (or_value) {
    block.bits = packed::Maxbits64(or_value);
    const size_t buf_size =
      packed::BytesRequired64(pack_docs_count, block.bits);
    auto* buf = _ctx.u64buf;
    // TODO(mbkkt) avoid zeroing the buffer
    std::memset(buf, 0, buf_size);
    for (const auto& addr_block : addr_blocks) {
      buf = packed::Pack(addr_block.data, addr_block.data + addr_block.size,
                         buf, block.bits);
    }

    data_out.WriteBytes(_ctx.u8buf, buf_size);
    _fixed_length = false;
  } else {
    block.bits = bitpack::kAllEqual;
    SDB_ASSERT(absl::c_all_of(addr_blocks, [](const auto& addr_block) {
      return absl::c_all_of(std::span{addr_block.data, addr_block.size},
                            [](auto v) { return v == 0; });
    }));

    // column is fixed length IFF
    // * it is still a fixed length column
    // * values in a block are of the same length including the last one
    // * values in all blocks have the same length
    _fixed_length = _fixed_length && block.last_size == block.avg &&
                    (0 == _docs_count || block.avg == _prev_avg);
    _prev_avg = block.avg;
  }

#ifdef SDB_DEV
  block.size = data_len;
#endif

  auto offset = data_out.Position();
  block.data = offset + first_offset;
  if (data_len) {
    if (_ctx.cipher) {
      auto encrypt_and_copy = [&](byte_type* b, size_t len) {
        if (!_ctx.cipher->Encrypt(offset, b, len)) {
          return false;
        }

        data_out.WriteBytes(b, len);
        offset += len;
        return true;
      };

      if (!_data.file.Visit(encrypt_and_copy)) {
        throw IoError("failed to encrypt columnstore");
      }
    } else {
      _data.file >> data_out;
    }

    _data.stream.Truncate(0);
    _data.file.Reset();
  }

  std::move(rollback_addr_blocks).Invoke();
  _addr_table.reset();

  _docs_count += used_docs_count;
}

Column::Column(const Context& ctx, field_id id, ValueType value_type,
               ColumnFinalizer&& finalizer, IResourceManager& resource_manager)
  : _ctx{ctx},
    _finalizer{std::move(finalizer)},
    _blocks{{resource_manager}},
    _data{resource_manager},
    _docs{resource_manager},
    _addr_table{{resource_manager}},
    _id{id},
    _value_type{value_type} {
  SDB_ASSERT(field_limits::valid(_id));
}

void Column::finish(IndexOutput& index_out) {
  SDB_ASSERT(_id < field_limits::invalid());
  SDB_ASSERT(_ctx.data_out);

  _docs_writer.finish();
  _docs.stream.Flush();

  ColumnHeader hdr;
  hdr.docs_count = _docs_count;
  hdr.id = _id;

  MemoryIndexInput in{_docs.file};
  SparseBitmapIterator it{
    &in, SparseBitmapIterator::Options{.version = _ctx.version,
                                       .track_prev_doc = false,
                                       .use_block_index = false,
                                       .blocks = {}}};
  if (it.next()) {
    hdr.min = it.value();
  }

  // FIXME(gnusi): how to deal with rollback() and docs_writer_.back()?
  if (_docs_count && (hdr.min + _docs_count - doc_limits::min() != _pend)) {
    auto& data_out = *_ctx.data_out;

    // we don't need to store bitmap index in case
    // if every document in a column has a value
    hdr.docs_index = data_out.Position();
    _docs.file >> data_out;
  }

  if (IsNull(_name)) {
    hdr.props |= ColumnProperty::NoName;
  }

  if (_ctx.cipher) {
    hdr.props |= ColumnProperty::Encrypt;
  }

  if (_docs_writer.version() == SparseBitmapVersion::PrevDoc) {
    hdr.props |= ColumnProperty::PrevDoc;
  }

  if (_fixed_length) {
    if (0 == _prev_avg) {
      hdr.type = ColumnType::Mask;
    } else if (_ctx.consolidation) {
#ifdef SDB_DEV
      // ensure blocks are dense after consolidation
      auto prev = std::begin(_blocks);
      if (prev != std::end(_blocks)) {
        auto next = std::next(prev);

        for (; next != std::end(_blocks); ++next) {
          SDB_ASSERT(next->data == prev->size + prev->data);
          prev = next;
        }
      }
#endif
      hdr.type = ColumnType::DenseFixed;
    } else {
      hdr.type = ColumnType::Fixed;
    }
  }
  hdr.value_type = _value_type;
  hdr.hnsw_info = _ctx.hnsw_info;
  if (hdr.hnsw_info) {
    hdr.hnsw_info->max_doc = _pend;
  }

  WriteHeader(index_out, hdr);
  FinalizePayload(index_out);
  if (!IsNull(_name)) {
    if (_ctx.cipher) {
      auto name = static_cast<std::string>(_name);
      _ctx.cipher->Encrypt(index_out.Position(),
                           reinterpret_cast<byte_type*>(name.data()),
                           name.size());
      WriteStr(index_out, name);
    } else {
      WriteStr(index_out, _name);
    }
  } else {
    SDB_ASSERT(ColumnProperty::NoName == (ColumnProperty::NoName & hdr.props));
  }

  if (hdr.docs_index) {
    // write bitmap index IFF it's really necessary
    WriteBitmapIndex(index_out, _docs_writer.index());
  }

  if (ColumnType::Sparse == hdr.type) {
    WriteBlocksSparse(index_out, _blocks);
  } else if (ColumnType::Mask != hdr.type) {
    index_out.WriteU64(_blocks.front().avg);
    if (ColumnType::DenseFixed == hdr.type) {
      index_out.WriteU64(_blocks.front().data);
    } else {
      SDB_ASSERT(ColumnType::Fixed == hdr.type);
      WriteBlocksDense(index_out, _blocks);
    }
  }
}

Writer::Writer(Version version, bool consolidation,
               IResourceManager& resource_manager)
  : _columns{{resource_manager}}, _ver{version}, _consolidation{consolidation} {
  // TODO(mbkkt) avoid 0.5 MB per column writer
  ManagedTypedAllocator<byte_type> alloc{_columns.get_allocator()};
  _buf = alloc.allocate(kWriterBufSize);
}

Writer::~Writer() {
  ManagedTypedAllocator<byte_type> alloc{_columns.get_allocator()};
  alloc.deallocate(_buf, kWriterBufSize);
}

void Writer::prepare(Directory& dir, const SegmentMeta& meta) {
  _columns.clear();

  auto filename = DataFileName(meta.name);
  auto data_out = dir.create(filename);

  if (!data_out) {
    throw IoError{absl::StrCat("Failed to create file, path: ", filename)};
  }

  format_utils::WriteHeader(*data_out, kDataFormatName,
                            static_cast<int32_t>(_ver));

  Encryption::Stream::ptr data_cipher;
  bstring enc_header;
  auto* enc = dir.attributes().encryption();
  [[maybe_unused]] const auto encrypt =
    irs::Encrypt(filename, *data_out, enc, enc_header, data_cipher);
  SDB_ASSERT(!encrypt || (data_cipher && data_cipher->block_size()));

  // noexcept block
  _dir = &dir;
  _data_filename = std::move(filename);
  _data_out = std::move(data_out);
  _data_cipher = std::move(data_cipher);
}

ColumnstoreWriter::ColumnT Writer::push_column(const ColumnInfo& info,
                                               ColumnFinalizer finalizer) {
  Encryption::Stream* cipher = info.encryption ? _data_cipher.get() : nullptr;

  const auto id = _columns.size();

  if (id >= std::numeric_limits<uint32_t>::max()) {
    throw IllegalState{"Too many columns."};
  }

  // in case of consolidation we write columns one-by-one to
  // ensure that blocks from different columns don't interleave
  if (_consolidation && id) {
    _columns.back().flush();
  }

  auto& column = _columns.emplace_back(
    Column::Context{.data_out = _data_out.get(),
                    .cipher = cipher,
                    .hnsw_info = info.hnsw_info,
                    .u8buf = _buf,
                    .consolidation = _consolidation,
                    .version = ToSparseBitmapVersion(info)},
    static_cast<field_id>(id), info.value_type, std::move(finalizer),
    _columns.get_allocator().Manager());

  return {id, column};
}

bool Writer::commit(const FlushState& /*state*/) {
  SDB_ASSERT(_dir);

  // remove all empty columns from tail
  while (!_columns.empty() && _columns.back().empty()) {
    _columns.pop_back();
  }

  // remove file if there is no data to write
  if (_columns.empty()) {
    _data_out.reset();

    if (!_dir->remove(_data_filename)) {  // ignore error
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to remove file, path: ", _data_filename));
    }

    return false;  // nothing to flush
  }

  SDB_ASSERT(_sorted_columns.empty());
  _sorted_columns.reserve(_columns.size());

  for (auto& column : _columns) {
    column.FinalizeName();
    _sorted_columns.emplace_back(&column);
  }

  absl::c_sort(_sorted_columns, [](const auto* lhs, const auto* rhs) {
    return Less(lhs->name(), rhs->name());
  });

  // Ensured by `push_column(...)`
  SDB_ASSERT(_columns.size() < field_limits::invalid());
  SDB_ASSERT(_columns.size() == _sorted_columns.size());
  const field_id count = static_cast<field_id>(_columns.size());

  const std::string_view segment_name{
    _data_filename.data(), _data_filename.size() - kDataFormatExt.size() - 1};
  auto index_filename = IndexFileName(segment_name);
  auto index_out = _dir->create(index_filename);

  if (!index_out) {
    throw IoError{
      absl::StrCat("Failed to create file, path: ", index_filename)};
  }

  format_utils::WriteHeader(*index_out, kIndexFormatName,
                            static_cast<int32_t>(_ver));

  index_out->WriteV32(static_cast<uint32_t>(count));
  for (auto* column : _sorted_columns) {
    column->finish(*index_out);
  }

  format_utils::WriteFooter(*index_out);
  format_utils::WriteFooter(*_data_out);

  rollback();

  return true;
}

void Writer::rollback() noexcept {
  _data_filename.clear();
  _dir = nullptr;
  _data_out.reset();  // close output
  _columns.clear();
  _sorted_columns.clear();
}

const ColumnHeader* Reader::header(field_id field) const {
  auto* column = field >= _columns.size()
                   ? nullptr  // can't find column with the specified identifier
                   : _columns[field];

  if (column) {
    return &sdb::basics::downCast<ColumnBase>(*column).Header();
  }

  return nullptr;
}

void Reader::prepare_data(const Directory& dir, std::string_view filename) {
  auto data_in = dir.open(filename, IOAdvice::RANDOM);

  if (!data_in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  [[maybe_unused]] const auto version = format_utils::CheckHeader(
    *data_in, Writer::kDataFormatName, static_cast<int32_t>(Version::Min),
    static_cast<int32_t>(Version::Max));

  Encryption::Stream::ptr cipher;
  auto* enc = dir.attributes().encryption();
  if (irs::Decrypt(filename, *data_in, enc, cipher)) {
    SDB_ASSERT(cipher && cipher->block_size());
  }

  // since columns data are too large
  // it is too costly to verify checksum of
  // the entire file. here we perform cheap
  // error detection which could recognize
  // some forms of corruption
  [[maybe_unused]] const auto checksum = format_utils::ReadChecksum(*data_in);

  // noexcept
  _data_cipher = std::move(cipher);
  _data_in = std::move(data_in);
}

// FIXME return result???
void Reader::prepare_index(const Directory& dir, const SegmentMeta& meta,
                           std::string_view filename,
                           std::string_view data_filename,
                           const Options& opts) {
  auto index_in = dir.open(filename, IOAdvice::READONCE | IOAdvice::SEQUENTIAL);

  if (!index_in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", filename)};
  }

  const auto checksum = format_utils::Checksum(*index_in);

  [[maybe_unused]] const auto version = format_utils::CheckHeader(
    *index_in, Writer::kIndexFormatName, static_cast<int32_t>(Version::Min),
    static_cast<int32_t>(Version::Max));

  const field_id count = index_in->ReadV32();

  decltype(_sorted_columns) sorted_columns;
  sorted_columns.reserve(count);
  decltype(_columns) columns;
  columns.resize(count);

  for (field_id i = 0; i < count; ++i) {
    ColumnHeader hdr = ReadHeader(*index_in);

    const bool encrypted = IsEncrypted(hdr);

    if (encrypted && !_data_cipher) {
      throw IndexError{absl::StrCat("Failed to load encrypted column id=", i,
                                    " without a cipher")};
    }

    if (ColumnType::Mask != hdr.type && 0 == hdr.docs_count) {
      throw IndexError{absl::StrCat("Failed to load column id=", i,
                                    ", only mask column may be empty")};
    }

    if (hdr.id >= std::numeric_limits<uint32_t>::max() || hdr.id >= count) {
      throw IndexError{absl::StrCat("Failed to load column id=", i,
                                    ", invalid ordinal position")};
    }

    bstring payload;
    faiss::HNSW hnsw;

    if (hdr.hnsw_info) {
      ReadHNSW(*index_in, hnsw);
    } else {
      uint32_t payload_size = irs::read<uint32_t>(*index_in);
      payload.resize(payload_size);
      index_in->ReadBytes(payload.data(), payload_size);
    }

    std::optional<std::string> name;
    if (ColumnProperty::NoName != (hdr.props & ColumnProperty::NoName)) {
      [[maybe_unused]] const auto offset = index_in->Position();

      name = ReadString<std::string>(*index_in);

      if (encrypted) {
        SDB_ASSERT(_data_cipher);
        _data_cipher->Decrypt(
          offset, reinterpret_cast<byte_type*>(name->data()), name->size());
      }
    }

    auto index = hdr.docs_index ? ReadBitmapIndex(*index_in) : ColumnIndex{};

    if (const auto idx = static_cast<size_t>(hdr.type);
        idx < std::size(kFactories)) [[likely]] {
      const auto hdr_id = hdr.id;
      bool has_hnsw = hdr.hnsw_info.has_value();
      auto column =
        kFactories[idx](std::move(name), *dir.ResourceManager().readers,
                        *dir.ResourceManager().cached_columns,
                        std::move(payload), std::move(hdr), std::move(index),
                        *index_in, *_data_in, {}, _data_cipher.get());
      SDB_ASSERT(column);

      if (!sorted_columns.empty() &&
          Less(column->name(), sorted_columns.back()->name())) {
        throw IndexError{
          absl::StrCat("Invalid column order in segment '", meta.name, "'")};
      }

      SDB_ASSERT(hdr_id < columns.size());
      if (has_hnsw) {
        column->WithHNSW(std::move(hnsw));
      }
      columns[hdr_id] = column.get();
      sorted_columns.emplace_back(std::move(column));
    } else {
      throw IndexError{
        absl::StrCat("Failed to load column id=", i,
                     ", got invalid type=", static_cast<uint32_t>(hdr.type))};
    }
  }
  if (opts.warmup_column) {
    IndexInput::ptr direct_data_input;
    for (size_t i = 0; i < sorted_columns.size(); ++i) {
      auto cb = static_cast<ColumnBase*>(sorted_columns[i].get());
      if (opts.warmup_column(*cb)) {
        if (!direct_data_input) {
          direct_data_input = dir.open(data_filename, IOAdvice::DirectRead);
          if (!direct_data_input) {
            SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                     "Failed to open direct access file, path: ", data_filename,
                     ". Columns buffering stopped.");
            break;
          }
        }
        SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH,
                  "Making buffered: ", cb->Header().id);
        cb->MakeBuffered(*direct_data_input,
                         std::span(sorted_columns.data() + i + 1,
                                   sorted_columns.size() - i - 1));
        SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH,
                  "Finished buffered: ", cb->Header().id);
      }
    }
  }

  format_utils::CheckFooter(*index_in, checksum);

  // noexcept block
  _columns = std::move(columns);
  _sorted_columns = std::move(sorted_columns);

  SDB_ASSERT(_columns.size() == _sorted_columns.size());
}

bool Reader::prepare(const Directory& dir, const SegmentMeta& meta,
                     const Options& opts) {
  bool exists;
  const auto data_filename = DataFileName(meta.name);

  if (!dir.exists(exists, data_filename)) {
    throw IoError{
      absl::StrCat("Failed to check existence of file, path: ", data_filename)};
  }

  if (!exists) {
    // possible that the file does not exist
    // since columnstore is optional
    return false;
  }

  prepare_data(dir, data_filename);
  SDB_ASSERT(_data_in);

  const auto index_filename = IndexFileName(meta.name);

  if (!dir.exists(exists, index_filename)) {
    throw IoError{absl::StrCat("Failed to check existence of file, path: ",
                               index_filename)};
  }

  if (!exists) {
    // more likely index is currupted
    throw IndexError{
      absl::StrCat("Columnstore index file '", index_filename, "' is missing")};
  }

  prepare_index(dir, meta, index_filename, data_filename, opts);

  return true;
}

bool Reader::visit(const column_visitor_f& visitor) const {
  for (const auto& column : _sorted_columns) {
    SDB_ASSERT(column);
    if (!visitor(*column)) {
      return false;
    }
  }
  return true;
}

ColumnstoreWriter::ptr MakeWriter(Version version, bool consolidation,
                                  IResourceManager& resource_manager) {
  return std::make_unique<Writer>(version, consolidation, resource_manager);
}

ColumnstoreReader::ptr MakeReader() { return std::make_unique<Reader>(); }

}  // namespace irs::columnstore2

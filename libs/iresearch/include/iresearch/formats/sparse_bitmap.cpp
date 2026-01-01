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

#include "sparse_bitmap.hpp"

#include "iresearch/search/bitset_doc_iterator.hpp"

namespace irs {
namespace {

// We use dense container for blocks having
// more than this number of documents.
constexpr uint32_t kBitSetThreshold = (1 << 12) - 1;

// We don't use index for blocks located at a distance that is
// closer than this value.
constexpr uint32_t kBlockScanThreshold = 2;

enum BlockType : uint32_t {
  // Dense block is represented as a bitset container
  kBtDense = 0,

  // Sparse block is represented as an array of values
  kBtSparse,

  // Range block is represented as a [Min,Max] pair
  kBtRange
};

enum AccessType : uint32_t {
  // Access data via stream API
  kAtStream,

  // Direct memory access
  kAtDirect,

  // Aligned direct memory access
  kAtDirectAligned
};

constexpr size_t kDenseBlockIndexBlockSize = 512;
constexpr size_t kDenseBlockIndexNumBlocks =
  SparseBitmapWriter::kBlockSize / kDenseBlockIndexBlockSize;
constexpr size_t kDenseIndexBlockSizeInBytes =
  kDenseBlockIndexNumBlocks * sizeof(uint16_t);
constexpr uint32_t kDenseBlockIndexWordsPerBlock =
  kDenseBlockIndexBlockSize / BitsRequired<size_t>();

template<size_t N>
void WriteBlockIndex(IndexOutput& out, size_t (&bits)[N]) {
  // TODO(mbkkt) rewrite this function with new interface
  uint16_t popcnt = 0;
  uint16_t data[kDenseBlockIndexNumBlocks];

  auto* const index = reinterpret_cast<byte_type*>(data);
  auto* block = index;

  for (auto begin = std::begin(bits), end = std::end(bits); begin != end;
       begin += kDenseBlockIndexWordsPerBlock) {
    // TODO(mbkkt) should use little endian!
    WriteBE<uint16_t>(popcnt, block);

    for (uint32_t i = 0; i < kDenseBlockIndexWordsPerBlock; ++i) {
      popcnt += std::popcount(begin[i]);
    }
  }

  out.WriteBytes(index, kDenseIndexBlockSizeInBytes);
}

}  // namespace

void SparseBitmapWriter::finish() {
  flush(_block);

  if (_block_index.size() < kBlockSize) {
    add_block(_block ? _block + 1 : 0);
  }

  // create a sentinel block to issue doc_limits::eof() automatically
  _block = doc_limits::eof() / kBlockSize;
  set(doc_limits::eof() % kBlockSize);
  do_flush(1);
}

void SparseBitmapWriter::do_flush(uint32_t popcnt) {
  SDB_ASSERT(popcnt);
  SDB_ASSERT(_block < kBlockSize);
  SDB_ASSERT(popcnt <= kBlockSize);

  _out->WriteU16(static_cast<uint16_t>(_block));
  _out->WriteU16(static_cast<uint16_t>(popcnt - 1));  // -1 to fit uint16_t

  if (_opts.track_prev_doc) {
    // write last value in the previous block
    _out->WriteU32(_last_in_flushed_block);
    _last_in_flushed_block = _prev_value;
  }

  if (popcnt > kBitSetThreshold) {
    if (popcnt != kBlockSize) {
      WriteBlockIndex(*_out, _bits);

      if constexpr (std::endian::native != std::endian::big) {
        for (auto& v : _bits) {
          v = absl::big_endian::FromHost(v);
        }
      }

      _out->WriteBytes(reinterpret_cast<const byte_type*>(_bits), sizeof _bits);
    }
  } else {
    BitsetDocIterator it(std::begin(_bits), std::end(_bits));

    while (it.next()) {
      _out->WriteU16(static_cast<uint16_t>(it.value()));
    }
  }
}

template<uint32_t Type, bool TrackPrev>
struct container_iterator;

template<bool TrackPrev>
struct container_iterator<kBtRange, TrackPrev> {
  static bool Seek(SparseBitmapIterator* self, doc_id_t target) noexcept {
    std::get<DocAttr>(self->_attrs).value = target;

    auto& index = std::get<ValueIndex>(self->_attrs).value;
    index = target - self->_ctx.all.missing;

    if constexpr (TrackPrev) {
      if (index != self->_index) {
        self->_prev = target - 1;
      }
    }

    return true;
  }
};

template<bool TrackPrev>
struct container_iterator<kBtSparse, TrackPrev> {
  template<AccessType Access>
  static bool Seek(SparseBitmapIterator* self, doc_id_t target) {
    target &= 0x0000FFFF;

    auto& ctx = self->_ctx.sparse;
    const doc_id_t index_max = self->_index_max;

    [[maybe_unused]] doc_id_t prev = std::get<DocAttr>(self->_attrs).value;

    for (; ctx.index < index_max; ++ctx.index) {
      doc_id_t doc;
      if constexpr (kAtStream == Access) {
        doc = self->_in->ReadI16();
      } else {
        if constexpr (kAtDirectAligned == Access) {
          doc = *ctx.u16data;
        } else {
          static_assert(Access == kAtDirect);
          std::memcpy(&doc, self->_ctx.u8data, sizeof(uint16_t));
        }
        ++ctx.u16data;
        doc = absl::big_endian::ToHost16(doc);
      }

      if (doc >= target) {
        std::get<DocAttr>(self->_attrs).value = self->_block | doc;
        std::get<ValueIndex>(self->_attrs).value = ctx.index;

        if constexpr (TrackPrev) {
          if (ctx.index != self->_index) {
            self->_prev = self->_block | prev;
          }
        }

        ++ctx.index;

        return true;
      }

      if constexpr (TrackPrev) {
        prev = doc;
      }
    }

    return false;
  }
};

template<>
struct container_iterator<kBtDense, false> {
  template<AccessType Access>
  static bool Seek(SparseBitmapIterator* self, doc_id_t target) {
    auto& ctx = self->_ctx.dense;

    const doc_id_t target_block{target & 0x0000FFFF};
    const int32_t target_word_idx = target_block / BitsRequired<uint64_t>();
    SDB_ASSERT(target_word_idx >= ctx.word_idx);

    if (ctx.index.u16data && uint32_t(target_word_idx - ctx.word_idx) >=
                               kDenseBlockIndexWordsPerBlock) {
      const size_t index_block = target_block / kDenseBlockIndexBlockSize;

      const auto popcnt = absl::big_endian::Load16(
        ctx.index.u8data + index_block * sizeof(uint16_t));

      const auto word_idx = index_block * kDenseBlockIndexWordsPerBlock;
      const auto delta = word_idx - ctx.word_idx;
      SDB_ASSERT(delta > 0);

      if constexpr (kAtStream == Access) {
        self->_in->Seek(self->_in->Position() + (delta - 1) * sizeof(uint64_t));
        ctx.word = self->_in->ReadI64();
      } else {
        ctx.u64data += delta;
        if constexpr (kAtDirectAligned == Access) {
          ctx.word = ctx.u64data[-1];
        } else {
          static_assert(kAtDirect == Access);
          std::memcpy(&ctx.word, self->_ctx.u8data - sizeof(uint64_t),
                      sizeof(uint64_t));
        }
        ctx.word = absl::big_endian::ToHost64(ctx.word);
      }
      ctx.popcnt = self->_index + popcnt + std::popcount(ctx.word);
      ctx.word_idx = static_cast<int32_t>(word_idx);
    }

    uint32_t word_delta = target_word_idx - ctx.word_idx;

    if constexpr (kAtStream == Access) {
      for (; word_delta; --word_delta) {
        ctx.word = self->_in->ReadI64();
        ctx.popcnt += std::popcount(ctx.word);
      }
      ctx.word_idx = target_word_idx;
    } else {
      if (word_delta) {
        // FIMXE consider using SSE/avx256/avx512 extensions for large skips

        const uint64_t* end = ctx.u64data + word_delta;
        for (; ctx.u64data < end; ++ctx.u64data) {
          if constexpr (kAtDirectAligned == Access) {
            ctx.word = *ctx.u64data;
          } else {
            static_assert(kAtDirect == Access);
            std::memcpy(&ctx.word, self->_ctx.u8data, sizeof(uint64_t));
          }
          ctx.popcnt += std::popcount(ctx.word);
        }
        ctx.word = absl::big_endian::ToHost64(ctx.word);
        ctx.word_idx = target_word_idx;
      }
    }

    const uint64_t left = ctx.word >> (target % BitsRequired<uint64_t>());

    if (left) {
      const doc_id_t offset = std::countr_zero(left);
      std::get<DocAttr>(self->_attrs).value = target + offset;

      auto& index = std::get<ValueIndex>(self->_attrs).value;
      index = ctx.popcnt - std::popcount(left);

      return true;
    }

    constexpr int32_t kNumBlocks = SparseBitmapWriter::kNumBlocks;

    ++ctx.word_idx;
    for (; ctx.word_idx < kNumBlocks; ++ctx.word_idx) {
      if constexpr (kAtStream == Access) {
        ctx.word = self->_in->ReadI64();
      } else {
        if constexpr (kAtDirectAligned == Access) {
          ctx.word = *ctx.u64data;
        } else {
          static_assert(kAtDirect == Access);
          std::memcpy(&ctx.word, self->_ctx.u8data, sizeof(uint64_t));
        }
        ++ctx.u64data;
      }

      if (ctx.word) {
        if constexpr (kAtStream != Access) {
          ctx.word = absl::big_endian::ToHost64(ctx.word);
        }

        const doc_id_t offset = std::countr_zero(ctx.word);

        std::get<DocAttr>(self->_attrs).value =
          self->_block + ctx.word_idx * BitsRequired<uint64_t>() + offset;
        auto& index = std::get<ValueIndex>(self->_attrs).value;
        index = ctx.popcnt;
        ctx.popcnt += std::popcount(ctx.word);

        return true;
      }
    }

    return false;
  }
};

template<>
struct container_iterator<kBtDense, true> {
  template<AccessType Access>
  static bool Seek(SparseBitmapIterator* self, doc_id_t target) {
    const auto res =
      container_iterator<kBtDense, false>::Seek<Access>(self, target);

    if (std::get<ValueIndex>(self->_attrs).value != self->_index) {
      self->_seek_func = &container_iterator<kBtDense, false>::Seek<Access>;
      std::get<irs::PrevDocAttr>(self->_attrs).reset(&SeekPrev<Access>, self);
    }

    return res;
  }

 private:
  template<AccessType Access>
  static doc_id_t SeekPrev(const void* arg) noexcept(Access != kAtStream) {
    const auto* self = static_cast<const SparseBitmapIterator*>(arg);
    const auto& ctx = self->_ctx.dense;

    const uint64_t offs = self->value() % BitsRequired<uint64_t>();
    const uint64_t mask = (uint64_t{1} << offs) - 1;
    uint64_t word = ctx.word & mask;
    uint64_t word_idx = ctx.word_idx;

    if (!word) {
      // FIXME(gnusi): we can use block
      // index to perform huge skips

      if constexpr (kAtStream == Access) {
        auto& in = *self->_in;
        // original position
        const auto pos = in.Position();

        auto prev = pos - 2 * sizeof(uint64_t);
        do {
          in.Seek(prev);
          word = in.ReadI64();
          SDB_ASSERT(word_idx);
          --word_idx;
          prev -= sizeof(uint64_t);
        } while (!word);
        self->_in->Seek(pos);
      } else {
        for (auto* prev_word = ctx.u64data - 1; !word; --word_idx) {
          --prev_word;
          if constexpr (kAtDirectAligned == Access) {
            word = *prev_word;
          } else {
            static_assert(kAtDirect == Access);
            std::memcpy(&word, reinterpret_cast<const byte_type*>(prev_word),
                        sizeof(uint64_t));
          }
          SDB_ASSERT(word_idx);
        }
        word = absl::big_endian::ToHost64(word);
      }
    }

    return self->_block +
           static_cast<doc_id_t>((word_idx + 1) * BitsRequired<uint64_t>() -
                                 std::countl_zero(word) - 1);
  }
};

template<BlockType Type>
constexpr auto GetSeekFunc(bool direct, bool track_prev) noexcept {
  auto impl = [&]<AccessType A>(bool track_prev) noexcept {
    if (track_prev) {
      return &container_iterator<Type, true>::template Seek<A>;
    } else {
      return &container_iterator<Type, false>::template Seek<A>;
    }
  };

  // FIXME(gnusi): check alignment
  if (direct) {
    return impl.template operator()<kAtDirect>(track_prev);
  } else {
    return impl.template operator()<kAtStream>(track_prev);
  }
}

bool SparseBitmapIterator::initial_seek(SparseBitmapIterator* self,
                                        doc_id_t target) {
  SDB_ASSERT(!doc_limits::valid(self->value()));
  SDB_ASSERT(0 == (target & 0xFFFF0000));

  // we can get there iff the very
  // first block is not yet read
  self->read_block_header();
  self->seek(target);
  return true;
}

SparseBitmapIterator::SparseBitmapIterator(Ptr&& in, const Options& opts)
  : _in{std::move(in)},
    _seek_func{&SparseBitmapIterator::initial_seek},
    _block_index{opts.blocks},
    _cont_begin{_in->Position()},
    _origin{_cont_begin},
    _use_block_index{opts.use_block_index},
    _prev_doc_written{opts.version >= SparseBitmapVersion::PrevDoc},
    _track_prev_doc{_prev_doc_written && opts.track_prev_doc} {
  SDB_ASSERT(_in);

  if (_track_prev_doc) {
    std::get<PrevDocAttr>(_attrs).reset(
      [](const void* ctx) noexcept {
        return *static_cast<const doc_id_t*>(ctx);
      },
      &_prev);
  }
}

void SparseBitmapIterator::reset() {
  std::get<DocAttr>(_attrs).value = irs::doc_limits::invalid();
  _seek_func = &SparseBitmapIterator::initial_seek;
  _cont_begin = _origin;
  _index_max = 0;
  _in->Seek(_cont_begin);
}

void SparseBitmapIterator::read_block_header() {
  _block = doc_id_t{uint16_t(_in->ReadI16())} << 16;
  const uint32_t popcnt = 1 + static_cast<uint16_t>(_in->ReadI16());
  _index = _index_max;
  _index_max += popcnt;
  if (_prev_doc_written) {
    _prev = _in->ReadI32();  // last doc in a previously filled block
  }

  if (_track_prev_doc) {
    std::get<PrevDocAttr>(_attrs).reset(
      [](const void* ctx) noexcept {
        return *static_cast<const doc_id_t*>(ctx);
      },
      &_prev);
  }

  if (popcnt == SparseBitmapWriter::kBlockSize) {
    _ctx.all.missing = _block - _index;
    _cont_begin = _in->Position();

    _seek_func = _track_prev_doc ? &container_iterator<kBtRange, true>::Seek
                                 : &container_iterator<kBtRange, false>::Seek;
  } else if (popcnt <= kBitSetThreshold) {
    const size_t block_size = 2 * popcnt;
    _cont_begin = _in->Position() + block_size;
    _ctx.u8data = _in->ReadBuffer(block_size, BufferHint::NORMAL);
    _ctx.sparse.index = _index;

    _seek_func = GetSeekFunc<kBtSparse>(_ctx.u8data, _track_prev_doc);
  } else {
    constexpr size_t kBlockSize =
      SparseBitmapWriter::kBlockSize / BitsRequired<byte_type>();

    _ctx.dense.word_idx = -1;
    _ctx.dense.popcnt = _index;
    if (_use_block_index) {
      _ctx.dense.index.u8data =
        _in->ReadBuffer(kDenseIndexBlockSizeInBytes, BufferHint::PERSISTENT);

      if (!_ctx.dense.index.u8data) {
        if (!_block_index_data) {
          _block_index_data =
            std::make_unique<byte_type[]>(kDenseIndexBlockSizeInBytes);
        }

        _ctx.dense.index.u8data = _block_index_data.get();

        _in->ReadBytes(_block_index_data.get(), kDenseIndexBlockSizeInBytes);
      }
    } else {
      _ctx.dense.index.u8data = nullptr;
      _in->Seek(_in->Position() + kDenseIndexBlockSizeInBytes);
    }

    _cont_begin = _in->Position() + kBlockSize;
    _ctx.u8data = _in->ReadBuffer(kBlockSize, BufferHint::NORMAL);

    _seek_func = GetSeekFunc<kBtDense>(_ctx.u8data, _track_prev_doc);
  }
}

void SparseBitmapIterator::seek_to_block(doc_id_t target) {
  SDB_ASSERT(target / SparseBitmapWriter::kBlockSize);

  if (!_block_index.empty()) {
    const doc_id_t target_block = target / SparseBitmapWriter::kBlockSize;
    if (target_block >=
        (_block / SparseBitmapWriter::kBlockSize + kBlockScanThreshold)) {
      const auto offset =
        std::min(size_t{target_block}, _block_index.size() - 1);
      const auto& block = _block_index[offset];

      _index_max = block.index;
      _in->Seek(_origin + block.offset);
      read_block_header();
      return;
    }
  }

  do {
    _in->Seek(_cont_begin);
    read_block_header();
  } while (_block < target);
}

doc_id_t SparseBitmapIterator::seek(doc_id_t target) {
  // FIXME
  if (target <= value()) {
    return value();
  }

  const doc_id_t target_block = target & 0xFFFF0000;
  if (_block < target_block) {
    seek_to_block(target_block);
  }

  if (_block == target_block) {
    SDB_ASSERT(_seek_func);
    if (_seek_func(this, target)) {
      return value();
    }
    read_block_header();
  }

  SDB_ASSERT(_seek_func);
  _seek_func(this, _block);

  return value();
}

}  // namespace irs

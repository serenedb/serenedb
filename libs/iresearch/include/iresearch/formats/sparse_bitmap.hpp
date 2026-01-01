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

#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "basics/math_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/prev_doc.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class SparseBitmapVersion {
  Min = 0,

  // Version supporting access to previous document
  PrevDoc = 1,

  // Max supported version
  Max = PrevDoc
};

struct SparseBitmapWriterOptions {
  explicit SparseBitmapWriterOptions(SparseBitmapVersion version) noexcept
    : track_prev_doc{version >= SparseBitmapVersion::PrevDoc} {}

  // Track previous document
  bool track_prev_doc;
};

class SparseBitmapWriter {
 public:
  using value_type = doc_id_t;  // for compatibility with back_inserter

  static constexpr uint32_t kBlockSize = 1 << 16;
  static constexpr uint32_t kNumBlocks = kBlockSize / BitsRequired<size_t>();

  static_assert(math::IsPower2(kBlockSize));

  struct Block {
    doc_id_t index;
    uint32_t offset;
  };

  explicit SparseBitmapWriter(IndexOutput& out,
                              SparseBitmapVersion version) noexcept
    : _out{&out}, _origin{out.Position()}, _opts{version} {}

  void push_back(doc_id_t value) {
    static_assert(math::IsPower2(kBlockSize));
    SDB_ASSERT(doc_limits::valid(value));
    SDB_ASSERT(!doc_limits::eof(value));

    const uint32_t block = value / kBlockSize;

    if (block != _block) {
      flush(_block);
      _block = block;
    }

    set(value % kBlockSize);
    _prev_value = value;
  }

  bool erase(doc_id_t value) noexcept {
    if ((value / kBlockSize) < _block) {
      // value is already flushed
      return false;
    }

    reset(value % kBlockSize);
    return true;
  }

  void finish();

  std::span<const Block> index() const noexcept { return _block_index; }

  SparseBitmapVersion version() const noexcept {
    static_assert(SparseBitmapVersion::Min == SparseBitmapVersion{false});
    static_assert(SparseBitmapVersion::PrevDoc == SparseBitmapVersion{true});
    return SparseBitmapVersion{_opts.track_prev_doc};
  }

 private:
  void flush(uint32_t next_block) {
    const uint32_t popcnt =
      static_cast<uint32_t>(math::Popcount(std::begin(_bits), std::end(_bits)));
    if (popcnt) {
      add_block(next_block);
      do_flush(popcnt);
      _popcnt += popcnt;
      std::memset(_bits, 0, sizeof _bits);
    }
  }

  IRS_FORCE_INLINE void set(doc_id_t value) noexcept {
    SDB_ASSERT(value < kBlockSize);

    irs::SetBit(_bits[value / BitsRequired<size_t>()],
                value % BitsRequired<size_t>());
  }

  IRS_FORCE_INLINE void reset(doc_id_t value) noexcept {
    SDB_ASSERT(value < kBlockSize);

    irs::UnsetBit(_bits[value / BitsRequired<size_t>()],
                  value % BitsRequired<size_t>());
  }

  IRS_FORCE_INLINE void add_block(uint32_t block_id) {
    const uint64_t offset = _out->Position() - _origin;
    SDB_ASSERT(offset <= std::numeric_limits<uint32_t>::max());

    uint32_t count = 1 + block_id - static_cast<uint32_t>(_block_index.size());

    while (count) {
      _block_index.emplace_back(Block{_popcnt, static_cast<uint32_t>(offset)});
      --count;
    }
  }

  void do_flush(uint32_t popcnt);

  IndexOutput* _out;
  uint64_t _origin;
  size_t _bits[kNumBlocks]{};
  std::vector<Block> _block_index;
  uint32_t _popcnt{};
  uint32_t _block{};  // last flushed block
  doc_id_t _prev_value{};
  doc_id_t _last_in_flushed_block{};
  SparseBitmapWriterOptions _opts;
};

// Denotes a position of a value associated with a document.
struct ValueIndex : DocAttr {
  static constexpr std::string_view type_name() noexcept {
    return "value_index";
  }
};

class SparseBitmapIterator : public ResettableDocIterator {
  static void Noop(IndexInput* /*in*/) noexcept {}
  static void Delete(IndexInput* in) noexcept { delete in; }
  // TODO(mbkkt) unique_ptr isn't optimal here
  using Ptr = std::unique_ptr<IndexInput, void (*)(IndexInput*)>;

 public:
  using block_index_t = std::span<const SparseBitmapWriter::Block>;

  struct Options {
    // Format version
    SparseBitmapVersion version{SparseBitmapVersion::Max};

    // Use previous doc tracking
    bool track_prev_doc{true};

    // Use per block index (for dense blocks)
    bool use_block_index{true};

    // Blocks index
    block_index_t blocks;
  };

  SparseBitmapIterator(IndexInput::ptr&& in, const Options& opts)
    : SparseBitmapIterator{Ptr{in.release(), &Delete}, opts} {}
  SparseBitmapIterator(IndexInput* in, const Options& opts)
    : SparseBitmapIterator{Ptr{in, &Noop}, opts} {}

  template<typename Cost>
  SparseBitmapIterator(IndexInput::ptr&& in, const Options& opts, Cost&& est)
    : SparseBitmapIterator{std::move(in), opts} {
    std::get<CostAttr>(_attrs).reset(std::forward<Cost>(est));
  }

  template<typename Cost>
  SparseBitmapIterator(IndexInput* in, const Options& opts, Cost&& est)
    : SparseBitmapIterator{in, opts} {
    std::get<CostAttr>(_attrs).reset(std::forward<Cost>(est));
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t value() const noexcept final {
    return std::get<DocAttr>(_attrs).value;
  }

  bool next() final { return !doc_limits::eof(seek(value() + 1)); }

  doc_id_t seek(doc_id_t target) final;

  void reset() final;

  // The value is undefined for
  // doc_limits::invalid() and doc_limits::eof()
  doc_id_t index() const noexcept { return std::get<ValueIndex>(_attrs).value; }

 private:
  using block_seek_f = bool (*)(SparseBitmapIterator*, doc_id_t);

  template<uint32_t, bool>
  friend struct container_iterator;

  static bool initial_seek(SparseBitmapIterator* self, doc_id_t target);

  struct ContainerIteratorContext {
    union {
      // We can also have an inverse sparse container (where we
      // most of values are 0). In this case only zeros can be
      // stored as an array.
      struct {
        const uint16_t* u16data;
        doc_id_t index;
      } sparse;
      struct {
        const uint64_t* u64data;
        doc_id_t popcnt;
        int32_t word_idx;
        uint64_t word;
        union {
          const uint16_t* u16data;
          const byte_type* u8data;
        } index;
      } dense;
      struct {
        const void* ignore;
        doc_id_t missing;
      } all;
      const byte_type* u8data;
    };
  };

  using Attributes =
    std::tuple<DocAttr, ValueIndex, PrevDocAttr, CostAttr, ScoreAttr>;

  explicit SparseBitmapIterator(Ptr&& in, const Options& opts);

  void seek_to_block(doc_id_t block);
  void read_block_header();

  ContainerIteratorContext _ctx;
  Attributes _attrs;
  Ptr _in;
  std::unique_ptr<byte_type[]> _block_index_data;
  block_seek_f _seek_func;
  block_index_t _block_index;
  uint64_t _cont_begin;
  uint64_t _origin;
  doc_id_t _index{};  // beginning of the block
  doc_id_t _index_max{};
  doc_id_t _block{};
  doc_id_t _prev{};  // previous doc
  const bool _use_block_index;
  const bool _prev_doc_written;
  const bool _track_prev_doc;
};

}  // namespace irs

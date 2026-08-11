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
////////////////////////////////////////////////////////////////////////////////

#include "burst_trie.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/internal/resize_uninitialized.h>
#include <absl/strings/str_cat.h>

#include <iresearch/index/index_reader_options.hpp>
#include <variant>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/containers/monotonic_buffer.hpp"
#include "basics/containers/small_vector.h"
#include "basics/log.h"
#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "basics/string_utils.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/fstext/fst_builder.hpp"
#include "iresearch/utils/fstext/fst_decl.hpp"
#include "iresearch/utils/fstext/fst_matcher.hpp"
#include "iresearch/utils/fstext/fst_string_ref_weight.hpp"
#include "iresearch/utils/fstext/fst_string_weight.hpp"
#include "iresearch/utils/fstext/fst_utils.hpp"
#include "iresearch/utils/fstext/immutable_fst.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "pg/sql_exception_macro.h"

namespace {

using namespace irs;

template<typename Char>
class VolatileRef : util::Noncopyable {
 public:
  using RefT = basic_string_view<Char>;
  using StrT = basic_string<Char>;

  VolatileRef() = default;

  VolatileRef(VolatileRef&& rhs) noexcept
    : _str(std::move(rhs._str)), _ref(_str.empty() ? rhs._ref : RefT(_str)) {
    rhs._ref = _ref;
  }

  VolatileRef& operator=(VolatileRef&& rhs) noexcept {
    if (this != &rhs) {
      _str = std::move(rhs._str);
      _ref = (_str.empty() ? rhs._ref : RefT(_str));
      rhs._ref = _ref;
    }
    return *this;
  }

  void Clear() {
    _str.clear();
    _ref = {};
  }

  template<bool Volatile>
  void Assign(RefT str) {
    if constexpr (Volatile) {
      _str.assign(str.data(), str.size());
      _ref = _str;
    } else {
      _ref = str;
      _str.clear();
    }
  }

  IRS_FORCE_INLINE void Assign(const RefT& str, bool Volatile) {
    (Volatile ? VolatileRef<Char>::Assign<true>(str)
              : VolatileRef<Char>::Assign<false>(str));
  }

  void Assign(RefT str, Char label) {
    _str.resize(str.size() + 1);
    std::memcpy(_str.data(), str.data(), str.size() * sizeof(Char));
    _str[str.size()] = label;
    _ref = _str;
  }

  RefT View() const noexcept { return _ref; }

  operator RefT() const noexcept { return _ref; }

 private:
  StrT _str;
  RefT _ref{};
};

using VolatileByteRef = VolatileRef<byte_type>;

template<typename T>
struct Node {
  T* next = nullptr;
};

template<typename T>
struct IntrusiveList {
 public:
  IntrusiveList& operator=(const IntrusiveList&) = delete;
  IntrusiveList(const IntrusiveList&) = delete;

  IntrusiveList() noexcept = default;

  IntrusiveList(IntrusiveList&& other) noexcept
    : tail{std::exchange(other.tail, nullptr)} {}

  IntrusiveList& operator=(IntrusiveList&& other) noexcept {
    std::swap(tail, other.tail);
    return *this;
  }

  void Append(IntrusiveList&& rhs) noexcept {
    SDB_ASSERT(this != &rhs);
    if (rhs.tail == nullptr) {
      return;
    }
    if (tail == nullptr) {
      tail = rhs.tail;
      rhs.tail = nullptr;
      return;
    }
    // h1->t1->h1 h2->t2->h2
    auto* head = tail->next;
    // h1->t1->h2->t2->h2
    tail->next = rhs.tail->next;
    // h1->t1->h2->t2->h1
    rhs.tail->next = head;
    // h1->**->h2->t2/t1->h1
    tail = rhs.tail;
    // h1->**->h2->t1->h1
    rhs.tail = nullptr;
  }

  void PushFront(T& front) noexcept {
    SDB_ASSERT(front.next == &front);
    if (tail == nullptr) [[likely]] {
      tail = &front;
      return;
    }
    front.next = tail->next;
    tail->next = &front;
  }

  template<typename Func>
  IRS_FORCE_INLINE void Visit(Func&& func) const {
    if (tail == nullptr) [[likely]] {
      return;
    }
    auto* head = tail->next;
    auto* it = head;
    do {
      func(*std::exchange(it, it->next));
    } while (it != head);
  }

  T* tail = nullptr;
};

// Block of terms
struct Block : private util::Noncopyable {
  struct PrefixedOutput final : DataOutput,
                                Node<PrefixedOutput>,
                                private util::Noncopyable {
    explicit PrefixedOutput(VolatileByteRef&& prefix) noexcept
      : Node<PrefixedOutput>{this}, prefix{std::move(prefix)} {}

    void WriteByte(byte_type b) final { weight.PushBack(b); }

    void WriteData(const byte_type* b, uint64_t len) final {
      weight.PushBack(b, b + len);
    }

    VolatileByteRef prefix;
    byte_weight weight;
  };

  static constexpr uint16_t kInvalidLabel{std::numeric_limits<uint16_t>::max()};

  using BlockIndex = IntrusiveList<PrefixedOutput>;

  Block(BlockIndex&& other, uint64_t block_start, uint8_t meta,
        uint16_t label) noexcept
    : index{std::move(other)}, start{block_start}, label{label}, meta{meta} {}

  Block(Block&& rhs) noexcept
    : index(std::move(rhs.index)),
      start(rhs.start),
      label(rhs.label),
      meta(rhs.meta) {}

  ~Block() {
    index.Visit([](PrefixedOutput& output) {  //
      output.~PrefixedOutput();
    });
  }

  BlockIndex index;  // fst index data
  uint64_t start;    // file pointer
  uint16_t label;    // block lead label
  uint8_t meta;      // block metadata
};

using OutputBuffer = MonotonicBuffer<Block::PrefixedOutput, 1, 0>;

enum class EntryType : uint8_t {
  Term = 0,
  Block,
  Invalid,
};

// Block or term
class Entry : private util::Noncopyable {
 public:
  Entry(bytes_view term, PostingMeta&& attrs, bool volatile_term);

  Entry(bytes_view prefix, Block::BlockIndex&& index, uint64_t block_start,
        uint8_t meta, uint16_t label, bool volatile_term);
  Entry(Entry&& rhs) noexcept;
  Entry& operator=(Entry&& rhs) noexcept;
  ~Entry() { Destroy(); }

  PostingMeta& Term() noexcept { return _term; }

  auto& Block(this auto& self) noexcept { return self._block; }

  const VolatileByteRef& Data() const noexcept { return _data; }
  VolatileByteRef& Data() noexcept { return _data; }

  EntryType Type() const noexcept { return _type; }

 private:
  void Destroy() noexcept;
  void MoveUnion(Entry&& rhs) noexcept;

  VolatileByteRef _data;
  union {
    char _empty{};
    PostingMeta _term;
    ::Block _block;
  };
  EntryType _type;
};

Entry::Entry(bytes_view term, PostingMeta&& attrs, bool volatile_term)
  : _type{EntryType::Term} {
  _data.Assign(term, volatile_term);
  new (&_term) PostingMeta{std::move(attrs)};
}

Entry::Entry(bytes_view prefix, Block::BlockIndex&& index, uint64_t block_start,
             uint8_t meta, uint16_t label, bool volatile_term)
  : _type{EntryType::Block} {
  if (Block::kInvalidLabel != label) {
    _data.Assign(prefix, static_cast<byte_type>(label & 0xFF));
  } else {
    _data.Assign(prefix, volatile_term);
  }
  new (&_block)::Block{std::move(index), block_start, meta, label};
}

Entry::Entry(Entry&& rhs) noexcept : _data{std::move(rhs._data)} {
  MoveUnion(std::move(rhs));
}

Entry& Entry::operator=(Entry&& rhs) noexcept {
  if (this != &rhs) {
    _data = std::move(rhs._data);
    Destroy();
    MoveUnion(std::move(rhs));
  }

  return *this;
}

void Entry::MoveUnion(Entry&& rhs) noexcept {
  _type = rhs._type;
  switch (_type) {
    case EntryType::Term:
      new (&_term) PostingMeta{std::move(rhs._term)};
      rhs._term.~PostingMeta();
      break;
    case EntryType::Block:
      new (&_block)::Block{std::move(rhs._block)};
      rhs._block.~Block();
      break;
    default:
      break;
  }
  rhs._type = EntryType::Invalid;
}

void Entry::Destroy() noexcept {
  switch (_type) {
    case EntryType::Term:
      _term.~PostingMeta();
      break;
    case EntryType::Block:
      _block.~Block();
      break;
    default:
      break;
  }
  _type = EntryType::Invalid;
}

// Provides set of helper functions to work with block metadata
struct BlockMeta {
  // mask bit layout:
  // 0 - has terms
  // 1 - has sub blocks
  // 2 - is floor block

  // block has terms
  static bool Terms(uint8_t mask) noexcept {
    return CheckBit(mask, std::to_underlying(EntryType::Term));
  }

  // block has sub-blocks
  static bool Blocks(uint8_t mask) noexcept {
    return CheckBit(mask, std::to_underlying(EntryType::Block));
  }

  static void Type(uint8_t& mask, EntryType type) noexcept {
    SetBit(mask, std::to_underlying(type));
  }

  // block is floor block
  static bool Floor(uint8_t mask) noexcept {
    return CheckBit(mask, std::to_underlying(EntryType::Invalid));
  }
  static void Floor(uint8_t& mask, bool b) noexcept {
    SetBit(mask, std::to_underlying(EntryType::Invalid), b);
  }

  // resets block meta
  static void Reset(uint8_t mask) noexcept {
    UnsetBit(mask, std::to_underlying(EntryType::Term));
    UnsetBit(mask, std::to_underlying(EntryType::Block));
  }
};

// mininum size of string weight we store in FST
[[maybe_unused]] constexpr const size_t kMinWeightSize = 2;

using Blocks = ManagedVector<Entry>;

void MergeBlocks(Blocks& blocks, OutputBuffer& buffer) {
  SDB_ASSERT(!blocks.empty());

  auto it = blocks.begin();

  auto& root = *it;
  auto& root_block = root.Block();
  auto& root_index = root_block.index;

  auto& out = *buffer.Construct(std::move(root.Data()));
  root_index.PushFront(out);

  // First byte in block header must not be equal to fst::kStringInfinity
  // Consider the following:
  //   StringWeight0 -> { fst::kStringInfinity 0x11 ... }
  //   StringWeight1 -> { fst::kStringInfinity 0x22 ... }
  //   CommonPrefix = fst::Plus(StringWeight0, StringWeight1) -> {
  //   fst::kStringInfinity } Suffix = fst::Divide(StringWeight1, CommonPrefix)
  //   -> { fst::kStringBad }
  // But actually Suffix should be equal to { 0x22 ... }
  SDB_ASSERT(static_cast<int8_t>(root_block.meta) != fst::kStringInfinity);

  // will store just several bytes here
  out.WriteByte(static_cast<byte_type>(root_block.meta));  // block metadata
  out.WriteV64(root_block.start);  // start pointer of the block

  if (BlockMeta::Floor(root_block.meta)) {
    SDB_ASSERT(blocks.size() - 1 < std::numeric_limits<uint32_t>::max());
    out.WriteV32(static_cast<uint32_t>(blocks.size() - 1));
    for (++it; it != blocks.end(); ++it) {
      const auto* block = &it->Block();
      SDB_ASSERT(block->label != Block::kInvalidLabel);
      SDB_ASSERT(block->start > root_block.start);

      const uint64_t start_delta = it->Block().start - root_block.start;
      out.WriteByte(static_cast<byte_type>(block->label & 0xFF));
      out.WriteV64(start_delta);
      out.WriteByte(static_cast<byte_type>(block->meta));

      root_index.Append(std::move(it->Block().index));
    }
  } else {
    for (++it; it != blocks.end(); ++it) {
      root_index.Append(std::move(it->Block().index));
    }
  }

  // ensure weight we've written doesn't interfere
  // with semiring members and other constants
  SDB_ASSERT(out.weight != byte_weight::One() &&
             out.weight != byte_weight::Zero() &&
             out.weight != byte_weight::NoWeight() &&
             out.weight.Size() >= kMinWeightSize &&
             byte_weight::One().Size() < kMinWeightSize &&
             byte_weight::Zero().Size() < kMinWeightSize &&
             byte_weight::NoWeight().Size() < kMinWeightSize);
}

// Resetable FST buffer
class FstBuffer : public vector_byte_fst {
 public:
  // Fst builder stats
  struct FstStatsImpl : FstStats {
    size_t total_weight_size{};

    void operator()(const byte_weight& w) noexcept {
      total_weight_size += w.Size();
    }

    [[maybe_unused]] bool operator==(const FstStatsImpl& rhs) const noexcept {
      return num_states == rhs.num_states && num_arcs == rhs.num_arcs &&
             total_weight_size == rhs.total_weight_size;
    }
  };

  FstBuffer(IResourceManager& rm)
    : vector_byte_fst{ManagedTypedAllocator<byte_arc>{rm}} {}

  using FstByteBuilder = FstBuilder<byte_type, vector_byte_fst, FstStatsImpl>;

  FstStatsImpl Reset(const Block::BlockIndex& index) {
    _builder.reset();

    index.Visit([&](Block::PrefixedOutput& output) {
      _builder.add(output.prefix, output.weight);
      // TODO(mbkkt) Call dtor here?
    });

    return _builder.finish();
  }

 private:
  FstByteBuilder _builder{*this};
};

}  // namespace
namespace irs::burst_trie {

class FieldWriter::Impl {
 public:
  static constexpr uint32_t kDefaultMinBlockSize = 25;
  static constexpr uint32_t kDefaultMaxBlockSize = 48;

  Impl(PostingsWriter::ptr&& pw, bool compaction, IResourceManager& rm,
       uint32_t min_block_size = kDefaultMinBlockSize,
       uint32_t max_block_size = kDefaultMaxBlockSize);

  ~Impl();

  void prepare(const FlushState& state);

  void end();

  void write(const BasicTermReader& reader);

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }

 private:
  static constexpr size_t kDefaultSize = 8;

  void BeginField(const FieldProperties& meta);

  void EndField(field_id id, FieldProperties props, bytes_view min_term,
                bytes_view max_term, uint64_t total_doc_freq,
                uint64_t total_term_freq, uint64_t doc_count);

  // prefix - prefix length (in last_term)
  // begin - index of the first entry in the block
  // end - index of the last entry in the block
  // meta - block metadata
  // label - block lead label (if present)
  void WriteBlock(size_t prefix, size_t begin, size_t end, uint8_t meta,
                  uint16_t label);

  // prefix - prefix length ( in last_term
  // count - number of entries to write into block
  void WriteBlocks(size_t prefix, size_t count);

  void Push(bytes_view term);

  OutputBuffer _output_buffer;
  Blocks _blocks;
  MemoryOutput _suffix;        // term suffix column (per-field scratch)
  MemoryOutput _stats;         // term stats column (per-field scratch)
  IdxWriter* _idx{};           // destination .idx (set via SetIdxWriter)
  IndexOutput* _blocks_out{};  // borrowed from _idx->BlocksOut() at prepare()
  PostingsWriter::ptr _pw;     // postings writer
  ManagedVector<Entry> _stack;
  FstBuffer* _fst_buf;         // pimpl buffer used for building FST for fields
  VolatileByteRef _last_term;  // last pushed term
  std::vector<size_t> _prefixes;
  const uint32_t _min_block_size;
  const uint32_t _max_block_size;
  const bool _compaction;
};

void FieldWriter::Impl::WriteBlock(size_t prefix, size_t begin, size_t end,
                                   uint8_t meta, uint16_t label) {
  SDB_ASSERT(end > begin);

  // begin of the block
  const uint64_t block_start = _blocks_out->Position();

  // write block header
  _blocks_out->WriteV32(
    ShiftPack32(static_cast<uint32_t>(end - begin), end == _stack.size()));

  // write block entries
  const bool leaf = !BlockMeta::Blocks(meta);

  Block::BlockIndex index;

  _pw->BeginBlock();

  for (; begin < end; ++begin) {
    auto& e = _stack[begin];
    const bytes_view data = e.Data();
    const EntryType type = e.Type();
    SDB_ASSERT(data.starts_with({_last_term.View().data(), prefix}));

    // only terms under 32k are allowed
    SDB_ASSERT(data.size() - prefix <= UINT32_C(0x7FFFFFFF));
    const uint32_t suf_size = static_cast<uint32_t>(data.size() - prefix);

    _suffix.stream.WriteV32(
      leaf ? suf_size : ((suf_size << 1) | static_cast<uint32_t>(type)));
    _suffix.stream.WriteData(data.data() + prefix, suf_size);

    if (EntryType::Term == type) {
      _pw->Encode(_stats.stream, e.Term());
    } else {
      SDB_ASSERT(EntryType::Block == type);

      // current block start pointer should be greater
      SDB_ASSERT(block_start > e.Block().start);
      _suffix.stream.WriteV64(block_start - e.Block().start);
      index.Append(std::move(e.Block().index));
    }
  }

  const auto block_size = _suffix.stream.Position();

  _suffix.stream.Flush();
  _stats.stream.Flush();

  _blocks_out->WriteV64(ShiftPack64(block_size, leaf));

  auto copy = [this](const byte_type* b, size_t len) {
    _blocks_out->WriteData(b, len);
    return true;
  };

  _suffix.file.Visit(copy);

  _blocks_out->WriteV64(static_cast<uint64_t>(_stats.stream.Position()));
  _stats.file.Visit(copy);

  _suffix.stream.Reset();
  _stats.stream.Reset();

  // add new block to the list of created blocks
  _blocks.emplace_back(bytes_view{_last_term.View().data(), prefix},
                       std::move(index), block_start, meta, label, _compaction);
}

void FieldWriter::Impl::WriteBlocks(size_t prefix, size_t count) {
  // only root node able to write whole stack
  SDB_ASSERT(prefix || count == _stack.size());
  SDB_ASSERT(_blocks.empty());

  // block metadata
  uint8_t meta{};

  const size_t end = _stack.size();
  const size_t begin = end - count;
  size_t block_start = begin;  // begin of current block to write

  size_t min_suffix = std::numeric_limits<size_t>::max();
  size_t max_suffix = 0;

  uint16_t last_label{Block::kInvalidLabel};  // last lead suffix label
  uint16_t next_label{
    Block::kInvalidLabel};  // next lead suffix label in current block
  for (size_t i = begin; i < end; ++i) {
    const Entry& e = _stack[i];
    const bytes_view data = e.Data();

    const size_t suffix = data.size() - prefix;
    min_suffix = std::min(suffix, min_suffix);
    max_suffix = std::max(suffix, max_suffix);

    const uint16_t label =
      data.size() == prefix ? Block::kInvalidLabel : data[prefix];

    if (last_label != label) {
      const size_t block_size = i - block_start;

      if (block_size >= _min_block_size &&
          end - block_start > _max_block_size) {
        BlockMeta::Floor(meta, block_size < count);
        WriteBlock(prefix, block_start, i, meta, next_label);
        next_label = label;
        BlockMeta::Reset(meta);
        block_start = i;
        min_suffix = std::numeric_limits<size_t>::max();
        max_suffix = 0;
      }

      last_label = label;
    }

    BlockMeta::Type(meta, e.Type());
  }

  // write remaining block
  if (block_start < end) {
    BlockMeta::Floor(meta, end - block_start < count);
    WriteBlock(prefix, block_start, end, meta, next_label);
  }

  // merge blocks into 1st block
  ::MergeBlocks(_blocks, _output_buffer);

  // remove processed entries from the
  // top of the stack
  _stack.erase(_stack.begin() + begin, _stack.end());

  // move root block from temporary storage
  // to the top of the stack
  if (!_blocks.empty()) {
    _stack.emplace_back(std::move(_blocks.front()));
    _blocks.clear();
  }
}

void FieldWriter::Impl::Push(bytes_view term) {
  const bytes_view last = _last_term;
  const size_t limit = std::min(last.size(), term.size());

  // find common prefix
  size_t pos = 0;
  while (pos < limit && term[pos] == last[pos]) {
    ++pos;
  }

  for (size_t i = last.empty() ? 0 : last.size() - 1; i > pos;) {
    --i;  // should use it here as we use size_t
    const size_t top = _stack.size() - _prefixes[i];
    if (top > _min_block_size) {
      WriteBlocks(i + 1, top);
      _prefixes[i] -= (top - 1);
    }
  }

  _prefixes.resize(term.size());
  std::fill(_prefixes.begin() + pos, _prefixes.end(), _stack.size());
  _last_term.Assign(term, _compaction);
}

FieldWriter::Impl::Impl(PostingsWriter::ptr&& pw, bool compaction,
                        IResourceManager& rm, uint32_t min_block_size,
                        uint32_t max_block_size)
  : _output_buffer{rm, 32},
    _blocks{ManagedTypedAllocator<Entry>{rm}},
    _suffix{rm},
    _stats{rm},
    _pw{std::move(pw)},
    _stack{ManagedTypedAllocator<Entry>{rm}},
    _fst_buf{new FstBuffer{rm}},
    _prefixes{kDefaultSize, 0},
    _min_block_size{min_block_size},
    _max_block_size{max_block_size},
    _compaction{compaction} {
  SDB_ASSERT(this->_pw);
  SDB_ASSERT(min_block_size > 1);
  SDB_ASSERT(min_block_size <= max_block_size);
  SDB_ASSERT(2 * (min_block_size - 1) <= max_block_size);
}

FieldWriter::Impl::~Impl() { delete _fst_buf; }

void FieldWriter::Impl::prepare(const FlushState& state) {
  SDB_ASSERT(_idx, "FieldWriter::Impl::prepare requires SetIdxWriter first");

  // reset writer state
  _last_term.Clear();
  _prefixes.assign(kDefaultSize, 0);
  _stack.clear();
  _stats.Reset();
  _suffix.Reset();

  _blocks_out = &_idx->BlocksOut();

  _pw->Prepare(*_blocks_out, state);

  _suffix.Reset();
  _stats.Reset();
}

void FieldWriter::Impl::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  const auto index_features = props.index_features;
  BeginField(props);
  _pw->SetTermPayloadWriter(reader.PayloadWriter());

  uint64_t term_count = 0;
  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;

  const bool freq_exists =
    IndexFeatures::None != (index_features & IndexFeatures::Freq);

  auto terms = reader.iterator();
  SDB_ASSERT(terms != nullptr);
  while (terms->next()) {
    auto postings = terms->postings(index_features);
    PostingMeta meta;
    _pw->Write(*postings, meta);

    if (freq_exists) {
      sum_tfreq += meta.freq;
    }

    if (meta.docs_count != 0) {
      sum_dfreq += meta.docs_count;

      const bytes_view term = terms->value();
      Push(term);

      // push term to the top of the stack
      _stack.emplace_back(term, std::move(meta), _compaction);

      ++term_count;
    }
  }

  EndField(reader.id(), props, reader.min(), reader.max(), sum_dfreq, sum_tfreq,
           term_count);
}

void FieldWriter::Impl::BeginField(const FieldProperties& meta) {
  SDB_ASSERT(_blocks_out);

  // At the beginning of the field there should be no pending entries at all
  SDB_ASSERT(_stack.empty());

  _pw->BeginField(meta);
}

void FieldWriter::Impl::EndField(field_id id, FieldProperties props,
                                 bytes_view min_term, bytes_view max_term,
                                 uint64_t total_doc_freq,
                                 uint64_t total_term_freq,
                                 uint64_t term_count) {
  SDB_ASSERT(_blocks_out);

  if (!term_count) {
    // nothing to write
    return;
  }

  const auto [has_score_bounds, doc_count] = _pw->EndField();

  // cause creation of all final blocks
  Push(kEmptyStringView<byte_type>);

  // write root block with empty prefix
  WriteBlocks(0, _stack.size());
  SDB_ASSERT(1 == _stack.size());

  const Entry& root = *_stack.begin();
  SDB_ASSERT(_fst_buf);
  [[maybe_unused]] const auto fst_stats = _fst_buf->Reset(root.Block().index);
  _stack.clear();
  _output_buffer.Clear();

  const vector_byte_fst& fst = *_fst_buf;

#ifdef SDB_DEV
  // ensure evaluated stats are correct
  struct FstBuffer::FstStatsImpl stats{};
  for (fst::StateIterator<vector_byte_fst> states(fst); !states.Done();
       states.Next()) {
    const auto stateid = states.Value();
    ++stats.num_states;
    stats.num_arcs += fst.NumArcs(stateid);
    stats(fst.Final(stateid));
    for (fst::ArcIterator<vector_byte_fst> arcs(fst, stateid); !arcs.Done();
         arcs.Next()) {
      stats(arcs.Value().weight);
    }
  }
  SDB_ASSERT(stats == fst_stats);
#endif

  const uint64_t body_offset = _blocks_out->Position();
  WriteStr(*_blocks_out, min_term);
  WriteStr(*_blocks_out, max_term);
  const bool ok = immutable_byte_fst::Write(fst, *_blocks_out, fst_stats);
  if (!ok) [[unlikely]] {
    throw IndexError{
      absl::StrCat("Failed to write term index for field id ", id)};
  }

  TermDictMeta meta;
  meta.features = props.index_features;
  meta.term_count = term_count;
  meta.doc_count = doc_count;
  meta.total_doc_freq = total_doc_freq;
  meta.total_term_freq = total_term_freq;
  meta.has_score_bounds = has_score_bounds != 0;
  meta.body_offset = body_offset;
  meta.norm = props.norm;
  _idx->AddTermDictEntry(id, std::move(meta));
}

void FieldWriter::Impl::end() {
  _output_buffer.Reset();
  _pw->End();
  _idx = nullptr;
  _blocks_out = nullptr;
}

}  // namespace irs::burst_trie
namespace {

using namespace irs;

class TermReaderBase : public TermReader, private util::Noncopyable {
 public:
  TermReaderBase() = default;
  TermReaderBase(TermReaderBase&& rhs) = default;
  TermReaderBase& operator=(TermReaderBase&&) = delete;

  const FieldMeta& meta() const noexcept final { return _field; }
  size_t size() const noexcept final { return _terms_count; }
  uint64_t docs_count() const noexcept final { return _doc_count; }
  bytes_view min() const noexcept final { return _min_term; }
  bytes_view max() const noexcept final { return _max_term; }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;
  bool HasScoreBounds() const noexcept final { return _has_score_bounds; }

  void LoadFromMeta(field_id id, const TermDictMeta& meta, DataInput& in);

 private:
  FieldMeta _field;
  bstring _min_term;
  bstring _max_term;
  uint64_t _terms_count{};
  uint64_t _doc_count{};
  bool _has_score_bounds{};
  FreqAttr _freq;  // total term freq
};

void TermReaderBase::LoadFromMeta(field_id id, const TermDictMeta& meta,
                                  DataInput& in) {
  _field.id = id;
  _field.norm = meta.norm;
  _field.index_features = meta.features;
  _terms_count = meta.term_count;
  _doc_count = meta.doc_count;
  _min_term = ReadString<bstring>(in);
  _max_term = ReadString<bstring>(in);
  if (IndexFeatures::None != (meta.features & IndexFeatures::Freq)) {
    // TODO(mbkkt) for what reason we store uint64_t if we read to uint32_t
    SDB_ENSURE(meta.total_term_freq <= std::numeric_limits<uint32_t>::max(),
               "TermReaderBase: total_term_freq ", meta.total_term_freq,
               " exceeds uint32_t::max");
    _freq.value = static_cast<uint32_t>(meta.total_term_freq);
  }
  _has_score_bounds = meta.has_score_bounds;
}

Attribute* TermReaderBase::GetMutable(TypeInfo::type_id type) noexcept {
  if (IndexFeatures::None != (_field.index_features & IndexFeatures::Freq) &&
      irs::Type<FreqAttr>::id() == type) {
    return &_freq;
  }

  return nullptr;
}

class BlockIterator : util::Noncopyable {
 public:
  static constexpr uint32_t kUndefinedCount{
    std::numeric_limits<uint32_t>::max()};
  static constexpr uint64_t kUndefinedAddress{
    std::numeric_limits<uint64_t>::max()};

  BlockIterator(byte_weight&& header, size_t prefix) noexcept;

  BlockIterator(bytes_view header, size_t prefix)
    : BlockIterator{byte_weight{header}, prefix} {}

  BlockIterator(uint64_t start, size_t prefix) noexcept
    : _start{start},
      _cur_start{start},
      _cur_end{start},
      _prefix{static_cast<uint32_t>(prefix)},
      _sub_count{kUndefinedCount} {
    SDB_ASSERT(prefix <= std::numeric_limits<uint32_t>::max());
  }

  void Load(IndexInput& in);

  template<bool ReadHeader>
  bool NextSubBlock() noexcept {
    if (!_sub_count) {
      return false;
    }

    _cur_start = _cur_end;
    if (_sub_count != kUndefinedCount) {
      --_sub_count;
      if constexpr (ReadHeader) {
        vskip<uint64_t>(_header.begin);
        _cur_meta = *_header.begin++;
        if (_sub_count) {
          _next_label = *_header.begin++;
        }
      }
    }
    _dirty = true;
    _header.AssertBlockBoundaries();
    return true;
  }

  template<typename Reader>
  void Next(Reader&& reader) {
    SDB_ASSERT(!_dirty && _cur_ent < _ent_count);
    if (_leaf) {
      ReadEntryLeaf(std::forward<Reader>(reader));
    } else {
      ReadEntryNonleaf(std::forward<Reader>(reader));
    }
    ++_cur_ent;
  }

  void Reset();

  const PostingMeta& State() const noexcept { return _state; }
  bool Dirty() const noexcept { return _dirty; }
  uint8_t Meta() const noexcept { return _cur_meta; }
  size_t Prefix() const noexcept { return _prefix; }
  EntryType Type() const noexcept { return _cur_type; }
  uint64_t BlockStart() const noexcept { return _cur_block_start; }
  uint16_t NextLabel() const noexcept { return _next_label; }
  uint32_t SubCount() const noexcept { return _sub_count; }
  uint64_t Start() const noexcept { return _start; }
  bool Done() const noexcept { return _cur_ent == _ent_count; }
  bool NoTerms() const noexcept {
    // FIXME(gnusi): add term mark to block entry?
    //
    // Block was loaded using address and doesn't have metadata,
    // assume such blocks have terms
    return _sub_count != kUndefinedCount && !BlockMeta::Terms(Meta());
  }

  template<typename Reader>
  SeekResult ScanToTerm(bytes_view term, Reader&& reader) {
    SDB_ASSERT(term.size() >= _prefix);
    SDB_ASSERT(!_dirty);

    return _leaf ? ScanToTermLeaf(term, std::forward<Reader>(reader))
                 : ScanToTermNonleaf(term, std::forward<Reader>(reader));
  }

  template<typename Reader>
  SeekResult Scan(Reader&& reader) {
    SDB_ASSERT(!_dirty);

    return _leaf ? ScanLeaf(std::forward<Reader>(reader))
                 : ScanNonleaf(std::forward<Reader>(reader));
  }

  // scan to floor block
  void ScanToSubBlock(byte_type label);

  // scan to entry with the following start address
  void ScanToBlock(uint64_t ptr);

  // read attributes
  void LoadData(const FieldMeta& meta, PostingMeta& state, PostingsReader& pr);

 private:
  struct DataBlock : util::Noncopyable {
    using Blockype = bstring;

    DataBlock() = default;
    DataBlock(Blockype&& block) noexcept
      : block{std::move(block)}, begin{this->block.c_str()} {
#ifdef SDB_DEV
      end = begin + this->block.size();
      AssertBlockBoundaries();
#endif
    }
    DataBlock(DataBlock&& rhs) noexcept { *this = std::move(rhs); }
    DataBlock& operator=(DataBlock&& rhs) noexcept {
      if (this != &rhs) {
        if (rhs.block.empty()) {
          begin = rhs.begin;
#ifdef SDB_DEV
          end = rhs.end;
#endif
        } else {
          const size_t offset = std::distance(rhs.block.c_str(), rhs.begin);
          block = std::move(rhs.block);
          begin = block.c_str() + offset;
#ifdef SDB_DEV
          end = block.c_str() + block.size();
#endif
        }
      }
      AssertBlockBoundaries();
      return *this;
    }

    [[maybe_unused]] void AssertBlockBoundaries() {
#ifdef SDB_DEV
      SDB_ASSERT(begin <= end);
      if (!block.empty()) {
        SDB_ASSERT(end <= (block.c_str() + block.size()));
        SDB_ASSERT(block.c_str() <= begin);
      }
#endif
    }

    Blockype block;
    const byte_type* begin{block.c_str()};
#ifdef SDB_DEV
    const byte_type* end{begin};
#endif
  };

  template<typename Reader>
  void ReadEntryNonleaf(Reader&& reader);

  template<typename Reader>
  void ReadEntryLeaf(Reader&& reader) {
    SDB_ASSERT(_leaf && _cur_ent < _ent_count);
    _cur_type = EntryType::Term;  // always term
    ++_term_count;
    _suffix_length = vread<uint32_t>(_suffix.begin);
    reader(_suffix.begin, _suffix_length);
    _suffix_begin = _suffix.begin;
    _suffix.begin += _suffix_length;
    _suffix.AssertBlockBoundaries();
  }

  template<typename Reader>
  SeekResult ScanToTermNonleaf(bytes_view term, Reader&& reader);
  template<typename Reader>
  SeekResult ScanToTermLeaf(bytes_view term, Reader&& reader);

  template<typename Reader>
  SeekResult ScanNonleaf(Reader&& reader);
  template<typename Reader>
  SeekResult ScanLeaf(Reader&& reader);

  DataBlock _header;  // suffix block header
  DataBlock _suffix;  // suffix data block
  DataBlock _stats;   // stats data block
  PostingMeta _state;
  size_t _suffix_length{};  // last matched suffix length
  const byte_type* _suffix_begin{};
  uint64_t _start;      // initial block start pointer
  uint64_t _cur_start;  // current block start pointer
  uint64_t _cur_end;    // block end pointer
  // start pointer of the current sub-block entry
  uint64_t _cur_block_start{kUndefinedAddress};
  uint32_t _prefix;           // block prefix length, 32k at most
  uint32_t _cur_ent{};        // current entry in a block
  uint32_t _ent_count{};      // number of entries in a current block
  uint32_t _term_count{};     // number terms in a block we have seen
  uint32_t _cur_stats_ent{};  // current position of loaded stats
  uint32_t _sub_count;        // number of sub-blocks
  // next label (of the next sub-block)
  uint16_t _next_label{Block::kInvalidLabel};
  EntryType _cur_type{EntryType::Invalid};  // term or block
  byte_type _meta{};                        // initial block metadata
  byte_type _cur_meta{};                    // current block metadata
  bool _dirty{true};                        // current block is dirty
  bool _leaf{false};                        // current block is leaf block
};

BlockIterator::BlockIterator(byte_weight&& header, size_t prefix) noexcept
  : _header{std::move(header)},
    _prefix{static_cast<uint32_t>(prefix)},
    _sub_count{0} {
  SDB_ASSERT(prefix <= std::numeric_limits<uint32_t>::max());

  _cur_meta = _meta = *_header.begin++;
  _cur_end = _cur_start = _start = vread<uint64_t>(_header.begin);
  if (BlockMeta::Floor(_meta)) {
    _sub_count = vread<uint32_t>(_header.begin);
    _next_label = *_header.begin++;
  }
  _header.AssertBlockBoundaries();
}

void BlockIterator::Load(IndexInput& in) {
  if (!_dirty) {
    return;
  }

  in.Seek(_cur_start);
  if (ShiftUnpack32(in.ReadV32(), _ent_count)) {
    _sub_count = 0;  // no sub-blocks
  }

  // read suffix block
  uint64_t block_size;
  _leaf = ShiftUnpack64(in.ReadV64(), block_size);

  // try direct buffer access first
  _suffix.begin = in.ReadStable(block_size);
  _suffix.block.clear();

  if (!_suffix.begin) {
    _suffix.block.resize(block_size);
    in.ReadData(_suffix.block.data(), block_size);
    _suffix.begin = _suffix.block.c_str();
  }
#ifdef SDB_DEV
  _suffix.end = _suffix.begin + block_size;
#endif
  _suffix.AssertBlockBoundaries();

  // read stats block
  block_size = in.ReadV64();

  // try direct buffer access first
  _stats.begin = in.ReadStable(block_size);
  _stats.block.clear();

  if (!_stats.begin) {
    _stats.block.resize(block_size);
    in.ReadData(_stats.block.data(), block_size);
    _stats.begin = _stats.block.c_str();
  }
#ifdef SDB_DEV
  _stats.end = _stats.begin + block_size;
#endif
  _stats.AssertBlockBoundaries();

  _cur_end = in.Position();
  _cur_ent = 0;
  _cur_block_start = kUndefinedAddress;
  _term_count = 0;
  _cur_stats_ent = 0;
  _dirty = false;
}

template<typename Reader>
void BlockIterator::ReadEntryNonleaf(Reader&& reader) {
  SDB_ASSERT(!_leaf && _cur_ent < _ent_count);

  _cur_type = ShiftUnpack32<EntryType, size_t>(vread<uint32_t>(_suffix.begin),
                                               _suffix_length);
  _suffix_begin = _suffix.begin;
  _suffix.begin += _suffix_length;
  _suffix.AssertBlockBoundaries();

  if (EntryType::Term == _cur_type) {
    ++_term_count;
  } else {
    SDB_ASSERT(EntryType::Block == _cur_type);
    _cur_block_start = _cur_start - vread<uint64_t>(_suffix.begin);
    _suffix.AssertBlockBoundaries();
  }

  // read after state is updated
  reader(_suffix_begin, _suffix_length);
}

template<typename Reader>
SeekResult BlockIterator::ScanLeaf(Reader&& reader) {
  SDB_ASSERT(_leaf);
  SDB_ASSERT(!_dirty);
  SDB_ASSERT(_ent_count >= _cur_ent);

  SeekResult res = SeekResult::End;
  _cur_type = EntryType::Term;  // leaf block contains terms only

  size_t suffix_length = _suffix_length;
  size_t count = 0;

  for (const size_t left = _ent_count - _cur_ent; count < left;) {
    ++count;
    suffix_length = vread<uint32_t>(_suffix.begin);
    res = reader(_suffix.begin, suffix_length);
    _suffix.begin += suffix_length;  // skip to the next term

    if (res != SeekResult::NotFound) {
      break;
    }
  }

  _cur_ent += count;
  _term_count = _cur_ent;

  _suffix_begin = _suffix.begin - suffix_length;
  _suffix_length = suffix_length;
  _suffix.AssertBlockBoundaries();

  return res;
}

template<typename Reader>
SeekResult BlockIterator::ScanNonleaf(Reader&& reader) {
  SDB_ASSERT(!_leaf);
  SDB_ASSERT(!_dirty);

  SeekResult res = SeekResult::End;

  while (_cur_ent < _ent_count) {
    ++_cur_ent;
    _cur_type = ShiftUnpack32<EntryType, size_t>(vread<uint32_t>(_suffix.begin),
                                                 _suffix_length);
    const bool is_block = _cur_type == EntryType::Block;
    _suffix.AssertBlockBoundaries();

    _suffix_begin = _suffix.begin;
    _suffix.begin += _suffix_length;  // skip to the next entry
    _suffix.AssertBlockBoundaries();

    if (EntryType::Term == _cur_type) {
      ++_term_count;
    } else {
      SDB_ASSERT(_cur_type == EntryType::Block);
      _cur_block_start = _cur_start - vread<uint64_t>(_suffix.begin);
      _suffix.AssertBlockBoundaries();
    }

    // FIXME
    // we're not allowed to access/modify any block_iterator's
    // member as current instance might be already moved due
    // to a reallocation
    res = reader(_suffix_begin, _suffix_length);

    if (res != SeekResult::NotFound || is_block) {
      break;
    }
  }

  return res;
}

template<typename Reader>
SeekResult BlockIterator::ScanToTermLeaf(bytes_view term, Reader&& reader) {
  SDB_ASSERT(_leaf);
  SDB_ASSERT(!_dirty);
  SDB_ASSERT(term.size() >= _prefix);

  const size_t term_suffix_length = term.size() - _prefix;
  const byte_type* term_suffix = term.data() + _prefix;
  size_t suffix_length = _suffix_length;
  _cur_type = EntryType::Term;  // leaf block contains terms only
  SeekResult res = SeekResult::End;

  uint32_t count = 0;
  for (uint32_t left = _ent_count - _cur_ent; count < left;) {
    ++count;
    suffix_length = vread<uint32_t>(_suffix.begin);
    _suffix.AssertBlockBoundaries();

    ptrdiff_t cmp = std::memcmp(_suffix.begin, term_suffix,
                                std::min(suffix_length, term_suffix_length));

    if (cmp == 0) {
      cmp = suffix_length - term_suffix_length;
    }

    _suffix.begin += suffix_length;  // skip to the next term
    _suffix.AssertBlockBoundaries();

    if (cmp >= 0) {
      res = (cmp == 0 ? SeekResult::Found       // match!
                      : SeekResult::NotFound);  // after the target, not found
      break;
    }
  }

  _cur_ent += count;
  _term_count = _cur_ent;
  _suffix_begin = _suffix.begin - suffix_length;
  _suffix_length = suffix_length;
  reader(_suffix_begin, suffix_length);

  _suffix.AssertBlockBoundaries();
  return res;
}

template<typename Reader>
SeekResult BlockIterator::ScanToTermNonleaf(bytes_view term, Reader&& reader) {
  SDB_ASSERT(!_leaf);
  SDB_ASSERT(!_dirty);
  SDB_ASSERT(term.size() >= _prefix);

  const size_t term_suffix_length = term.size() - _prefix;
  const byte_type* term_suffix = term.data() + _prefix;
  const byte_type* suffix_begin = _suffix_begin;
  size_t suffix_length = _suffix_length;
  SeekResult res = SeekResult::End;

  while (_cur_ent < _ent_count) {
    ++_cur_ent;
    _cur_type = ShiftUnpack32<EntryType, size_t>(vread<uint32_t>(_suffix.begin),
                                                 suffix_length);
    _suffix.AssertBlockBoundaries();
    suffix_begin = _suffix.begin;
    _suffix.begin += suffix_length;  // skip to the next entry
    _suffix.AssertBlockBoundaries();

    if (EntryType::Term == _cur_type) {
      ++_term_count;
    } else {
      SDB_ASSERT(EntryType::Block == _cur_type);
      _cur_block_start = _cur_start - vread<uint64_t>(_suffix.begin);
      _suffix.AssertBlockBoundaries();
    }

    auto cmp = bytes_view{suffix_begin, suffix_length} <=>
               bytes_view{term_suffix, term_suffix_length};

    if (cmp >= 0) {
      res =
        (cmp == 0 ? SeekResult::Found       // match!
                  : SeekResult::NotFound);  // we after the target, not found
      break;
    }
  }

  _suffix_begin = suffix_begin;
  _suffix_length = suffix_length;
  reader(suffix_begin, suffix_length);

  _suffix.AssertBlockBoundaries();
  return res;
}

void BlockIterator::ScanToSubBlock(byte_type label) {
  SDB_ASSERT(_sub_count != kUndefinedCount);

  if (!_sub_count || !BlockMeta::Floor(_meta)) {
    // no sub-blocks, nothing to do
    return;
  }

  const uint16_t target = label;  // avoid byte_type vs uint16_t comparison

  if (target < _next_label) {
    // we don't need search
    return;
  }

  // FIXME: binary search???
  uint64_t start_delta = 0;
  for (;;) {
    start_delta = vread<uint64_t>(_header.begin);
    _cur_meta = *_header.begin++;
    if (--_sub_count) {
      _next_label = *_header.begin++;

      if (target < _next_label) {
        break;
      }
    } else {
      _next_label = Block::kInvalidLabel;
      break;
    }
  }

  if (start_delta) {
    _cur_start = _start + start_delta;
    _cur_ent = 0;
    _dirty = true;
  }

  _header.AssertBlockBoundaries();
}

void BlockIterator::ScanToBlock(uint64_t start) {
  if (_leaf) {
    // must be a non leaf block
    return;
  }

  if (_cur_block_start == start) {
    // nothing to do
    return;
  }

  const uint64_t target = _cur_start - start;  // delta
  for (; _cur_ent < _ent_count;) {
    ++_cur_ent;
    const auto type = ShiftUnpack32<EntryType, size_t>(
      vread<uint32_t>(_suffix.begin), _suffix_length);
    _suffix.AssertBlockBoundaries();
    _suffix.begin += _suffix_length;
    _suffix.AssertBlockBoundaries();

    if (EntryType::Term == type) {
      ++_term_count;
    } else {
      SDB_ASSERT(EntryType::Block == type);
      if (vread<uint64_t>(_suffix.begin) == target) {
        _suffix.AssertBlockBoundaries();
        _cur_block_start = target;
        return;
      }
      _suffix.AssertBlockBoundaries();
    }
  }

  SDB_ASSERT(false);
}

void BlockIterator::LoadData(const FieldMeta& meta, PostingMeta& state,
                             PostingsReader& pr) {
  SDB_ASSERT(EntryType::Term == _cur_type);

  if (_cur_stats_ent >= _term_count) {
    return;
  }

  if (0 == _cur_stats_ent) {
    // clear state at the beginning
    state.clear();
  } else {
    state = _state;
  }

  for (; _cur_stats_ent < _term_count; ++_cur_stats_ent) {
    _stats.begin += pr.decode(_stats.begin, meta.index_features, state);
    _stats.AssertBlockBoundaries();
  }

  _state = state;
}

void BlockIterator::Reset() {
  if (_sub_count != kUndefinedCount) {
    _sub_count = 0;
  }
  _next_label = Block::kInvalidLabel;
  _cur_start = _start;
  _cur_meta = _meta;
  if (BlockMeta::Floor(_meta)) {
    SDB_ASSERT(_sub_count != kUndefinedCount);
    _header.begin = _header.block.c_str() + 1;  // +1 to skip meta
    vskip<uint64_t>(_header.begin);             // skip address
    _sub_count = vread<uint32_t>(_header.begin);
    _next_label = *_header.begin++;
  }
  _dirty = true;
  _header.AssertBlockBoundaries();
}

// use explicit matcher to avoid implicit loops
template<typename FST>
using ExplicitMatcher = fst::explicit_matcher<fst::SortedMatcher<FST>>;

template<typename FST>
class TermIteratorBase {
 public:
  using WeightT = typename FST::Weight;
  using StateidT = typename FST::StateId;

  TermIteratorBase(const TermReaderBase& field, PostingsReader& postings,
                   const IndexInput& terms_in, const FST& fst)
    : _field{&field},
      _postings{&postings},
      _terms_in_source{&terms_in},
      _fst{&fst},
      _matcher{&fst, fst::MATCH_INPUT} {  // pass pointer to avoid copying FST
  }

 protected:
  ~TermIteratorBase() = default;

  Attribute* GetAttribute(TypeInfo::type_id type) noexcept {
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view Value() const noexcept { return _term.value; }

  const PostingMeta& Cookie() const {
    SDB_ASSERT(_cur_block);
    _cur_block->LoadData(_field->meta(), _posting_meta, *_postings);
    return _posting_meta;
  }

  DocIterator::ptr Postings(IndexFeatures features) const {
    SDB_ASSERT(_cur_block);
    const auto& field_meta = _field->meta();
    _cur_block->LoadData(field_meta, _posting_meta, *_postings);
    return _postings->Iterator(
      field_meta.index_features, features, {.cookie = &_posting_meta},
      IteratorFieldOptions{.has_score_bounds = _field->HasScoreBounds()});
  }

  struct Arc {
    Arc() = default;
    Arc(StateidT state, bytes_view weight, size_t block) noexcept
      : state(state), weight(weight), block(block) {}

    StateidT state;
    bytes_view weight;
    size_t block;
  };

  static_assert(std::is_nothrow_move_constructible_v<Arc>);

  ptrdiff_t SeekCached(size_t& prefix, StateidT& state, size_t& block,
                       byte_weight& weight, bytes_view term);

  // Seek to the closest block which contain a specified term
  // prefix - size of the common term/block prefix
  // Returns true if we're already at a requested term
  bool SeekToBlock(bytes_view term, size_t& prefix);

  // Seeks to the specified term using FST
  // There may be several sutuations:
  //   1. There is no term in a block (SeekResult::NOT_FOUND)
  //   2. There is no term in a block and we have
  //      reached the end of the block (SeekResult::End)
  //   3. We have found term in a block (SeekResult::Found)
  //
  // Note, that search may end up on a BLOCK entry. In all cases
  // "owner_->term_" will be refreshed with the valid number of
  // common bytes
  SeekResult SeekEqual(bytes_view term, bool exact);

  BlockIterator* PopBlock() noexcept {
    _block_stack.pop_back();
    SDB_ASSERT(!_block_stack.empty());
    return &_block_stack.back();
  }

  BlockIterator* PushBlock(bytes_view out, size_t prefix) {
    // ensure final weight correctess
    SDB_ASSERT(out.size() >= kMinWeightSize);

    return &_block_stack.emplace_back(out, prefix);
  }

  BlockIterator* PushBlock(byte_weight&& out, size_t prefix) {
    // ensure final weight correctess
    SDB_ASSERT(out.Size() >= kMinWeightSize);

    return &_block_stack.emplace_back(std::move(out), prefix);
  }

  BlockIterator* PushBlock(uint64_t start, size_t prefix) {
    return &_block_stack.emplace_back(start, prefix);
  }

  IndexInput& TermsInput() const {
    if (!_terms_in) {
      _terms_in = _terms_in_source->Reopen();  // reopen thread-safe stream

      if (!_terms_in) {
        // implementation returned wrong pointer
        SDB_ERROR(IRESEARCH, "Failed to reopen terms input");

        throw IoError("failed to reopen terms input");
      }
    }

    return *_terms_in;
  }

  void PopToParent() {
    const uint64_t start = _cur_block->Start();
    _cur_block = PopBlock();
    _posting_meta = _cur_block->State();
    if (_cur_block->Dirty() || _cur_block->BlockStart() != start) {
      SDB_ASSERT(_cur_block->Prefix() < _term_buf.size());
      _cur_block->ScanToSubBlock(_term_buf[_cur_block->Prefix()]);
      _cur_block->Load(TermsInput());
      _cur_block->ScanToBlock(start);
    }
  }

  void Copy(const byte_type* suffix, size_t prefix_size, size_t suffix_size) {
    sdb::basics::StrResizeAmortized(_term_buf, prefix_size + suffix_size);
    std::memcpy(_term_buf.data() + prefix_size, suffix, suffix_size);
  }

  void RefreshValue() noexcept { _term.value = _term_buf; }
  void ResetValue() noexcept { _term.value = {}; }

  mutable PostingMeta _posting_meta;
  TermAttr _term;
  const TermReaderBase* _field;
  PostingsReader* _postings;
  const IndexInput* _terms_in_source;
  mutable IndexInput::ptr _terms_in;
  const FST* _fst;
  ExplicitMatcher<FST> _matcher;
  bstring _term_buf;
  byte_weight _weight;  // aggregated fst output
  std::vector<Arc> _sstate;
  std::vector<BlockIterator> _block_stack;
  BlockIterator* _cur_block{};
};

template<typename FST>
ptrdiff_t TermIteratorBase<FST>::SeekCached(size_t& prefix, StateidT& state,
                                            size_t& block, byte_weight& weight,
                                            bytes_view target) {
  SDB_ASSERT(!_block_stack.empty());
  const auto term = Value();
  const byte_type* pterm = term.data();
  const byte_type* ptarget = target.data();

  // determine common prefix between target term and current
  {
    auto begin = _sstate.begin();
    auto end = begin + std::min(target.size(), _sstate.size());

    for (; begin != end && *pterm == *ptarget; ++begin, ++pterm, ++ptarget) {
      auto& state_weight = begin->weight;
      weight.PushBack(state_weight.begin(), state_weight.end());
      state = begin->state;
      block = begin->block;
    }

    prefix = size_t(pterm - term.data());
  }

  // inspect suffix and determine our current position
  // with respect to target term (before, after, equal)
  ptrdiff_t cmp = irs::char_traits<byte_type>::compare(
    pterm, ptarget, std::min(target.size(), term.size()) - prefix);

  if (!cmp) {
    cmp = term.size() - target.size();
  }

  if (cmp) {
    // truncate block_stack_ to match path
    const auto begin = _block_stack.begin() + (block + 1);
    SDB_ASSERT(begin <= _block_stack.end());
    _block_stack.erase(begin, _block_stack.end());
  }

  // cmp < 0 : target term is after the current term
  // cmp == 0 : target term is current term
  // cmp > 0 : target term is before current term
  return cmp;
}

template<typename FST>
bool TermIteratorBase<FST>::SeekToBlock(bytes_view term, size_t& prefix) {
  SDB_ASSERT(_fst->GetImpl());
  auto& fst = *_fst->GetImpl();

  prefix = 0;                    // number of current symbol to process
  StateidT state = fst.Start();  // start state
  _weight.Clear();               // clear aggregated fst output
  size_t block = 0;

  if (_cur_block) {
    const ptrdiff_t cmp = SeekCached(prefix, state, block, _weight, term);

    if (cmp > 0) {
      // target term is before the current term
      _block_stack[block].Reset();
    } else if (0 == cmp) {
      if (_cur_block->Type() == EntryType::Block) {
        // we're at the block with matching prefix
        _cur_block = PushBlock(_cur_block->BlockStart(), _term_buf.size());
        return false;
      } else {
        // we're already at current term
        return true;
      }
    }
  } else {
    PushBlock(fst.Final(state), prefix);
  }

  // reset to common seek prefix
  // TODO(mbkkt) why?
  sdb::basics::StrReserveAmortized(_term_buf, term.size());
  sdb::basics::StrResize(_term_buf, prefix);
  _sstate.resize(prefix);  // remove invalid cached arcs

  while (FstBuffer::FstByteBuilder::kFinal != state && prefix < term.size()) {
    _matcher.SetState(state);

    if (!_matcher.Find(term[prefix])) {
      break;
    }

    const auto& arc = _matcher.Value();

    _term_buf += byte_type(arc.ilabel);  // aggregate arc label
    _weight.PushBack(arc.weight.begin(),
                     arc.weight.end());  // aggregate arc weight
    ++prefix;

    const auto& weight = fst.FinalRef(arc.nextstate);

    if (!weight.Empty()) {
      PushBlock(fst::Times(_weight, weight), prefix);
      ++block;
    } else if (FstBuffer::FstByteBuilder::kFinal == arc.nextstate) {
      // ensure final state has no weight assigned
      // the only case when it's wrong is degenerated FST composed of only
      // 'fst_byte_builder::final' state.
      // in that case we'll never get there due to the loop condition above.
      SDB_ASSERT(fst.FinalRef(FstBuffer::FstByteBuilder::kFinal).Empty());

      PushBlock(std::move(_weight), prefix);
      ++block;
    }

    // cache found arcs, we can reuse it in further seeks
    // avoiding relatively expensive FST lookups
    _sstate.emplace_back(arc.nextstate, arc.weight, block);

    // proceed to the next state
    state = arc.nextstate;
  }

  _cur_block = &_block_stack[block];
  prefix = _cur_block->Prefix();
  _sstate.resize(prefix);

  if (prefix < term.size()) {
    _cur_block->ScanToSubBlock(term[prefix]);
  }

  return false;
}

template<typename FST>
SeekResult TermIteratorBase<FST>::SeekEqual(bytes_view term, bool exact) {
  [[maybe_unused]] size_t prefix;
  if (SeekToBlock(term, prefix)) {
    SDB_ASSERT(EntryType::Term == _cur_block->Type());
    return SeekResult::Found;
  }

  SDB_ASSERT(_cur_block);

  if (exact && _cur_block->NoTerms()) {
    // current block has no terms
    _term.value = {_term_buf.c_str(), prefix};
    return SeekResult::NotFound;
  }

  auto append_suffix = [this](const byte_type* suffix, size_t suffix_size) {
    const auto prefix = _cur_block->Prefix();
    sdb::basics::StrResizeAmortized(_term_buf, prefix + suffix_size);
    std::memcpy(_term_buf.data() + prefix, suffix, suffix_size);
  };

  _cur_block->Load(TermsInput());

  Finally refresh_value = [this]() noexcept { this->RefreshValue(); };

  SDB_ASSERT(term.starts_with(_term_buf));
  return _cur_block->ScanToTerm(term, append_suffix);
}

template<typename FST>
class TermIteratorImpl : public SeekTermIterator, public TermIteratorBase<FST> {
 public:
  using Base = TermIteratorBase<FST>;
  using Base::Base;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return Base::GetAttribute(type);
  }

  bytes_view value() const noexcept final { return Base::Value(); }

  bool next() final;
  SeekResult seek_ge(bytes_view term) final;
  bool seek(bytes_view term) final {
    return SeekResult::Found == Base::SeekEqual(term, true);
  }

  const PostingMeta& cookie() const final { return Base::Cookie(); }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return Base::Postings(features);
  }
};

template<typename FST>
bool TermIteratorImpl<FST>::next() {
  if (!this->_cur_block) {  // iterator at the beginning
    SDB_ASSERT(value().empty());
    this->_cur_block =
      this->PushBlock(this->_fst->Final(this->_fst->Start()), 0);
    this->_cur_block->Load(this->TermsInput());
  }

  auto copy_suffix = [this](const byte_type* suffix, size_t suffix_size) {
    this->Copy(suffix, this->_cur_block->Prefix(), suffix_size);
  };

  while (this->_cur_block->Done()) {
    if (this->_cur_block->template NextSubBlock<false>()) {
      this->_cur_block->Load(this->TermsInput());
    } else if (&this->_block_stack.front() == this->_cur_block) {  // root
      this->ResetValue();
      this->_cur_block->Reset();
      this->_sstate.clear();
      return false;
    } else {
      this->PopToParent();
    }
  }

  this->_sstate.resize(
    std::min(this->_sstate.size(), this->_cur_block->Prefix()));

  for (this->_cur_block->Next(copy_suffix);;
       this->_cur_block->Next(copy_suffix)) {
    if (EntryType::Block != this->_cur_block->Type()) {
      break;
    }
    this->_cur_block =
      this->PushBlock(this->_cur_block->BlockStart(), this->_term_buf.size());
    this->_cur_block->Load(this->TermsInput());
  }

  this->RefreshValue();
  return true;
}

template<typename FST>
SeekResult TermIteratorImpl<FST>::seek_ge(bytes_view term) {
  switch (Base::SeekEqual(term, false)) {
    case SeekResult::Found:
      SDB_ASSERT(EntryType::Term == this->_cur_block->Type());
      return SeekResult::Found;
    case SeekResult::NotFound:
      switch (this->_cur_block->Type()) {
        case EntryType::Term:
          // we're already at greater term
          return SeekResult::NotFound;
        case EntryType::Block:
          // we're at the greater block, load it and call next
          this->_cur_block = this->PushBlock(this->_cur_block->BlockStart(),
                                             this->_term_buf.size());
          this->_cur_block->Load(this->TermsInput());
          break;
        default:
          SDB_ASSERT(false);
          return SeekResult::End;
      }
      [[fallthrough]];
    case SeekResult::End:
      return next() ? SeekResult::NotFound  // have moved to the next entry
                    : SeekResult::End;      // have no more terms
  }

  SDB_ASSERT(false);
  return SeekResult::End;
}

template<typename FST, typename A>
class AcceptorTermIterator : public SeekTermIterator,
                             public TermIteratorBase<FST> {
 public:
  using Base = TermIteratorBase<FST>;
  using StateidT = typename Base::StateidT;
  using State = typename A::State;

  AcceptorTermIterator(const TermReaderBase& field, PostingsReader& postings,
                       const IndexInput& terms_in, const FST& fst, const A& a)
    : Base{field, postings, terms_in, fst}, _a{&a} {
    if constexpr (A::kHasPayload) {
      _pay.value = {&_payload, sizeof(typename A::PayloadType)};
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (A::kHasPayload) {
      if (type == Type<PayAttr>::id()) {
        return &_pay;
      }
    }
    return Base::GetAttribute(type);
  }

  bytes_view value() const noexcept final { return Base::Value(); }

  const PostingMeta& cookie() const final { return Base::Cookie(); }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return Base::Postings(features);
  }

  bool next() final {
    if (this->_cur_block == nullptr) [[unlikely]] {
      if (const auto lower = _a->LowerBound(); !lower.empty()) {
        return SeekResult::End != seek_ge(lower);
      }
    }
    return NextImpl();
  }

  SeekResult seek_ge(bytes_view target) final;

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

 private:
  struct Level {
    State state;
    uint32_t lo;
    uint32_t hi;
    size_t weight_size;
    StateidT fst_state;
    bool alive;

    bool HasLabels() const noexcept { return lo <= hi; }
  };

  bool NextImpl();

  bool Extend(State from, const byte_type* suffix, size_t n) {
    if constexpr (A::kCheapRuns) {
      for (size_t i = 0; i != n;) {
        State moved{};
        i += _a->StepRun(from, suffix + i, n - i, moved);
        if (i == n) {
          break;
        }
        if (!A::Alive(moved)) {
          return false;
        }
        from = moved;
        ++i;
      }
    } else {
      for (size_t i = 0; i != n; ++i) {
        from = _a->Step(from, suffix[i]);
        if (!A::Alive(from)) {
          return false;
        }
      }
    }
    _live = from;
    return true;
  }

  bool Accepts() { return _a->Accept(_live, _payload); }

  Level MakeLevel(State state, size_t weight_size, StateidT fst_state) const {
    Level level{state, 1, 0, weight_size, fst_state, A::Alive(state)};
    if (level.alive) {
      _a->LiveRange(state, level.lo, level.hi);
    }
    return level;
  }

  void ResetLevels() {
    this->_weight.Clear();
    _levels.assign(1, MakeLevel(_a->Start(), 0, this->_fst->Start()));
  }

  bool PushSubBlock(const byte_type* suffix, size_t n);

  bool NextFloorSubBlock();

  void RebuildLevels();

  const A* _a;
  sdb::containers::SmallVector<Level, 8> _levels;
  State _live{};
  typename A::PayloadType _payload{};
  PayAttr _pay;
};

template<typename FST, typename A>
bool AcceptorTermIterator<FST, A>::PushSubBlock(const byte_type* suffix,
                                                size_t n) {
  uint32_t lo = 1;
  uint32_t hi = 0;
  _a->LiveRange(_live, lo, hi);
  const bool accepts = Accepts();
  if (lo > hi && !accepts) {
    return false;
  }

  auto& fst = *this->_fst->GetImpl();
  const auto& parent = _levels.back();
  this->_weight.Resize(parent.weight_size);
  auto fst_state = parent.fst_state;
  for (size_t i = 0; i != n; ++i) {
    this->_matcher.SetState(fst_state);
    [[maybe_unused]] const bool found = this->_matcher.Find(suffix[i]);
    SDB_ASSERT(found);
    const auto& arc = this->_matcher.Value();
    this->_weight.PushBack(arc.weight.begin(), arc.weight.end());
    fst_state = arc.nextstate;
  }
  const size_t weight_size = this->_weight.Size();
  const auto& final_weight = fst.FinalRef(fst_state);
  SDB_ASSERT(!final_weight.Empty() ||
             FstBuffer::FstByteBuilder::kFinal == fst_state);
  this->_weight.PushBack(final_weight.begin(), final_weight.end());

  _levels.emplace_back(Level{_live, lo, hi, weight_size, fst_state, true});
  this->_cur_block =
    this->PushBlock(bytes_view{this->_weight}, this->_term_buf.size());
  if (!accepts && lo <= hi) {
    this->_cur_block->ScanToSubBlock(static_cast<byte_type>(lo));
  }
  this->_cur_block->Load(this->TermsInput());
  return true;
}

template<typename FST, typename A>
bool AcceptorTermIterator<FST, A>::NextFloorSubBlock() {
  auto* block = this->_cur_block;
  const auto sub_count = block->SubCount();
  if (sub_count == BlockIterator::kUndefinedCount) {
    if (!block->template NextSubBlock<false>()) {
      return false;
    }
    block->Load(this->TermsInput());
    return true;
  }
  if (sub_count == 0) {
    return false;
  }
  const auto& level = _levels.back();
  const uint32_t next_label = block->NextLabel();
  SDB_ASSERT(next_label != Block::kInvalidLabel);
  if (!level.HasLabels() || next_label > level.hi) {
    return false;
  }
  if (next_label < level.lo) {
    block->ScanToSubBlock(static_cast<byte_type>(level.lo));
  } else {
    block->template NextSubBlock<true>();
  }
  block->Load(this->TermsInput());
  return true;
}

template<typename FST, typename A>
void AcceptorTermIterator<FST, A>::RebuildLevels() {
  SDB_ASSERT(!this->_block_stack.empty());
  auto& fst = *this->_fst->GetImpl();
  this->_weight.Clear();
  _levels.clear();
  auto fst_state = fst.Start();
  auto state = _a->Start();
  bool alive = true;
  size_t depth = 0;
  for (const auto& block : this->_block_stack) {
    const size_t prefix = block.Prefix();
    SDB_ASSERT(prefix <= this->_term_buf.size());
    for (; depth != prefix; ++depth) {
      const auto label = this->_term_buf[depth];
      this->_matcher.SetState(fst_state);
      [[maybe_unused]] const bool found = this->_matcher.Find(label);
      SDB_ASSERT(found);
      const auto& arc = this->_matcher.Value();
      this->_weight.PushBack(arc.weight.begin(), arc.weight.end());
      fst_state = arc.nextstate;
      if (alive) {
        state = _a->Step(state, label);
        alive = A::Alive(state);
      }
    }
    _levels.emplace_back(MakeLevel(state, this->_weight.Size(), fst_state));
  }
}

template<typename FST, typename A>
SeekResult AcceptorTermIterator<FST, A>::seek_ge(bytes_view target) {
  const auto res = this->SeekEqual(target, false);
  RebuildLevels();

  bool accepted = false;
  if (res != SeekResult::End && _levels.back().alive) {
    const auto prefix = this->_cur_block->Prefix();
    SDB_ASSERT(prefix <= this->_term_buf.size());
    const auto* suffix = this->_term_buf.data() + prefix;
    const size_t suffix_len = this->_term_buf.size() - prefix;
    const auto state = _levels.back().state;
    if (Extend(state, suffix, suffix_len)) {
      if (this->_cur_block->Type() == EntryType::Term) {
        accepted = Accepts();
      } else {
        PushSubBlock(suffix, suffix_len);
      }
    }
  }

  if (accepted) {
    this->RefreshValue();
  } else if (!NextImpl()) {
    return SeekResult::End;
  }
  return this->value() == target ? SeekResult::Found : SeekResult::NotFound;
}

template<typename FST, typename A>
bool AcceptorTermIterator<FST, A>::NextImpl() {
  if (!this->_cur_block) {
    SDB_ASSERT(this->value().empty());
    ResetLevels();
    this->_cur_block =
      this->PushBlock(this->_fst->Final(this->_fst->Start()), 0);
    this->_cur_block->Load(this->TermsInput());
  }

  const byte_type* suffix_ptr = nullptr;
  size_t suffix_len = 0;
  auto read_suffix = [&](const byte_type* suffix, size_t suffix_size) {
    suffix_ptr = suffix;
    suffix_len = suffix_size;
  };

  bool frame_done = !_levels.back().alive;
  State state{};
  uint32_t lo = 1;
  uint32_t hi = 0;
  const auto load_frame = [&] {
    const auto& frame = _levels.back();
    state = frame.state;
    lo = frame.lo;
    hi = frame.hi;
  };
  for (;;) {
    while (frame_done || this->_cur_block->Done()) {
      if (!frame_done && NextFloorSubBlock()) {
        continue;
      }
      if (&this->_block_stack.front() == this->_cur_block) {  // root
        this->ResetValue();
        this->_cur_block->Reset();
        this->_sstate.clear();
        ResetLevels();
        return false;
      }
      _levels.pop_back();
      this->PopToParent();
      frame_done = !_levels.back().alive;
    }

    this->_sstate.resize(
      std::min(this->_sstate.size(), this->_cur_block->Prefix()));
    load_frame();

    bool matched = false;
    for (;;) {
      this->_cur_block->Next(read_suffix);
      if (suffix_len != 0) {
        const uint32_t lead = *suffix_ptr;
        if (lead > hi) {
          frame_done = true;
          break;
        }
        if (lead < lo) {
          if (this->_cur_block->Done()) {
            break;
          }
          continue;
        }
      }
      if (Extend(state, suffix_ptr, suffix_len)) {
        if (EntryType::Block != this->_cur_block->Type()) {
          if (Accepts()) {
            this->Copy(suffix_ptr, this->_cur_block->Prefix(), suffix_len);
            matched = true;
            break;
          }
        } else {
          this->Copy(suffix_ptr, this->_cur_block->Prefix(), suffix_len);
          if (PushSubBlock(suffix_ptr, suffix_len)) {
            load_frame();
            continue;
          }
        }
      }
      if (this->_cur_block->Done()) {
        break;
      }
    }

    if (matched) {
      this->RefreshValue();
      return true;
    }
  }
}

template<typename FST>
class SingleTermLookup {
 public:
  SingleTermLookup(const TermReaderBase& field, PostingsReader& postings,
                   IndexInput::ptr&& terms_in, const FST& fst) noexcept
    : _terms_in{std::move(terms_in)},
      _postings{&postings},
      _field{&field},
      _fst{&fst} {
    SDB_ASSERT(_terms_in);
  }

  bool seek(bytes_view term);

  const PostingMeta& cookie() const noexcept { return _meta; }

  DocIterator::ptr postings(IndexFeatures features) const {
    return _postings->Iterator(
      _field->meta().index_features, features, {.cookie = &_meta},
      IteratorFieldOptions{.has_score_bounds = _field->HasScoreBounds()});
  }

 private:
  PostingMeta _meta;
  IndexInput::ptr _terms_in;
  PostingsReader* _postings;
  const TermReaderBase* _field;
  const FST* _fst;
};

template<typename FST>
bool SingleTermLookup<FST>::seek(bytes_view term) {
  SDB_ASSERT(_fst->GetImpl());
  auto& fst = *_fst->GetImpl();

  auto state = fst.Start();
  ExplicitMatcher<FST> matcher{_fst, fst::MATCH_INPUT};

  byte_weight weight_prefix;
  const auto* weight_suffix = &fst.FinalRef(state);
  size_t weight_prefix_length = 0;
  size_t block_prefix = 0;

  matcher.SetState(state);

  for (size_t prefix = 0; prefix < term.size() && matcher.Find(term[prefix]);
       matcher.SetState(state)) {
    const auto& arc = matcher.Value();
    state = arc.nextstate;
    weight_prefix.PushBack(arc.weight.begin(), arc.weight.end());
    ++prefix;

    auto& weight = fst.FinalRef(state);

    if (!weight.Empty() || FstBuffer::FstByteBuilder::kFinal == state) {
      weight_prefix_length = weight_prefix.Size();
      weight_suffix = &weight;
      block_prefix = prefix;

      if (FstBuffer::FstByteBuilder::kFinal == state) {
        break;
      }
    }
  }

  weight_prefix.Resize(weight_prefix_length);
  weight_prefix.PushBack(weight_suffix->begin(), weight_suffix->end());
  BlockIterator cur_block{std::move(weight_prefix), block_prefix};

  if (block_prefix < term.size()) {
    cur_block.ScanToSubBlock(term[block_prefix]);
  }

  if (!BlockMeta::Terms(cur_block.Meta())) {
    return false;
  }

  cur_block.Load(*_terms_in);

  if (SeekResult::Found == cur_block.ScanToTerm(term, [](auto, auto) {})) {
    cur_block.LoadData(_field->meta(), _meta, *_postings);
    return true;
  }

  return false;
}

}  // namespace
namespace irs::burst_trie {

class FieldReader::Impl {
 public:
  explicit Impl(PostingsReader::ptr&& pr, IResourceManager& rm);

  uint64_t CountMappedMemory() const {
    uint64_t bytes = 0;
    if (_pr != nullptr) {
      bytes += _pr->CountMappedMemory();
    }
    if (_terms_in != nullptr) {
      bytes += _terms_in->CountMappedMemory();
    }
    return bytes;
  }

  void prepare(const ReaderState& state);

  const TermReader* field(field_id id) const;
  std::span<const field_id> field_ids() const noexcept { return _sorted_ids; }
  size_t size() const noexcept { return _id_to_field.size(); }

 private:
  template<typename FST>
  class TermReaderImpl final : public TermReaderBase {
   public:
    explicit TermReaderImpl(FieldReader::Impl& owner) noexcept
      : _owner(&owner) {}
    TermReaderImpl(TermReaderImpl&& rhs) = default;
    TermReaderImpl& operator=(TermReaderImpl&& rhs) = delete;

    void PrepareFromMeta(field_id id, const TermDictMeta& meta,
                         IndexInput& blocks_in) {
      blocks_in.Seek(meta.body_offset);
      LoadFromMeta(id, meta, blocks_in);
      _fst.reset(FST::Read(blocks_in, _owner->_resource_manager));
      if (!_fst) {
        throw IndexError{
          absl::StrCat("Failed to read term index for field id ", id)};
      }
    }

    SeekTermIterator::ptr iterator() const final {
      return memory::make_managed<TermIteratorImpl<FST>>(
        *this, *_owner->_pr, *_owner->_terms_in, *_fst);
    }

    PostingMeta Lookup(bytes_view term) const final {
      // Order is important here!
      if (max() < term || term < min()) {
        return {};
      }

      SingleTermLookup<FST> it{*this, *_owner->_pr, _owner->_terms_in->Reopen(),
                               *_fst};

      if (!it.seek(term)) {
        return {};
      }

      return it.cookie();
    }

    void ReadDocs(bytes_view term, Acceptor acceptor) const final {
      // Order is important here!
      if (max() < term || term < min()) {
        return;
      }

      SingleTermLookup<FST> it{*this, *_owner->_pr, _owner->_terms_in->Reopen(),
                               *_fst};

      if (!it.seek(term)) {
        return;
      }

      if (const auto& meta = it.cookie(); meta.docs_count == 1) {
        acceptor(doc_limits::min() + meta.doc_delta);
        return;
      }

      auto docs_it = it.postings(IndexFeatures::None);

      if (!docs_it) [[unlikely]] {
        SDB_ASSERT(false);
        return;
      }

      doc_id_t d;
      while (!doc_limits::eof(d = docs_it->advance())) {
        SDB_ASSERT(doc_limits::valid(d));
        if (!acceptor(d)) {
          break;
        }
      }
    }

    size_t BitUnion(CookieProvider provider, uint64_t* set) const final {
      SDB_ASSERT(_owner != nullptr);
      SDB_ASSERT(_owner->_pr != nullptr);
      return _owner->_pr->BitUnion(meta().index_features, provider, set,
                                   HasScoreBounds());
    }

    template<typename A>
    SeekTermIterator::ptr MakeAcceptorIterator(const A& a) const {
      return memory::make_managed<AcceptorTermIterator<FST, A>>(
        *this, *_owner->_pr, *_owner->_terms_in, *_fst, a);
    }

    SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final {
      return MakeAcceptorIterator(a);
    }

    SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final {
      return MakeAcceptorIterator(a);
    }

    DocIterator::ptr Iterator(IndexFeatures features,
                              std::span<const PostingCookie> cookies,
                              bool score_prune, size_t min_match,
                              ScoreMergeType type) const final {
      SDB_ASSERT(_owner);
      SDB_ASSERT(_owner->_pr);
      SDB_ASSERT(!cookies.empty());
      SDB_ASSERT(1 <= min_match);
      SDB_ASSERT(min_match <= cookies.size());
      const IteratorFieldOptions field_options{
        .score_prune = score_prune, .has_score_bounds = HasScoreBounds()};

      return _owner->_pr->Iterator(meta().index_features, features, cookies,
                                   field_options, min_match, type);
    }

    std::unique_ptr<IndexInput> ReopenPayload() const final {
      SDB_ASSERT(_owner && _owner->_pr);
      return _owner->_pr->ReopenPayload();
    }

   private:
    FieldReader::Impl* _owner;
    std::unique_ptr<FST> _fst;
  };

  using ImmutableFstReader = TermReaderImpl<immutable_byte_fst>;
  using ImmutableFstReaders = std::vector<TermReaderImpl<immutable_byte_fst>>;

  ImmutableFstReaders _fields;
  absl::flat_hash_map<field_id, TermReader*> _id_to_field;
  std::vector<field_id> _sorted_ids;
  PostingsReader::ptr _pr;
  IndexInput::ptr _terms_in;
  IResourceManager& _resource_manager;
};

FieldReader::Impl::Impl(PostingsReader::ptr&& pr, IResourceManager& rm)
  : _pr{std::move(pr)}, _resource_manager{rm} {
  SDB_ASSERT(_pr);
}

void FieldReader::Impl::prepare(const ReaderState& state) {
  SDB_ASSERT(state.dir);
  SDB_ASSERT(state.meta);
  SDB_ASSERT(state.idx, "FieldReader::Impl::prepare requires an IdxReader");

  auto entries = state.idx->TermDicts();
  _fields.reserve(entries.size());
  _id_to_field.reserve(entries.size());

  _terms_in = state.idx->ReopenIn();
  if (!_terms_in) {
    SDB_ENSURE(entries.empty(), "burst_trie: TermDicts span has ",
               entries.size(), " entries but `.idx` body stream is null");
    return;
  }
  _terms_in->Seek(state.idx->BodyStart());

  IndexFeatures features = IndexFeatures::None;
  for (const auto& [id, meta] : entries) {
    features = features | meta.features;
  }
  _pr->prepare(*_terms_in, state, features);

  _sorted_ids.reserve(entries.size());
  for (const auto& [id, meta] : entries) {
    auto& field = _fields.emplace_back(*this);
    field.PrepareFromMeta(id, meta, *_terms_in);
    auto [it, ok] = _id_to_field.emplace(id, &field);
    SDB_ENSURE(ok, ".idx footer: duplicate term-dict field_id ", id);
    _sorted_ids.push_back(id);
  }
  SDB_ENSURE(std::is_sorted(_sorted_ids.begin(), _sorted_ids.end()),
             "burst_trie: term-dict entries are not sorted by field_id");
}

const TermReader* FieldReader::Impl::field(field_id id) const {
  auto it = _id_to_field.find(id);
  return it == _id_to_field.end() ? nullptr : it->second;
}

FieldWriter::FieldWriter(PostingsWriter::ptr pw, bool compaction,
                         IResourceManager& rm)
  : _impl{std::make_unique<Impl>(std::move(pw), compaction, rm)} {}

FieldWriter::~FieldWriter() = default;

void FieldWriter::SetIdxWriter(IdxWriter& idx) noexcept {
  _impl->SetIdxWriter(idx);
}

void FieldWriter::prepare(const FlushState& state) { _impl->prepare(state); }

void FieldWriter::write(const BasicTermReader& reader) { _impl->write(reader); }

void FieldWriter::end() { _impl->end(); }

FieldReader::FieldReader(PostingsReader::ptr pr, IResourceManager& rm)
  : _impl{std::make_unique<Impl>(std::move(pr), rm)} {}

FieldReader::~FieldReader() = default;

uint64_t FieldReader::CountMappedMemory() const {
  return _impl->CountMappedMemory();
}

void FieldReader::prepare(const ReaderState& state) { _impl->prepare(state); }

const TermReader* FieldReader::field(field_id id) const {
  return _impl->field(id);
}

std::span<const field_id> FieldReader::field_ids() const noexcept {
  return _impl->field_ids();
}

size_t FieldReader::size() const noexcept { return _impl->size(); }

}  // namespace irs::burst_trie

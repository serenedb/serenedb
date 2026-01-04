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

#include "formats_burst_trie.hpp"

#include <variant>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "iresearch/index/index_features.hpp"

// clang-format off

#include "iresearch/utils/fstext/fst_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

#include "format_utils.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats_attributes.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/automaton.hpp"
#include "iresearch/utils/encryption.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "basics/containers/monotonic_buffer.hpp"
#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/utils/directory_utils.hpp"
#include "iresearch/utils/fstext/fst_string_weight.hpp"
#include "iresearch/utils/fstext/fst_builder.hpp"
#include "iresearch/utils/fstext/fst_decl.hpp"
#include "iresearch/utils/fstext/fst_matcher.hpp"
#include "iresearch/utils/fstext/fst_string_ref_weight.hpp"
#include "iresearch/utils/fstext/fst_table_matcher.hpp"
#include "iresearch/utils/fstext/immutable_fst.hpp"
#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/string.hpp"
#include "basics/logger/logger.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/internal/resize_uninitialized.h>
#include <absl/strings/str_cat.h>

// clang-format on

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

    void WriteBytes(const byte_type* b, size_t len) final {
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
  Entry(bytes_view term, TermMetaImpl&& attrs, bool volatile_term);

  Entry(bytes_view prefix, Block::BlockIndex&& index, uint64_t block_start,
        uint8_t meta, uint16_t label, bool volatile_term);
  Entry(Entry&& rhs) noexcept;
  Entry& operator=(Entry&& rhs) noexcept;
  ~Entry() { Destroy(); }

  TermMetaImpl& Term() noexcept { return _term; }

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
    TermMetaImpl _term;
    ::Block _block;
  };
  EntryType _type;
};

Entry::Entry(bytes_view term, TermMetaImpl&& attrs, bool volatile_term)
  : _type{EntryType::Term} {
  _data.Assign(term, volatile_term);
  new (&_term) TermMetaImpl{std::move(attrs)};
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
      new (&_term) TermMetaImpl{std::move(rhs._term)};
      rhs._term.~TermMetaImpl();
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
      _term.~TermMetaImpl();
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
    return CheckBit<std::to_underlying(EntryType::Term)>(mask);
  }

  // block has sub-blocks
  static bool Blocks(uint8_t mask) noexcept {
    return CheckBit<std::to_underlying(EntryType::Block)>(mask);
  }

  static void Type(uint8_t& mask, EntryType type) noexcept {
    SetBit(mask, std::to_underlying(type));
  }

  // block is floor block
  static bool Floor(uint8_t mask) noexcept {
    return CheckBit<std::to_underlying(EntryType::Invalid)>(mask);
  }
  static void Floor(uint8_t& mask, bool b) noexcept {
    SetBit<std::to_underlying(EntryType::Invalid)>(b, mask);
  }

  // resets block meta
  static void Reset(uint8_t mask) noexcept {
    UnsetBit<std::to_underlying(EntryType::Term)>(mask);
    UnsetBit<std::to_underlying(EntryType::Block)>(mask);
  }
};

void WriteFeatures(IndexOutput& out, IndexFeatures features) {
  SDB_ASSERT(features < IndexFeatures{0x80});
  out.WriteByte(static_cast<uint8_t>(features));
}

IndexFeatures ReadFeatures(DataInput& in) {
  const IndexFeatures features{in.ReadByte()};

  if (features > IndexFeatures::Max) {
    throw IndexError{absl::StrCat("Invalid index features ", features)};
  }

  return features;
}

void WriteFieldFeatures(IndexOutput& out, FieldProperties props) {
  SDB_ASSERT(!field_limits::valid(props.norm) ||
             IsSubsetOf(IndexFeatures::Norm, props.index_features));

  WriteFeatures(out, props.index_features);
  if (IsSubsetOf(IndexFeatures::Norm, props.index_features)) {
    out.WriteV64(props.norm + 1U);
  }
}

void ReadFieldFeatures(DataInput& in, FieldMeta& field) {
  field.index_features = ReadFeatures(in);

  if (IsSubsetOf(IndexFeatures::Norm, field.index_features)) {
    field.norm = in.ReadV64() - 1U;
  }

  SDB_ASSERT(!field_limits::valid(field.norm) ||
             IsSubsetOf(IndexFeatures::Norm, field.index_features));
}

inline void PrepareOutput(std::string& str, IndexOutput::ptr& out,
                          const FlushState& state, std::string_view ext,
                          std::string_view format, const int32_t version) {
  SDB_ASSERT(!out);

  FileName(str, state.name, ext);
  out = state.dir->create(str);

  if (!out) {
    throw IoError{absl::StrCat("failed to create file, path: ", str)};
  }

  format_utils::WriteHeader(*out, format, version);
}

inline int32_t PrepareInput(std::string& str, IndexInput::ptr& in,
                            irs::IOAdvice advice, const ReaderState& state,
                            std::string_view ext, std::string_view format,
                            const int32_t min_ver, const int32_t max_ver,
                            int64_t* checksum = nullptr) {
  SDB_ASSERT(!in);

  FileName(str, state.meta->name, ext);
  in = state.dir->open(str, advice);

  if (!in) {
    throw IoError{absl::StrCat("Failed to open file, path: ", str)};
  }

  if (checksum) {
    *checksum = format_utils::Checksum(*in);
  }

  return format_utils::CheckHeader(*in, format, min_ver, max_ver);
}

struct Cookie final : SeekCookie {
  explicit Cookie(const TermMetaImpl& meta) noexcept : meta(meta) {}

  Attribute* GetMutable(TypeInfo::type_id type) final {
    if (type == irs::Type<TermMeta>::id()) [[likely]] {
      return &meta;
    }

    return nullptr;
  }

  bool IsEqual(const irs::SeekCookie& rhs) const noexcept final {
    // We intentionally don't check `rhs` cookie type.
    const auto& rhs_meta = sdb::basics::downCast<Cookie>(rhs).meta;
    return meta.doc_start == rhs_meta.doc_start &&
           meta.pos_start == rhs_meta.pos_start;
  }

  size_t Hash() const noexcept final {
    return absl::HashOf(meta.doc_start, meta.pos_start);
  }

  TermMetaImpl meta;
};

const fst::FstReadOptions& FstReadOptions() {
  static const auto kInstance = [] {
    fst::FstReadOptions options;
    options.read_osymbols = false;  // we don't need output symbols

    return options;
  }();

  return kInstance;
}

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
  struct FstStats : irs::FstStats {
    size_t total_weight_size{};

    void operator()(const byte_weight& w) noexcept {
      total_weight_size += w.Size();
    }

    [[maybe_unused]] bool operator==(const FstStats& rhs) const noexcept {
      return num_states == rhs.num_states && num_arcs == rhs.num_arcs &&
             total_weight_size == rhs.total_weight_size;
    }
  };

  FstBuffer(IResourceManager& rm)
    : vector_byte_fst{ManagedTypedAllocator<byte_arc>{rm}} {}

  using FstByteBuilder = FstBuilder<byte_type, vector_byte_fst, FstStats>;

  FstStats Reset(const Block::BlockIndex& index) {
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

class FieldWriter final : public irs::FieldWriter {
 public:
  static constexpr uint32_t kDefaultMinBlockSize = 25;
  static constexpr uint32_t kDefaultMaxBlockSize = 48;

  static constexpr std::string_view kFormatTerms = "block_tree_terms_dict";
  static constexpr std::string_view kTermsExt = "tm";
  static constexpr std::string_view kFormatTermsIndex =
    "block_tree_terms_index";
  static constexpr std::string_view kTermsIndexExt = "ti";

  FieldWriter(irs::PostingsWriter::ptr&& pw, bool consolidation,
              IResourceManager& rm,
              burst_trie::Version version = burst_trie::Version::Max,
              uint32_t min_block_size = kDefaultMinBlockSize,
              uint32_t max_block_size = kDefaultMaxBlockSize);

  ~FieldWriter() final;

  void prepare(const irs::FlushState& state) final;

  void end() final;

  void write(const BasicTermReader& reader) final;

 private:
  static constexpr size_t kDefaultSize = 8;

  void BeginField(const FieldProperties& meta);

  void EndField(std::string_view name, FieldProperties props,
                bytes_view min_term, bytes_view max_term,
                uint64_t total_doc_freq, uint64_t total_term_freq,
                uint64_t doc_count);

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
  MemoryOutput _suffix;  // term suffix column
  MemoryOutput _stats;   // term stats column
  Encryption::Stream::ptr _terms_out_cipher;
  IndexOutput::ptr _terms_out;  // output stream for terms
  Encryption::Stream::ptr _index_out_cipher;
  IndexOutput::ptr _index_out;  // output stream for indexes
  PostingsWriter::ptr _pw;      // postings writer
  ManagedVector<Entry> _stack;
  FstBuffer* _fst_buf;         // pimpl buffer used for building FST for fields
  VolatileByteRef _last_term;  // last pushed term
  std::vector<size_t> _prefixes;
  size_t _fields_count{};
  const burst_trie::Version _version;
  const uint32_t _min_block_size;
  const uint32_t _max_block_size;
  const bool _consolidation;
};

void FieldWriter::WriteBlock(size_t prefix, size_t begin, size_t end,
                             uint8_t meta, uint16_t label) {
  SDB_ASSERT(end > begin);

  // begin of the block
  const uint64_t block_start = _terms_out->Position();

  // write block header
  _terms_out->WriteV32(
    ShiftPack32(static_cast<uint32_t>(end - begin), end == _stack.size()));

  // write block entries
  const bool leaf = !BlockMeta::Blocks(meta);

  Block::BlockIndex index;

  _pw->begin_block();

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
    _suffix.stream.WriteBytes(data.data() + prefix, suf_size);

    if (EntryType::Term == type) {
      _pw->encode(_stats.stream, e.Term());
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

  _terms_out->WriteV64(ShiftPack64(block_size, leaf));

  auto copy = [this](const byte_type* b, size_t len) {
    _terms_out->WriteBytes(b, len);
    return true;
  };

  if (_terms_out_cipher) {
    auto offset = block_start;

    auto encrypt_and_copy = [this, &offset](byte_type* b, size_t len) {
      SDB_ASSERT(_terms_out_cipher);

      if (!_terms_out_cipher->Encrypt(offset, b, len)) {
        return false;
      }

      _terms_out->WriteBytes(b, len);
      offset += len;
      return true;
    };

    if (!_suffix.file.Visit(encrypt_and_copy)) {
      throw IoError("failed to encrypt term dictionary");
    }
  } else {
    _suffix.file.Visit(copy);
  }

  _terms_out->WriteV64(static_cast<uint64_t>(_stats.stream.Position()));
  _stats.file.Visit(copy);

  _suffix.stream.Reset();
  _stats.stream.Reset();

  // add new block to the list of created blocks
  _blocks.emplace_back(bytes_view{_last_term.View().data(), prefix},
                       std::move(index), block_start, meta, label,
                       _consolidation);
}

void FieldWriter::WriteBlocks(size_t prefix, size_t count) {
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

void FieldWriter::Push(bytes_view term) {
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
  _last_term.Assign(term, _consolidation);
}

FieldWriter::FieldWriter(irs::PostingsWriter::ptr&& pw, bool consolidation,
                         IResourceManager& rm, burst_trie::Version version,
                         uint32_t min_block_size, uint32_t max_block_size)
  : _output_buffer{rm, 32},
    _blocks{ManagedTypedAllocator<Entry>{rm}},
    _suffix{rm},
    _stats{rm},
    _pw{std::move(pw)},
    _stack{ManagedTypedAllocator<Entry>{rm}},
    _fst_buf{new FstBuffer{rm}},
    _prefixes{kDefaultSize, 0},
    _version{version},
    _min_block_size{min_block_size},
    _max_block_size{max_block_size},
    _consolidation{consolidation} {
  SDB_ASSERT(this->_pw);
  SDB_ASSERT(min_block_size > 1);
  SDB_ASSERT(min_block_size <= max_block_size);
  SDB_ASSERT(2 * (min_block_size - 1) <= max_block_size);
  SDB_ASSERT(_version >= burst_trie::Version::Min &&
             _version <= burst_trie::Version::Max);
}

FieldWriter::~FieldWriter() { delete _fst_buf; }

void FieldWriter::prepare(const FlushState& state) {
  SDB_ASSERT(state.dir);

  // reset writer state
  _last_term.Clear();
  _prefixes.assign(kDefaultSize, 0);
  _stack.clear();
  _stats.Reset();
  _suffix.Reset();
  _fields_count = 0;

  std::string filename;
  bstring enc_header;
  auto* enc = state.dir->attributes().encryption();

  // prepare term dictionary
  PrepareOutput(filename, _terms_out, state, kTermsExt, kFormatTerms,
                static_cast<int32_t>(_version));

  // encrypt term dictionary
  [[maybe_unused]] const auto encrypt =
    irs::Encrypt(filename, *_terms_out, enc, enc_header, _terms_out_cipher);
  SDB_ASSERT(!encrypt ||
             (_terms_out_cipher && _terms_out_cipher->block_size()));

  // prepare term index
  PrepareOutput(filename, _index_out, state, kTermsIndexExt, kFormatTermsIndex,
                static_cast<int32_t>(_version));

  // encrypt term index
  if (irs::Encrypt(filename, *_index_out, enc, enc_header, _index_out_cipher)) {
    SDB_ASSERT(_index_out_cipher && _index_out_cipher->block_size());

    const auto blocks_in_buffer = math::DivCeil64(
      kDefaultEncryptionBufferSize, _index_out_cipher->block_size());

    _index_out = irs::IndexOutput::ptr{new EncryptedOutput{
      std::move(_index_out), *_index_out_cipher, blocks_in_buffer}};
  }

  WriteFeatures(*_index_out, state.index_features);

  // prepare postings writer
  _pw->prepare(*_terms_out, state);

  _suffix.Reset();
  _stats.Reset();
}

void FieldWriter::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  const auto index_features = props.index_features;
  BeginField(props);

  uint64_t term_count = 0;
  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;

  const bool freq_exists =
    IndexFeatures::None != (index_features & IndexFeatures::Freq);

  auto terms = reader.iterator();
  SDB_ASSERT(terms != nullptr);
  while (terms->next()) {
    auto postings = terms->postings(index_features);
    TermMetaImpl meta;
    _pw->write(*postings, meta);

    if (freq_exists) {
      sum_tfreq += meta.freq;
    }

    if (meta.docs_count != 0) {
      sum_dfreq += meta.docs_count;

      const bytes_view term = terms->value();
      Push(term);

      // push term to the top of the stack
      _stack.emplace_back(term, std::move(meta), _consolidation);

      ++term_count;
    }
  }

  EndField(reader.name(), props, reader.min(), reader.max(), sum_dfreq,
           sum_tfreq, term_count);
}

void FieldWriter::BeginField(const FieldProperties& meta) {
  SDB_ASSERT(_terms_out);
  SDB_ASSERT(_index_out);

  // At the beginning of the field there should be no pending entries at all
  SDB_ASSERT(_stack.empty());

  _pw->begin_field(meta);
}

void FieldWriter::EndField(std::string_view name, FieldProperties props,
                           bytes_view min_term, bytes_view max_term,
                           uint64_t total_doc_freq, uint64_t total_term_freq,
                           uint64_t term_count) {
  SDB_ASSERT(_terms_out);
  SDB_ASSERT(_index_out);

  if (!term_count) {
    // nothing to write
    return;
  }

  const auto [wand_mask, doc_count] = _pw->end_field();

  // cause creation of all final blocks
  Push(kEmptyStringView<byte_type>);

  // write root block with empty prefix
  WriteBlocks(0, _stack.size());
  SDB_ASSERT(1 == _stack.size());

  // write field meta
  WriteStr(*_index_out, name);
  WriteFieldFeatures(*_index_out, props);

  _index_out->WriteV64(term_count);
  _index_out->WriteV64(doc_count);
  _index_out->WriteV64(total_doc_freq);
  WriteStr<bytes_view>(*_index_out, min_term);
  WriteStr<bytes_view>(*_index_out, max_term);
  if (IndexFeatures::None != (props.index_features & IndexFeatures::Freq)) {
    _index_out->WriteV64(total_term_freq);
  }
  _index_out->WriteU64(wand_mask);

  // build fst
  const Entry& root = *_stack.begin();
  SDB_ASSERT(_fst_buf);
  [[maybe_unused]] const auto fst_stats = _fst_buf->Reset(root.Block().index);
  _stack.clear();
  _output_buffer.Clear();

  const vector_byte_fst& fst = *_fst_buf;

#ifdef SDB_DEV
  // ensure evaluated stats are correct
  struct FstBuffer::FstStats stats{};
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

  // write FST
  const bool ok = immutable_byte_fst::Write(fst, *_index_out, fst_stats);

  if (!ok) [[unlikely]] {
    throw irs::IndexError{
      absl::StrCat("Failed to write term index for field: ", name)};
  }

  ++_fields_count;
}

void FieldWriter::end() {
  _output_buffer.Reset();

  SDB_ASSERT(_terms_out);
  SDB_ASSERT(_index_out);

  // finish postings
  _pw->end();

  format_utils::WriteFooter(*_terms_out);
  _terms_out.reset();  // ensure stream is closed

  if (_index_out_cipher) {
    auto& out = static_cast<EncryptedOutput&>(*_index_out);
    out.Flush();
    _index_out = out.Release();
  }

  _index_out->WriteU64(_fields_count);
  format_utils::WriteFooter(*_index_out);
  _index_out.reset();  // ensure stream is closed
}

class TermReaderBase : public irs::TermReader, private util::Noncopyable {
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
  bool has_scorer(uint8_t index) const noexcept final;

  virtual void Prepare(burst_trie::Version version, IndexInput& in);

  uint8_t WandCount() const noexcept { return _wand_count; }

 protected:
  uint8_t WandIndex(uint8_t i) const noexcept;

 private:
  FieldMeta _field;
  bstring _min_term;
  bstring _max_term;
  uint64_t _terms_count;
  uint64_t _doc_count;
  uint64_t _doc_freq;
  uint64_t _wand_mask{};
  uint32_t _wand_count{};
  FreqAttr _freq;  // total term freq
};

uint8_t TermReaderBase::WandIndex(uint8_t i) const noexcept {
  if (i >= kMaxScorers) {
    return WandContext::kDisable;
  }

  const uint64_t mask{uint64_t{1} << i};

  if (0 == (_wand_mask & mask)) {
    return WandContext::kDisable;
  }

  return static_cast<uint8_t>(std::popcount(_wand_mask & (mask - 1)));
}

bool TermReaderBase::has_scorer(uint8_t index) const noexcept {
  return WandIndex(index) != WandContext::kDisable;
}

void TermReaderBase::Prepare(burst_trie::Version version, IndexInput& in) {
  // read field metadata
  _field.name = ReadString<std::string>(in);

  ReadFieldFeatures(in, _field);

  _terms_count = in.ReadV64();
  _doc_count = in.ReadV64();
  _doc_freq = in.ReadV64();
  _min_term = ReadString<bstring>(in);
  _max_term = ReadString<bstring>(in);

  if (IndexFeatures::None != (_field.index_features & IndexFeatures::Freq)) {
    // TODO(mbkkt) for what reason we store uint64_t if we read to uint32_t
    _freq.value = static_cast<uint32_t>(in.ReadV64());
  }

  _wand_mask = in.ReadI64();
  _wand_count = static_cast<uint8_t>(std::popcount(_wand_mask));

  SDB_ASSERT(!field_limits::valid(_field.norm) ||
             IsSubsetOf(IndexFeatures::Norm, _field.index_features));
}

Attribute* TermReaderBase::GetMutable(TypeInfo::type_id type) noexcept {
  if (IndexFeatures::None != (_field.index_features & IndexFeatures::Freq) &&
      irs::Type<irs::FreqAttr>::id() == type) {
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

  void Load(IndexInput& in, Encryption::Stream* cipher);

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

  const TermMetaImpl& State() const noexcept { return _state; }
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
  void LoadData(const FieldMeta& meta, TermMetaImpl& state,
                irs::PostingsReader& pr);

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
  TermMetaImpl _state;
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

void BlockIterator::Load(IndexInput& in, irs::Encryption::Stream* cipher) {
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

  // for non-encrypted index try direct buffer access first
  _suffix.begin =
    cipher ? nullptr : in.ReadBuffer(block_size, BufferHint::PERSISTENT);
  _suffix.block.clear();

  if (!_suffix.begin) {
    _suffix.block.resize(block_size);
    [[maybe_unused]] const auto read =
      in.ReadBytes(_suffix.block.data(), block_size);
    SDB_ASSERT(read == block_size);
    _suffix.begin = _suffix.block.c_str();

    if (cipher) {
      cipher->Decrypt(_cur_start, _suffix.block.data(), block_size);
    }
  }
#ifdef SDB_DEV
  _suffix.end = _suffix.begin + block_size;
#endif
  _suffix.AssertBlockBoundaries();

  // read stats block
  block_size = in.ReadV64();

  // try direct buffer access first
  _stats.begin = in.ReadBuffer(block_size, BufferHint::PERSISTENT);
  _stats.block.clear();

  if (!_stats.begin) {
    _stats.block.resize(block_size);
    [[maybe_unused]] const auto read =
      in.ReadBytes(_stats.block.data(), block_size);
    SDB_ASSERT(read == block_size);
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

void BlockIterator::LoadData(const FieldMeta& meta, TermMetaImpl& state,
                             irs::PostingsReader& pr) {
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

// Base class for TermIterator and automaton_term_iterator
class TermIteratorBase : public SeekTermIterator {
 public:
  TermIteratorBase(const TermReaderBase& field, PostingsReader& postings,
                   irs::Encryption::Stream* terms_cipher,
                   PayAttr* pay = nullptr)
    : _field{&field}, _postings{&postings}, _terms_cipher{terms_cipher} {
    std::get<AttributePtr<PayAttr>>(_attrs) = pay;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  SeekCookie::ptr cookie() const final {
    return std::make_unique<::Cookie>(std::get<TermMetaImpl>(_attrs));
  }

  bytes_view value() const noexcept final {
    return std::get<TermAttr>(_attrs).value;
  }

  irs::Encryption::Stream* TermsCipher() const noexcept {
    return _terms_cipher;
  }

 protected:
  using Attributes = std::tuple<TermMetaImpl, TermAttr, AttributePtr<PayAttr>>;

  void ReadImpl(BlockIterator& it) {
    it.LoadData(_field->meta(), std::get<TermMetaImpl>(_attrs), *_postings);
  }

  DocIterator::ptr PostingsImpl(BlockIterator* it,
                                IndexFeatures features) const {
    auto& meta = std::get<TermMetaImpl>(_attrs);
    const auto& field_meta = _field->meta();
    if (it) {
      it->LoadData(field_meta, meta, *_postings);
    }
    return _postings->iterator(field_meta.index_features, features, meta,
                               _field->WandCount());
  }

  void Copy(const byte_type* suffix, size_t prefix_size, size_t suffix_size) {
    sdb::basics::StrResizeAmortized(_term_buf, prefix_size + suffix_size);
    std::memcpy(_term_buf.data() + prefix_size, suffix, suffix_size);
  }

  void RefreshValue() noexcept { std::get<TermAttr>(_attrs).value = _term_buf; }
  void ResetValue() noexcept { std::get<TermAttr>(_attrs).value = {}; }

  mutable Attributes _attrs;
  const TermReaderBase* _field;
  PostingsReader* _postings;
  irs::Encryption::Stream* _terms_cipher;
  bstring _term_buf;
  byte_weight _weight;  // aggregated fst output
};

// use explicit matcher to avoid implicit loops
template<typename FST>
using ExplicitMatcher = fst::explicit_matcher<fst::SortedMatcher<FST>>;

template<typename FST>
class TermIterator : public TermIteratorBase {
 public:
  using WeightT = typename FST::Weight;
  using StateidT = typename FST::StateId;

  TermIterator(const TermReaderBase& field, PostingsReader& postings,
               const IndexInput& terms_in,
               irs::Encryption::Stream* terms_cipher, const FST& fst)
    : TermIteratorBase{field, postings, terms_cipher, nullptr},
      _terms_in_source{&terms_in},
      _fst{&fst},
      _matcher{&fst, fst::MATCH_INPUT} {  // pass pointer to avoid copying FST
  }

  bool next() final;
  SeekResult seek_ge(bytes_view term) final;
  bool seek(bytes_view term) final {
    return SeekResult::Found == SeekEqual(term, true);
  }

  void read() final {
    SDB_ASSERT(_cur_block);
    ReadImpl(*_cur_block);
  }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return PostingsImpl(_cur_block, features);
  }

 private:
  friend class BlockIterator;

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

  // as TermIterator is usually used by prepared queries
  // to access term metadata by stored cookie, we initialize
  // term dictionary stream in lazy fashion
  IndexInput& TermsInput() const {
    if (!_terms_in) {
      _terms_in = _terms_in_source->Reopen();  // reopen thread-safe stream

      if (!_terms_in) {
        // implementation returned wrong pointer
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen terms input");

        throw IoError("failed to reopen terms input");
      }
    }

    return *_terms_in;
  }

  const IndexInput* _terms_in_source;
  mutable IndexInput::ptr _terms_in;
  const FST* _fst;
  ExplicitMatcher<FST> _matcher;
  std::vector<Arc> _sstate;
  std::vector<BlockIterator> _block_stack;
  BlockIterator* _cur_block{};
};

template<typename FST>
bool TermIterator<FST>::next() {
  // iterator at the beginning or seek to cached state was called
  if (!_cur_block) {
    if (value().empty()) {
      // iterator at the beginning
      _cur_block = PushBlock(_fst->Final(_fst->Start()), 0);
      _cur_block->Load(TermsInput(), TermsCipher());
    } else {
      SDB_ASSERT(false);
      // FIXME(gnusi): consider removing this, as that seems to be impossible
      // anymore

      // seek to the term with the specified state was called from
      // TermIterator::seek(bytes_view, const attribute&),
      // need create temporary "bytes_view" here, since "seek" calls
      // term_.reset() internally,
      // note, that since we do not create extra copy of term_
      // make sure that it does not reallocate memory !!!
      [[maybe_unused]] const auto res = SeekEqual(value(), true);
      SDB_ASSERT(SeekResult::Found == res);
    }
  }

  // pop finished blocks
  while (_cur_block->Done()) {
    if (_cur_block->NextSubBlock<false>()) {
      _cur_block->Load(TermsInput(), TermsCipher());
    } else if (&_block_stack.front() == _cur_block) {  // root
      ResetValue();
      _cur_block->Reset();
      _sstate.clear();
      return false;
    } else {
      const uint64_t start = _cur_block->Start();
      _cur_block = PopBlock();
      std::get<TermMetaImpl>(_attrs) = _cur_block->State();
      if (_cur_block->Dirty() || _cur_block->BlockStart() != start) {
        // here we're currently at non block that was not loaded yet
        SDB_ASSERT(_cur_block->Prefix() < _term_buf.size());
        // to sub-block
        _cur_block->ScanToSubBlock(_term_buf[_cur_block->Prefix()]);
        _cur_block->Load(TermsInput(), TermsCipher());
        _cur_block->ScanToBlock(start);
      }
    }
  }

  _sstate.resize(std::min(_sstate.size(), _cur_block->Prefix()));

  auto copy_suffix = [this](const byte_type* suffix, size_t suffix_size) {
    Copy(suffix, _cur_block->Prefix(), suffix_size);
  };

  // push new block or next term
  for (_cur_block->Next(copy_suffix); EntryType::Block == _cur_block->Type();
       _cur_block->Next(copy_suffix)) {
    _cur_block = PushBlock(_cur_block->BlockStart(), _term_buf.size());
    _cur_block->Load(TermsInput(), TermsCipher());
  }

  RefreshValue();
  return true;
}

template<typename FST>
ptrdiff_t TermIterator<FST>::SeekCached(size_t& prefix, StateidT& state,
                                        size_t& block, byte_weight& weight,
                                        bytes_view target) {
  SDB_ASSERT(!_block_stack.empty());
  const auto term = value();
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
  ptrdiff_t cmp = vpack::char_traits<byte_type>::compare(
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
bool TermIterator<FST>::SeekToBlock(bytes_view term, size_t& prefix) {
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
SeekResult TermIterator<FST>::SeekEqual(bytes_view term, bool exact) {
  [[maybe_unused]] size_t prefix;
  if (SeekToBlock(term, prefix)) {
    SDB_ASSERT(EntryType::Term == _cur_block->Type());
    return SeekResult::Found;
  }

  SDB_ASSERT(_cur_block);

  if (exact && _cur_block->NoTerms()) {
    // current block has no terms
    std::get<TermAttr>(_attrs).value = {_term_buf.c_str(), prefix};
    return SeekResult::NotFound;
  }

  auto append_suffix = [this](const byte_type* suffix, size_t suffix_size) {
    const auto prefix = _cur_block->Prefix();
    sdb::basics::StrResizeAmortized(_term_buf, prefix + suffix_size);
    std::memcpy(_term_buf.data() + prefix, suffix, suffix_size);
  };

  _cur_block->Load(TermsInput(), TermsCipher());

  Finally refresh_value = [this]() noexcept { this->RefreshValue(); };

  SDB_ASSERT(term.starts_with(_term_buf));
  return _cur_block->ScanToTerm(term, append_suffix);
}

template<typename FST>
SeekResult TermIterator<FST>::seek_ge(bytes_view term) {
  switch (SeekEqual(term, false)) {
    case SeekResult::Found:
      SDB_ASSERT(EntryType::Term == _cur_block->Type());
      return SeekResult::Found;
    case SeekResult::NotFound:
      switch (_cur_block->Type()) {
        case EntryType::Term:
          // we're already at greater term
          return SeekResult::NotFound;
        case EntryType::Block:
          // we're at the greater block, load it and call next
          _cur_block = PushBlock(_cur_block->BlockStart(), _term_buf.size());
          _cur_block->Load(TermsInput(), TermsCipher());
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

// An iterator optimized for performing exact single seeks
//
// WARNING: we intentionally do not copy term value to avoid
//          unnecessary allocations as this is mostly useless
//          in case of exact single seek
template<typename FST>
class SingleTermIterator : public SeekTermIterator {
 public:
  explicit SingleTermIterator(const TermReaderBase& field,
                              PostingsReader& postings,
                              IndexInput::ptr&& terms_in,
                              irs::Encryption::Stream* terms_cipher,
                              const FST& fst) noexcept
    : _terms_in{std::move(terms_in)},
      _cipher{terms_cipher},
      _postings{&postings},
      _field{&field},
      _fst{&fst} {
    SDB_ASSERT(_terms_in);
  }

  Attribute* GetMutable(TypeInfo::type_id type) final {
    if (type == irs::Type<TermMeta>::id()) {
      return &_meta;
    }

    return type == irs::Type<TermAttr>::id() ? &_value : nullptr;
  }

  bytes_view value() const final { return _value.value; }

  bool next() final { throw NotSupported(); }

  SeekResult seek_ge(bytes_view) final { throw NotSupported(); }

  bool seek(bytes_view term) final;

  SeekCookie::ptr cookie() const final {
    return std::make_unique<::Cookie>(_meta);
  }

  void read() final { /*NOOP*/ }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return _postings->iterator(_field->meta().index_features, features, _meta,
                               _field->WandCount());
  }

  const TermMetaImpl& Meta() const noexcept { return _meta; }

 private:
  friend class BlockIterator;

  TermMetaImpl _meta;
  TermAttr _value;
  IndexInput::ptr _terms_in;
  irs::Encryption::Stream* _cipher;
  PostingsReader* _postings;
  const TermReaderBase* _field;
  const FST* _fst;
};

template<typename FST>
bool SingleTermIterator<FST>::seek(bytes_view term) {
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

  cur_block.Load(*_terms_in, _cipher);

  if (SeekResult::Found == cur_block.ScanToTerm(term, [](auto, auto) {})) {
    cur_block.LoadData(_field->meta(), _meta, *_postings);
    _value.value = term;
    return true;
  }

  _value = {};
  return false;
}

class AutomatonArcMatcher {
 public:
  AutomatonArcMatcher(const automaton::Arc* arcs, size_t narcs) noexcept
    : _begin(arcs), _end(arcs + narcs) {}

  const automaton::Arc* Seek(uint32_t label) noexcept {
    SDB_ASSERT(_begin != _end && _begin->max < label);
    // linear search is faster for a small number of arcs

    while (++_begin != _end && _begin->max < label) {
    }

    SDB_ASSERT(_begin == _end || label <= _begin->max);

    return _begin != _end && _begin->min <= label ? _begin : nullptr;
  }

  const automaton::Arc* Value() const noexcept { return _begin; }

  bool Done() const noexcept { return _begin == _end; }

 private:
  const automaton::Arc* _begin;  // current arc
  const automaton::Arc* _end;    // end of arcs range
};

template<typename FST>
class FstArcMatcher {
 public:
  FstArcMatcher(const FST& fst, typename FST::StateId state) noexcept {
    fst::ArcIteratorData<typename FST::Arc> data;
    fst.InitArcIterator(state, &data);
    _begin = data.arcs;
    _end = _begin + data.narcs;
  }

  void Seek(typename FST::Arc::Label label) noexcept {
    // linear search is faster for a small number of arcs
    for (; _begin != _end; ++_begin) {
      if (label <= _begin->ilabel) {
        break;
      }
    }
  }

  const typename FST::Arc* Value() const noexcept { return _begin; }

  bool Done() const noexcept { return _begin == _end; }

 private:
  const typename FST::Arc* _begin;  // current arc
  const typename FST::Arc* _end;    // end of arcs range
};

template<typename FST>
class AutomatonTermIterator : public TermIteratorBase {
 public:
  AutomatonTermIterator(const TermReaderBase& field, PostingsReader& postings,
                        IndexInput::ptr&& terms_in,
                        irs::Encryption::Stream* terms_cipher, const FST& fst,
                        automaton_table_matcher& matcher)
    : TermIteratorBase{field, postings, terms_cipher, &_payload},
      _terms_in{std::move(terms_in)},
      _fst{&fst},
      _acceptor{&matcher.GetFst()},
      _matcher{&matcher},
      _fst_matcher{&fst, fst::MATCH_INPUT},
      _sink{matcher.sink()} {
    SDB_ASSERT(_terms_in);
    SDB_ASSERT(fst::kNoStateId != _acceptor->Start());
    SDB_ASSERT(_acceptor->NumArcs(_acceptor->Start()));

    // init payload value
    _payload.value = {&_payload_value, sizeof(_payload_value)};
  }

  bool next() final;

  SeekResult seek_ge(bytes_view term) final {
    if (!irs::seek(*this, term)) {
      return SeekResult::End;
    }

    return value() == term ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view term) final {
    return SeekResult::Found == seek_ge(term);
  }

  void read() final {
    SDB_ASSERT(_cur_block);
    ReadImpl(*_cur_block);
  }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return PostingsImpl(_cur_block, features);
  }

 private:
  class BlockIterator : public ::BlockIterator {
   public:
    BlockIterator(bytes_view out, const FST& fst, size_t prefix,
                  size_t weight_prefix, automaton::StateId state,
                  typename FST::StateId fst_state, const automaton::Arc* arcs,
                  size_t narcs) noexcept
      : ::BlockIterator(out, prefix),
        _arcs(arcs, narcs),
        _fst_arcs(fst, fst_state),
        _weight_prefix(weight_prefix),
        _state(state),
        _fst_state(fst_state) {}

   public:
    FstArcMatcher<FST>& FstArcs() noexcept { return _fst_arcs; }
    AutomatonArcMatcher& Arcs() noexcept { return _arcs; }
    automaton::StateId AcceptorState() const noexcept { return _state; }
    typename FST::StateId FstState() const noexcept { return _fst_state; }
    size_t WeightPrefix() const noexcept { return _weight_prefix; }

   private:
    AutomatonArcMatcher _arcs;
    FstArcMatcher<FST> _fst_arcs;
    size_t _weight_prefix;
    automaton::StateId _state;  // state to which current block belongs
    typename FST::StateId _fst_state;
  };

  BlockIterator* PopBlock() noexcept {
    _block_stack.pop_back();
    SDB_ASSERT(!_block_stack.empty());
    return &_block_stack.back();
  }

  BlockIterator* PushBlock(bytes_view out, const FST& fst, size_t prefix,
                           size_t weight_prefix, automaton::StateId state,
                           typename FST::StateId fst_state) {
    // ensure final weight correctness
    SDB_ASSERT(out.size() >= kMinWeightSize);

    fst::ArcIteratorData<automaton::Arc> data;
    _acceptor->InitArcIterator(state, &data);
    SDB_ASSERT(data.narcs);  // ensured by term_reader::iterator(...)

    return &_block_stack.emplace_back(out, fst, prefix, weight_prefix, state,
                                      fst_state, data.arcs, data.narcs);
  }

  // automaton_term_iterator usually accesses many term blocks and
  // isn't used by prepared statements for accessing term metadata,
  // therefore we prefer greedy strategy for term dictionary stream
  // initialization
  IndexInput& TermsInput() const noexcept {
    SDB_ASSERT(_terms_in);
    return *_terms_in;
  }

  IndexInput::ptr _terms_in;
  const FST* _fst;
  const automaton* _acceptor;
  automaton_table_matcher* _matcher;
  ExplicitMatcher<FST> _fst_matcher;
  std::vector<BlockIterator> _block_stack;
  BlockIterator* _cur_block{};
  automaton::Weight::PayloadType _payload_value;
  PayAttr _payload;  // payload of the matched automaton state
  automaton::StateId _sink;
};

template<typename FST>
bool AutomatonTermIterator<FST>::next() {
  SDB_ASSERT(_fst_matcher.GetFst().GetImpl());
  auto& fst = *_fst_matcher.GetFst().GetImpl();

  // iterator at the beginning or seek to cached state was called
  if (!_cur_block) {
    if (value().empty()) {
      const auto fst_start = fst.Start();
      _cur_block = PushBlock(fst.Final(fst_start), *_fst, 0, 0,
                             _acceptor->Start(), fst_start);
      _cur_block->Load(TermsInput(), TermsCipher());
    } else {
      SDB_ASSERT(false);
      // FIXME(gnusi): consider removing this, as that seems to be impossible
      // anymore

      // seek to the term with the specified state was called from
      // TermIterator::seek(bytes_view, const attribute&),
      [[maybe_unused]] const SeekResult res = seek_ge(value());
      SDB_ASSERT(SeekResult::Found == res);
    }
  }

  automaton::StateId state;

  auto read_suffix = [this, &state, &fst](const byte_type* suffix,
                                          size_t suffix_size) -> SeekResult {
    if (suffix_size) {
      auto& arcs = _cur_block->Arcs();
      SDB_ASSERT(!arcs.Done());

      const auto* arc = arcs.Value();

      const uint32_t lead_label = *suffix;

      if (lead_label < arc->min) {
        return SeekResult::NotFound;
      }

      if (lead_label > arc->max) {
        arc = arcs.Seek(lead_label);

        if (!arc) {
          if (arcs.Done()) {
            return SeekResult::End;  // pop current block
          }

          return SeekResult::NotFound;
        }
      }

      SDB_ASSERT(*suffix >= arc->min && *suffix <= arc->max);
      state = arc->nextstate;

      if (state == _sink) {
        return SeekResult::NotFound;
      }

#ifdef SDB_DEV
      SDB_ASSERT(_matcher->Peek(_cur_block->AcceptorState(), *suffix) == state);
#endif

      const auto* end = suffix + suffix_size;
      const auto* begin = suffix + 1;  // already match first suffix label

      for (; begin < end; ++begin) {
        state = _matcher->Peek(state, *begin);

        if (fst::kNoStateId == state) {
          // suffix doesn't match
          return SeekResult::NotFound;
        }
      }
    } else {
      state = _cur_block->AcceptorState();
    }

    if (EntryType::Term == _cur_block->Type()) {
      const auto weight = _acceptor->Final(state);
      if (weight) {
        _payload_value = weight.Payload();
        Copy(suffix, _cur_block->Prefix(), suffix_size);

        return SeekResult::Found;
      }
    } else {
      SDB_ASSERT(EntryType::Block == _cur_block->Type());
      fst::ArcIteratorData<automaton::Arc> data;
      _acceptor->InitArcIterator(state, &data);

      if (data.narcs) {
        Copy(suffix, _cur_block->Prefix(), suffix_size);

        _weight.Resize(_cur_block->WeightPrefix());
        auto fst_state = _cur_block->FstState();

        if (const auto* end = suffix + suffix_size; suffix < end) {
          auto& fst_arcs = _cur_block->FstArcs();
          fst_arcs.Seek(*suffix++);
          SDB_ASSERT(!fst_arcs.Done());
          const auto* arc = fst_arcs.Value();
          _weight.PushBack(arc->weight.begin(), arc->weight.end());

          fst_state = fst_arcs.Value()->nextstate;
          for (_fst_matcher.SetState(fst_state); suffix < end; ++suffix) {
            [[maybe_unused]] const bool found = _fst_matcher.Find(*suffix);
            SDB_ASSERT(found);

            const auto& arc = _fst_matcher.Value();
            fst_state = arc.nextstate;
            _fst_matcher.SetState(fst_state);
            _weight.PushBack(arc.weight.begin(), arc.weight.end());
          }
        }

        const auto& weight = fst.FinalRef(fst_state);
        SDB_ASSERT(!weight.Empty() ||
                   FstBuffer::FstByteBuilder::kFinal == fst_state);
        const auto weight_prefix = _weight.Size();
        _weight.PushBack(weight.begin(), weight.end());
        _block_stack.emplace_back(static_cast<bytes_view>(_weight), *_fst,
                                  _term_buf.size(), weight_prefix, state,
                                  fst_state, data.arcs, data.narcs);
        _cur_block = &_block_stack.back();

        SDB_ASSERT(_block_stack.size() < 2 ||
                   (++_block_stack.rbegin())->BlockStart() ==
                     _cur_block->Start());

        if (!_acceptor->Final(state)) {
          _cur_block->ScanToSubBlock(data.arcs->min);
        }

        _cur_block->Load(TermsInput(), TermsCipher());
      }
    }

    return SeekResult::NotFound;
  };

  for (;;) {
    // pop finished blocks
    while (_cur_block->Done()) {
      if (_cur_block->SubCount()) {
        // we always instantiate block with header
        SDB_ASSERT(Block::kInvalidLabel != _cur_block->NextLabel());

        const uint32_t next_label = _cur_block->NextLabel();

        auto& arcs = _cur_block->Arcs();
        SDB_ASSERT(!arcs.Done());
        const auto* arc = arcs.Value();

        if (next_label < arc->min) {
          SDB_ASSERT(arc->min <= std::numeric_limits<uint8_t>::max());
          _cur_block->ScanToSubBlock(byte_type(arc->min));
          SDB_ASSERT(_cur_block->NextLabel() == Block::kInvalidLabel ||
                     arc->min < uint32_t(_cur_block->NextLabel()));
        } else if (arc->max < next_label) {
          arc = arcs.Seek(next_label);

          if (arcs.Done()) {
            if (&_block_stack.front() == _cur_block) {
              // need to pop root block, we're done
              ResetValue();
              _cur_block->Reset();
              return false;
            }

            _cur_block = PopBlock();
            continue;
          }

          if (!arc) {
            SDB_ASSERT(arcs.Value()->min <=
                       std::numeric_limits<uint8_t>::max());
            _cur_block->ScanToSubBlock(byte_type(arcs.Value()->min));
            SDB_ASSERT(_cur_block->NextLabel() == Block::kInvalidLabel ||
                       arcs.Value()->min < uint32_t(_cur_block->NextLabel()));
          } else {
            SDB_ASSERT(arc->min <= next_label && next_label <= arc->max);
            _cur_block->template NextSubBlock<true>();
          }
        } else {
          SDB_ASSERT(arc->min <= next_label && next_label <= arc->max);
          _cur_block->template NextSubBlock<true>();
        }

        _cur_block->Load(TermsInput(), TermsCipher());
      } else if (&_block_stack.front() == _cur_block) {  // root
        ResetValue();
        _cur_block->Reset();
        return false;
      } else {
        const uint64_t start = _cur_block->Start();
        _cur_block = PopBlock();
        std::get<TermMetaImpl>(_attrs) = _cur_block->State();
        if (_cur_block->Dirty() || _cur_block->BlockStart() != start) {
          // here we're currently at non block that was not loaded yet
          SDB_ASSERT(_cur_block->Prefix() < _term_buf.size());
          // to sub-block
          _cur_block->ScanToSubBlock(_term_buf[_cur_block->Prefix()]);
          _cur_block->Load(TermsInput(), TermsCipher());
          _cur_block->ScanToBlock(start);
        }
      }
    }

    const auto res = _cur_block->Scan(read_suffix);

    if (SeekResult::Found == res) {
      RefreshValue();
      return true;
    } else if (SeekResult::End == res) {
      if (&_block_stack.front() == _cur_block) {
        // need to pop root block, we're done
        ResetValue();
        _cur_block->Reset();
        return false;
      }

      // continue with popped block
      _cur_block = PopBlock();
    }
  }
}

class FieldReader final : public irs::FieldReader {
 public:
  explicit FieldReader(irs::PostingsReader::ptr&& pr, IResourceManager& rm);

  uint64_t CountMappedMemory() const final {
    uint64_t bytes = 0;
    if (_pr != nullptr) {
      bytes += _pr->CountMappedMemory();
    }
    if (_terms_in != nullptr) {
      bytes += _terms_in->CountMappedMemory();
    }
    return bytes;
  }

  void prepare(const ReaderState& state) final;

  const irs::TermReader* field(std::string_view field) const final;
  irs::FieldIterator::ptr iterator() const final;
  size_t size() const noexcept final { return _name_to_field.size(); }

 private:
  template<typename FST>
  class TermReader final : public TermReaderBase {
   public:
    explicit TermReader(FieldReader& owner) noexcept : _owner(&owner) {}
    TermReader(TermReader&& rhs) = default;
    TermReader& operator=(TermReader&& rhs) = delete;

    void Prepare(burst_trie::Version version, IndexInput& in) final {
      TermReaderBase::Prepare(version, in);

      // TODO(mbkkt) make reads more effective than istream
      // read FST
      InputBuf isb(&in);
      std::istream input(&isb);  // wrap stream to be OpenFST compliant
      _fst.reset(
        FST::Read(input, FstReadOptions(), {_owner->_resource_manager}));
      if (!_fst) {
        throw IndexError{absl::StrCat("Failed to read term index for field '",
                                      meta().name, "'")};
      }
    }

    SeekTermIterator::ptr iterator(SeekMode mode) const final {
      if (mode == SeekMode::RandomOnly) {
        auto terms_in =
          _owner->_terms_in->Reopen();  // reopen thread-safe stream

        if (!terms_in) {
          // implementation returned wrong pointer
          SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                    "Failed to reopen terms input");

          throw IoError("failed to reopen terms input");
        }

        return memory::make_managed<SingleTermIterator<FST>>(
          *this, *_owner->_pr, std::move(terms_in),
          _owner->_terms_in_cipher.get(), *_fst);
      }

      return memory::make_managed<TermIterator<FST>>(
        *this, *_owner->_pr, *_owner->_terms_in, _owner->_terms_in_cipher.get(),
        *_fst);
    }

    TermMeta term(bytes_view term) const final {
      SingleTermIterator<FST> it{*this, *_owner->_pr,
                                 _owner->_terms_in->Reopen(),
                                 _owner->_terms_in_cipher.get(), *_fst};

      it.seek(term);
      return it.Meta();
    }

    size_t read_documents(bytes_view term,
                          std::span<doc_id_t> docs) const final {
      // Order is important here!
      if (max() < term || term < min() || docs.empty()) {
        return 0;
      }

      SingleTermIterator<FST> it{*this, *_owner->_pr,
                                 _owner->_terms_in->Reopen(),
                                 _owner->_terms_in_cipher.get(), *_fst};

      if (!it.seek(term)) {
        return 0;
      }

      if (const auto& meta = it.Meta(); meta.docs_count == 1) {
        docs.front() = doc_limits::min() + meta.e_single_doc;
        return 1;
      }

      auto docs_it = it.postings(IndexFeatures::None);

      if (!docs_it) [[unlikely]] {
        SDB_ASSERT(false);
        return 0;
      }

      const auto* doc = irs::get<DocAttr>(*docs_it);

      if (!doc) [[unlikely]] {
        SDB_ASSERT(false);
        return 0;
      }

      auto begin = docs.begin();

      for (auto end = docs.end(); begin != end && docs_it->next(); ++begin) {
        *begin = doc->value;
      }

      return std::distance(docs.begin(), begin);
    }

    size_t BitUnion(const cookie_provider& provider, size_t* set) const final {
      auto term_provider = [&provider]() mutable -> const TermMeta* {
        if (auto* cookie = provider()) {
          return &sdb::basics::downCast<::Cookie>(*cookie).meta;
        }

        return nullptr;
      };

      SDB_ASSERT(_owner != nullptr);
      SDB_ASSERT(_owner->_pr != nullptr);
      return _owner->_pr->BitUnion(meta().index_features, term_provider, set,
                                   WandCount());
    }

    SeekTermIterator::ptr iterator(
      automaton_table_matcher& matcher) const final {
      auto& acceptor = matcher.GetFst();

      const auto start = acceptor.Start();

      if (fst::kNoStateId == start) {
        return SeekTermIterator::empty();
      }

      if (!acceptor.NumArcs(start)) {
        if (acceptor.Final(start)) {
          // match all
          return this->iterator(SeekMode::NORMAL);
        }

        return SeekTermIterator::empty();
      }

      auto terms_in = _owner->_terms_in->Reopen();

      if (!terms_in) {
        // implementation returned wrong pointer
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  "Failed to reopen terms input");

        throw IoError{"Failed to reopen terms input"};  // FIXME
      }

      return memory::make_managed<AutomatonTermIterator<FST>>(
        *this, *_owner->_pr, std::move(terms_in),
        _owner->_terms_in_cipher.get(), *_fst, matcher);
    }

    DocIterator::ptr postings(const SeekCookie& cookie,
                              IndexFeatures features) const final {
      return _owner->_pr->iterator(meta().index_features, features,
                                   sdb::basics::downCast<::Cookie>(cookie).meta,
                                   WandCount());
    }

    DocIterator::ptr wanderator(const SeekCookie& cookie,
                                IndexFeatures features,
                                const WanderatorOptions& options,
                                WandContext ctx) const final {
      return _owner->_pr->wanderator(
        meta().index_features, features,
        sdb::basics::downCast<::Cookie>(cookie).meta, options, ctx,
        {.mapped_index = WandIndex(ctx.index), .count = WandCount()});
    }

   private:
    FieldReader* _owner;
    std::unique_ptr<FST> _fst;
  };

  using ImmutableFstReader = TermReader<immutable_byte_fst>;
  using ImmutableFstReaders = std::vector<TermReader<immutable_byte_fst>>;

  ImmutableFstReaders _fields;
  absl::flat_hash_map<hashed_string_view, irs::TermReader*> _name_to_field;
  irs::PostingsReader::ptr _pr;
  Encryption::Stream::ptr _terms_in_cipher;
  IndexInput::ptr _terms_in;
  IResourceManager& _resource_manager;
};

FieldReader::FieldReader(PostingsReader::ptr&& pr, IResourceManager& rm)
  : _pr{std::move(pr)}, _resource_manager{rm} {
  SDB_ASSERT(_pr);
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.dir);
  SDB_ASSERT(state.meta);

  // check index header
  IndexInput::ptr index_in;

  int64_t checksum = 0;
  std::string filename;
  const auto term_index_version = burst_trie::Version(PrepareInput(
    filename, index_in, irs::IOAdvice::SEQUENTIAL | irs::IOAdvice::READONCE,
    state, FieldWriter::kTermsIndexExt, FieldWriter::kFormatTermsIndex,
    static_cast<int32_t>(burst_trie::Version::Min),
    static_cast<int32_t>(burst_trie::Version::Max), &checksum));

  constexpr const size_t kFooterLen = sizeof(uint64_t)  // fields count
                                      + format_utils::kFooterLen;

  // read total number of indexed fields
  size_t fields_count{0};
  {
    const uint64_t ptr = index_in->Position();

    index_in->Seek(index_in->Length() - kFooterLen);

    fields_count = index_in->ReadI64();

    // check index checksum
    format_utils::CheckFooter(*index_in, checksum);

    index_in->Seek(ptr);
  }

  auto* enc = state.dir->attributes().encryption();
  Encryption::Stream::ptr index_in_cipher;

  if (irs::Decrypt(filename, *index_in, enc, index_in_cipher)) {
    SDB_ASSERT(index_in_cipher && index_in_cipher->block_size());

    const auto blocks_in_buffer = math::DivCeil64(
      kDefaultEncryptionBufferSize, index_in_cipher->block_size());

    index_in = std::make_unique<EncryptedInput>(
      std::move(index_in), *index_in_cipher, blocks_in_buffer, kFooterLen);
  }

  IndexFeatures features = ReadFeatures(*index_in);

  const auto& meta = *state.meta;

  _fields.reserve(fields_count);
  _name_to_field.reserve(fields_count);

  for (std::string_view previous_field_name{""}; fields_count; --fields_count) {
    auto& field = _fields.emplace_back(*this);
    field.Prepare(term_index_version, *index_in);

    const auto& name = field.meta().name;

    // ensure that fields are sorted properly
    if (previous_field_name > name) {
      throw IndexError{
        absl::StrCat("Invalid field order in segment '", meta.name, "'")};
    }

    const auto res = _name_to_field.emplace(hashed_string_view{name}, &field);

    if (!res.second) {
      throw IndexError{absl::StrCat("Duplicated field: '", meta.name,
                                    "' found in segment: '", name, "'")};
    }

    previous_field_name = name;
  }
  //-----------------------------------------------------------------
  // prepare terms input
  //-----------------------------------------------------------------

  // check term header
  const auto term_dict_version = burst_trie::Version(PrepareInput(
    filename, _terms_in, irs::IOAdvice::RANDOM, state, FieldWriter::kTermsExt,
    FieldWriter::kFormatTerms, static_cast<int32_t>(burst_trie::Version::Min),
    static_cast<int32_t>(burst_trie::Version::Max)));

  if (term_index_version != term_dict_version) {
    throw IndexError(absl::StrCat("Term index version '", term_index_version,
                                  "' mismatches term dictionary version '",
                                  term_dict_version, "' in segment '",
                                  meta.name, "'"));
  }

  if (irs::Decrypt(filename, *_terms_in, enc, _terms_in_cipher)) {
    SDB_ASSERT(_terms_in_cipher && _terms_in_cipher->block_size());
  }

  // prepare postings reader
  _pr->prepare(*_terms_in, state, features);

  // Since terms dictionary are too large
  // it is too expensive to verify checksum of
  // the entire file. Here we perform cheap
  // error detection which could recognize
  // some forms of corruption.
  format_utils::ReadChecksum(*_terms_in);
}

const irs::TermReader* FieldReader::field(std::string_view field) const {
  auto it = _name_to_field.find(hashed_string_view{field});
  return it == _name_to_field.end() ? nullptr : it->second;
}

irs::FieldIterator::ptr FieldReader::iterator() const {
  struct Less {
    bool operator()(const irs::TermReader& lhs,
                    std::string_view rhs) const noexcept {
      return lhs.meta().name < rhs;
    }
  };
  using ReaderType =
    typename std::remove_reference_t<decltype(_fields)>::value_type;

  using IteratorT =
    IteratorAdaptor<std::string_view, ReaderType, decltype(_fields.data()),
                    irs::FieldIterator, Less>;

  return memory::make_managed<IteratorT>(_fields.data(),
                                         _fields.data() + _fields.size());
}

/*
// Implements generalized visitation logic for term dictionary
template<typename FST>
class term_reader_visitor {
 public:
  explicit term_reader_visitor(const FST& field, IndexInput& terms_in,
                               Encryption::Stream* terms_in_cipher)
    : fst_(&field),
      terms_in_(terms_in.Reopen()),
      terms_in_cipher_(terms_in_cipher) {}

  template<typename Visitor>
  void operator()(Visitor& visitor) {
    auto* cur_block = push_block(fst_->Final(fst_->Start()), 0);
    cur_block->load(*terms_in_, terms_in_cipher_);
    visitor.push_block(term_, *cur_block);

    auto copy_suffix = [&cur_block, this](const byte_type* suffix,
                                          size_t suffix_size) {
      copy(suffix, cur_block->prefix(), suffix_size);
    };

    while (true) {
      while (cur_block->done()) {
        if (cur_block->template NextSubBlock<false>()) {
          cur_block->load(*terms_in_, terms_in_cipher_);
          visitor.sub_block(*cur_block);
        } else if (&block_stack_.front() == cur_block) {  // root
          cur_block->reset();
          return;
        } else {
          visitor.pop_block(*cur_block);
          cur_block = pop_block();
        }
      }

      while (!cur_block->done()) {
        cur_block->next(copy_suffix);
        if (EntryType::Term == cur_block->type()) {
          visitor.push_term(term_);
        } else {
          SDB_ASSERT(EntryType::Block == cur_block->type());
          cur_block = push_block(cur_block->block_start(), term_.size());
          cur_block->load(*terms_in_, terms_in_cipher_);
          visitor.push_block(term_, *cur_block);
        }
      }
    }
  }

 private:
  void copy(const byte_type* suffix, size_t prefix_size, size_t suffix_size) {
    sdb::basics::StrResizeAmortized(&term_, prefix_size + suffix_size);
    std::memcpy(term_.data() + prefix_size, suffix, suffix_size);
  }

  BlockIterator* pop_block() noexcept {
    block_stack_.pop_back();
    SDB_ASSERT(!block_stack_.empty());
    return &block_stack_.back();
  }

  BlockIterator* push_block(byte_weight&& out, size_t prefix) {
    // ensure final weight correctess
    SDB_ASSERT(out.Size() >= kMinWeightSize);

    block_stack_.emplace_back(std::move(out), prefix);
    return &block_stack_.back();
  }

  BlockIterator* push_block(uint64_t start, size_t prefix) {
    block_stack_.emplace_back(start, prefix);
    return &block_stack_.back();
  }

  const FST* fst_;
  std::deque<BlockIterator> block_stack_;
  bstring term_;
  IndexInput::ptr terms_in_;
  Encryption::Stream* terms_in_cipher_;
};

// "Dumper" visitor for term_reader_visitor. Suitable for debugging needs.
class dumper : util::Noncopyable {
 public:
  explicit dumper(std::ostream& out) : out_(out) {}

  void push_term(bytes_view term) {
    indent();
    out_ << "TERM|" << suffix(term) << "\n";
  }

  void push_block(bytes_view term, const BlockIterator& block) {
    indent();
    out_ << "BLOCK|" << block.size() << "|" << suffix(term) << "\n";
    indent_ += 2;
    prefix_ = block.Prefix();
  }

  void sub_block(const BlockIterator&) {
    indent();
    out_ << "|\n";
    indent();
    out_ << "V\n";
  }

  void pop_block(const BlockIterator& block) {
    indent_ -= 2;
    prefix_ -= block.Prefix();
  }

 private:
  void indent() {
    for (size_t i = 0; i < indent_; ++i) {
      out_ << " ";
    }
  }

  std::string_view suffix(bytes_view term) {
    return ViewCast<char>(
      bytes_view{term.data() + prefix_, term.size() - prefix_});
  }

  std::ostream& out_;
  size_t indent_ = 0;
  size_t prefix_ = 0;
};
*/

}  // namespace

namespace irs {
namespace burst_trie {

FieldWriter::ptr MakeWriter(Version version, PostingsWriter::ptr&& writer,
                            bool consolidation,
                            IResourceManager& resource_manager) {
  // Here we can parametrize field_writer via version20::TermMeta
  return std::make_unique<::FieldWriter>(std::move(writer), consolidation,
                                         resource_manager, version);
}

FieldReader::ptr MakeReader(PostingsReader::ptr&& reader,
                            IResourceManager& resource_manager) {
  return std::make_shared<::FieldReader>(std::move(reader), resource_manager);
}

}  // namespace burst_trie
}  // namespace irs

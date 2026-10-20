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

#include "iresearch/formats/index/term_dict.hpp"

#include <absl/algorithm/container.h>

#include <cstring>
#include <limits>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr uint8_t kFieldBlocks = 1;
constexpr byte_type kZeros[kPage]{};

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

uint64_t VarBytes(uint64_t count, uint64_t restarts, uint64_t headers,
                  uint64_t suffixes, uint64_t records) noexcept {
  return 1 + VSize(count) + VSize(restarts) + VSize(headers) + VSize(suffixes) +
         VSize(records) + kVarRestart * restarts + headers + suffixes +
         records + kSlack;
}

uint64_t FixedBytes(uint64_t count, uint64_t restarts, uint64_t records,
                    uint64_t width, uint64_t prefix) noexcept {
  const uint64_t stride = width - prefix;
  return 1 + VSize(count) + VSize(restarts) + VSize(records) + VSize(stride) +
         VSize(prefix) + prefix + kFixedRestart * restarts + count * stride +
         records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

void Clear(BlockBuilder& block) noexcept {
  block.keys.clear();
  block.ends.clear();
  block.headers.clear();
  block.suffixes.clear();
  block.records.clear();
  block.restarts.clear();
  block.size = 0;
  block.width = 0;
  block.prefix = 0;
  block.uniform = true;
  block.kind = BlockKind::Var;
}

void Serialize(const BlockBuilder& block, bstring& page) {
  page.clear();
  BytesOutput out{page};
  const auto count = static_cast<uint32_t>(block.ends.size());
  out.WriteByte(static_cast<byte_type>(block.kind));
  out.WriteV32(count);
  out.WriteV32(static_cast<uint32_t>(block.restarts.size()));
  if (block.kind == BlockKind::Var) {
    out.WriteV64(block.headers.size());
    out.WriteV64(block.suffixes.size());
    out.WriteV64(block.records.size());
    for (const auto& restart : block.restarts) {
      for (const auto offset : restart) {
        out.WriteU16(offset);
      }
    }
    out.WriteData(block.headers.data(), block.headers.size());
    out.WriteData(block.suffixes.data(), block.suffixes.size());
  } else {
    const uint32_t stride = block.width - block.prefix;
    out.WriteV64(block.records.size());
    out.WriteV32(stride);
    out.WriteV32(block.prefix);
    out.WriteData(block.keys.data(), block.prefix);
    for (const auto& restart : block.restarts) {
      out.WriteU16(restart[2]);
    }
    for (size_t i = 0; i != count; ++i) {
      out.WriteData(KeyAt(block, i).data() + block.prefix, stride);
    }
  }
  out.WriteData(block.records.data(), block.records.size());
  out.WriteData(kZeros, kSlack);
  SDB_ASSERT(page.size() == block.size);
}

uint32_t VarFind(const BlockView& view, bytes_view key,
                 uint32_t interval) noexcept {
  const uint32_t restarts = RestartsNotAfter(view, key);
  if (restarts == 0) {
    return kNoEntry;
  }
  const uint32_t group = restarts - 1;
  uint32_t entry = group * interval;
  const uint32_t end = std::min(entry + interval, view.count);
  const auto* hp = view.headers + HeaderOffset(view, group);
  const auto* sp = view.suffixes + SuffixOffset(view, group);
  size_t matched = 0;
  for (; entry != end; ++entry) {
    const auto header = ReadHeader(hp);
    const auto* suffix = sp;
    sp += header.len;
    if (header.lcp > matched) {
      continue;
    }
    if (header.lcp < matched) {
      return kNoEntry;
    }
    const bytes_view rest = key.substr(matched);
    const size_t common =
      CommonPrefixLength(bytes_view{suffix, header.len}, rest);
    if (common == header.len && common == rest.size()) {
      return entry;
    }
    if (common == rest.size() ||
        (common != header.len && suffix[common] > rest[common])) {
      return kNoEntry;
    }
    matched += common;
  }
  return kNoEntry;
}

uint32_t Find(const BlockView& view, bytes_view key,
              uint32_t interval) noexcept {
  if (view.kind == BlockKind::Var) {
    return VarFind(view, key, interval);
  }
  if (key.size() != view.prefix + view.stride ||
      !key.starts_with(Prefix(view))) {
    return kNoEntry;
  }
  const auto entry = FixedLowerBound(view, key);
  if (entry == view.count || FixedKey(view, entry) != key.substr(view.prefix)) {
    return kNoEntry;
  }
  return entry;
}

const byte_type* Map(IndexInput& in, uint64_t size, bstring& copy) {
  const uint64_t offset = in.Position();
  const auto* p = in.ReadStable(offset, size);
  if (!p) {
    copy.resize(size);
    in.ReadData(offset, copy.data(), size);
    p = copy.data();
  }
  in.Seek(offset + size);
  return p;
}

PostingMeta DecodeRecord(const BlockView& view, uint32_t entry,
                         uint32_t interval, IndexFeatures features) noexcept {
  const uint32_t group = entry / interval;
  auto cursor = GroupStart(view, group);
  PostingMeta meta;
  DecodeRecords(cursor, group * interval, entry, features, meta);
  return meta;
}

class TermIteratorImpl : public SeekTermIterator {
 public:
  explicit TermIteratorImpl(const TermReaderImpl& field) noexcept
    : _cursor{field} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before:
        if (_cursor.Field().size() == 0) {
          return End();
        }
        _cursor.Load(0);
        _cursor.Next();
        break;
      case Position::At:
        if (!_cursor.Advance()) {
          return End();
        }
        break;
    }
    _position = Position::At;
    _term.value = _cursor.Key();
    return true;
  }

  SeekResult seek_ge(bytes_view term) final {
    const auto result = _cursor.SeekGe(term);
    if (result == SeekResult::End) {
      End();
      return result;
    }
    _position = Position::At;
    _term.value = _cursor.Key();
    return result;
  }

  bool seek(bytes_view term) final {
    return seek_ge(term) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _cursor.Meta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    const auto& field = _cursor.Field();
    return field.Owner().Postings().Postings(
      field.meta().index_features, features, _cursor.Meta(),
      field.HasScoreBounds(), std::move(reuse));
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  mutable Cursor _cursor;
  TermAttr _term;
  Position _position = Position::Before;
};

}  // namespace

void RecordOutput::WriteDirect(const byte_type*, size_t) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("term dictionary: posting record exceeds ",
                          RecordBytes{}.bytes.size(), " bytes"));
}

FieldWriter::FieldWriter(bool compaction, IResourceManager&)
  : _compaction{compaction} {}

void FieldWriter::prepare(const FlushState& state) {
  SDB_ASSERT(_idx);
  _out = &_idx->BlocksOut();
  _pw.Prepare(state);
}

void FieldWriter::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  _pw.BeginField(props);
  _pw.SetTermPayloadWriter(reader.PayloadWriter());

  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;
  const bool freq_exists =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);

  const auto consume = [&](auto&& term_of, const PostingMeta& meta) {
    if (freq_exists) {
      sum_tfreq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    sum_dfreq += meta.docs_count;
    Add(term_of(), meta);
  };

  auto terms = reader.iterator();
  SDB_ASSERT(terms);

  if (_compaction) {
    while (terms->next()) {
      PostingMeta meta;
      auto postings = terms->postings(props.index_features);
      _pw.Write(*postings, meta);
      consume([&] { return terms->value(); }, meta);
    }
  } else {
    constexpr size_t kTermBatch = 128;
    bytes_view term_batch[kTermBatch];
    PostingRows posting_batch[kTermBatch];
    while (const size_t n = terms->NextTermsWithPostings(
             term_batch, posting_batch, props.index_features)) {
      for (size_t i = 0; i != n; ++i) {
        PostingMeta meta;
        _pw.WritePostings(posting_batch[i], meta);
        consume([&] { return term_batch[i]; }, meta);
      }
    }
  }

  EndField(reader.id(), props, sum_dfreq, sum_tfreq);
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_terms++ == 0) {
    _min.assign(term);
  }
  if (!Append(term, meta)) {
    Cut();
    Append(term, meta);
  }
  if (_block.size > kPage) {
    Cut();
  }
}

bool FieldWriter::Append(bytes_view term, const PostingMeta& meta) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const bool restart = count % kRestartInterval == 0;
  if (restart) {
    _pw.BeginBlock();
  }
  _record.Clear();
  _pw.Encode(_record, meta);
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();

  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, count - 1), term);
  const size_t len = term.size() - lcp;
  const uint64_t head = uint64_t{lcp} << 5 | std::min<uint64_t>(len, 31);
  const uint64_t header = VSize(head) + (len >= 31 ? VSize(len - 31) : 0);
  const uint64_t restarts = count / kRestartInterval + 1;

  const uint64_t var =
    VarBytes(count + 1, restarts, block.headers.size() + header,
             block.suffixes.size() + len, records);
  const bool uniform =
    count == 0 || (block.uniform && term.size() == block.width);
  const auto width =
    count == 0 ? static_cast<uint32_t>(term.size()) : block.width;
  uint32_t prefix = 0;
  uint64_t size = var;
  auto kind = BlockKind::Var;
  if (uniform) {
    prefix =
      count == 0
        ? width
        : static_cast<uint32_t>(CommonPrefixLength(KeyAt(block, 0), term));
    const uint64_t fixed =
      FixedBytes(count + 1, restarts, records, width, prefix);
    if (width <= kMaxStride || fixed <= var + var / 32) {
      kind = BlockKind::FixedStride;
      size = fixed;
    }
  }
  if (count != 0 && size > kPage) {
    return false;
  }

  if (restart) {
    block.restarts.push_back({
      static_cast<uint16_t>(block.headers.size()),
      static_cast<uint16_t>(block.suffixes.size()),
      static_cast<uint16_t>(block.records.size()),
    });
  }
  BytesOutput headers{block.headers};
  headers.WriteV64(head);
  if (len >= 31) {
    headers.WriteV64(len - 31);
  }
  block.suffixes.append(term.substr(lcp));
  block.records.append(record);
  block.keys.append(term);
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  block.size = size;
  block.width = width;
  block.prefix = prefix;
  block.uniform = uniform;
  block.kind = kind;
  return true;
}

void FieldWriter::Cut() {
  if (_block.ends.empty()) {
    return;
  }
  Serialize(_block, _page);
  const auto first = KeyAt(_block, 0);
  if (_blocks != 0) {
    const auto separator =
      first.substr(0, CommonPrefixLength(bytes_view{_last}, first) + 1);
    const auto shared = CommonPrefixLength(bytes_view{_separator}, separator);
    BytesOutput out{_separators};
    out.WriteV32(static_cast<uint32_t>(shared));
    WriteStr(out, separator.substr(shared));
    _separator.assign(separator);
  }
  WriteBlock(_page);
  _last.assign(KeyAt(_block, _block.ends.size() - 1));
  Clear(_block);
}

void FieldWriter::WriteBlock(bytes_view block) {
  const uint64_t position = _out->Position();
  const uint64_t pad = (kPage - position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  BytesOutput directory{_directory};
  if (_blocks == 0) {
    directory.WriteV64((position + pad) / kPage);
  }
  directory.WriteV32(static_cast<uint32_t>((block.size() + kPage - 1) / kPage));
  _out->WriteData(block.data(), block.size());
  ++_blocks;
}

void FieldWriter::EndField(field_id id, const FieldProperties& props,
                           uint64_t total_doc_freq, uint64_t total_term_freq) {
  const auto stats = _pw.EndField();
  if (_terms == 0) {
    return;
  }

  const bool small = _blocks == 0;
  if (small) {
    _last.assign(KeyAt(_block, _block.ends.size() - 1));
    Serialize(_block, _page);
  } else {
    Cut();
  }

  auto& out = *_out;
  const uint64_t body_offset = out.Position();
  out.WriteByte(small ? 0 : kFieldBlocks);
  out.WriteV32(kRestartInterval);
  WriteStr(out, _min);
  WriteStr(out, _last);
  if (small) {
    out.WriteV64(_page.size());
    out.WriteData(_page.data(), _page.size());
  } else {
    out.WriteV32(_blocks);
    out.WriteData(_directory.data(), _directory.size());
    out.WriteV64(_separators.size());
    out.WriteData(_separators.data(), _separators.size());
  }

  _idx->AddTermDictEntry(id, TermDictMeta{
                               .features = props.index_features,
                               .term_count = _terms,
                               .doc_count = stats.docs_count,
                               .total_doc_freq = total_doc_freq,
                               .total_term_freq = total_term_freq,
                               .has_score_bounds = stats.has_score_bounds,
                               .body_offset = body_offset,
                               .norm = props.norm,
                             });

  Clear(_block);
  _directory.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _min.clear();
  _terms = 0;
  _blocks = 0;
}

void FieldWriter::end() {
  _pw.End();
  _idx = nullptr;
  _out = nullptr;
}

TermReaderImpl::TermReaderImpl(const FieldReader& owner, field_id id,
                               const TermDictMeta& meta, IndexInput& in)
  : _owner{&owner},
    _meta{id, meta.features},
    _terms{meta.term_count},
    _docs{meta.doc_count},
    _has_score_bounds{meta.has_score_bounds} {
  _meta.norm = meta.norm;
  const auto total =
    IndexFeatures::None != (meta.features & IndexFeatures::Freq)
      ? meta.total_term_freq
      : meta.total_doc_freq;
  SDB_ENSURE(total <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: total frequency ", total,
             " exceeds uint32_t::max");
  _freq.value = static_cast<uint32_t>(total);

  in.Seek(meta.body_offset);
  const auto flags = in.ReadByte();
  _restart_interval = in.ReadV32();
  _min = ReadString<bstring>(in);
  _max = ReadString<bstring>(in);
  if ((flags & kFieldBlocks) == 0) {
    const auto size = in.ReadV64();
    _small = Map(in, size, _small_copy);
    _blocks = 1;
    return;
  }
  _blocks = in.ReadV32();
  _pages.reserve(_blocks + 1);
  _pages.push_back(static_cast<uint32_t>(in.ReadV64()));
  for (uint32_t i = 0; i != _blocks; ++i) {
    _pages.push_back(_pages.back() + in.ReadV32());
  }
  const auto size = in.ReadV64();
  bstring copy;
  const auto* separators = Map(in, size, copy);
  _root = Root{DecodeSeparators(bytes_view{separators, size}, _blocks - 1)};
  const uint64_t bytes =
    _root.MemoryBytes() + _pages.capacity() * sizeof(uint32_t);
  owner.ResourceManager().Increase(bytes);
  _root_bytes = bytes;
}

TermReaderImpl::~TermReaderImpl() {
  _owner->ResourceManager().DecreaseChecked(_root_bytes);
}

const byte_type* TermReaderImpl::Block(size_t block,
                                       BlockBuffer& buffer) const {
  if (_small) {
    return _small;
  }
  const uint64_t offset = BlockOffset(block);
  if (const auto* base = _owner->Base()) {
    return base + offset;
  }
  if (!buffer.in) {
    buffer.in = _owner->Input().Reopen();
  }
  const uint64_t size =
    std::min(uint64_t{_pages[block + 1] - _pages[block]} * kPage,
             buffer.in->Length() - offset);
  if (const auto* p = buffer.in->ReadStable(offset, size)) {
    return p;
  }
  buffer.copy.resize(size);
  buffer.in->ReadData(offset, buffer.copy.data(), size);
  return buffer.copy.data();
}

SeekTermIterator::ptr TermReaderImpl::iterator() const {
  return memory::make_managed<TermIteratorImpl>(*this);
}

PostingMeta TermReaderImpl::Lookup(bytes_view term) const {
  if (_terms == 0 || term < _min || _max < term) {
    return {};
  }
  const size_t block = _small ? 0 : GetRoot().Find(term);
  BlockBuffer buffer;
  const auto view = Parse(Block(block, buffer));
  const auto entry = Find(view, term, _restart_interval);
  if (entry == kNoEntry) {
    return {};
  }
  return DecodeRecord(view, entry, _restart_interval, _meta.index_features);
}

void TermReaderImpl::ReadDocs(bytes_view term, Acceptor acceptor) const {
  const auto meta = Lookup(term);
  if (meta.docs_count == 0) {
    return;
  }
  if (meta.docs_count == 1) {
    acceptor(doc_limits::min() + meta.doc_delta);
    return;
  }
  auto docs_it = _owner->Postings().Postings(
    _meta.index_features, IndexFeatures::None, meta, _has_score_bounds);
  doc_id_t docs[doc_limits::kBlockSize];
  while (const auto n = docs_it->NextDocs(docs, nullptr)) {
    for (uint32_t i = 0; i != n; ++i) {
      SDB_ASSERT(doc_limits::valid(docs[i]));
      if (!acceptor(docs[i])) {
        return;
      }
    }
  }
}

std::unique_ptr<IndexInput> TermReaderImpl::ReopenPayload() const {
  return _owner->Postings().ReopenPayload();
}

PostingsHandles TermReaderImpl::Handles() const noexcept {
  return _owner->Postings().Handles();
}

Attribute* TermReaderImpl::GetMutable(TypeInfo::type_id type) noexcept {
  return type == irs::Type<FreqAttr>::id() ? &_freq : nullptr;
}

uint64_t FieldReader::CountMappedMemory() const {
  uint64_t bytes = _pr.CountMappedMemory();
  if (_in) {
    bytes += _in->CountMappedMemory();
  }
  return bytes;
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  const auto entries = state.idx->TermDicts();
  _in = state.idx->ReopenIn();
  if (!_in) {
    SDB_ENSURE(entries.empty(), "term dictionary: ", entries.size(),
               " fields but no `.idx` stream");
    return;
  }

  IndexFeatures features = IndexFeatures::None;
  for (const auto& [id, meta] : entries) {
    features |= meta.features;
  }
  _pr.prepare(state, features);

  _base = _in->ReadStable(0, _in->Length());
  _ids.reserve(entries.size());
  _by_id.reserve(entries.size());
  for (const auto& [id, meta] : entries) {
    const auto& field = _fields.emplace_back(*this, id, meta, *_in);
    const auto [it, ok] = _by_id.emplace(id, &field);
    SDB_ENSURE(ok, "term dictionary: duplicate field ", id);
    _ids.push_back(id);
  }
  SDB_ENSURE(absl::c_is_sorted(_ids),
             "term dictionary: fields are not sorted by id");
  _in->Advise(IOAdvice::RANDOM);
}

const TermReader* FieldReader::field(field_id id) const {
  const auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr : it->second;
}

}  // namespace irs::term_dict

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

#pragma once

#include <span>

#include "basics/memory.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/column_info.hpp"
#include "pg/sql_exception_macro.h"

namespace duckdb {

class DatabaseInstance;
}
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_info.hpp"

namespace irs {

class Comparer;
struct SegmentMeta;
struct FieldMeta;
struct FlushState;
struct ReaderState;
struct NormProvider;
class IndexOutput;
class DataInput;
class IndexInput;
struct PostingsWriter;
struct Scorer;
struct WandWriter;

using DocMap = ManagedVector<doc_id_t>;
using DocMapView = std::span<const doc_id_t>;

struct SegmentWriterOptions {
  const IndexFeatures scorers_features;
  ScorerPtr scorer = nullptr;
  const Comparer* const comparator{};
  // TODO(mbkkt) Remove it from here? We could use directory
  IResourceManager& resource_manager{IResourceManager::gNoop};
  // Enables the typed .col on the segment. Lifetime of `*db` must
  // extend at least until SegmentWriter::flush() returns.
  duckdb::DatabaseInstance* db = nullptr;
  // Non-owning. For a segment writer just the fallback (the owning override
  // comes via SetFieldOptions); for a merge writer the whole config.
  const IndexFieldOptions* field_options = nullptr;
};

struct TermPayloadWriter {
  virtual ~TermPayloadWriter() = default;

  // Doc-space base of the row group the next payload belongs to: a term is
  // written as one posting list per row group, so `WriteTermPayload` is called
  // once per (term, row group) with row-group-local ids and a writer that has
  // to reach the source row adds this base to them. Zero while a field is not
  // partitioned.
  virtual void SetRowGroupBase(doc_id_t /*base*/) {}

  virtual void WriteTermPayload(IndexOutput& out,
                                std::span<const doc_id_t> docs) = 0;

  virtual void Finish(IndexOutput& out) = 0;
};

// One term as the postings writer cut it: the stream anchors at the term's
// start plus one value set per run. Everything per run is term-scoped -- byte
// lengths and stream advances, never file positions -- so a record built from
// it is self-contained.
struct TermRuns {
  struct Run {
    uint32_t rg;
    uint32_t df;
    uint32_t tf;
    // Row-group-local id of a df == 1 run; such a run writes no `.doc` bytes.
    doc_id_t single_doc;
    // `.doc` bytes of the run, 0 when df == 1.
    uint64_t run_len;
    // Skip data offset from the run's `.doc` start, df > block size only.
    uint64_t e_skip;
    // `.pos` / `.pay` byte advance across the run. The payload plane has no
    // per-run advance: its runs are lanes of one stream, so they follow the
    // term's anchor by the df sums the record already spells.
    uint64_t pos_delta;
    uint64_t pay_delta;
  };

  uint64_t doc_start{};
  uint64_t pos_ptr{};
  // Byte position of the term's offset blocks, or -- for a payload field --
  // the stream ordinal ("lane") the term's first document's codes live at.
  uint64_t pay_ptr{};
  // Slot offset of the term's first position within the block at `pos_ptr`.
  uint32_t pos_anchor{};
  uint32_t docs_count{};
  uint32_t freq{};
  absl::InlinedVector<Run, 1> runs;

  void clear() noexcept {
    doc_start = pos_ptr = pay_ptr = 0;
    pos_anchor = docs_count = freq = 0;
    runs.clear();
  }
};

// One source run a merge transplants verbatim instead of decoding: the run is
// whole (no masked documents), its row-group-local ids are unchanged (the
// source row group starts exactly on an output row group boundary), and no
// other source contributes to that output row group -- so its `.doc` bytes,
// which never hold a file position, are already the output's. Positions are
// slot-addressed within shared blocks, so they are re-emitted from `pos_start`
// rather than byte-copied, but in whole decoded blocks, never per document.
struct BlitRun {
  IndexInput* doc_in;
  IndexInput* pos_in;
  uint64_t doc_start;
  uint64_t doc_len;
  uint64_t pos_start;
  uint32_t rg;
  uint32_t df;
  uint32_t tf;
  // single_doc of a df == 1 run, e_skip of a run past one postings block.
  uint32_t inlined;
  uint8_t pos_slot;
};

// The current term's transplanted runs, ascending by output row group and
// disjoint from the row groups the doc iterator yields -- together they are
// the whole term. A doc iterator without this attribute transplants nothing.
struct BlitPlanAttr final : Attribute {
  static constexpr std::string_view type_name() noexcept { return "blit_plan"; }

  std::span<const BlitRun> runs;
  bool has_wand{false};
};

struct PostingsWriter {
  using ptr = std::unique_ptr<PostingsWriter>;

  struct FieldStats {
    bool has_wand;
    doc_id_t docs_count;
    // Where the field's payload stream starts in `.pay`. Lane ordinals are
    // counted from there, and the codec turns one into a byte position.
    uint64_t pay_base;
  };

  virtual ~PostingsWriter() = default;
  // out - corresponding terms stream
  virtual void Prepare(IndexOutput& out, const FlushState& state) = 0;
  virtual void BeginField(const FieldProperties& meta) = 0;
  virtual void SetTermPayloadWriter(TermPayloadWriter*) {}
  // The field's exact docs count when the caller already knows it -- a merge
  // of unmasked sources sums it from the source fields -- so `EndField`
  // reports it without the writer counting documents, which transplanted runs
  // never hand over one by one. Valid for the current field only.
  virtual void SetKnownDocsCount(doc_id_t /*count*/) {}
  // One lifecycle per term: consumes the term's iterator once, cutting runs at
  // row group boundaries -- a cut closes the block/tail and resets the id
  // delta base, nothing more. A df == 1 run writes nothing and reports its id.
  virtual void WriteTerm(DocIterator& docs, uint32_t row_group_size,
                         TermRuns& out) = 0;
  virtual FieldStats EndField() = 0;
  virtual void End() = 0;
};

struct BasicTermReader : public AttributeProvider {
  virtual SourceTermIterator::ptr iterator() const = 0;

  virtual field_id id() const = 0;

  virtual FieldProperties properties() const = 0;

  // Returns the least significant term
  virtual bytes_view(min)() const = 0;

  // Returns the most significant term
  virtual bytes_view(max)() const = 0;

  virtual TermPayloadWriter* PayloadWriter() const { return nullptr; }

  // The field's exact docs count when this reader can state it without the
  // writer counting documents -- a pure concatenation of unmasked sources sums
  // the source fields' counts over disjoint id ranges. 0 means unknown.
  virtual doc_id_t KnownDocsCount() const { return 0; }
};

struct IteratorFieldOptions : WandContext {
  explicit IteratorFieldOptions(bool has_wand) : has_wand{has_wand} {}

  IteratorFieldOptions(WandContext options, bool has_wand)
    : WandContext{options}, has_wand{has_wand} {}

  bool has_wand;
};

// One posting list handed to the postings reader, plus the term-level scoring
// inputs that go with it.
struct PostingCookie {
  TermMetaImpl meta;
  const byte_type* stats = nullptr;
  score_t boost = kNoBoost;
  FieldProperties field;
};

// One term as a query leaf: the whole dictionary state of the term -- its
// posting list per row group -- plus the same scoring inputs. A row group's
// worth of it is a `PostingCookie`.
struct TermLeaf {
  const TermCookie* cookie = nullptr;
  const byte_type* stats = nullptr;
  score_t boost = kNoBoost;
  FieldProperties field;
};

struct PostingsReader {
  using ptr = std::unique_ptr<PostingsReader>;
  using term_provider_f = std::function<const TermMeta*()>;

  virtual ~PostingsReader() = default;

  virtual uint64_t CountMappedMemory() const = 0;

  // in - corresponding stream
  // features - the set of features available for segment
  virtual void prepare(DataInput& in, const ReaderState& state,
                       IndexFeatures features) = 0;

  // Evaluates a union of all docs denoted by attribute supplied via a
  // speciified 'provider'. Each doc is represented by a bit in a
  // specified 'bitset'.
  // Returns a number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t BitUnion(IndexFeatures field_features,
                          const term_provider_f& provider, size_t* set,
                          bool has_wand) = 0;

  virtual DocIterator::ptr Iterator(IndexFeatures field_features,
                                    IndexFeatures required_features,
                                    std::span<const PostingCookie> metas,
                                    IteratorFieldOptions options,
                                    size_t min_match,
                                    ScoreMergeType type) const = 0;

  // One posting list, recycling `reuse` -- an iterator this same method
  // returned earlier for the same (field_features, required_features,
  // options) on this reader -- instead of allocating a new one. `reuse` must
  // have been drained with `advance()` alone and never sought; null creates.
  // Never the WAND shape: the caller wanting scored iteration goes through
  // `Iterator`.
  virtual DocIterator::ptr ReuseIterator(DocIterator::ptr&& reuse,
                                         IndexFeatures field_features,
                                         IndexFeatures required_features,
                                         const PostingCookie& meta,
                                         IteratorFieldOptions options) const {
    return Iterator(field_features, required_features, {&meta, 1}, options, 1,
                    ScoreMergeType::Noop);
  }

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }

  // Raw plane streams, for a consumer that moves stored bytes without going
  // through an iterator -- the merge byte transplant. Null when absent.
  virtual std::unique_ptr<IndexInput> ReopenDoc() const { return nullptr; }
  virtual std::unique_ptr<IndexInput> ReopenPos() const { return nullptr; }

  DocIterator::ptr Iterator(IndexFeatures field_features,
                            IndexFeatures required_features,
                            const PostingCookie& meta,
                            IteratorFieldOptions options,
                            ScoreMergeType type = ScoreMergeType::Noop) const {
    return Iterator(field_features, required_features, {&meta, 1}, options, 1,
                    type);
  }
};

// Row-group directory of a segment. Uniform row groups make it derivable, so
// nothing is stored per row group; this accessor is the seam a non-uniform
// layout would replace with a real table. `rows_per_group == 0` means the
// dictionary does not partition: one row group spans the segment and its
// extent is unknown here.
struct RowGroupLayout {
  uint32_t count = 1;
  uint32_t rows_per_group = 0;
  uint32_t segment_docs = 0;

  // The grid of a dictionary that does not partition: one row group spanning
  // the segment. `Rows()` asserts on `rows_per_group == 0`, so this is the one
  // place the missing extent is filled in from the segment itself.
  static RowGroupLayout Whole(uint64_t segment_docs) noexcept {
    const auto docs = static_cast<uint32_t>(segment_docs);
    return {
      .count = 1, .rows_per_group = docs != 0 ? docs : 1, .segment_docs = docs};
  }

  doc_id_t Base(uint32_t rg) const noexcept {
    return doc_limits::min() + rg * rows_per_group;
  }

  uint32_t Rows(uint32_t rg) const noexcept {
    SDB_ASSERT(rows_per_group != 0);
    const uint32_t left = segment_docs - rg * rows_per_group;
    return left < rows_per_group ? left : rows_per_group;
  }
};

// Expected usage pattern of SeekTermIterator
enum class SeekMode : uint32_t {
  /// Default mode, e.g. multiple consequent seeks are expected
  NORMAL = 0,

  // Only random exact seeks are supported
  RandomOnly,
};

// How much of a field's dictionary each plane costs, as the field itself
// reports it. Sizes are the stored bytes, so a plane a layout does not have is
// zero -- a dictless field is all zeros but `terms`/`docs`.
struct DictFieldStorage {
  // Leaf representation: "VAR" or "FIXED_STRIDE" (the leaf codec of a blocked
  // field), "SINGLE" (a one-term field, no dictionary planes). Empty for a
  // reader that has no dictionary planes at all.
  std::string_view layout;
  uint64_t blocks_size{};
  uint64_t separators_size{};
  uint64_t fsst_table_size{};
  uint64_t body_offset{};
  uint32_t block_count{};
  uint32_t row_groups{};
  uint64_t terms{};
  uint64_t docs{};
  bool fsst{false};
  bool partitioned{false};
  // Walked, and therefore only filled when the caller asks for it: posting
  // lists summed over the field's terms, and the longest single term's list.
  // A term of one row group contributes one, so `rg_lists == terms` means no
  // term crosses a row group boundary.
  uint64_t rg_lists{};
  uint32_t max_rg_per_term{};
  // Leaf blocks per codec, also walked. `layout` answers the all-or-nothing
  // question -- it is "FIXED_STRIDE" only when every block took the fixed leaf
  // at one key width -- so a field whose key width varies reports "VAR"
  // however many of its blocks are fixed. These two are what says which.
  uint32_t fixed_blocks{};
  uint32_t var_blocks{};
  bool walked{false};
};

struct TermReader : public AttributeProvider {
  using ptr = std::unique_ptr<TermReader>;
  using cookie_provider = std::function<const TermCookie*()>;

  // `mode` argument defines seek mode for term iterator
  // Returns an iterator over terms for a field.
  virtual SeekTermIterator::ptr iterator(SeekMode mode) const = 0;

  // Cuts this field's terms into at most `out.size()` disjoint ranges of
  // equal term count and returns how many were written -- what a parallel
  // enumeration of one dictionary claims. The cuts are block boundaries, so
  // the count is also capped by the field's block count, and the bounds
  // borrow from the reader's resident separators. Zero means no terms.
  virtual size_t TermRanges(std::span<TermRange> out) const {
    if (out.empty() || size() == 0) {
      return 0;
    }
    out.front() = {};
    return 1;
  }

  // A forward walk of `range` alone. The default answers the one unbounded
  // range the default `TermRanges` produces; a dictionary that cuts real
  // ranges overrides it and skips a block whose separator is already past
  // `hi` without reading it.
  virtual SeekTermIterator::ptr RangeIterator(const TermRange& range) const {
    SDB_ASSERT(range.lo.empty() && range.hi.empty());
    return iterator(SeekMode::NORMAL);
  }

  // An intersection of this reader's terms with an acceptor's language, driven
  // by stepping the parametric Levenshtein tables directly. `nullptr` means
  // this dictionary has no direct-stepping backend and the caller scans
  // instead. The acceptor is borrowed and must outlive the iterator.
  virtual SeekTermIterator::ptr iterator(const LevenshteinAcceptor&) const {
    return nullptr;
  }

  // The same intersection driven by a regexp acceptor's transition table.
  // `nullptr` means this dictionary has no regexp backend and the caller scans
  // instead. The acceptor is borrowed and must outlive the iterator.
  virtual SeekTermIterator::ptr iterator(const RegexpAcceptor&) const {
    return nullptr;
  }

  // Resolves a whole probe set in one forward pass over the dictionary.
  // `terms` must be sorted ascending and free of duplicates -- unsorted input
  // is the caller's bug and fails in a dev build. `next()` reports the probes
  // that hit, in probe order, each positioned exactly as a `seek` to it would
  // leave a term iterator; a probe never reported is a miss. `terms` is
  // borrowed and must outlive the iterator.
  //
  // `nullptr` means this dictionary has no batch backend and the caller seeks
  // the probes itself, which is what it did before the batch API existed.
  virtual SeekTermIterator::ptr BatchIterator(
    std::span<const bytes_view> /*terms*/) const {
    return nullptr;
  }

  // Evaluates a union of all docs denoted by cookies supplied via a specified
  // 'provider', restricted to row group `rg` and in its local id space: a bit
  // index is a row-group-local doc id, so the caller sizes the bitset by the
  // row group's rows. Terms with no posting list in `rg` contribute nothing.
  // Returns a number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t RowGroupBitUnion(const cookie_provider& provider, uint32_t rg,
                                  size_t* bitset) const = 0;

  // Row groups this field's postings are partitioned into.
  virtual RowGroupLayout RowGroups() const noexcept { return {}; }

  // Iterator over the row-group-local doc ids of `cookies` inside row group
  // `rg`. This is the native read currency: no consumer of it reconstructs a
  // segment-wide id, the conversion is one addition of the row group's base at
  // a true output boundary.
  virtual DocIterator::ptr RowGroupIterator(IndexFeatures features,
                                            std::span<const TermLeaf> terms,
                                            uint32_t rg, WandContext options,
                                            size_t min_match,
                                            ScoreMergeType type) const = 0;

  DocIterator::ptr RowGroupIterator(IndexFeatures features,
                                    const TermLeaf& term, uint32_t rg,
                                    WandContext options = {}) const {
    return RowGroupIterator(features, {&term, 1}, rg, options, 1,
                            ScoreMergeType::Noop);
  }

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }

  // Raw plane streams, as `PostingsReader` exposes them; what the merge byte
  // transplant copies run bytes out of. Null when this reader has none.
  virtual std::unique_ptr<IndexInput> ReopenDoc() const { return nullptr; }
  virtual std::unique_ptr<IndexInput> ReopenPos() const { return nullptr; }

  // Byte position the field's payload stream starts at, which is where its
  // lane 0 sits. A term's payload anchor is a lane ordinal, so this is what
  // turns it into a file position.
  virtual uint64_t PayloadBase() const { return 0; }

  // Returns field metadata.
  virtual const FieldMeta& meta() const = 0;

  // Returns total number of terms.
  virtual size_t size() const = 0;

  // Returns total number of documents with at least 1 term in a field.
  virtual uint64_t docs_count() const = 0;

  // Returns the least significant term.
  virtual bytes_view(min)() const = 0;

  // Returns the most significant term.
  virtual bytes_view(max)() const = 0;

  // Returns true if scorer denoted by the is supported by the field.
  virtual bool has_scorer(uint8_t index) const = 0;

  // Storage decomposition of this field's dictionary. `walk` asks for the
  // rg-list statistics, which cost one pass over the field's terms; without it
  // every number comes from the field header.
  virtual DictFieldStorage StorageInfo(bool walk) const { return {}; }
};

struct SegmentMetaWriter : memory::Managed {
  using ptr = memory::managed_ptr<SegmentMetaWriter>;

  virtual void write(Directory& dir, std::string& filename,
                     SegmentMeta& meta) = 0;
};

struct SegmentMetaReader : memory::Managed {
  using ptr = memory::managed_ptr<SegmentMetaReader>;

  virtual void read(const Directory& dir, SegmentMeta& meta,
                    std::string_view filename = {}) = 0;  // null == use meta
};

struct IndexMetaWriter {
  using ptr = std::unique_ptr<IndexMetaWriter>;

  virtual ~IndexMetaWriter() = default;
  virtual bool prepare(Directory& dir, IndexMeta& meta,
                       std::string& pending_filename,
                       std::string& filename) = 0;
  virtual bool commit() = 0;
  virtual void rollback() noexcept = 0;
};

struct IndexMetaReader : memory::Managed {
  using ptr = memory::managed_ptr<IndexMetaReader>;

  virtual bool last_segments_file(const Directory& dir,
                                  std::string& name) const = 0;

  // null == use meta
  virtual void read(const Directory& dir, IndexMeta& meta,
                    std::string_view filename) = 0;
};

class Format {
 public:
  using ptr = std::shared_ptr<const Format>;

  virtual ~Format() = default;

  virtual IndexMetaWriter::ptr get_index_meta_writer() const = 0;
  virtual IndexMetaReader::ptr get_index_meta_reader() const = 0;

  virtual SegmentMetaWriter::ptr get_segment_meta_writer() const = 0;
  virtual SegmentMetaReader::ptr get_segment_meta_reader() const = 0;

  virtual PostingsWriter::ptr get_postings_writer(
    bool compaction, IResourceManager& resource_manager) const = 0;
  virtual PostingsReader::ptr get_postings_reader() const = 0;

  virtual TypeInfo::type_id type() const noexcept = 0;
};

struct FlushState {
  Directory* const dir{};
  // In-flight norm reader source (SegmentWriter during initial flush,
  // null during merge). Posting writers consult it to read per-doc norms
  // for Wand metadata while the segment is still being written.
  const NormProvider* norms{};
  const std::string_view name;  // segment name
  ScorerPtr scorer = nullptr;
  const size_t doc_count;
  IndexFeatures index_features{IndexFeatures::None};
};

struct ReaderState {
  const Directory* dir;
  const SegmentMeta* meta;
  ScorerPtr scorer = nullptr;
  IdxReader* idx = nullptr;
};

void FormatBlock128Init();

namespace formats {

// Find a format by name, or nullptr if not found
// indirect call to <class>::make(...)
// NOTE: make(...) MUST be defined in CPP to ensire proper code scope
Format::ptr Get(std::string_view name, bool load_library = true) noexcept;

// For static lib reference all known formats in lib
// no explicit call of fn is required, existence of fn is sufficient.
inline void Init() { FormatBlock128Init(); }

// Visit all loaded formats, terminate early if visitor returns false.
bool Visit(const std::function<bool(std::string_view)>& visitor);

}  // namespace formats
class FormatRegistrar {
 public:
  FormatRegistrar(const TypeInfo& type, Format::ptr (*factory)(),
                  const char* source = nullptr);

  explicit operator bool() const noexcept { return _registered; }

 private:
  bool _registered;
};

#define REGISTER_FORMAT_IMPL(format_name, line, source)    \
  static ::irs::FormatRegistrar format_registrar##_##line( \
    ::irs::Type<format_name>::get(), &format_name::make, source)
#define REGISTER_FORMAT_EXPANDER(format_name, file, line) \
  REGISTER_FORMAT_IMPL(format_name, line, file ":" IRS_TO_STRING(line))
#define REGISTER_FORMAT(format_name) \
  REGISTER_FORMAT_EXPANDER(format_name, __FILE__, __LINE__)

}  // namespace irs

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
#include "iresearch/utils/automaton_decl.hpp"
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

// Represents metadata associated with the term
struct TermMeta : Attribute {
  static constexpr std::string_view type_name() noexcept { return "term_meta"; }

  void clear() noexcept {
    docs_count = 0;
    freq = 0;
  }

  // How many documents a particular term contains
  uint32_t docs_count = 0;

  // How many times a particular term occur in documents
  uint32_t freq = 0;
};

struct TermPayloadWriter {
  virtual ~TermPayloadWriter() = default;

  virtual void WriteTermPayload(IndexOutput& out,
                                std::span<const doc_id_t> docs) = 0;

  virtual void Finish(IndexOutput& out) = 0;
};

struct PostingsWriter {
  using ptr = std::unique_ptr<PostingsWriter>;

  struct FieldStats {
    bool has_wand;
    doc_id_t docs_count;
  };

  virtual ~PostingsWriter() = default;
  // out - corresponding terms stream
  virtual void Prepare(IndexOutput& out, const FlushState& state) = 0;
  virtual void BeginField(const FieldProperties& meta) = 0;
  virtual void SetTermPayloadWriter(TermPayloadWriter*) {}
  virtual void Write(DocIterator& docs, TermMeta& meta) = 0;
  virtual void BeginBlock() = 0;
  virtual void Encode(BufferedOutput& out, const TermMeta& state) = 0;
  virtual FieldStats EndField() = 0;
  virtual void End() = 0;
};

struct BasicTermReader : public AttributeProvider {
  virtual TermIterator::ptr iterator() const = 0;

  virtual field_id id() const = 0;

  virtual FieldProperties properties() const = 0;

  // Returns the least significant term
  virtual bytes_view(min)() const = 0;

  // Returns the most significant term
  virtual bytes_view(max)() const = 0;

  virtual TermPayloadWriter* PayloadWriter() const { return nullptr; }
};

struct IteratorFieldOptions : WandContext {
  explicit IteratorFieldOptions(bool has_wand) : has_wand{has_wand} {}

  IteratorFieldOptions(WandContext options, bool has_wand)
    : WandContext{options}, has_wand{has_wand} {}

  bool has_wand;
};

struct PostingCookie {
  const SeekCookie* cookie = nullptr;
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

  // Parses input block "in" and populate "attrs" collection with
  // attributes.
  // Returns number of bytes read from in.
  virtual size_t decode(const byte_type* in, IndexFeatures features,
                        TermMeta& state) = 0;

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

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }

  DocIterator::ptr Iterator(IndexFeatures field_features,
                            IndexFeatures required_features,
                            const PostingCookie& meta,
                            IteratorFieldOptions options,
                            ScoreMergeType type = ScoreMergeType::Noop) const {
    return Iterator(field_features, required_features, {&meta, 1}, options, 1,
                    type);
  }
};

// Expected usage pattern of SeekTermIterator
enum class SeekMode : uint32_t {
  /// Default mode, e.g. multiple consequent seeks are expected
  NORMAL = 0,

  // Only random exact seeks are supported
  RandomOnly
};

struct TermReader : public AttributeProvider {
  using ptr = std::unique_ptr<TermReader>;
  using cookie_provider = std::function<const SeekCookie*()>;
  using Acceptor = absl::FunctionRef<bool(doc_id_t)>;

  // `mode` argument defines seek mode for term iterator
  // Returns an iterator over terms for a field.
  virtual SeekTermIterator::ptr iterator(SeekMode mode) const = 0;

  // Read 'count' number of documents containing 'term' to 'docs'
  // Returns number of read documents
  virtual void read_documents(bytes_view term, Acceptor acceptor) const = 0;

  // Returns term metadata for a given 'term'
  virtual TermMeta term(bytes_view term) const = 0;

  // Returns an intersection of a specified automaton and term reader.
  virtual SeekTermIterator::ptr iterator(
    const automaton_table_matcher& matcher) const = 0;

  // Evaluates a union of all docs denoted by cookies supplied via a
  // speciified 'provider'. Each doc is represented by a bit in a
  // specified 'bitset'.
  // A number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t BitUnion(const cookie_provider& provider,
                          size_t* bitset) const = 0;

  virtual DocIterator::ptr Iterator(
    IndexFeatures features, std::span<const PostingCookie> cookies,
    WandContext options = {}, size_t min_match = 1,
    ScoreMergeType type = ScoreMergeType::Noop) const = 0;

  DocIterator::ptr Iterator(IndexFeatures features, const PostingCookie& cookie,
                            WandContext options = {}) const {
    return Iterator(features, {&cookie, 1}, options);
  }

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }

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

// Checks whether a format with the specified name is registered.
bool Exists(std::string_view name, bool load_library = true);

// Find a format by name, or nullptr if not found
// indirect call to <class>::make(...)
// NOTE: make(...) MUST be defined in CPP to ensire proper code scope
Format::ptr Get(std::string_view name, bool load_library = true) noexcept;

// For static lib reference all known formats in lib
// no explicit call of fn is required, existence of fn is sufficient.
inline void Init() { FormatBlock128Init(); }

// Load all formats from plugins directory.
void LoadAll(std::string_view path);

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

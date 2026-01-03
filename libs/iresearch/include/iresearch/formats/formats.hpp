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

#include <absl/container/flat_hash_set.h>

#include <set>
#include <vector>

#include "iresearch/formats/hnsw_index.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/column_finalizer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/score_function.hpp"
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
class IndexOutput;
struct DataInput;
struct IndexInput;
struct PostingsWriter;
struct Scorer;
struct WandWriter;

using DocMap = ManagedVector<doc_id_t>;
using DocMapView = std::span<const doc_id_t>;

using ScoreFunctionFactory =
  std::function<ScoreFunction(const AttributeProvider&)>;

struct WanderatorOptions {
  ScoreFunctionFactory factory;
};

struct SegmentWriterOptions {
  const ColumnInfoProvider& column_info;
  const FeatureInfoProvider& feature_info;
  const IndexFeatures scorers_features;
  ScorersView scorers;
  const Comparer* const comparator{};
  // TODO(mbkkt) Remove it from here? We could use directory
  IResourceManager& resource_manager{IResourceManager::gNoop};
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

struct PostingsWriter {
  using ptr = std::unique_ptr<PostingsWriter>;

  struct FieldStats {
    uint64_t wand_mask;
    doc_id_t docs_count;
  };

  virtual ~PostingsWriter() = default;
  // out - corresponding terms stream
  virtual void prepare(IndexOutput& out, const FlushState& state) = 0;
  virtual void begin_field(const FieldProperties& meta) = 0;
  virtual void write(DocIterator& docs, TermMeta& meta) = 0;
  virtual void begin_block() = 0;
  virtual void encode(BufferedOutput& out, const TermMeta& state) = 0;
  virtual FieldStats end_field() = 0;
  virtual void end() = 0;
};

struct BasicTermReader : public AttributeProvider {
  virtual TermIterator::ptr iterator() const = 0;

  virtual std::string_view name() const = 0;

  virtual FieldProperties properties() const = 0;

  // Returns the least significant term
  virtual bytes_view(min)() const = 0;

  // Returns the most significant term
  virtual bytes_view(max)() const = 0;
};

struct FieldWriter {
  using ptr = std::unique_ptr<FieldWriter>;

  virtual ~FieldWriter() = default;
  virtual void prepare(const FlushState& state) = 0;
  virtual void write(const BasicTermReader& reader) = 0;
  virtual void end() = 0;
};

struct WandInfo {
  uint8_t mapped_index{WandContext::kDisable};
  uint8_t count{0};
};

struct PostingsReader {
  using ptr = std::unique_ptr<PostingsReader>;
  using term_provider_f = std::function<const TermMeta*()>;

  virtual ~PostingsReader() = default;

  virtual uint64_t CountMappedMemory() const = 0;

  // in - corresponding stream
  // features - the set of features available for segment
  virtual void prepare(IndexInput& in, const ReaderState& state,
                       IndexFeatures features) = 0;

  // Parses input block "in" and populate "attrs" collection with
  // attributes.
  // Returns number of bytes read from in.
  virtual size_t decode(const byte_type* in, IndexFeatures features,
                        TermMeta& state) = 0;

  // Returns document iterator for a specified 'cookie' and 'features'
  virtual DocIterator::ptr iterator(IndexFeatures field_features,
                                    IndexFeatures required_features,
                                    const TermMeta& meta,
                                    uint8_t wand_count) = 0;

  virtual DocIterator::ptr wanderator(IndexFeatures field_features,
                                      IndexFeatures required_features,
                                      const TermMeta& meta,
                                      const WanderatorOptions& options,
                                      WandContext ctx, WandInfo info) = 0;

  // Evaluates a union of all docs denoted by attribute supplied via a
  // speciified 'provider'. Each doc is represented by a bit in a
  // specified 'bitset'.
  // Returns a number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t BitUnion(IndexFeatures field_features,
                          const term_provider_f& provider, size_t* set,
                          uint8_t wand_count) = 0;
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

  // `mode` argument defines seek mode for term iterator
  // Returns an iterator over terms for a field.
  virtual SeekTermIterator::ptr iterator(SeekMode mode) const = 0;

  // Read 'count' number of documents containing 'term' to 'docs'
  // Returns number of read documents
  virtual size_t read_documents(bytes_view term,
                                std::span<doc_id_t> docs) const = 0;

  // Returns term metadata for a given 'term'
  virtual TermMeta term(bytes_view term) const = 0;

  // Returns an intersection of a specified automaton and term reader.
  virtual SeekTermIterator::ptr iterator(
    automaton_table_matcher& matcher) const = 0;

  // Evaluates a union of all docs denoted by cookies supplied via a
  // speciified 'provider'. Each doc is represented by a bit in a
  // specified 'bitset'.
  // A number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t BitUnion(const cookie_provider& provider,
                          size_t* bitset) const = 0;

  virtual DocIterator::ptr postings(const SeekCookie& cookie,
                                    IndexFeatures features) const = 0;

  virtual DocIterator::ptr wanderator(const SeekCookie& cookie,
                                      IndexFeatures features,
                                      const WanderatorOptions& options,
                                      WandContext context) const = 0;

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

struct FieldReader {
  using ptr = std::shared_ptr<FieldReader>;

  virtual ~FieldReader() = default;

  virtual uint64_t CountMappedMemory() const = 0;

  virtual void prepare(const ReaderState& stat) = 0;

  virtual const TermReader* field(std::string_view field) const = 0;
  virtual FieldIterator::ptr iterator() const = 0;
  virtual size_t size() const = 0;
};

struct ColumnOutput : DataOutput {
  // Resets stream to previous persisted state
  virtual void Reset() = 0;
  // NOTE: doc_limits::invalid() < doc && doc < doc_limits::eof()
  virtual void Prepare(doc_id_t doc) = 0;

#ifdef SDB_GTEST
  ColumnOutput& operator()(doc_id_t doc) {
    Prepare(doc);
    return *this;
  }
#endif
};

struct ColumnstoreWriter {
  using ptr = std::unique_ptr<ColumnstoreWriter>;

  // Finalizer can be used to assign name and payload to a column.
  // Returned `std::string_view` must be valid during `commit(...)`.

  struct ColumnT {
    field_id id;
    ColumnOutput& out;
  };

  virtual ~ColumnstoreWriter() = default;

  virtual void prepare(Directory& dir, const SegmentMeta& meta) = 0;
  virtual ColumnT push_column(const ColumnInfo& info,
                              ColumnFinalizer header_writer) = 0;
  virtual void rollback() noexcept = 0;

  // Return was anything actually flushed.
  virtual bool commit(const FlushState& state) = 0;
};

enum class ColumnHint : uint32_t {
  // Nothing special
  Normal = 0,
  // Open iterator for conosolidation
  Consolidation = 1,
  // Reading payload isn't necessary
  Mask = 2,
  // Allow accessing prev document
  PrevDoc = 4
};

ENABLE_BITMASK_ENUM(ColumnHint);

struct ColumnReader : public memory::Managed {
  // Returns column id.
  virtual field_id id() const = 0;

  // Returns optional column name.
  virtual std::string_view name() const = 0;

  // Returns column header.
  virtual bytes_view payload() const = 0;

  // FIXME(gnusi): implement mode
  //  Returns the corresponding column iterator.
  //  If the column implementation supports document payloads then it
  //  can be accessed via the 'payload' attribute.
  virtual ResettableDocIterator::ptr iterator(ColumnHint hint) const = 0;

  // Returns total number of columns.
  virtual doc_id_t size() const = 0;

  virtual void Search(HNSWSearchContext& context) const {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "Search not implemented for this column type");
  }
};

struct ColumnstoreReader {
  using ptr = std::unique_ptr<ColumnstoreReader>;

  using column_visitor_f = std::function<bool(const ColumnReader&)>;

  struct Options {
    // allows to select "cached" columns
    column_visitor_f warmup_column;
  };

  virtual ~ColumnstoreReader() = default;

  virtual uint64_t CountMappedMemory() const = 0;

  // Returns true if conlumnstore is present in a segment, false - otherwise.
  // May throw `io_error` or `index_error`.
  virtual bool prepare(const Directory& dir, const SegmentMeta& meta,
                       const Options& opts = Options{}) = 0;

  virtual bool visit(const column_visitor_f& visitor) const = 0;

  virtual const ColumnReader* column(field_id field) const = 0;

  // Returns total number of columns.
  virtual size_t size() const = 0;
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

  virtual FieldWriter::ptr get_field_writer(
    bool consolidation, IResourceManager& resource_manager) const = 0;
  virtual FieldReader::ptr get_field_reader(
    IResourceManager& resource_manager) const = 0;

  virtual ColumnstoreWriter::ptr get_columnstore_writer(
    bool consolidation, IResourceManager& resource_manager) const = 0;
  virtual ColumnstoreReader::ptr get_columnstore_reader() const = 0;

  virtual PostingsWriter::ptr get_postings_writer(bool consolidation,
                                                  IResourceManager&) const = 0;
  virtual PostingsReader::ptr get_postings_reader() const = 0;

  virtual TypeInfo::type_id type() const noexcept = 0;
};

struct FlushState {
  Directory* const dir{};
  const DocMap* docmap{};
  const ColumnProvider* columns{};
  const std::string_view name;  // segment name
  ScorersView scorers;
  const size_t doc_count;
  // Accumulated segment index features
  IndexFeatures index_features{IndexFeatures::None};
};

struct ReaderState {
  const Directory* dir;
  const SegmentMeta* meta;
  ScorersView scorers;
};

namespace formats {
// Checks whether a format with the specified name is registered.
bool Exists(std::string_view name, bool load_library = true);

// Find a format by name, or nullptr if not found
// indirect call to <class>::make(...)
// NOTE: make(...) MUST be defined in CPP to ensire proper code scope
Format::ptr Get(std::string_view name, bool load_library = true) noexcept;

// For static lib reference all known formats in lib
// no explicit call of fn is required, existence of fn is sufficient.
void Init();

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

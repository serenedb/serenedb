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

#include <absl/functional/function_ref.h>

#include "basics/memory.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/formats/posting_meta.hpp"
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
struct ScoreBoundWriter;

using DocMap = ManagedVector<doc_id_t>;
using DocMapView = std::span<const doc_id_t>;

struct AnnBuildEnv;

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
  // Non-owning. Null builds the segment's ANN graph on the flushing thread.
  const AnnBuildEnv* ann_env = nullptr;
};

struct TermPayloadWriter {
  virtual ~TermPayloadWriter() = default;

  virtual void WriteTermPayload(IndexOutput& out,
                                std::span<const doc_id_t> docs) = 0;

  virtual void Finish(IndexOutput& out) = 0;

  virtual uint32_t PendingLanes() const noexcept { return 0; }
};

struct PostingsWriter {
  using ptr = std::unique_ptr<PostingsWriter>;

  struct FieldStats {
    bool has_score_bounds;
    doc_id_t docs_count;
  };

  virtual ~PostingsWriter() = default;
  // out - corresponding terms stream
  virtual void Prepare(IndexOutput& out, const FlushState& state) = 0;
  virtual void BeginField(const FieldProperties& meta) = 0;
  virtual void SetTermPayloadWriter(TermPayloadWriter*) {}
  virtual void Write(TermPostings& docs, PostingMeta& meta) = 0;
  virtual void BeginBlock() = 0;
  virtual void Encode(BufferedOutput& out, const PostingMeta& state) = 0;
  virtual FieldStats EndField() = 0;
  virtual void End() = 0;
};

struct BasicTermReader : public AttributeProvider {
  virtual TermOnlyIterator::ptr iterator() const = 0;

  virtual field_id id() const = 0;

  virtual FieldProperties properties() const = 0;

  virtual bytes_view min() const = 0;
  virtual bytes_view max() const = 0;

  virtual TermPayloadWriter* PayloadWriter() const { return nullptr; }
};

// The streams a posting list lives in. Handed to whatever decodes a term's
// postings so it can reopen what it needs; `pos` and `pay` are null for a
// field that stores neither.
struct PostingsHandles {
  const IndexInput* doc = nullptr;
  const IndexInput* pos = nullptr;
  const IndexInput* pay = nullptr;
};

struct PostingsReader {
  using ptr = std::unique_ptr<PostingsReader>;
  using TermProvider = absl::FunctionRef<const PostingMeta*()>;

  virtual ~PostingsReader() = default;

  virtual uint64_t CountMappedMemory() const = 0;

  virtual PostingsHandles Handles() const noexcept = 0;

  // in - corresponding stream
  // features - the set of features available for segment
  virtual void prepare(DataInput& in, const ReaderState& state,
                       IndexFeatures features) = 0;

  // Parses input block "in" and populate "attrs" collection with
  // attributes.
  // Returns number of bytes read from in.
  virtual size_t decode(const byte_type* in, IndexFeatures features,
                        PostingMeta& state) = 0;

  // Evaluates a union of all docs denoted by attribute supplied via a
  // speciified 'provider'. Each doc is represented by a bit in a
  // specified 'bitset'.
  // Returns a number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t BitUnion(IndexFeatures field_features, TermProvider provider,
                          uint64_t* set, bool has_score_bounds) = 0;

  // One term's whole posting list as the write side reads it: front to back,
  // with the frequency and the positions the field stores. Nothing here
  // seeks, so no skip list is parsed. `required_features` narrows what is
  // decoded; what the field carries beyond that is stepped over.
  virtual TermPostings::ptr Postings(IndexFeatures field_features,
                                     IndexFeatures required_features,
                                     const PostingMeta& meta,
                                     bool has_score_bounds) const = 0;

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }
};

struct TermReader : public AttributeProvider {
  using ptr = std::unique_ptr<TermReader>;
  using Acceptor = absl::FunctionRef<bool(doc_id_t)>;
  using CookieProvider = absl::FunctionRef<const PostingMeta*()>;

  // Returns an iterator over terms for a field.
  virtual SeekTermIterator::ptr iterator() const = 0;

  // Feeds `acceptor` the documents containing `term`, stopping when it returns
  // false. Bounds-checks against the field's term range first, and answers a
  // df == 1 term straight from its record -- which is why a primary-key probe
  // goes through here rather than building a term iterator and a postings
  // iterator per key.
  virtual void ReadDocs(bytes_view term, Acceptor acceptor) const = 0;

  // The record of `term`; `docs_count == 0` when the field does not hold it,
  // which no record of a real term has -- a term is in the dictionary because
  // some document contains it. Bounds-checks against the field's term range
  // first and walks the dictionary on the stack, so an exact-match probe costs
  // no iterator at all -- which is what an exact-match filter wants, since it
  // has nowhere to walk to afterwards.
  virtual PostingMeta Lookup(bytes_view term) const = 0;

  // Returns an intersection of a specified automaton and term reader.
  virtual SeekTermIterator::ptr iterator(
    const automaton_table_matcher& matcher) const = 0;

  // Evaluates a union of all docs denoted by cookies supplied via a
  // speciified 'provider'. Each doc is represented by a bit in a
  // specified 'bitset'.
  // A number of bits set.
  // It's up to the caller to allocate enough space for a bitset.
  // This API is experimental.
  virtual size_t BitUnion(CookieProvider provider, uint64_t* bitset) const = 0;

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }

  // Returns field metadata.
  virtual const FieldMeta& meta() const = 0;

  // Returns total number of terms.
  virtual size_t size() const = 0;

  // Returns total number of documents with at least 1 term in a field.
  virtual uint64_t docs_count() const = 0;

  // Returns the least significant term.
  virtual bytes_view min() const = 0;

  // Returns the most significant term.
  virtual bytes_view max() const = 0;

  // Returns true if the field has per-block score bounds persisted.
  virtual bool HasScoreBounds() const = 0;

  // The streams this field's postings live in.
  virtual PostingsHandles Handles() const noexcept = 0;
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
  // for score bounds while the segment is still being written.
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

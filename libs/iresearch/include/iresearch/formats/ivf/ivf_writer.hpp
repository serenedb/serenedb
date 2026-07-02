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

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/formats.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ColumnReader;
class ColWriter;
class ReadContext;
class IvfTermIterator;
class IdxWriter;

struct BuiltIvf {
  bool empty = true;
  uint32_t d = 0;
  uint64_t resident_offset = 0;
  uint64_t resident_size = 0;
  std::vector<doc_id_t> cluster_docs;
  std::vector<uint64_t> cluster_offsets;
  std::vector<float> fine_centroids;
};

class QuantizerWriter;

class IvfBuilder {
 public:
  explicit IvfBuilder(IvfInfo info) : _info{std::move(info)} {}

  BuiltIvf Build(const ColumnReader& vector_column, ReadContext& ctx,
                 IdxWriter& idx, QuantizerWriter* qw) const;

 private:
  IvfInfo _info;
};

class IvfTermReader final : public BasicTermReader, public TermPayloadWriter {
 public:
  IvfTermReader(field_id postings_id, std::span<const doc_id_t> cluster_docs,
                std::span<const uint64_t> cluster_offsets, QuantizerWriter* qw,
                const ColumnReader* vectors, ReadContext* ctx, uint32_t d,
                std::span<const float> fine_centroids);
  ~IvfTermReader() final;

  TermIterator::ptr iterator() const final;
  field_id id() const final { return _meta.id; }
  FieldProperties properties() const final { return _meta; }
  bytes_view(min)() const final { return _min; }
  bytes_view(max)() const final { return _max; }
  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  TermPayloadWriter* PayloadWriter() const final {
    return _qw != nullptr ? const_cast<IvfTermReader*>(this) : nullptr;
  }

  void WriteTermPayload(IndexOutput& out, std::span<const doc_id_t> docs) final;
  void Finish(IndexOutput& out) final;

 private:
  std::span<const doc_id_t> _cluster_docs;
  std::span<const uint64_t> _cluster_offsets;
  QuantizerWriter* _qw;
  const ColumnReader* _vectors;
  ReadContext* _ctx;
  uint32_t _d;
  std::span<const float> _fine_centroids;
  size_t _count;
  size_t _term_idx = 0;
  FieldMeta _meta;
  std::array<byte_type, 4> _min_buf;
  std::array<byte_type, 4> _max_buf;
  bytes_view _min;
  bytes_view _max;
  mutable std::unique_ptr<IvfTermIterator> _it;
};

class IvfWriter {
 public:
  IvfWriter() noexcept;

  ~IvfWriter();

  void OnCommit(ColWriter& cw, IdxWriter& idx,
                std::span<const field_id> column_ids,
                const IndexFieldOptions* field_options);

  bool Empty() const noexcept { return _results.empty(); }

  std::span<const BasicTermReader* const> ClusterReaders(ReadContext& ctx);

 private:
  struct Result {
    field_id postings_id;
    std::vector<doc_id_t> cluster_docs;
    std::vector<uint64_t> cluster_offsets;
    std::vector<float> fine_centroids;
    std::unique_ptr<QuantizerWriter> qw;
    std::unique_ptr<ColumnReader> vector_column;
    uint32_t d = 0;
  };

  std::vector<Result> _results;
  std::vector<std::unique_ptr<IvfTermReader>> _readers;
  std::vector<const BasicTermReader*> _reader_ptrs;
};

}  // namespace irs

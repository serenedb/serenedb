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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_map.h>

#include <deque>
#include <memory>
#include <vector>

#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/postings.hpp"
#include "iresearch/utils/block_pool.hpp"

namespace irs {

class ColWriter;

using int_block_pool = BlockPool<size_t, 8192, ManagedTypedAllocator<size_t>>;

class FieldData : util::Noncopyable {
 public:
  FieldData(field_id id, byte_block_pool::inserter& byte_writer,
            int_block_pool::inserter& int_writer, IndexFeatures index_features,
            ColWriter* col_writer = nullptr,
            NormColumnOptions norm_options = {});

  doc_id_t doc() const noexcept { return _last_doc; }

  const FieldMeta& meta() const noexcept { return _meta; }

  bool empty() const noexcept { return !doc_limits::valid(_last_doc); }

  bool invert(Tokenizer& tokens, doc_id_t id);

  const FieldStats& stats() const noexcept { return _stats; }

  bool seen() const noexcept { return _seen; }
  void seen(bool value) noexcept { _seen = value; }

  IndexFeatures requested_features() const noexcept {
    return _requested_features;
  }

  void compute_features() const;

  bool has_features() const noexcept { return _col_writer; }

 private:
  friend class TermIteratorImpl;
  friend class DocIteratorImpl;
  friend class SortingDocIteratorImpl;
  friend class FieldsData;

  using ProcessTerm = void (FieldData::*)(Posting&, doc_id_t, const OffsAttr*);

  void reset(doc_id_t doc_id);

  void new_term(Posting& p, doc_id_t did, const OffsAttr* offs);
  void add_term(Posting& p, doc_id_t did, const OffsAttr* offs);

  void new_term_random_access(Posting& p, doc_id_t did, const OffsAttr* offs);
  void add_term_random_access(Posting& p, doc_id_t did, const OffsAttr* offs);

  static constexpr ProcessTerm kTermProcessingTables[2][2] = {
    {&FieldData::add_term, &FieldData::new_term},
    {&FieldData::add_term_random_access, &FieldData::new_term_random_access}};

  bool prox_random_access() const noexcept {
    return kTermProcessingTables[1] == _proc_table;
  }

  ColWriter* _col_writer = nullptr;
  uint32_t _norm_row_group_size = 0;
  mutable NormColumnWriter* _norm_writer = nullptr;
  mutable FieldMeta _meta;
  Postings _terms;
  byte_block_pool::inserter* _byte_writer;
  int_block_pool::inserter* _int_writer;
  const ProcessTerm* _proc_table;
  FieldStats _stats;
  IndexFeatures _requested_features{};
  doc_id_t _last_doc{doc_limits::invalid()};
  uint32_t _pos;
  uint32_t _last_pos;
  uint32_t _offs;
  uint32_t _last_start_offs;
  bool _seen{false};
};

class FieldsData : util::Noncopyable {
 private:
  using Fields = std::deque<FieldData, ManagedTypedAllocator<FieldData>>;
  using FieldsMap = absl::flat_hash_map<field_id, FieldData*>;

 public:
  using postings_ref_t = std::vector<const Posting*>;

  FieldsData(IResourceManager& rm, IndexFeatures scorers_features);

  void SetColWriter(ColWriter* w) noexcept { _col_writer = w; }

  // Set on open, re-pointed on an equal resume (see
  // ColWriter::SetFieldOptions).
  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept {
    _field_options = field_options;
  }

  FieldData* emplace(field_id id, IndexFeatures index_features);

  size_t memory_active() const noexcept;
  size_t memory_reserved() const noexcept;

  size_t size() const { return _fields.size(); }
  void flush(burst_trie::FieldWriter& fw, FlushState& state);
  void reset() noexcept;

 private:
  Fields _fields;
  FieldsMap _fields_map;
  postings_ref_t _sorted_postings;
  std::vector<const FieldData*> _sorted_fields;
  byte_block_pool _byte_pool;
  byte_block_pool::inserter _byte_writer;
  int_block_pool _int_pool;  // FIXME why don't to use std::vector<size_t>?
  int_block_pool::inserter _int_writer;
  IndexFeatures _scorers_features;
  ColWriter* _col_writer = nullptr;
  const IndexFieldOptions* _field_options = nullptr;
};

}  // namespace irs

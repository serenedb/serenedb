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

#include <iresearch/formats/formats.hpp>
#include <iresearch/index/field_meta.hpp>
#include <unordered_set>

#include "doc_generator.hpp"

namespace tests {

class Posting {
 public:
  struct Position {
    Position(uint32_t pos, uint32_t start, uint32_t end,
             const irs::bytes_view& pay)
      : pos{pos}, start{start}, end{end}, payload{pay} {}

    bool operator<(const Position& rhs) const { return pos < rhs.pos; }

    uint32_t pos;
    uint32_t start;
    uint32_t end;
    irs::bstring payload;
  };

  explicit Posting(irs::doc_id_t id) : _id{id} {}
  Posting(irs::doc_id_t id, std::set<Position>&& positions)
    : _positions(std::move(positions)), _id(id) {}
  Posting(Posting&& rhs) noexcept = default;
  Posting& operator=(Posting&& rhs) noexcept = default;

  void insert(uint32_t pos, uint32_t offs_start,
              const irs::AttributeProvider& attrs);

  bool operator<(const Posting& rhs) const { return _id < rhs._id; }

  const auto& positions() const { return _positions; }
  irs::doc_id_t id() const { return _id; }
  size_t size() const { return _positions.size(); }

 private:
  friend struct Term;

  std::set<Position> _positions;
  irs::doc_id_t _id;
};

struct Term {
  explicit Term(irs::bytes_view data);

  Posting& insert(irs::doc_id_t id);

  bool operator<(const Term& rhs) const;

  uint64_t docs_count() const { return postings.size(); }

  void sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs);

  std::set<tests::Posting> postings;
  irs::bstring value;
};

struct Field : public irs::FieldMeta {
  struct FeatureInfo {
    irs::field_id id;
    irs::FeatureWriterFactory factory;
    irs::FeatureWriter::ptr writer;
  };

  struct FieldStats : irs::FieldStats {
    uint32_t pos{};
    uint32_t offs{};
  };

  Field(const std::string_view& name, irs::IndexFeatures index_features);
  Field(Field&& rhs) = default;
  Field& operator=(Field&& rhs) = default;

  Term& insert(irs::bytes_view term);
  Term* find(irs::bytes_view term);
  size_t remove(irs::bytes_view term);
  void sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs);

  irs::bytes_view min() const;
  irs::bytes_view max() const;
  uint64_t total_freq() const;

  irs::SeekTermIterator::ptr iterator() const;

  std::vector<FeatureInfo> feature_infos;
  std::set<Term> terms;
  std::unordered_set<irs::doc_id_t> docs;
  FieldStats stats;
};

class ColumnValues {
 public:
  explicit ColumnValues(irs::field_id id, irs::FeatureWriterFactory factory,
                        irs::FeatureWriter* writer)
    : _id{id}, _factory{factory}, _writer{writer} {}

  ColumnValues(std::string name, irs::field_id id)
    : _id{id}, _name{std::move(name)} {}

  void insert(irs::doc_id_t key, irs::bytes_view value);

  irs::field_id id() const noexcept { return _id; }
  std::string_view name() const noexcept {
    return _name.has_value() ? _name.value() : std::string_view{};
  }

  irs::bstring payload() const;

  auto begin() const { return _values.begin(); }
  auto end() const { return _values.end(); }
  auto size() const { return _values.size(); }
  auto empty() const { return _values.empty(); }

  void sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs);
  void rewrite();

 private:
  irs::field_id _id;
  std::optional<std::string> _name;
  mutable std::optional<irs::bstring> _payload;
  std::map<irs::doc_id_t, irs::bstring> _values;
  irs::FeatureWriterFactory _factory;
  irs::FeatureWriter* _writer{};
};

class IndexSegment : irs::util::Noncopyable {
 public:
  using field_map_t = std::map<std::string_view, Field>;
  using columns_t = std::deque<ColumnValues>;  // pointers remain valid
  using named_columns_t = std::map<std::string, ColumnValues*>;

  explicit IndexSegment(const irs::FeatureInfoProvider& features)
    : _field_features{features} {}
  IndexSegment(IndexSegment&& rhs) = default;
  IndexSegment& operator=(IndexSegment&& rhs) = default;

  irs::doc_id_t doc_count() const noexcept { return _count; }
  size_t size() const noexcept { return _fields.size(); }
  auto& fields() const noexcept { return _fields; }
  auto& columns() noexcept { return _columns; }
  auto& columns() const noexcept { return _columns; }
  auto& named_columns() const noexcept { return _named_columns; }
  auto& named_columns() noexcept { return _named_columns; }
  auto& pk() const noexcept { return _sort; };

  template<typename IndexedFieldIterator, typename StoredFieldIterator>
  void insert(IndexedFieldIterator indexed_begin,
              IndexedFieldIterator indexed_end,
              StoredFieldIterator stored_begin, StoredFieldIterator stored_end,
              const Ifield* sorted = nullptr);

  void insert(const Document& doc, size_t count = 1, bool has_sorted = true) {
    for (; count; --count) {
      insert(std::begin(doc.indexed), std::end(doc.indexed),
             std::begin(doc.stored), std::end(doc.stored),
             has_sorted ? doc.sorted.get() : nullptr);
    }
  }

  void sort(const irs::Comparer& comparator);

  void clear() noexcept {
    _fields.clear();
    _count = 0;
  }

 private:
  IndexSegment(const IndexSegment& rhs) noexcept = delete;
  IndexSegment& operator=(const IndexSegment& rhs) noexcept = delete;

  irs::doc_id_t doc() const noexcept { return _count + irs::doc_limits::min(); }

  void insert_indexed(const Ifield& field);
  void insert_stored(const Ifield& field);
  void insert_sorted(const Ifield* field);
  void compute_features();

  irs::FeatureInfoProvider _field_features;
  named_columns_t _named_columns;
  std::vector<std::tuple<irs::bstring, irs::doc_id_t, irs::doc_id_t>> _sort;
  std::vector<const Field*> _id_to_field;
  std::set<Field*> _doc_fields;
  field_map_t _fields;
  columns_t _columns;
  irs::bstring _buf;
  irs::doc_id_t _count{};
  irs::doc_id_t _empty_count{};
};

template<typename IndexedFieldIterator, typename StoredFieldIterator>
void IndexSegment::insert(IndexedFieldIterator indexed_begin,
                          IndexedFieldIterator indexed_end,
                          StoredFieldIterator stored_begin,
                          StoredFieldIterator stored_end,
                          const Ifield* sorted /*= nullptr*/) {
  // reset field per-document state
  _doc_fields.clear();
  for (auto it = indexed_begin; it != indexed_end; ++it) {
    auto field = _fields.find(it->Name());

    if (field != _fields.end()) {
      field->second.stats = {};
    }
  }

  for (; indexed_begin != indexed_end; ++indexed_begin) {
    insert_indexed(*indexed_begin);
  }

  for (; stored_begin != stored_end; ++stored_begin) {
    insert_stored(*stored_begin);
  }

  insert_sorted(sorted);

  compute_features();

  ++_count;
}

using index_t = std::vector<IndexSegment>;

void AssertColumnstore(
  const irs::Directory& dir, irs::Format::ptr codec,
  const index_t& expected_index,
  size_t skip = 0);  // do not validate the first 'skip' segments

void AssertColumnstore(
  irs::IndexReader::ptr actual_index, const index_t& expected_index,
  size_t skip = 0);  // do not validate the first 'skip' segments

void AssertIndex(irs::IndexReader::ptr actual_index,
                 const index_t& expected_index, irs::IndexFeatures features,
                 size_t skip = 0,  // do not validate the first 'skip' segments
                 irs::automaton_table_matcher* matcher = nullptr);

void AssertIndex(const irs::Directory& dir, irs::Format::ptr codec,
                 const index_t& index, irs::IndexFeatures features,
                 size_t skip = 0,  // no not validate the first 'skip' segments
                 irs::automaton_table_matcher* matcher = nullptr);

}  // namespace tests

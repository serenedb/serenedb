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

#include "iresearch/index/merge/postings_unit.hpp"

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/field_writer.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/merged_postings.hpp"
#include "iresearch/index/merge/merged_terms.tpp"
#include "iresearch/index/merge/partition_tasks.hpp"
#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

struct DictKey {
  field_id id;
  DictMember member;

  auto operator<=>(const DictKey&) const = default;
};

template<typename Postings>
class CompoundFieldIterator final : public BasicTermReader {
 public:
  static constexpr const size_t kProgressStepFields = size_t{1};

  CompoundFieldIterator(size_t size, const MergeProgress& progress,
                        const IndexFieldOptions* field_options)
    : _term_itr(progress),
      _progress(progress, kProgressStepFields),
      _field_options{field_options} {
    _field_iterators.reserve(size);
    _field_iterator_mask.reserve(size);
  }

  void Add(const SubReader& reader, const DocRemap& remap);
  bool Next();

  const FieldMeta& Meta() const noexcept {
    SDB_ASSERT(_current_meta);
    return *_current_meta;
  }

  DictRef Dict() const noexcept {
    return {.layout = _layout, .member = _current.member};
  }

  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return _props; }
  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final;
  TermOnlyIterator::ptr iterator() const final;

  bool Aborted() const {
    return !static_cast<bool>(_progress) || _term_itr.Aborted();
  }

  void SetProperties(FieldProperties props) noexcept { _props = props; }

 private:
  struct FieldIteratorImpl {
    std::span<const field_id> ids;
    size_t pos;
    size_t at;
    const SubReader* reader;
    const DocRemap* remap;
  };

  struct TermIteratorImpl {
    size_t itr_id;
    const TermReader* reader;
  };

  const TermReader* Current(FieldIteratorImpl& it) const;

  FieldProperties _props;
  DictKey _current{.id = field_limits::invalid(), .member = {}};
  DictLayout _layout = DictLayout::String;
  FieldMeta _synthesized_meta;
  const FieldMeta* _current_meta{&FieldMeta::kEmpty};
  std::vector<TermIteratorImpl> _field_iterator_mask;
  std::vector<FieldIteratorImpl> _field_iterators;
  mutable CompoundTermIterator<Postings> _term_itr;
  ProgressTracker _progress;
  const IndexFieldOptions* _field_options;
};

template<typename Postings>
void CompoundFieldIterator<Postings>::Add(const SubReader& reader,
                                          const DocRemap& remap) {
  _field_iterators.emplace_back(reader.field_ids(), size_t{0}, size_t{0},
                                &reader, &remap);
}

template<typename Postings>
const TermReader* CompoundFieldIterator<Postings>::Current(
  FieldIteratorImpl& it) const {
  for (; it.pos < it.ids.size(); ++it.pos, it.at = 0) {
    const auto id = it.ids[it.pos];
    if (_field_options &&
        _field_options->GetFieldKind(id) != FieldKind::Index) {
      continue;
    }
    if (const auto fields = it.reader->fields(id); it.at < fields.size()) {
      return fields[it.at];
    }
  }
  return nullptr;
}

template<typename Postings>
bool CompoundFieldIterator<Postings>::Next() {
  if (!Aborted()) {
    _progress();
  }

  const DictKey none{.id = field_limits::invalid(), .member = {}};
  if (Aborted()) {
    _field_iterator_mask.clear();
    _field_iterators.clear();
    _current = none;
    return false;
  }

  for (auto& entry : _field_iterator_mask) {
    ++_field_iterators[entry.itr_id].at;
  }

  _field_iterator_mask.clear();
  _current = none;

  for (size_t i = 0, count = _field_iterators.size(); i != count; ++i) {
    auto& field_itr = _field_iterators[i];
    const auto* field_terms = Current(field_itr);
    if (!field_terms) {
      continue;
    }
    const auto dict = field_terms->meta().dict;
    const DictKey key{.id = field_itr.ids[field_itr.pos],
                      .member = dict.member};
    const bool first = !field_limits::valid(_current.id) || key < _current;
    if (!first && key != _current) {
      continue;
    }
    if (first) {
      _field_iterator_mask.clear();
      _layout = dict.layout;
    }
    SDB_ASSERT(dict.layout == _layout);
    const auto features = field_terms->meta().index_features;
    _synthesized_meta.index_features =
      _field_iterator_mask.empty()
        ? features
        : _synthesized_meta.index_features & features;
    key_width =
      _field_iterator_mask.empty() || key_width == field_terms->key_width
        ? field_terms->key_width
        : 0;
    _current = key;
    _field_iterator_mask.emplace_back(TermIteratorImpl{i, field_terms});
  }

  if (!field_limits::valid(_current.id)) {
    _current_meta = &FieldMeta::kEmpty;
    return false;
  }
  _synthesized_meta.id = _current.id;
  _current_meta = &_synthesized_meta;
  return true;
}

template<typename Postings>
void CompoundFieldIterator<Postings>::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  if (_field_iterator_mask.empty()) {
    return;
  }
  const uint64_t share = bytes / _field_iterator_mask.size();
  for (const auto& segment : _field_iterator_mask) {
    segment.reader->SampleTerms(share, visit);
  }
}

template<typename Postings>
TermOnlyIterator::ptr CompoundFieldIterator<Postings>::iterator() const {
  _term_itr.Reset(Meta());
  for (const auto& segment : _field_iterator_mask) {
    _term_itr.Add(*(segment.reader), *(_field_iterators[segment.itr_id].remap));
  }
  return memory::to_managed<TermOnlyIterator>(_term_itr);
}

TermRangeMerge* RangeOf(std::span<const std::unique_ptr<TermRangeMerge>> ranges,
                        field_id id, DictRef dict) {
  if (dict.member != DictMember::Strings) {
    return nullptr;
  }
  for (const auto& range : ranges) {
    if (range->Field() == id) {
      return range.get();
    }
  }
  return nullptr;
}

void WriteParts(term_dict::FieldWriter& writer,
                const term_dict::PartsField& field,
                duckdb::TaskScheduler& scheduler, size_t threads) {
  const auto count = field.parts.size();
  std::vector<std::unique_ptr<term_dict::FieldWriter>> built(count);
  absl::Mutex mutex;
  size_t appended = 0;
  ForEachPartition(count, scheduler, threads, [&](size_t part) {
    auto run = std::make_unique<term_dict::FieldWriter>(field, part);
    absl::MutexLock lock{&mutex};
    built[part] = std::move(run);
    for (; appended != count && built[appended]; ++appended) {
      writer.AppendPart(field, appended, *built[appended]);
      built[appended].reset();
    }
  });
  writer.EndParts(field);
}

template<typename Postings>
bool WriteFields(const irs::FlushState& flush_state,
                 CompoundFieldIterator<Postings>& field_itr,
                 const MergedNormIdMap& merged_norm_ids, IdxWriter& idx,
                 std::span<const BasicTermReader* const> extra,
                 std::span<const std::unique_ptr<TermRangeMerge>> ranges,
                 uint64_t keyed_dicts, duckdb::TaskScheduler& scheduler,
                 size_t threads) {
  SDB_ASSERT(
    absl::c_all_of(ranges, [](const auto& range) { return range->Done(); }));
  auto field_writer =
    std::make_unique<term_dict::FieldWriter>(/*compaction=*/true);
  field_writer->SetIdxWriter(idx);
  field_writer->prepare(flush_state, keyed_dicts + extra.size());

  std::vector<const BasicTermReader*> sorted_extra(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });
  const auto extra_key = [](const BasicTermReader* reader) {
    return DictKey{
      .id = reader->id(),
      .member = StringLayout::kStrings.member,
    };
  };
  const auto write_extra = [&](const BasicTermReader& reader) {
    field_writer->write(StringLayout::kStrings, reader);
  };
  size_t ei = 0;
  const size_t en = sorted_extra.size();

  while (field_itr.Next()) {
    const auto dict = field_itr.Dict();
    const DictKey key{.id = field_itr.Meta().id, .member = dict.member};
    while (ei < en && extra_key(sorted_extra[ei]) < key) {
      write_extra(*sorted_extra[ei]);
      ++ei;
    }

    if (ei < en && extra_key(sorted_extra[ei]) == key) {
      SDB_ASSERT(dict.layout == DictLayout::String);
      write_extra(*sorted_extra[ei]);
      ++ei;
      continue;
    }

    if (IsSubsetOf(IndexFeatures::Vec, field_itr.Meta().index_features)) {
      continue;
    }

    if (!IsTermMember(dict.member)) {
      if (auto terms = field_itr.iterator(); terms->next()) {
        SDB_ASSERT(terms->value() == kSlotTerm);
        field_writer->WriteSlot(key.id, dict,
                                *terms->postings(IndexFeatures::None));
      }
      continue;
    }

    field_itr.SetProperties(MergedFieldProperties(
      key.id, field_itr.Meta().index_features, merged_norm_ids));

    if (auto* range = RangeOf(ranges, key.id, dict)) {
      const auto parts = range->Parts();
      WriteParts(
        *field_writer,
        field_writer->BeginParts(dict, field_itr, parts, range->DocCount()),
        scheduler, threads);
      range->ReleaseParts();
      continue;
    }

    field_writer->write(dict, field_itr);
  }

  while (ei < en) {
    write_extra(*sorted_extra[ei]);
    ++ei;
  }

  field_writer->end();
  field_writer.reset();

  return !field_itr.Aborted();
}

}  // namespace

FieldProperties MergedFieldProperties(field_id id, IndexFeatures features,
                                      const MergedNormIdMap& merged_norm_ids) {
  FieldProperties props{.index_features = features};
  if (IsSubsetOf(IndexFeatures::Norm, features)) {
    if (const auto it = merged_norm_ids.find(id); it != merged_norm_ids.end()) {
      props.norm = it->second;
    }
  }
  return props;
}

bool WritePostings(const FlushState& flush_state, const MergeDocMap& inputs,
                   bool sorted, const IndexFieldOptions* field_options,
                   const MergeProgress& progress,
                   const MergedNormIdMap& merged_norm_ids, IdxWriter& idx,
                   std::span<const BasicTermReader* const> extra,
                   std::span<const std::unique_ptr<TermRangeMerge>> ranges,
                   uint64_t keyed_dicts, duckdb::TaskScheduler& scheduler,
                   size_t threads) {
  const auto write = [&](auto postings) {
    CompoundFieldIterator<typename decltype(postings)::type> fields{
      inputs.size(), progress, field_options};
    for (const auto& input : inputs) {
      fields.Add(*input.reader, input.remap);
    }
    return WriteFields(flush_state, fields, merged_norm_ids, idx, extra, ranges,
                       keyed_dicts, scheduler, threads);
  };
  return sorted ? write(std::type_identity<SortedPostings>{})
                : write(std::type_identity<CompoundPostings>{});
}

}  // namespace irs

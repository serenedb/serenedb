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

#include <utility>
#include <variant>
#include <vector>

#include "iresearch/formats/basic_term_reader.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/merged_postings.hpp"
#include "iresearch/index/merge/merged_terms.hpp"
#include "iresearch/index/merge/term_ranges.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

template<typename Postings>
class CompoundFieldIterator final : public BasicTermReader {
 public:
  static constexpr const size_t kProgressStepFields = size_t{1};

  explicit CompoundFieldIterator(size_t size, const MergeProgress& progress)
    : _term_itr(progress), _progress(progress, kProgressStepFields) {
    _field_iterators.reserve(size);
    _field_iterator_mask.reserve(size);
  }

  void Add(const SubReader& reader, const DocRemap& remap);
  bool Next();

  const FieldMeta& Meta() const noexcept {
    SDB_ASSERT(_current_meta);
    return *_current_meta;
  }

  field_id id() const noexcept final { return Meta().id; }
  FieldProperties properties() const noexcept final { return _props; }
  bytes_view min() const noexcept final { return _term_itr.MinTerm(); }
  bytes_view max() const noexcept final { return _term_itr.MaxTerm(); }
  TermOnlyIterator::ptr iterator() const final;

  bool Aborted() const {
    return !static_cast<bool>(_progress) || _term_itr.Aborted();
  }

  void SetProperties(FieldProperties props) noexcept { _props = props; }

 private:
  struct FieldIteratorImpl {
    std::span<const field_id> ids;
    size_t pos;
    const SubReader* reader;
    const DocRemap* remap;
  };

  struct TermIteratorImpl {
    size_t itr_id;
    const TermReader* reader;
  };

  FieldProperties _props;
  field_id _current_id{field_limits::invalid()};
  FieldMeta _synthesized_meta;
  const FieldMeta* _current_meta{&FieldMeta::kEmpty};
  std::vector<TermIteratorImpl> _field_iterator_mask;
  std::vector<FieldIteratorImpl> _field_iterators;
  mutable CompoundTermIterator<Postings> _term_itr;
  ProgressTracker _progress;
};

template<typename Postings>
void CompoundFieldIterator<Postings>::Add(const SubReader& reader,
                                          const DocRemap& remap) {
  _field_iterators.emplace_back(reader.field_ids(), size_t{0}, &reader, &remap);
}

template<typename Postings>
bool CompoundFieldIterator<Postings>::Next() {
  if (!Aborted()) {
    _progress();
  }

  if (Aborted()) {
    _field_iterator_mask.clear();
    _field_iterators.clear();
    _current_id = field_limits::invalid();
    return false;
  }

  for (auto& entry : _field_iterator_mask) {
    ++_field_iterators[entry.itr_id].pos;
  }

  _field_iterator_mask.clear();
  _current_id = field_limits::invalid();

  for (size_t i = 0, count = _field_iterators.size(); i != count; ++i) {
    auto& field_itr = _field_iterators[i];
    if (field_itr.pos >= field_itr.ids.size()) {
      continue;
    }
    const auto id = field_itr.ids[field_itr.pos];
    if (field_limits::valid(_current_id) && id > _current_id) {
      continue;
    }
    const auto* field_terms = field_itr.reader->field(id);
    SDB_ASSERT(field_terms);
    if (field_limits::valid(_current_id) && id < _current_id) {
      _field_iterator_mask.clear();
    }
    const auto features = field_terms->meta().index_features;
    _synthesized_meta.index_features =
      _field_iterator_mask.empty()
        ? features
        : _synthesized_meta.index_features & features;
    _current_id = id;
    _field_iterator_mask.emplace_back(TermIteratorImpl{i, field_terms});
  }

  if (!field_limits::valid(_current_id)) {
    _current_meta = &FieldMeta::kEmpty;
    return false;
  }
  _synthesized_meta.id = _current_id;
  _current_meta = &_synthesized_meta;
  return true;
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
                        field_id id) {
  for (const auto& range : ranges) {
    if (range->Field() == id) {
      return range.get();
    }
  }
  return nullptr;
}

template<typename Postings>
bool WriteFields(const irs::FlushState& flush_state,
                 CompoundFieldIterator<Postings>& field_itr,
                 const MergedNormIdMap& merged_norm_ids, IResourceManager& rm,
                 IdxWriter& idx, std::span<const BasicTermReader* const> extra,
                 std::span<const std::unique_ptr<TermRangeMerge>> ranges) {
  for (const auto& range : ranges) {
    if (!range->Done()) {
      return false;
    }
  }
  auto field_writer =
    std::make_unique<burst_trie::FieldWriter>(/*compaction=*/true, rm);
  field_writer->SetIdxWriter(idx);
  field_writer->prepare(flush_state);

  std::vector<const BasicTermReader*> sorted_extra(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });
  size_t ei = 0;
  const size_t en = sorted_extra.size();

  while (field_itr.Next()) {
    const auto fid = field_itr.Meta().id;
    while (ei < en && sorted_extra[ei]->id() < fid) {
      field_writer->write(*sorted_extra[ei]);
      ++ei;
    }

    if (ei < en && sorted_extra[ei]->id() == fid) {
      field_writer->write(*sorted_extra[ei]);
      ++ei;
      continue;
    }

    if (IsSubsetOf(IndexFeatures::Vec, field_itr.Meta().index_features)) {
      continue;
    }

    FieldProperties props;
    props.index_features = field_itr.Meta().index_features;

    if (IsSubsetOf(IndexFeatures::Norm, props.index_features)) {
      const auto it = merged_norm_ids.find(field_itr.Meta().id);
      if (it != merged_norm_ids.end()) {
        props.norm = it->second;
      }
    }

    if (auto* range = RangeOf(ranges, fid)) {
      field_writer->write(fid, props, range->Parts(), range->DocCount());
      range->ReleaseParts();
      continue;
    }

    field_itr.SetProperties(props);
    field_writer->write(field_itr);
  }

  while (ei < en) {
    field_writer->write(*sorted_extra[ei]);
    ++ei;
  }

  field_writer->end();
  field_writer.reset();

  return !field_itr.Aborted();
}

}  // namespace

struct PostingsMerger::Impl {
  using Fields = std::variant<CompoundFieldIterator<CompoundPostings>,
                              CompoundFieldIterator<SortedPostings>>;

  Impl(size_t inputs, const MergeProgress& progress, bool sorted)
    : fields{sorted ? Fields{std::in_place_index<1>, inputs, progress}
                    : Fields{std::in_place_index<0>, inputs, progress}} {}

  Fields fields;
};

PostingsMerger::PostingsMerger(size_t inputs, const MergeProgress& progress,
                               bool sorted)
  : _impl{std::make_unique<Impl>(inputs, progress, sorted)} {}

PostingsMerger::~PostingsMerger() = default;

void PostingsMerger::Add(const SubReader& reader, const DocRemap& remap) {
  std::visit([&](auto& fields) { fields.Add(reader, remap); }, _impl->fields);
}

bool PostingsMerger::Write(
  const FlushState& flush_state, const MergedNormIdMap& merged_norm_ids,
  IResourceManager& rm, IdxWriter& idx,
  std::span<const BasicTermReader* const> extra,
  std::span<const std::unique_ptr<TermRangeMerge>> ranges) {
  return std::visit(
    [&](auto& fields) {
      return WriteFields(flush_state, fields, merged_norm_ids, rm, idx, extra,
                         ranges);
    },
    _impl->fields);
}

}  // namespace irs

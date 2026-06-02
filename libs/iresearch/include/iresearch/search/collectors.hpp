////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <span>
#include <vector>

#include "basics/down_cast.h"
#include "basics/shared.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

inline size_t GetStatsSize(const Scorer* scorer) noexcept {
  return scorer ? scorer->stats_size() : 0;
}

struct FieldCollector {
  void Collect(const TermReader& field) noexcept;

  uint64_t docs_with_field = 0;
  uint64_t total_term_freq = 0;
};

struct TermCollector {
  uint64_t docs_with_term = 0;
  uint64_t total_term_freq = 0;

  void Collect(const AttributeProvider& term_attrs) noexcept;
};

class CollectorBase {
 public:
  CollectorBase(const Scorer* scorer) noexcept : _scorer{scorer} {}
  const Scorer* GetScorer() const noexcept { return _scorer; }

 protected:
  void Finish(bstring& stats_buf, const TermCollector* collector,
              const FieldCollector* field_data) const {
    SDB_ASSERT(_scorer);
    SDB_ASSERT(field_data);
    SDB_ASSERT(collector);
    stats_buf.resize(GetStatsSize(_scorer));
    _scorer->collect(stats_buf.data(), field_data, collector);
  }

  bool HasScorer() const noexcept { return _scorer != nullptr; }

 private:
  const Scorer* _scorer = nullptr;
};

class FlatTermBuffer {
 public:
  FlatTermBuffer(size_t size) : _collectors{size} {}

  size_t Size() const noexcept { return _collectors.size(); }
  bool Empty() const noexcept { return _collectors.empty(); }

  size_t PushBack() {
    _collectors.emplace_back();
    return _collectors.size() - 1;
  }

  void Collect(size_t term_idx, const AttributeProvider& attrs) {
    SDB_ASSERT(term_idx < _collectors.size());
    _collectors[term_idx].Collect(attrs);
  }

  const TermCollector& Get(size_t term_idx) const {
    SDB_ASSERT(term_idx < _collectors.size());
    return _collectors[term_idx];
  }

  TermCollector& Get(size_t term_idx) {
    SDB_ASSERT(term_idx < _collectors.size());
    return _collectors[term_idx];
  }

 private:
  std::vector<TermCollector> _collectors;
};

class TermCollectorsFlat : public CollectorBase, public FlatTermBuffer {
 public:
  TermCollectorsFlat(const Scorer* scorer, size_t size)
    : CollectorBase{scorer}, FlatTermBuffer{scorer ? size : 0} {}

  size_t PushBack() {
    if (!HasScorer()) {
      return 0;
    }
    FlatTermBuffer::PushBack();
    return Size() - 1;
  }

  void Collect(size_t term_idx, const AttributeProvider& attrs) {
    if (HasScorer()) {
      SDB_ASSERT(term_idx < Size());
      Get(term_idx).Collect(attrs);
    }
  }

  void Finish(bstring& stats_buf, size_t term_idx,
              const FieldCollector* field_data) const {
    if (HasScorer()) {
      SDB_ASSERT(term_idx < Size());
      CollectorBase::Finish(stats_buf, &Get(term_idx), field_data);
    }
  }
};

class TermCollectorsVariadic : public CollectorBase {
 public:
  TermCollectorsVariadic(const Scorer* scorer, size_t phrase_size)
    : CollectorBase{scorer}, _collectors(phrase_size, FlatTermBuffer{0}) {}

  size_t Size() const noexcept { return _collectors.size(); }

  FlatTermBuffer* GetCollector(size_t idx) {
    if (!HasScorer()) {
      return nullptr;
    }
    SDB_ASSERT(idx < _collectors.size());
    return &_collectors[idx];
  }

  void Finish(bstring& stats_buf, size_t part_idx,
              const FieldCollector* field_data) const {
    if (!HasScorer()) {
      return;
    }
    SDB_ASSERT(part_idx < _collectors.size());
    const auto& part = _collectors[part_idx];
    for (size_t i = 0, size = part.Size(); i < size; ++i) {
      CollectorBase::Finish(stats_buf, &part.Get(i), field_data);
    }
  }

 private:
  std::vector<FlatTermBuffer> _collectors;
};

static_assert(std::is_nothrow_move_constructible_v<TermCollectorsFlat>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectorsFlat>);
static_assert(std::is_nothrow_move_constructible_v<TermCollectorsVariadic>);
static_assert(std::is_nothrow_move_assignable_v<TermCollectorsVariadic>);

class StatsBuffer {
 public:
  using Storage = ManagedVector<bstring>;

  StatsBuffer() noexcept : _stats{{IResourceManager::gNoop}} {}
  StatsBuffer(Storage&& stats, const Scorer* scorer)
    : _stats{std::move(stats)}, _scorer{scorer} {}

  bool HasScorer() const noexcept { return _scorer != nullptr; }
  bytes_view GetStats() const noexcept {
    return _stats.empty() ? bytes_view{} : bytes_view{_stats.front()};
  }
  const Storage& GetAllStats() const noexcept { return _stats; }
  const Scorer* GetScorer() const noexcept { return _scorer; }

  void AddChild(StatsBuffer&& child) {
    _children.emplace_back(std::move(child));
  }
  const StatsBuffer& Child(size_t i) const noexcept { return _children[i]; }
  size_t ChildCount() const noexcept { return _children.size(); }

 private:
  Storage _stats;
  const Scorer* _scorer = nullptr;
  std::vector<StatsBuffer> _children;
};

FieldCollector MergeFieldCollectors(
  std::span<const FieldCollector> collectors) noexcept;

TermCollector MergeTermCollectors(
  std::span<const TermCollector> collectors) noexcept;

void MergeFlatTermBuffers(std::span<const FlatTermBuffer> buffers,
                          FlatTermBuffer& out);

class PrepareCollector {
 public:
  using ptr = std::unique_ptr<PrepareCollector>;

  virtual ~PrepareCollector() = default;

  virtual void Merge(PrepareCollector&& other) = 0;

  virtual StatsBuffer Finish(IResourceManager& memory) = 0;
};

class TermsCollector final : public PrepareCollector {
 public:
  TermsCollector(const Scorer* scorer, size_t size) : _terms{scorer, size} {}

  FieldCollector& Field() noexcept { return _field; }
  TermCollectorsFlat& Terms() noexcept { return _terms; }

  void Merge(PrepareCollector&& other) final {
    auto& rhs = sdb::basics::downCast<TermsCollector>(other);
    const FieldCollector fields[]{_field, rhs._field};
    _field = MergeFieldCollectors(fields);
    const FlatTermBuffer buffers[]{rhs._terms};
    MergeFlatTermBuffers(buffers, _terms);
  }

  StatsBuffer Finish(IResourceManager& memory) final {
    StatsBuffer::Storage stats{{memory}};
    stats.reserve(_terms.Size());
    for (size_t i = 0, size = _terms.Size(); i < size; ++i) {
      bstring stat;
      _terms.Finish(stat, i, &_field);
      stats.emplace_back(std::move(stat));
    }
    return StatsBuffer{std::move(stats), _terms.GetScorer()};
  }

 private:
  FieldCollector _field;
  TermCollectorsFlat _terms;
};

class NGramCollector final : public PrepareCollector {
 public:
  NGramCollector(const Scorer* scorer, size_t size) : _terms{scorer, size} {}

  FieldCollector& Field() noexcept { return _field; }
  TermCollectorsFlat& Terms() noexcept { return _terms; }

  void Merge(PrepareCollector&& other) final {
    auto& rhs = sdb::basics::downCast<NGramCollector>(other);
    const FieldCollector fields[]{_field, rhs._field};
    _field = MergeFieldCollectors(fields);
    const FlatTermBuffer buffers[]{rhs._terms};
    MergeFlatTermBuffers(buffers, _terms);
  }

  StatsBuffer Finish(IResourceManager& memory) final {
    StatsBuffer::Storage stats{{memory}};
    if (_terms.GetScorer()) {
      bstring stat(GetStatsSize(_terms.GetScorer()), 0);
      for (size_t i = 0, size = _terms.Size(); i < size; ++i) {
        _terms.Finish(stat, i, &_field);
      }
      stats.emplace_back(std::move(stat));
    }
    return StatsBuffer{std::move(stats), _terms.GetScorer()};
  }

 private:
  FieldCollector _field;
  TermCollectorsFlat _terms;
};

class VariadicTermsCollector final : public PrepareCollector {
 public:
  VariadicTermsCollector(const Scorer* scorer, size_t phrase_size)
    : _scorer{scorer}, _terms{scorer, phrase_size} {}

  FieldCollector& Field() noexcept { return _field; }
  TermCollectorsVariadic& Terms() noexcept { return _terms; }

  void Merge(PrepareCollector&& other) final {
    auto& rhs = sdb::basics::downCast<VariadicTermsCollector>(other);
    const FieldCollector fields[]{_field, rhs._field};
    _field = MergeFieldCollectors(fields);
    for (size_t i = 0, size = _terms.Size(); i < size; ++i) {
      auto* dst = _terms.GetCollector(i);
      auto* src = rhs._terms.GetCollector(i);
      if (dst && src && dst->Size() == src->Size()) {
        const FlatTermBuffer buffers[]{*src};
        MergeFlatTermBuffers(buffers, *dst);
      }
    }
  }

  StatsBuffer Finish(IResourceManager& memory) final {
    StatsBuffer::Storage stats{{memory}};
    if (_scorer) {
      bstring stat(GetStatsSize(_scorer), 0);
      for (size_t i = 0, size = _terms.Size(); i < size; ++i) {
        _terms.Finish(stat, i, &_field);
      }
      stats.emplace_back(std::move(stat));
    }
    return StatsBuffer{std::move(stats), _scorer};
  }

 private:
  const Scorer* _scorer;
  FieldCollector _field;
  TermCollectorsVariadic _terms;
};

class AllCollector final : public PrepareCollector {
 public:
  explicit AllCollector(const Scorer* scorer) noexcept : _scorer{scorer} {}

  void Merge(PrepareCollector&& /*other*/) final {}

  StatsBuffer Finish(IResourceManager& memory) final {
    StatsBuffer::Storage stats{{memory}};
    if (_scorer) {
      bstring stat(GetStatsSize(_scorer), 0);
      _scorer->collect(stat.data(), nullptr, nullptr);
      stats.emplace_back(std::move(stat));
    }
    return StatsBuffer{std::move(stats), _scorer};
  }

 private:
  const Scorer* _scorer;
};

class NoopCollector final : public PrepareCollector {
 public:
  void Merge(PrepareCollector&& /*other*/) final {}

  StatsBuffer Finish(IResourceManager& memory) final {
    return StatsBuffer{StatsBuffer::Storage{{memory}}, nullptr};
  }
};

class CompoundCollector final : public PrepareCollector {
 public:
  explicit CompoundCollector(const Scorer* scorer = nullptr) noexcept
    : _scorer{scorer} {}

  void Add(PrepareCollector::ptr child) {
    _children.emplace_back(std::move(child));
  }

  const Scorer* GetScorer() const noexcept { return _scorer; }

  PrepareCollector& Child(size_t i) noexcept { return *_children[i]; }
  size_t Size() const noexcept { return _children.size(); }

  void Merge(PrepareCollector&& other) final {
    auto& rhs = sdb::basics::downCast<CompoundCollector>(other);
    SDB_ASSERT(_children.size() == rhs._children.size());
    for (size_t i = 0, size = _children.size(); i < size; ++i) {
      _children[i]->Merge(std::move(*rhs._children[i]));
    }
  }

  StatsBuffer Finish(IResourceManager& memory) final {
    StatsBuffer stats{StatsBuffer::Storage{{memory}}, nullptr};
    for (auto& child : _children) {
      stats.AddChild(child->Finish(memory));
    }
    return stats;
  }

 private:
  const Scorer* _scorer = nullptr;
  std::vector<PrepareCollector::ptr> _children;
};

}  // namespace irs

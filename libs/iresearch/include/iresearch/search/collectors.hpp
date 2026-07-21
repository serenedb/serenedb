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

#include <absl/functional/function_ref.h>

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

  static void Merge(FieldCollector& dst, const FieldCollector& src);

  uint64_t docs_with_field = 0;
  uint64_t total_term_freq = 0;
};

struct TermCollector {
  void Collect(const AttributeProvider& term_attrs) noexcept;

  static void Merge(TermCollector& dst, const TermCollector& src);

  uint64_t docs_with_term = 0;
  uint64_t total_term_freq = 0;
};

void FillStats(bstring& stats_buf, const Scorer* scorer,
               const FieldCollector* field, const TermCollector* term);

class StatsBuffer {
 public:
  using Storage = ManagedVector<bstring>;

  StatsBuffer() noexcept : _stats{{IResourceManager::gNoop}} {}
  StatsBuffer(Storage&& stats, const Scorer* scorer)
    : _stats{std::move(stats)}, _scorer{scorer} {}

  static const StatsBuffer& Empty() noexcept {
    static const StatsBuffer kEmpty;
    return kEmpty;
  }

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

class PrepareCollector {
 public:
  using ptr = std::unique_ptr<PrepareCollector>;

  using MergeSink = absl::FunctionRef<void(PrepareCollector&)>;
  using MergeVisitor = absl::FunctionRef<void(MergeSink)>;

  virtual ~PrepareCollector() = default;

  virtual void Merge(PrepareCollector&& other) = 0;

  virtual void MergeAll(MergeVisitor visit) {
    visit([this](PrepareCollector& other) { Merge(std::move(other)); });
  }

  virtual StatsBuffer Finish(IResourceManager& memory) = 0;

  virtual const Scorer* GetScorer() const noexcept { return nullptr; }
};

class FieldPrepareCollector : public PrepareCollector {
 public:
  explicit FieldPrepareCollector(const Scorer* scorer) noexcept
    : _scorer{scorer} {}

  auto& Field() noexcept { return _field; }

  void Merge(PrepareCollector&& other) override;

  StatsBuffer Finish(IResourceManager& memory) override;

  const Scorer* GetScorer() const noexcept final { return _scorer; }

 protected:
  FieldCollector _field;
  const Scorer* _scorer;
};

class ByTermsCollector final : public FieldPrepareCollector {
 public:
  using TermsData = absl::InlinedVector<TermCollector, 1>;
  ByTermsCollector(const Scorer* scorer, size_t size)
    : FieldPrepareCollector{scorer}, _terms(size) {}

  auto& Terms() noexcept { return _terms; }

  void Merge(PrepareCollector&& other) final;

  StatsBuffer Finish(IResourceManager& memory) final;

 private:
  TermsData _terms;
};

class PhraseCollector final : public FieldPrepareCollector {
 public:
  PhraseCollector(const Scorer* scorer, size_t size)
    : FieldPrepareCollector{scorer}, _parts(size) {}

  auto& Part(size_t i) noexcept { return _parts[i]; }
  auto Size() const noexcept { return _parts.size(); }

  void Merge(PrepareCollector&& other) final;

  StatsBuffer Finish(IResourceManager& memory) final;

 private:
  std::vector<std::vector<TermCollector>> _parts;
};

class AllCollector final : public PrepareCollector {
 public:
  explicit AllCollector(const Scorer* scorer) noexcept : _scorer{scorer} {}

  const Scorer* GetScorer() const noexcept final { return _scorer; }

  void Merge(PrepareCollector&&) final {}

  StatsBuffer Finish(IResourceManager& memory) final;

 private:
  const Scorer* _scorer;
};

class NoopCollector final : public PrepareCollector {
 public:
  void Merge(PrepareCollector&&) final {}

  StatsBuffer Finish(IResourceManager& memory) final;
};

class CompoundCollector final : public PrepareCollector {
 public:
  explicit CompoundCollector(const Scorer* scorer = nullptr) noexcept
    : _scorer{scorer} {}

  void Add(PrepareCollector::ptr child) {
    _children.emplace_back(std::move(child));
  }

  const Scorer* GetScorer() const noexcept { return _scorer; }

  auto& Child(size_t i) noexcept { return *_children[i]; }
  auto Size() const noexcept { return _children.size(); }

  void Merge(PrepareCollector&& other) final;

  void MergeAll(MergeVisitor visit) final;

  StatsBuffer Finish(IResourceManager& memory) final;

 private:
  const Scorer* _scorer = nullptr;
  std::vector<PrepareCollector::ptr> _children;
};

}  // namespace irs

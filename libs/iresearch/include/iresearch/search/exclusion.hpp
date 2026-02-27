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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/iterators.hpp"

namespace irs {

template<typename IncludeAdapter, typename ExcludeAdapters>
class Exclusion : public DocIterator {
 public:
  Exclusion(IncludeAdapter incl, ExcludeAdapters excl) noexcept
    : _incl{std::move(incl)}, _excl{std::move(excl)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _incl.GetMutable(type);
  }

  doc_id_t value() const noexcept final { return _incl.value(); }

  doc_id_t advance() final {
    const auto incl = _incl.advance();
    return converge(incl);
  }

  doc_id_t seek(doc_id_t target) final {
    if (const auto doc = value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto incl = _incl.seek(target);
    return converge(incl);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    return _incl.PrepareScore(ctx);
  }

  void FetchScoreArgs(uint16_t index) final { _incl.FetchScoreArgs(index); }

  uint32_t count() final { return CountImpl(*this); }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  doc_id_t converge(doc_id_t incl) {
    if (doc_limits::eof(incl)) [[unlikely]] {
      return incl;
    }

    if constexpr (requires { _excl.begin(); }) {
      for (auto& it : _excl) {
        auto excl = it.value();
        if (excl < incl) {
          excl = it.seek(incl);
        }
        if (excl == incl) {
          return advance();
        }
        SDB_ASSERT(excl > incl);
      }
    } else {
      auto excl = _excl.value();
      if (excl < incl) {
        excl = _excl.seek(incl);
      }
      if (excl == incl) {
        return advance();
      }
      SDB_ASSERT(excl > incl);
    }

    return incl;
  }

  IncludeAdapter _incl;
  ExcludeAdapters _excl;
};

}  // namespace irs

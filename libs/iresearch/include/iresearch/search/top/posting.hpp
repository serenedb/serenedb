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

#include <absl/base/optimization.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/score_args.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/term_block.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename InputType, typename Excludes, typename Table>
class Posting : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  using Block = TermBlock<InputType, Table>;

  template<typename ExcludesArgs>
  Posting(Table table, std::piecewise_construct_t, ExcludesArgs&& excludes)
    : _excludes{std::make_from_tuple<Excludes>(
        std::forward<ExcludesArgs>(excludes))},
      _admit{table} {}

  Posting(Posting&&) = delete;
  Posting& operator=(Posting&&) = delete;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const irs::detail::ScoreArgs& args, IndexFeatures layout,
               bool bounds) {
    _block.Prepare(meta, doc_in, segment, field, args, layout, bounds);
  }

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[Block::kFill + doc_limits::kDocsSlack];
    ABSL_CACHELINE_ALIGNED score_t scores[Block::kFill];
    for (;;) {
      if constexpr (kTable) {
        const auto from = _block.Last() + doc_limits::min();
        if (const auto live = _admit.Live(from);
            live != from && !_block.Step(live)) {
          break;
        }
      }
      auto len = _block.Fill(docs, scores);
      if (len == 0) {
        break;
      }
      if constexpr (kExcludes) {
        len = irs::detail::ExcludeBlock(_excludes, docs, scores, len);
      }
      if (len != 0) {
        _admit.AddDocs(collector, docs, len, scores);
      }
    }
    _admit.Flush(collector);
  }

 private:
  Block _block;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top

////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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

#include "iresearch/search/scorer.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

// Scorers allowed to be used in conjunction with wanderator.
using ScorerPtr = const Scorer*;

struct WandContext {
  bool Enabled() const noexcept { return wand_enabled; }

  // Index of the wand data in the IndexWriter to use for optimization.
  // Optimization is turned off by default.
  bool wand_enabled = false;
  bool strict = false;
};

struct IndexReaderOptions {
  ScorerPtr scorer = nullptr;  // A list of topk scorers
  duckdb::DatabaseInstance* db = nullptr;
  bool index = true;  // Open inverted index
};

}  // namespace irs

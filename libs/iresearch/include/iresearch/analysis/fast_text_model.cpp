////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "fast_text_model.hpp"

#include <densematrix.h>

namespace sdb::fast_text {
namespace {

size_t MatrixBytes(const std::shared_ptr<const fasttext::DenseMatrix>& m) {
  if (!m) {
    return 0;
  }
  return static_cast<size_t>(m->size(0)) * static_cast<size_t>(m->size(1)) *
         sizeof(fasttext::real);
}

}  // namespace

duckdb::optional_idx Model::GetEstimatedCacheMemory() const {
  return MatrixBytes(getInputMatrix()) + MatrixBytes(getOutputMatrix());
}

}  // namespace sdb::fast_text

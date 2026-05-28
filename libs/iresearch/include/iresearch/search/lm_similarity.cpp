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

#include "lm_similarity.hpp"

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/scorer_impl.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

void LMTermCollector::collect(const SubReader& /*segment*/,
                              const TermReader& /*field*/,
                              const AttributeProvider& term_attrs) {
  if (const auto* meta = irs::get<TermMeta>(term_attrs)) {
    total_term_freq += meta->freq;
  }
}

void LMTermCollector::collect(bytes_view in) {
  ByteRefIterator itr{in};
  const auto ttf = vread<uint64_t>(itr);
  if (itr.pos != itr.end) {
    throw IoError{"input not read fully"};
  }
  total_term_freq += ttf;
}

void LMTermCollector::write(DataOutput& out) const {
  out.WriteV64(total_term_freq);
}

}  // namespace irs

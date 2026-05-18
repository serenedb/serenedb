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

#include "basics/down_cast.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/scorer_impl.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

void LMFieldCollector::collect(const SubReader& /*segment*/,
                               const TermReader& field) noexcept {
  docs_with_field += field.docs_count();
  if (const auto* freq = irs::get<FreqAttr>(field)) {
    total_term_freq += freq->value;
  }
}

void LMFieldCollector::collect(bytes_view in) {
  ByteRefIterator itr{in};
  const auto docs_with_field_value = vread<uint64_t>(itr);
  const auto total_term_freq_value = vread<uint64_t>(itr);
  if (itr.pos != itr.end) {
    throw IoError{"input not read fully"};
  }
  docs_with_field += docs_with_field_value;
  total_term_freq += total_term_freq_value;
}

void LMFieldCollector::collect(FieldCollector&& other) noexcept {
  auto& rhs = sdb::basics::downCast<LMFieldCollector>(other);
  docs_with_field += rhs.docs_with_field;
  total_term_freq += rhs.total_term_freq;
}

void LMFieldCollector::write(DataOutput& out) const {
  out.WriteV64(docs_with_field);
  out.WriteV64(total_term_freq);
}

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

void LMTermCollector::collect(TermCollector&& other) noexcept {
  auto& rhs = sdb::basics::downCast<LMTermCollector>(other);
  total_term_freq += rhs.total_term_freq;
}

void LMTermCollector::write(DataOutput& out) const {
  out.WriteV64(total_term_freq);
}

}  // namespace irs

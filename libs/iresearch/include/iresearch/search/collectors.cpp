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

#include "collectors.hpp"

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"

namespace irs {

void FieldCollector::Collect(const TermReader& field) noexcept {
  docs_with_field += field.docs_count();
  if (const auto* freq = irs::get<FreqAttr>(field)) {
    total_term_freq += freq->value;
  }
}

void TermCollector::Collect(const AttributeProvider& term_attrs) noexcept {
  if (const auto* meta = irs::get<TermMeta>(term_attrs)) {
    docs_with_term += meta->docs_count;
    total_term_freq += meta->freq;
  }
}

}  // namespace irs

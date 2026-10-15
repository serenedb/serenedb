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

#include "iresearch/search/detail/exact_scan.hpp"
#include "iresearch/search/docs/make.hpp"

namespace irs::docs {
namespace {

class ExactDocs : public Root {
 public:
  explicit ExactDocs(const ExactVectorQuery& query) : _scanner{query} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    _scanner.Reset(min, max);
    uint32_t n = 0;
    for (;;) {
      const auto m = _scanner.Gated() ? _scanner.Next(out + n, _dists)
                                      : _scanner.NextDocs(out + n);
      if (m == 0) {
        return n;
      }
      n += m;
    }
  }

 private:
  irs::detail::ExactScanner _scanner;
  score_t _dists[irs::detail::ExactScanner::kRun];
};

}  // namespace

Root::ptr Make(const ExactVectorQuery& query, const Context&) {
  return memory::make_managed<ExactDocs>(query);
}

}  // namespace irs::docs

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

#include "term_set.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/term_iterator.hpp"

namespace irs {
namespace {

class TermSetIterator : public WrappedTermIterator {
 public:
  TermSetIterator(const TermReader& reader,
                  const TermSetOptions::search_terms& terms)
    : WrappedTermIterator{reader.iterator()},
      _cursor{terms.begin()},
      _end{terms.end()} {}

  score_t Boost() const noexcept { return _boost; }
  uint32_t Index() const noexcept { return _index; }

  bool next() final {
    if (_started) {
      if (_cursor == _end) {
        return false;
      }
      ++_cursor;
      ++_index;
    }
    _started = true;
    while (_cursor != _end) {
      if (_impl->seek(_cursor->term)) {
        _boost = _cursor->boost;
        return true;
      }
      ++_cursor;
      ++_index;
    }
    return false;
  }

 private:
  TermSetOptions::search_terms::const_iterator _cursor;
  TermSetOptions::search_terms::const_iterator _end;
  score_t _boost = kNoBoost;
  uint32_t _index = 0;
  bool _started = false;
};

}  // namespace

void VisitTermSet(const SubReader& segment, const TermReader& field,
                  const TermSetOptions& options, FilterVisitor& visitor) {
  TermSetIterator terms(field, options.terms);
  visitor.Prepare(segment, field, terms.GetImpl());
  if (!terms.next()) {
    return;
  }
  VisitTerms(terms, visitor);
}

}  // namespace irs

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

#include "basics/shared.hpp"

namespace fst {

// This class discards any implicit matches (e.g., the implicit epsilon
// self-loops in the SortedMatcher). Matchers are most often used in
// composition/intersection where the implicit matches are needed
// e.g. for epsilon processing. However, if a matcher is simply being
// used to look-up explicit label matches, this class saves the user
// from having to check for and discard the unwanted implicit matches
// themselves.
template<class MatcherImpl>
class explicit_matcher final  // NOLINT
  : public MatcherBase<typename MatcherImpl::FST::Arc> {
 public:
  typedef typename MatcherImpl::FST FST;
  typedef typename FST::Arc Arc;
  typedef typename Arc::Label Label;
  typedef typename Arc::StateId StateId;
  typedef typename Arc::Weight Weight;

  template<typename... Args>
  explicit explicit_matcher(Args&&... args)
    : _matcher(std::forward<Args>(args)...),
#ifdef SDB_DEV
      _match_type(_matcher.Type(true))  // test fst properties
#else
      _match_type(_matcher.Type(false))  // read type without checks
#endif
  {
  }

  explicit_matcher(const explicit_matcher& rhs, bool safe = false)
    : _matcher(rhs._matcher, safe), _match_type(rhs._match_type) {}

  explicit_matcher* Copy(bool safe = false) const final {
    return new explicit_matcher(*this, safe);
  }

  MatchType Type(bool test) const final { return _matcher.Type(test); }

  void SetState(StateId s) final { _matcher.SetState(s); }

  bool Find(Label match_label) final {
    _matcher.Find(match_label);
    CheckArc();
    return !Done();
  }

  bool Done() const final { return _matcher.Done(); }

  const Arc& Value() const final { return _matcher.Value(); }

  void Next() final {
    _matcher.Next();
    CheckArc();
  }

  Weight Final(StateId s) const final { return _matcher.Final(s); }

  ssize_t Priority(StateId s) final { return _matcher.Priority(s); }

  const FST& GetFst() const final { return _matcher.GetFst(); }

  uint64_t Properties(uint64_t inprops) const final {
    return _matcher.Properties(inprops);
  }

  uint32_t Flags() const final { return _matcher.Flags(); }

 private:
  // Checks current arc if available and explicit. If not available, stops. If
  // not explicit, checks next ones.
  void CheckArc() {
    for (; !_matcher.Done(); _matcher.Next()) {
      const auto& label = _match_type == MATCH_INPUT ? _matcher.Value().ilabel
                                                     : _matcher.Value().olabel;
      if (label != kNoLabel) {
        return;
      }
    }
  }

  MatcherImpl _matcher;
  MatchType _match_type;  // Type of match requested.
};

}  // namespace fst

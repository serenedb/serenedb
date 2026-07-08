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

#include <memory>

#include "iresearch/utils/string.hpp"

namespace irs {

struct TermPredicate {
  using ptr = std::unique_ptr<TermPredicate>;

  virtual ~TermPredicate() = default;

  virtual bool Accepts(bytes_view term) const = 0;
};

template<typename Acceptor>
class TermPredicateImpl final : public TermPredicate {
 public:
  explicit TermPredicateImpl(Acceptor acceptor) noexcept
    : _acceptor{std::move(acceptor)} {}

  bool Accepts(bytes_view term) const final { return _acceptor(term); }

 private:
  [[no_unique_address]] Acceptor _acceptor;
};

template<typename Acceptor>
TermPredicate::ptr MakeTermPredicate(Acceptor acceptor) {
  return std::make_unique<TermPredicateImpl<Acceptor>>(std::move(acceptor));
}

struct AcceptAllTerms {
  bool operator()(bytes_view) const noexcept { return true; }
};

struct AcceptNoTerms {
  bool operator()(bytes_view) const noexcept { return false; }
};

}  // namespace irs

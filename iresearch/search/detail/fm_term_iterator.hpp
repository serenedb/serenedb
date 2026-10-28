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

#include <absl/algorithm/container.h>

#include <memory>
#include <utility>
#include <vector>

#include "iresearch/index/iterators.hpp"
#include "iresearch/utils/fm_index.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class FmTermIterator : public SeekTermIterator {
 public:
  FmTermIterator(SeekTermIterator::ptr&& impl, const FmIndex& index,
                 const RegexpAcceptor& acceptor, std::vector<uint64_t>&& terms)
    : _impl{std::move(impl)},
      _index{index},
      _acceptor{acceptor},
      _terms{std::move(terms)} {}

  bytes_view value() const noexcept final { return _impl->value(); }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return _impl->GetMutable(id);
  }

  const PostingMeta& cookie() const final { return _impl->cookie(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return _impl->postings(features);
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    return _impl->ReusePostings(features, std::move(reuse));
  }

  bool next() final {
    while (_next != _terms.size()) {
      const auto term = _index.Term(_terms[_next++]);
      const auto bytes = ViewCast<byte_type>(std::string_view{term});
      if (_acceptor.Matches(bytes) && _impl->seek(bytes)) {
        return true;
      }
    }
    return false;
  }

  SeekResult seek_ge(bytes_view target) final {
    const auto from =
      absl::c_lower_bound(_terms, ViewCast<char>(target),
                          [this](uint64_t id, std::string_view key) {
                            return _index.Term(id) < key;
                          });
    _next = static_cast<size_t>(from - _terms.begin());
    if (!next()) {
      return SeekResult::End;
    }
    return value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return seek_ge(target) == SeekResult::Found;
  }

 private:
  SeekTermIterator::ptr _impl;
  const FmIndex& _index;
  const RegexpAcceptor& _acceptor;
  std::vector<uint64_t> _terms;
  size_t _next{0};
};

}  // namespace irs

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
////////////////////////////////////////////////////////////////////////////////

#include "multi_delimited_tokenizer.hpp"

#include <array>
#include <limits>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "basics/shared.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

template<typename Derived>
class MultiDelimitedTokenizerBase : public MultiDelimitedTokenizer {
 public:
  MultiDelimitedTokenizerBase() = default;

  bool next() override {
    while (true) {
      if (_data.begin() == _data.end()) {
        return false;
      }

      auto [next, skip] = static_cast<Derived*>(this)->FindNextDelim();

      if (next == _data.begin()) {
        // skip empty terms
        SDB_ASSERT(skip <= _data.size());
        _data = bytes_view(_data.data() + skip, _data.size() - skip);
        continue;
      }

      auto& term = std::get<TermAttr>(_attrs);
      term.value = bytes_view(_data.data(), std::distance(_data.begin(), next));
      auto& offset = std::get<OffsAttr>(_attrs);
      offset.start = std::distance(_start, _data.data());
      offset.end = offset.start + term.value.size();

      if (next == _data.end()) {
        _data = {};
      } else {
        _data =
          bytes_view(&(*next) + skip, std::distance(next, _data.end()) - skip);
      }

      return true;
    }
  }
};

template<typename Derived>
class MultiDelimitedTokenizerSingleCharsBase
  : public MultiDelimitedTokenizerBase<
      MultiDelimitedTokenizerSingleCharsBase<Derived>> {
 public:
  auto FindNextDelim() {
    auto where = static_cast<Derived*>(this)->FindNextDelim();
    return std::pair{where, size_t{1}};
  }
};

template<size_t N>
class MultiDelimitedTokenizerSingleChars final
  : public MultiDelimitedTokenizerSingleCharsBase<
      MultiDelimitedTokenizerSingleChars<N>> {
 public:
  explicit MultiDelimitedTokenizerSingleChars(
    const std::vector<bstring>& /*delimiters*/) {
    // according to benchmarks "table" version is
    // ~1.5x faster except cases for 0 and 1.
    // So this should not be used.
    SDB_ASSERT(false);
  }

  auto FindNextDelim() { return this->data.end(); }
};

template<>
class MultiDelimitedTokenizerSingleChars<1> final
  : public MultiDelimitedTokenizerSingleCharsBase<
      MultiDelimitedTokenizerSingleChars<1>> {
 public:
  explicit MultiDelimitedTokenizerSingleChars(
    const std::vector<bstring>& delimiters) {
    SDB_ASSERT(delimiters.size() == 1);
    SDB_ASSERT(delimiters[0].size() == 1);
    delim = delimiters[0][0];
  }

  auto FindNextDelim() {
    if (auto pos = this->_data.find(delim); pos != bstring::npos) {
      return this->_data.begin() + pos;
    }
    return this->_data.end();
  }

  byte_type delim;
};

template<>
class MultiDelimitedTokenizerSingleChars<0> final
  : public MultiDelimitedTokenizerSingleCharsBase<
      MultiDelimitedTokenizerSingleChars<0>> {
 public:
  explicit MultiDelimitedTokenizerSingleChars(
    const std::vector<bstring>& delimiters) {
    SDB_ASSERT(delimiters.empty());
    (void)delimiters;
  }

  auto FindNextDelim() { return this->_data.end(); }
};

class MultiDelimitedTokenizerGenericSingleChars final
  : public MultiDelimitedTokenizerSingleCharsBase<
      MultiDelimitedTokenizerGenericSingleChars> {
 public:
  explicit MultiDelimitedTokenizerGenericSingleChars(
    const std::vector<bstring>& delimiters) {
    for (const auto& delim : delimiters) {
      SDB_ASSERT(delim.size() == 1);
      bytes[delim[0]] = true;
    }
  }

  auto FindNextDelim() {
    return absl::c_find_if(_data, [&](auto c) { return bytes[c]; });
  }
  // TODO(mbkkt) maybe use a bitset instead?
  std::array<bool, 256> bytes{};
};

struct TrieNode {
  explicit TrieNode(uint32_t depth) : depth{depth} {}

  uint32_t code = 0;
  uint32_t depth;
  bool is_leaf = false;
  TrieNode* fail = nullptr;
  TrieNode* match = nullptr;

  absl::flat_hash_map<byte_type, TrieNode*> simple_trie;
};

struct StringTrieDfa {
  static constexpr size_t kAlphabet = 256;

  std::vector<uint32_t> next;
  uint32_t final_base = 0;
};

void FillTransitions(std::vector<std::unique_ptr<TrieNode>>& nodes,
                     StringTrieDfa& dfa) {
  auto* const table = dfa.next.data();
  auto* const root = nodes.front().get();

  const auto row_of = [table](const TrieNode* n) {
    return table + size_t{n->code} * StringTrieDfa::kAlphabet;
  };
  const auto target_of = [](const TrieNode* n) {
    return (n->match != nullptr ? n->match : n)->code;
  };

  root->fail = root;
  root->match = root->is_leaf ? root : nullptr;

  std::vector<TrieNode*> order;
  order.reserve(nodes.size());
  order.push_back(root);

  for (size_t head = 0; head != order.size(); ++head) {
    auto* const node = order[head];
    for (auto& [c, child] : node->simple_trie) {
      auto* f = node->fail;
      // Longest proper suffix that keeps `c` inside the trie.
      while (f != root && !f->simple_trie.contains(c)) {
        f = f->fail;
      }
      const auto it = f->simple_trie.find(c);
      child->fail =
        (it != f->simple_trie.end() && it->second != child) ? it->second : root;
      child->match = child->is_leaf ? child : child->fail->match;
      if (!child->is_leaf) {
        order.push_back(child);
      }
    }
  }

  for (auto* const node : order) {
    auto* const row = row_of(node);
    if (node == root) {
      std::fill_n(row, StringTrieDfa::kAlphabet, root->code);
    } else {
      const auto* const src = node->fail->is_leaf ? root : node->fail;
      std::copy_n(row_of(src), StringTrieDfa::kAlphabet, row);
    }
    for (const auto& [c, child] : node->simple_trie) {
      row[c] = target_of(child);
    }
  }
}

StringTrieDfa MakeStringTrie(const std::vector<bstring>& strings) {
  std::vector<std::unique_ptr<TrieNode>> nodes;
  nodes.emplace_back(std::make_unique<TrieNode>(uint32_t{0}));

  for (const auto& str : strings) {
    TrieNode* current = nodes.front().get();

    for (size_t k = 0; k < str.length(); k++) {
      auto c = str[k];
      if (current->is_leaf) {
        break;
      }

      if (auto it = current->simple_trie.find(c);
          it != current->simple_trie.end()) {
        current = it->second;
        continue;
      }

      auto& new_node = nodes.emplace_back(
        std::make_unique<TrieNode>(static_cast<uint32_t>(k)));
      current->simple_trie.emplace(c, new_node.get());
      current = new_node.get();
    }

    current->is_leaf = true;
  }

  StringTrieDfa dfa;
  for (auto& n : nodes) {
    if (!n->is_leaf) {
      n->code = dfa.final_base++;
    }
  }
  for (auto& n : nodes) {
    if (n->is_leaf) {
      n->code = dfa.final_base + n->depth;
    }
  }
  dfa.next.assign(size_t{dfa.final_base} * StringTrieDfa::kAlphabet, 0);

  SDB_ASSERT(nodes.front()->code == 0);
  FillTransitions(nodes, dfa);

  return dfa;
}

class MultiDelimitedTokenizerGeneric final
  : public MultiDelimitedTokenizerBase<MultiDelimitedTokenizerGeneric> {
 public:
  explicit MultiDelimitedTokenizerGeneric(
    const std::vector<bstring>& delimiters)
    : dfa(MakeStringTrie(delimiters)) {}

  auto FindNextDelim() {
    const auto* IRS_RESTRICT const table = dfa.next.data();
    const auto* IRS_RESTRICT const data = _data.data();
    const size_t size = _data.size();
    const uint32_t final_base = dfa.final_base;

    size_t k = 0;
    while (k != size) {
      uint32_t state = table[data[k]];
      ++k;
      while (state != 0) {
        if (state >= final_base) {
          const size_t length = state - final_base;
          SDB_ASSERT(length < k);

          return std::pair{_data.begin() + (k - 1 - length), length + 1};
        }
        if (k == size) {
          return std::pair{_data.end(), size_t{0}};
        }
        state = table[state * StringTrieDfa::kAlphabet + data[k]];
        ++k;
      }
    }

    return std::pair{_data.end(), size_t{0}};
  }

  StringTrieDfa dfa;
};

class MultiDelimitedTokenizerSingle final
  : public MultiDelimitedTokenizerBase<MultiDelimitedTokenizerSingle> {
 public:
  explicit MultiDelimitedTokenizerSingle(std::vector<bstring>& delimiters)
    : delim(std::move(delimiters[0])), searcher(delim.begin(), delim.end()) {}

  auto FindNextDelim() {
    auto next = std::search(_data.begin(), _data.end(), searcher);
    return std::pair{next, delim.size()};
  }

  bstring delim;
  std::boyer_moore_searcher<bstring::iterator> searcher;
};

template<size_t N>
Analyzer::ptr MakeSingleChar(std::vector<bstring>&& delimiters) {
  if constexpr (N >= 2) {
    return std::make_unique<MultiDelimitedTokenizerGenericSingleChars>(
      delimiters);
  } else if (delimiters.size() == N) {
    return std::make_unique<MultiDelimitedTokenizerSingleChars<N>>(delimiters);
  } else {
    return MakeSingleChar<N + 1>(std::move(delimiters));
  }
}

Analyzer::ptr MakeImpl(std::vector<bstring>&& delimiters) {
  const bool single_character_case = absl::c_all_of(
    delimiters, [](const auto& delim) { return delim.size() == 1; });
  if (single_character_case) {
    return MakeSingleChar<0>(std::move(delimiters));
  }
  if (delimiters.size() == 1) {
    return std::make_unique<MultiDelimitedTokenizerSingle>(delimiters);
  }
  return std::make_unique<MultiDelimitedTokenizerGeneric>(delimiters);
}

}  // namespace

Analyzer::ptr MultiDelimitedTokenizer::Make(
  MultiDelimitedTokenizer::Options opts) {
  for (size_t i = 0; i < opts.delimiters.size(); ++i) {
    const bytes_view view{opts.delimiters[i]};
    if (view.empty()) {
      THROW_SQL_ERROR(ERR_MSG("multi_delimited: empty delimiter"));
    }
    for (size_t j = 0; j < i; ++j) {
      const bytes_view known{opts.delimiters[j]};
      if (view.starts_with(known) || known.starts_with(view)) {
        THROW_SQL_ERROR(
          ERR_MSG("multi_delimited: delimiters must not be prefixes of one "
                  "another"));
      }
    }
  }
  return MakeImpl(std::move(opts.delimiters));
}

}  // namespace irs::analysis

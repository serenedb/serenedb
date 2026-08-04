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

#include <limits>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
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
    return std::make_pair(where, size_t{1});
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
    return absl::c_find_if(_data, [&](auto c) {
      if (c > SCHAR_MAX) {
        return false;
      }
      SDB_ASSERT(c <= SCHAR_MAX);
      return bytes[c];
    });
  }
  // TODO(mbkkt) maybe use a bitset instead?
  std::array<bool, SCHAR_MAX + 1> bytes{};
};

struct TrieNode {
  explicit TrieNode(int32_t id, int32_t depth) : state_id(id), depth(depth) {}
  int32_t state_id;
  int32_t depth;
  bool is_leaf{false};
  absl::flat_hash_map<byte_type, TrieNode*> simple_trie;
  absl::flat_hash_map<byte_type, TrieNode*> real_trie;
};

bytes_view FindLongestPrefixThatIsSuffix(bytes_view s, bytes_view str) {
  // TODO(mbkkt) this algorithm is quadratic
  for (size_t n = s.length() - 1; n > 0; n--) {
    auto prefix = s.substr(0, n);
    if (str.ends_with(prefix)) {
      return prefix;
    }
  }
  return {};
}

bytes_view FindLongestPrefixThatIsSuffix(const std::vector<bstring>& strings,
                                         std::string_view str) {
  bytes_view result = {};
  for (const auto& s : strings) {
    auto other = FindLongestPrefixThatIsSuffix(s, ViewCast<byte_type>(str));
    if (other.length() > result.length()) {
      result = other;
    }
  }
  return result;
}

void InsertErrorTransitions(const std::vector<bstring>& strings,
                            std::string& matched_word, TrieNode* node,
                            TrieNode* root) {
  if (node->is_leaf) {
    return;
  }

  for (size_t k = 0; k <= std::numeric_limits<byte_type>::max(); k++) {
    if (auto it = node->simple_trie.find(k); it != node->simple_trie.end()) {
      node->real_trie.emplace(k, it->second);
      matched_word.push_back(static_cast<char>(k));
      InsertErrorTransitions(strings, matched_word, it->second, root);
      matched_word.pop_back();
    } else {
      // if we find a character c that we don't expect, we have to find
      // the longest prefix of `str` that is a suffix of the already matched
      // text including c. then go to that state.
      matched_word.push_back(static_cast<char>(k));
      auto prefix = FindLongestPrefixThatIsSuffix(strings, matched_word);
      if (prefix.empty()) {
        matched_word.pop_back();
        continue;  // no prefix found implies going to the initial state
      }

      auto* dest = root;
      for (auto c : prefix) {
        auto itt = dest->simple_trie.find(c);
        SDB_ASSERT(itt != dest->simple_trie.end());
        dest = itt->second;
      }
      node->real_trie.emplace(k, dest);
      matched_word.pop_back();
    }
  }
}

// The trie already carries its own failure transitions, so every state has a
// complete transition function over all 256 bytes -- which is a flat table, and
// what the table matcher it used to be walked through built internally anyway.
struct StringTrieDfa {
  static constexpr uint8_t kNotFinal = std::numeric_limits<uint8_t>::max();
  static constexpr size_t kAlphabet = 256;

  uint32_t Step(uint32_t state, byte_type label) const noexcept {
    return next[state * kAlphabet + label];
  }

  std::vector<uint32_t> next;
  std::vector<uint8_t> depth;
};

StringTrieDfa MakeStringTrie(const std::vector<bstring>& strings) {
  std::vector<std::unique_ptr<TrieNode>> nodes;
  nodes.emplace_back(std::make_unique<TrieNode>(0, 0));

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

      auto& new_node =
        nodes.emplace_back(std::make_unique<TrieNode>(nodes.size(), k));
      current->simple_trie.emplace(c, new_node.get());
      current = new_node.get();
    }

    current->is_leaf = true;
  }

  std::string matched_word;
  auto* root = nodes.front().get();
  InsertErrorTransitions(strings, matched_word, root, root);

  StringTrieDfa dfa;
  dfa.next.assign(nodes.size() * StringTrieDfa::kAlphabet, 0);
  dfa.depth.assign(nodes.size(), StringTrieDfa::kNotFinal);

  for (auto& n : nodes) {
    // A leaf ends the walk, so its own transitions are never taken.
    if (n->is_leaf) {
      dfa.depth[n->state_id] = static_cast<uint8_t>(n->depth);
      continue;
    }
    auto* row = dfa.next.data() + n->state_id * StringTrieDfa::kAlphabet;
    for (size_t k = 0; k != StringTrieDfa::kAlphabet; ++k) {
      const auto it = n->real_trie.find(k);
      row[k] = static_cast<uint32_t>(
        it != n->real_trie.end() ? it->second->state_id : root->state_id);
    }
  }

  return dfa;
}

class MultiDelimitedTokenizerGeneric final
  : public MultiDelimitedTokenizerBase<MultiDelimitedTokenizerGeneric> {
 public:
  explicit MultiDelimitedTokenizerGeneric(
    const std::vector<bstring>& delimiters)
    : dfa(MakeStringTrie(delimiters)) {}

  auto FindNextDelim() {
    uint32_t state = 0;
    for (size_t k = 0; k < _data.length(); k++) {
      state = dfa.Step(state, _data[k]);

      const auto length = dfa.depth[state];
      if (length != StringTrieDfa::kNotFinal) {
        SDB_ASSERT(length <= k);

        return std::make_pair(_data.begin() + (k - length),
                              static_cast<size_t>(length + 1));
      }
    }

    return std::make_pair(_data.end(), size_t{0});
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
    return std::make_pair(next, delim.size());
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

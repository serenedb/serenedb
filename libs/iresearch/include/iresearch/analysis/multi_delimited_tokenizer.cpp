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

#include <fst/union.h>
#include <fstext/determinize-star.h>

#include <bit>
#include <concepts>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "iresearch/analysis/batch/classify.hpp"
#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/fstext/fst_draw.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

template<TokenLayout Layout, typename MaskFn, typename DelimFn>
void DrainSingleCharValue(TokenEmitter& sink, bytes_view data, size_t value_off,
                          MaskFn mask_at, DelimFn is_delim);

template<typename Derived>
class MultiDelimitedTokenizerBase : public TypedTokenizer<Derived>,
                                    public MultiDelimitedTokenizer {
 public:
  MultiDelimitedTokenizerBase() = default;

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink) {
    auto& buf = sink.buf;
    _data = ViewCast<byte_type>(value);
    _start = _data.data();
    while (_data.begin() != _data.end()) {
      auto [next, skip] = static_cast<Derived*>(this)->FindNextDelim();

      if (next == _data.begin()) {
        SDB_ASSERT(skip <= _data.size());
        _data = bytes_view(_data.data() + skip, _data.size() - skip);
        continue;
      }

      const auto size =
        static_cast<uint32_t>(std::distance(_data.begin(), next));
      const auto i = sink.Next();
      buf.terms[i] =
        MakeTermView(reinterpret_cast<const char*>(_data.data()), size);
      if constexpr (Layout == TokenLayout::TermsPosOffs) {
        const auto off =
          static_cast<uint32_t>(std::distance(_start, _data.data()));
        buf.offs_start[i] = off;
        buf.offs_end[i] = off + size;
      }

      if (next == _data.end()) {
        _data = {};
      } else {
        _data =
          bytes_view(&(*next) + skip, std::distance(next, _data.end()) - skip);
      }
    }
    return true;
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

  using Base = MultiDelimitedTokenizerBase<
    MultiDelimitedTokenizerSingleCharsBase<Derived>>;

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink) {
    if constexpr (kHasBlockScan) {
      auto* impl = static_cast<Derived*>(this);
      if (impl->CanBlockScan()) {
        DrainSingleCharValue<Layout>(
          sink, ViewCast<byte_type>(value), 0,
          [impl](const byte_type* p) { return impl->ClassifyBlock(p); },
          [impl](byte_type c) { return impl->IsDelimByte(c); });
        return true;
      }
    }
    return Base::template DoFill<Layout>(value, sink);
  }

 private:
  static constexpr bool kHasBlockScan =
    requires(Derived& d, const byte_type* p, byte_type c) {
      { d.ClassifyBlock(p) } -> std::same_as<uint32_t>;
      { d.IsDelimByte(c) } -> std::same_as<bool>;
      { d.CanBlockScan() } -> std::same_as<bool>;
    };
};

template<size_t N>
class MultiDelimitedTokenizerSingleChars;

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

  bool CanBlockScan() const { return true; }
  bool IsDelimByte(byte_type c) const { return c == delim; }
  uint32_t ClassifyBlock(const byte_type* block) const {
    return ClassifyEqBlock(block, delim);
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
  static constexpr size_t kMaxBlockDelims = 8;

  explicit MultiDelimitedTokenizerGenericSingleChars(
    const std::vector<bstring>& delimiters) {
    for (const auto& delim : delimiters) {
      SDB_ASSERT(delim.size() == 1);
      if (delim[0] > SCHAR_MAX) {
        // The table path never matches high bytes; keep that behavior and
        // disable the block path so both agree.
        high_byte_delim = true;
        continue;
      }
      bytes[delim[0]] = true;
      if (ndelims < kMaxBlockDelims) {
        delims[ndelims] = delim[0];
      }
      ++ndelims;
    }
  }

  auto FindNextDelim() {
    if (CanBlockScan()) {
      return FindNextDelimBlock();
    }
    return absl::c_find_if(_data, [&](auto c) {
      if (c > SCHAR_MAX) {
        return false;
      }
      SDB_ASSERT(c <= SCHAR_MAX);
      return bytes[c];
    });
  }

  bool CanBlockScan() const {
    return !high_byte_delim && ndelims <= kMaxBlockDelims;
  }
  bool IsDelimByte(byte_type c) const { return c <= SCHAR_MAX && bytes[c]; }
  uint32_t ClassifyBlock(const byte_type* block) const {
    return ClassifyAnyEqBlock(block, {delims.data(), ndelims});
  }

  // TODO(mbkkt) maybe use a bitset instead?
  std::array<bool, SCHAR_MAX + 1> bytes{};
  std::array<byte_type, kMaxBlockDelims> delims{};
  size_t ndelims = 0;
  bool high_byte_delim = false;

 private:
  bytes_view::iterator FindNextDelimBlock() {
    const auto* p = _data.data();
    const size_t size = _data.size();
    size_t offset = 0;
    while (size - offset >= kClassifyBlock) {
      const uint32_t mask =
        ClassifyAnyEqBlock(p + offset, {delims.data(), ndelims});
      if (mask != 0) {
        return _data.begin() + offset + std::countr_zero(mask);
      }
      offset += kClassifyBlock;
    }
    for (; offset < size; ++offset) {
      const auto c = p[offset];
      if (c <= SCHAR_MAX && bytes[c]) {
        return _data.begin() + offset;
      }
    }
    return _data.end();
  }
};

// Block-drained splitter for single-byte delimiter sets: one classification
// mask per 32-byte block, every set bit consumed, empty tokens skipped,
// zero-copy token views.
template<TokenLayout Layout, typename MaskFn, typename DelimFn>
void DrainSingleCharValue(TokenEmitter& sink, bytes_view data, size_t value_off,
                          MaskFn mask_at, DelimFn is_delim) {
  auto& buf = sink.buf;
  const auto* p = data.data();
  const size_t size = data.size();
  size_t tok_begin = 0;

  const auto emit = [&](size_t begin, size_t end) {
    if (begin == end) {
      return;
    }
    const auto i = sink.Next();
    buf.terms[i] = MakeTermView(reinterpret_cast<const char*>(p + begin),
                                static_cast<uint32_t>(end - begin));
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = static_cast<uint32_t>(value_off + begin);
      buf.offs_end[i] = static_cast<uint32_t>(value_off + end);
    }
  };

  size_t offset = 0;
  while (size - offset >= kClassifyBlock) {
    uint32_t mask = mask_at(p + offset);
    while (mask != 0) {
      const size_t pos = offset + std::countr_zero(mask);
      mask &= mask - 1;
      emit(tok_begin, pos);
      tok_begin = pos + 1;
    }
    offset += kClassifyBlock;
  }
  for (; offset < size; ++offset) {
    if (is_delim(p[offset])) {
      emit(tok_begin, offset);
      tok_begin = offset + 1;
    }
  }
  emit(tok_begin, size);
}

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

automaton MakeStringTrie(const std::vector<bstring>& strings) {
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

  automaton a;
  a.AddStates(nodes.size());
  a.SetStart(0);

  for (auto& n : nodes) {
    int64_t last_state = -1;
    size_t last_char = 0;

    if (n->is_leaf) {
      a.SetFinal(n->state_id, {true, static_cast<byte_type>(n->depth)});
      continue;
    }

    for (size_t k = 0; k <= std::numeric_limits<byte_type>::max(); k++) {
      int64_t next_state = root->state_id;
      if (auto it = n->real_trie.find(k); it != n->real_trie.end()) {
        next_state = it->second->state_id;
      }

      if (last_state == -1) {
        last_state = next_state;
        last_char = k;
      } else if (last_state != next_state) {
        a.EmplaceArc(n->state_id, RangeLabel::From(last_char, k - 1),
                     last_state);
        last_state = next_state;
        last_char = k;
      }
    }

    a.EmplaceArc(
      n->state_id,
      RangeLabel::From(last_char, std::numeric_limits<byte_type>::max()),
      last_state);
  }

  return a;
}

class MultiDelimitedTokenizerGeneric final
  : public MultiDelimitedTokenizerBase<MultiDelimitedTokenizerGeneric> {
 public:
  explicit MultiDelimitedTokenizerGeneric(
    const std::vector<bstring>& delimiters)
    : autom(MakeStringTrie(delimiters)), matcher(MakeAutomatonMatcher(autom)) {
    // fst::drawFst(automaton_, std::cout);

#ifdef SDB_DEV
    // ensure nfa is sorted
    static constexpr auto kExpectedNfaProperties =
      fst::kILabelSorted | fst::kOLabelSorted | fst::kAcceptor |
      fst::kUnweighted;

    SDB_ASSERT(kExpectedNfaProperties ==
               autom.Properties(kExpectedNfaProperties, true));
#endif
  }

  auto FindNextDelim() {
    auto state = matcher.GetFst().Start();
    matcher.SetState(state);
    for (size_t k = 0; k < _data.length(); k++) {
      matcher.Find(_data[k]);

      state = matcher.Value().nextstate;

      if (matcher.Final(state)) {
        auto length = matcher.Final(state).Payload();
        SDB_ASSERT(length <= k);

        return std::make_pair(_data.begin() + (k - length),
                              static_cast<size_t>(length + 1));
      }

      matcher.SetState(state);
    }

    return std::make_pair(_data.end(), size_t{0});
  }

  automaton autom;
  automaton_table_matcher matcher;
};

#ifdef __APPLE__
class MultiDelimitedTokenizerSingle final
  : public MultiDelimitedTokenizerBase<MultiDelimitedTokenizerSingle> {
 public:
  explicit MultiDelimitedTokenizerSingle(std::vector<bstring>& delimiters)
    : delim(std::move(delimiters[0])) {}

  auto FindNextDelim() {
    auto next = data.end();
    if (auto pos = this->data.find(delim); pos != bstring::npos) {
      next = this->data.begin() + pos;
    }
    return std::make_pair(next, delim.size());
  }

  bstring delim;
};
#else

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

#endif

}  // namespace irs::analysis
namespace irs {

template<typename Derived>
struct Type<analysis::MultiDelimitedTokenizerSingleCharsBase<Derived>>
  : Type<analysis::MultiDelimitedTokenizer> {};
template<>
struct Type<analysis::MultiDelimitedTokenizerGeneric>
  : Type<analysis::MultiDelimitedTokenizer> {};
template<>
struct Type<analysis::MultiDelimitedTokenizerSingle>
  : Type<analysis::MultiDelimitedTokenizer> {};

}  // namespace irs
namespace irs::analysis {
namespace {

template<size_t N>
Tokenizer::ptr MakeSingleChar(std::vector<bstring>&& delimiters) {
  if constexpr (N >= 2) {
    return std::make_unique<MultiDelimitedTokenizerGenericSingleChars>(
      delimiters);
  } else if (delimiters.size() == N) {
    return std::make_unique<MultiDelimitedTokenizerSingleChars<N>>(delimiters);
  } else {
    return MakeSingleChar<N + 1>(std::move(delimiters));
  }
}

Tokenizer::ptr MakeImpl(std::vector<bstring>&& delimiters) {
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

Tokenizer::ptr MultiDelimitedTokenizer::Make(
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

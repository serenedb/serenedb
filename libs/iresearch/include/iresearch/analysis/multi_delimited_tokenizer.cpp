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
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/fstext/fst_draw.hpp"
#include "iresearch/utils/vpack_utils.hpp"

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
      auto& offset = std::get<irs::OffsAttr>(_attrs);
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
    const MultiDelimitedTokenizer::Options&) {
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
    const MultiDelimitedTokenizer::Options& opts) {
    SDB_ASSERT(opts.delimiters.size() == 1);
    SDB_ASSERT(opts.delimiters[0].size() == 1);
    delim = opts.delimiters[0][0];
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
    const MultiDelimitedTokenizer::Options& opts) {
    SDB_ASSERT(opts.delimiters.empty());
  }

  auto FindNextDelim() { return this->_data.end(); }
};

class MultiDelimitedTokenizerGenericSingleChars final
  : public MultiDelimitedTokenizerSingleCharsBase<
      MultiDelimitedTokenizerGenericSingleChars> {
 public:
  explicit MultiDelimitedTokenizerGenericSingleChars(const Options& opts) {
    for (const auto& delim : opts.delimiters) {
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
  explicit MultiDelimitedTokenizerGeneric(const Options& opts)
    : autom(MakeStringTrie(opts.delimiters)),
      matcher(MakeAutomatonMatcher(autom)) {
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
  explicit MultiDelimitedTokenizerSingle(Options& opts)
    : delim(std::move(opts.delimiters[0])) {}

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
  explicit MultiDelimitedTokenizerSingle(Options& opts)
    : delim(std::move(opts.delimiters[0])),
      searcher(delim.begin(), delim.end()) {}

  auto FindNextDelim() {
    auto next = std::search(_data.begin(), _data.end(), searcher);
    return std::make_pair(next, delim.size());
  }

  bstring delim;
  std::boyer_moore_searcher<bstring::iterator> searcher;
};

#endif

template<size_t N>
Analyzer::ptr MakeSingleChar(MultiDelimitedTokenizer::Options&& opts) {
  if constexpr (N >= 2) {
    return std::make_unique<MultiDelimitedTokenizerGenericSingleChars>(
      std::move(opts));
  } else if (opts.delimiters.size() == N) {
    return std::make_unique<MultiDelimitedTokenizerSingleChars<N>>(
      std::move(opts));
  } else {
    return MakeSingleChar<N + 1>(std::move(opts));
  }
}

Analyzer::ptr MakeImpl(MultiDelimitedTokenizer::Options&& opts) {
  const bool single_character_case = absl::c_all_of(
    opts.delimiters, [](const auto& delim) { return delim.size() == 1; });
  if (single_character_case) {
    return MakeSingleChar<0>(std::move(opts));
  }
  if (opts.delimiters.size() == 1) {
    return std::make_unique<MultiDelimitedTokenizerSingle>(opts);
  }
  return std::make_unique<MultiDelimitedTokenizerGeneric>(std::move(opts));
}

constexpr std::string_view kDelimiterParamName{"delimiters"};

bool ParseVPackOptions(vpack::Slice slice,
                       MultiDelimitedTokenizer::Options& options) {
  if (!slice.isObject()) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Slice for multi_delimited_token_stream is not an object or string");
    return false;
  }
  auto delim_array_slice = slice.get(kDelimiterParamName);
  if (!delim_array_slice.isArray()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid type or missing '",
             kDelimiterParamName,
             "' (array expected) for multi_delimited_token_stream from "
             "VPack arguments");
    return false;
  }

  for (auto delim : vpack::ArrayIterator(delim_array_slice)) {
    if (!delim.isString()) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid type in '",
               kDelimiterParamName,
               "' (string expected) for multi_delimited_token_stream from "
               "VPack arguments");
      return false;
    }
    auto view = ViewCast<byte_type>(delim.stringView());

    if (view.empty()) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Delimiter list contains an empty string.");
      return false;
    }

    for (const auto& known : options.delimiters) {
      if (view.starts_with(known) || known.starts_with(view)) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Some delimiters are a prefix of others. See `",
                               ViewCast<char>(bytes_view{known}), "` and `",
                               delim.stringView(), "`"));
        return false;
      }
    }

    options.delimiters.emplace_back(view);
  }
  return true;
}

bool MakeVPackConfig(const MultiDelimitedTokenizer::Options& options,
                     vpack::Builder* vpack_builder) {
  vpack::ObjectBuilder object(vpack_builder);
  {
    vpack::ArrayBuilder array(vpack_builder, kDelimiterParamName);
    for (bytes_view delim : options.delimiters) {
      vpack_builder->add(ViewCast<char>(delim));
    }
  }

  return true;
}

Analyzer::ptr MakeVPack(vpack::Slice slice) {
  MultiDelimitedTokenizer::Options options;
  if (ParseVPackOptions(slice, options)) {
    return MultiDelimitedTokenizer::Make(std::move(options));
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

bool NormalizeVPackConfig(vpack::Slice slice, vpack::Builder* vpack_builder) {
  MultiDelimitedTokenizer::Options options;
  if (ParseVPackOptions(slice, options)) {
    return MakeVPackConfig(options, vpack_builder);
  }
  return false;
}

bool NormalizeVPackConfig(std::string_view args, std::string& definition) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  bool res = NormalizeVPackConfig(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

}  // namespace

void MultiDelimitedTokenizer::init() {
  REGISTER_ANALYZER_VPACK(MultiDelimitedTokenizer, MakeVPack,
                          NormalizeVPackConfig);
}

Analyzer::ptr MultiDelimitedTokenizer::Make(
  MultiDelimitedTokenizer::Options&& opts) {
  return MakeImpl(std::move(opts));
}

}  // namespace irs::analysis

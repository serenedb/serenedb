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

#include <absl/functional/function_ref.h>

#include <memory>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/conjunction_acceptor.hpp"
#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct FieldMeta;

// The streams a posting list lives in. Handed to whatever decodes a term's
// postings so it can reopen what it needs; `pos` and `pay` are null for a
// field that stores neither.
struct PostingsHandles {
  const IndexInput* doc = nullptr;
  const IndexInput* pos = nullptr;
  const IndexInput* pay = nullptr;
};

struct TermReader : public AttributeProvider {
  using ptr = std::unique_ptr<TermReader>;
  using Acceptor = absl::FunctionRef<bool(doc_id_t)>;

  // Returns an iterator over terms for a field.
  virtual SeekTermIterator::ptr iterator() const = 0;

  // Feeds `acceptor` the documents containing `term`, stopping when it returns
  // false. Bounds-checks against the field's term range first, and answers a
  // df == 1 term straight from its record -- which is why a primary-key probe
  // goes through here rather than building a term iterator and a postings
  // iterator per key.
  virtual void ReadDocs(bytes_view term, Acceptor acceptor) const = 0;

  // The record of `term`; `docs_count == 0` when the field does not hold it,
  // which no record of a real term has -- a term is in the dictionary because
  // some document contains it. Bounds-checks against the field's term range
  // first and walks the dictionary on the stack, so an exact-match probe costs
  // no iterator at all -- which is what an exact-match filter wants, since it
  // has nowhere to walk to afterwards.
  virtual PostingMeta Lookup(bytes_view term) const = 0;

  virtual SeekTermIterator::ptr iterator(
    const RegexpAcceptor& acceptor) const = 0;

  virtual SeekTermIterator::ptr iterator(
    const LevenshteinAcceptor& acceptor) const = 0;

  virtual SeekTermIterator::ptr iterator(
    const RegexpConjunction& acceptor) const = 0;

  virtual SeekTermIterator::ptr iterator(
    const FuzzyConjunction& acceptor) const = 0;

  virtual std::unique_ptr<IndexInput> ReopenPayload() const { return nullptr; }

  // Returns field metadata.
  virtual const FieldMeta& meta() const = 0;

  // Returns total number of terms.
  virtual size_t size() const = 0;

  // Returns total number of documents with at least 1 term in a field.
  virtual uint64_t docs_count() const = 0;

  // Returns the least significant term.
  virtual bytes_view min() const = 0;

  // Returns the most significant term.
  virtual bytes_view max() const = 0;

  // Returns true if the field has per-block score bounds persisted.
  virtual bool HasScoreBounds() const = 0;

  // The streams this field's postings live in.
  virtual PostingsHandles Handles() const noexcept = 0;
};

}  // namespace irs

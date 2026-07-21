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

#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/inverter/fields_inverter.hpp>
#include <memory>
#include <vector>

namespace tests {

// Engine-level per-doc drain of an id-mode batch into one inverter slot,
// expressed on the public block API.
inline bool InvertTokens(irs::FieldInverter& inverter, irs::doc_id_t doc,
                         irs::TokenBatch& batch, bool ends_value) {
  const irs::DocRun runs[2] = {{doc, batch.count},
                               {irs::DocRun::kOpenValue, 0}};
  return inverter.InvertBlock(batch, {runs, ends_value ? 1u : 2u});
}

// Per-value insertion of a Field-concept object (Id / GetIndexFeatures /
// GetTokens returning an unbound Tokenizer / Value returning the raw value),
// expressed on the public block API. Production ingest is block-only; this
// is the test-side shim for fixtures and per-value oracles.
template<typename InsertBlockFn>
class InsertSink final : public irs::TokenConsumer {
 public:
  InsertSink(irs::TokenLayout layout, InsertBlockFn& insert_block)
    : writer{*this}, layout{layout}, _insert_block(insert_block) {}

  void Consume(irs::TokenBatch& batch,
               std::span<const irs::DocRun> runs) final {
    _ok = _ok && _insert_block(batch, runs);
  }

  bool ok() const noexcept { return _ok; }

  irs::TokenWriter writer;
  irs::TokenLayout layout;

 private:
  InsertBlockFn& _insert_block;
  bool _ok = true;
};

template<typename FieldT, typename InsertBlockFn>
bool InsertFieldImpl(irs::doc_id_t doc, FieldT&& field,
                     InsertBlockFn&& insert_block) {
  auto& analyzer = field.GetTokens();
  const auto layout = irs::LayoutFromFeatures(field.GetIndexFeatures());
  InsertSink<InsertBlockFn> sink{layout, insert_block};
  sink.writer.BeginValue(doc);
  analyzer.Fill(field.Value(), sink.writer, sink.layout);
  sink.writer.EndValue();
  sink.writer.Finish();
  return sink.ok();
}

template<typename FieldT>
bool InsertFieldTokens(const irs::IndexWriter::Document& doc, FieldT&& field) {
  return InsertFieldImpl(
    doc.DocId(), field, [&](irs::TokenBatch& batch, auto& runs) {
      return doc.InsertBlock(field.Id(), field.GetIndexFeatures(), batch, runs);
    });
}

// Fields exposing a block-native insert (typed columns) take it; everything
// else drives GetTokens() through the driver onto the public block API.
template<typename FieldT>
bool InsertField(const irs::IndexWriter::Document& doc, FieldT&& field) {
  if constexpr (requires { field.InsertBlockInto(doc); }) {
    return field.InsertBlockInto(doc);
  } else {
    return InsertFieldTokens(doc, field);
  }
}

template<typename FieldT>
bool InsertField(irs::SegmentWriter& writer, irs::doc_id_t doc,
                 FieldT&& field) {
  return InsertFieldImpl(doc, field, [&](irs::TokenBatch& batch, auto& runs) {
    return writer.InsertBlock(field.Id(), field.GetIndexFeatures(), batch,
                              runs);
  });
}

template<typename FieldT>
bool InsertField(const irs::IndexWriter::Document& doc, FieldT* field) {
  return InsertField(doc, *field);
}

template<typename Iterator>
bool InsertFields(const irs::IndexWriter::Document& doc, Iterator begin,
                  Iterator end) {
  for (; doc && begin != end; ++begin) {
    InsertField(doc, *begin);
  }
  return static_cast<bool>(doc);
}

}  // namespace tests

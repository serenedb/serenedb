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

#include <cstdint>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <span>
#include <string>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/log.h"
#include "catalog/transaction.h"
#include "search/writes.h"

namespace sdb::catalog {

std::string SerializeAlter(const duckdb::AlterInfo& info);

std::vector<uint8_t> WriteRecord(duckdb::ClientContext& context,
                                 const SessionTransaction& transaction,
                                 search::Writes* writes);

std::vector<uint8_t> SequenceRecord(const BaseCatalog& catalog,
                                    const duckdb::SequenceValue& value);

std::vector<uint8_t> SearchRecord(duckdb::idx_t catalog, duckdb::idx_t table,
                                  const duckdb::vector<std::string>& entries);

std::span<const uint8_t> CatalogPart(std::span<const uint8_t> payload);

void ReplayRecord(duckdb::ClientContext& context, const Log::Range& range,
                  std::span<uint8_t> payload);

void ReplayCatalogPart(duckdb::ClientContext& context,
                       std::span<uint8_t> catalog);

}  // namespace sdb::catalog

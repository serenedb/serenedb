////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/common/unique_ptr.hpp>
#include <memory>

namespace duckdb {

struct CreateTableInfo;

struct CreateSequenceInfo;

struct CreateSchemaInfo;

struct CreateTypeInfo;
struct CreateViewInfo;
struct CreateMacroInfo;

}  // namespace duckdb
namespace sdb::catalog {

class CreateDatabaseInfo;
class CreateIndexInfo;
class Index;
class InvertedIndex;
class CreateTokenizerInfo;
class Tokenizer;
using TokenizerRef = std::shared_ptr<const Tokenizer>;
class CreateForeignServerInfo;
class VirtualTable;
class VirtualTableSnapshot;

}  // namespace sdb::catalog
namespace sdb {

class ObjectId;

}  // namespace sdb

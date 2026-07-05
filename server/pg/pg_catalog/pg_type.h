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

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/catalog-pg-type.html
// NOLINTBEGIN
struct PgType {
  static constexpr uint64_t kId = 163;
  static constexpr std::string_view kName = "pg_type";

  enum class Typetype : char {
    Base = 'b',
    Composite = 'c',
    Domain = 'd',
    Enum = 'e',
    Pseudo = 'p',
    Range = 'r',
    Multirange = 'm',
  };

  enum class Typcategory : char {
    Array = 'A',
    Boolean = 'B',
    Composite = 'C',
    DateTime = 'D',
    Enum = 'E',
    Geometric = 'G',
    Network = 'I',
    Numeric = 'N',
    Pseudo = 'P',
    Range = 'R',
    String = 'S',
    Timespan = 'T',
    UserDefined = 'U',
    BitString = 'V',
    Unknown = 'X',
    Internal = 'Z',
  };

  enum class Typalign : char {
    Char = 'c',
    Short = 's',
    Int = 'i',
    Double = 'd',
  };

  enum class Typstorage : char {
    Plain = 'p',
    External = 'e',
    Main = 'm',
    Extended = 'x',
  };

  Oid oid;
  Name typname;
  Oid typnamespace;
  Oid typowner;
  int16_t typlen;
  bool typbyval;
  Typetype typtype;
  Typcategory typcategory;
  bool typispreferred;
  bool typisdefined;
  char typdelim;
  Oid typrelid;
  Regproc typsubscript;
  Oid typelem;
  Oid typarray;
  Regproc typinput;
  Regproc typoutput;
  Regproc typreceive;
  Regproc typsend;
  Regproc typmodin;
  Regproc typmodout;
  Regproc typanalyze;
  Typalign typalign;
  Typstorage typstorage;
  bool typnotnull;
  Oid typbasetype;
  int32_t typtypmod;
  int32_t typndims;
  Oid typcollation;
  PgNodeTree typdefaultbin;
  Text typdefault;
  AclColumn typacl;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgType>::GetTableData();

}  // namespace sdb::pg

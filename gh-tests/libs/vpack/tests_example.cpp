////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "tests-common.h"
#include "vpack/common.h"

using sdb::basics::StrSink;

TEST(Example, Alias) {
  // create an array with 10 number members
  vpack::Builder b;

  b.add(vpack::Value(vpack::ValueType::Array));
  for (size_t i = 0; i < 10; ++i) {
    b.add(i);
  }
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  vpack::Slice s(b.start());

  // inspect the outermost value (should be an Array...)
  std::cout << "Slice: " << s << std::endl;
  std::cout << "Type: " << s.type() << std::endl;
  std::cout << "Bytesize: " << s.byteSize() << std::endl;
  std::cout << "Members: " << s.length() << std::endl;

  // now iterate over the array members
  std::cout << "Iterating Array members:" << std::endl;
  for (const auto& it : vpack::ArrayIterator(s)) {
    std::cout << it << ", number value: " << it.getUInt() << std::endl;
  }
}

TEST(Example, ArrayHandling) {
  // create an array with 10 number members
  Builder b;

  b.add(Value(ValueType::Array));
  for (size_t i = 0; i < 10; ++i) {
    b.add(i);
  }
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // filter out all array values less than 5
  Builder filtered = Collection::filter(s, [](Slice current, ValueLength) {
    if (current.getNumber<int>() < 5) {
      // exclude
      return false;
    }
    // include
    return true;
  });

  // print filtered array
  Slice f(filtered.slice());
  std::cout << "Filtered length: " << f.length() << std::endl;

  // now iterate over the (left over) array members
  std::cout << "Iterating Array members:" << std::endl;
  for (const auto& it : ArrayIterator(f)) {
    std::cout << it << ", number value: " << it.getUInt() << std::endl;
  }

  // iterate again, this time using Collection::forEach
  std::cout << "Iterating some Array members using forEach:" << std::endl;
  Collection::forEach(f, [](Slice current, ValueLength index) {
    std::cout << current << ", number value: " << current.getUInt()
              << std::endl;
    return (index != 2);  // stop after the 3rd element (indexes are 0-based)
  });
}

TEST(Example, ArrayIterator) {
  // create an array with 10 number members
  Builder b;

  b.add(Value(ValueType::Array));
  for (size_t i = 0; i < 10; ++i) {
    b.add(i);
  }
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // inspect the outermost value (should be an Array...)
  std::cout << "Slice: " << s << std::endl;
  std::cout << "Type: " << s.type() << std::endl;
  std::cout << "Bytesize: " << s.byteSize() << std::endl;
  std::cout << "Members: " << s.length() << std::endl;

  // now iterate over the array members
  std::cout << "Iterating Array members:" << std::endl;
  for (const auto& it : ArrayIterator(s)) {
    std::cout << it << ", number value: " << it.getUInt() << std::endl;
  }
}

TEST(Example, Builder) {
  // create an object with attributes "b", "a", "l" and "name"
  // note that the attribute names will be sorted in the target VPack object!
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("b", 12);
  b.add("a", true);
  b.add("l", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.add("name", "Gustav");
  b.close();

  // now dump the resulting VPack value
  std::cout << "Resulting VPack:" << std::endl;
  std::cout << "Resulting VPack:" << b.slice() << std::endl;
  std::cout << HexDump(b.slice()) << std::endl;
}

TEST(Example, BuilderFancy) {
  // create an object with attributes "b", "a", "l" and "name"
  // note that the attribute names will be sorted in the target VPack object!
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("b", 12);
  b.add("a", true);
  b.add("l", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.add("name", "Gustav");
  b.close();

  // now dump the resulting VPack value
  std::cout << "Resulting VPack:" << std::endl;
  std::cout << HexDump(b.slice()) << std::endl;
}

TEST(Example, Dumper) {
  Builder b;
  // build an object with attribute names "b", "a", "l", "name"
  b.add(Value(ValueType::Object));
  b.add("b", 12);
  b.add("a", true);
  b.add("l", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.add("name", "Gustav");
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // now dump the Slice into an std::string sink
  StrSink sink;
  Dumper dumper(&sink);
  dumper.Dump(s);

  // and print it
  std::cout << "Resulting VPack:" << std::endl << sink.Impl() << std::endl;
}

TEST(Example, DumperPretty) {
  Builder b;
  // build an object with attribute names "b", "a", "l", "name"
  b.add(Value(ValueType::Object));
  b.add("b", 12);
  b.add("a", true);
  b.add("l", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.add("name", "Gustav");
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  Options dumper_options;
  dumper_options.pretty_print = true;
  // now dump the Slice into an std::string
  StrSink sink;
  Dump(s, &sink, &dumper_options);

  // and print it
  std::cout << "Resulting JSON:" << std::endl << sink.Impl() << std::endl;
}

TEST(Example, ObjectHandling) {
  // create an object with a few members
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("foo", 42);
  b.add("bar", "some string value");
  b.add("baz", Value(ValueType::Object));
  b.add("qux", true);
  b.add("bart", "this is a string");
  b.close();
  b.add("quux", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // get all object keys. returns a vector of strings
  for (const auto& key : Collection::keys(s)) {
    // print key
    std::cout << "Object has key '" << key << "'" << std::endl;
  }

  // get all object values. returns a Builder object with an Array inside
  Builder values = Collection::values(s);
  for (const auto& value : ArrayIterator(values.slice())) {
    std::cout << "Object value is: " << value << ", as JSON: " << value.toJson()
              << std::endl;
  }

  // recursively visit all members in the Object
  // PostOrder here means we'll be visiting compound members before
  // we're diving into their subvalues
  Collection::visitRecursive(
    s, Collection::kPostOrder, [](Slice key, Slice value) -> bool {
      if (!key.isNone()) {
        // we are visiting an Object member
        std::cout << "Visiting Object member: " << key.copyString()
                  << ", value: " << value.toJson() << std::endl;
      } else {
        // we are visiting an Array member
        std::cout << "Visiting Array member: " << value.toJson() << std::endl;
      }
      // to continue visiting, return true. to abort visiting, return false
      return true;
    });
}

TEST(Example, ObjectIterator) {
  // create an object with a few members
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("foo", 42);
  b.add("bar", "some string value");
  b.add("baz", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.close();
  b.add("qux", true);
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // inspect the outermost value (should be an Object...)
  std::cout << "Slice: " << s << std::endl;
  std::cout << "Type: " << s.type() << std::endl;
  std::cout << "Bytesize: " << s.byteSize() << std::endl;
  std::cout << "Members: " << s.length() << std::endl;

  // now iterate over the object members
  std::cout << "Iterating Object members:" << std::endl;
  for (const auto& it : ObjectIterator(s, /*useSequentialIteration*/ true)) {
    std::cout << "key: " << it.key.stringView() << ", value: " << it.value()
              << std::endl;
  }
}

TEST(Example, ObjectLookup) {
  // create an object with a few members
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("foo", 42);
  b.add("bar", "some string value");
  b.add("baz", Value(ValueType::Object));
  b.add("qux", true);
  b.add("bart", "this is a string");
  b.close();
  b.add("quux", 12345);
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // now fetch the string in the object's "bar" attribute
  if (auto bar = s.get("bar"); !bar.isNone()) {
    std::cout << "'bar' attribute value has type: " << bar.type() << std::endl;
  }

  // fetch non-existing attribute "quetzal"
  Slice quetzal(s.get("quetzal"));
  // note: this returns a slice of type None
  std::cout << "'quetzal' attribute value has type: " << quetzal.type()
            << std::endl;
  std::cout << "'quetzal' attribute is None: " << std::boolalpha
            << quetzal.isNone() << std::endl;

  // fetch subattribute "baz.qux"
  Slice qux(s.get(std::vector<std::string>({"baz", "qux"})));
  std::cout << "'baz'.'qux' attribute has type: " << qux.type() << std::endl;
  std::cout << "'baz'.'qux' attribute has bool value: " << std::boolalpha
            << qux.getBool() << std::endl;
  std::cout << "Complete value of 'baz' is: " << s.get("baz").toJson()
            << std::endl;

  // fetch non-existing subattribute "bark.foobar"
  Slice foobar(s.get(std::vector<std::string>({"bark", "foobar"})));
  std::cout << "'bark'.'foobar' attribute is None: " << std::boolalpha
            << foobar.isNone() << std::endl;

  // check if subattribute "baz"."bart" does exist
  auto tmp = s.get(std::vector<std::string>({"baz", "bart"}));
  if (!tmp.isNone()) {
    // access subattribute using operator syntax
    std::cout << "'baz'.'bart' attribute has type: "
              << s.get("baz").get("bart").type() << std::endl;
    std::cout << "'baz'.'bart' attribute has value: '"
              << s.get("baz").get("bart").copyString() << "'" << std::endl;
  }
}

TEST(Example, Parser) {
  // this is the JSON string we are going to parse
  const std::string json = "{\"a\":12}";
  std::cout << "Parsing JSON string '" << json << "'" << std::endl;

  Parser parser;
  try {
    ValueLength nr = parser.parse(json);
    std::cout << "Number of values: " << nr << std::endl;
  } catch (const std::bad_alloc&) {
    std::cout << "Out of memory!" << std::endl;
    throw;
  } catch (const Exception& ex) {
    std::cout << "Parse error: " << ex.what() << std::endl;
    std::cout << "Position of error: " << parser.errorPos() << std::endl;
    throw;
  }

  // get a pointer to the start of the data
  std::shared_ptr<Builder> b = parser.steal();

  // now dump the resulting VPack value
  std::cout << "Resulting VPack:" << std::endl;
  std::cout << HexDump(b->slice()) << std::endl;
}

TEST(Example, Slice) {
  // create an object with attributes "b", "a", "l" and "name"
  // note that the attribute names will be sorted in the target VPack object!
  Builder b;

  b.add(Value(ValueType::Object));
  b.add("b", 12);
  b.add("a", true);
  b.add("l", Value(ValueType::Array));
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();
  b.add("name", "Gustav");
  b.close();

  // a Slice is a lightweight accessor for a VPack value
  Slice s(b.start());

  // inspect the outermost value (should be an Object...)
  std::cout << "Slice: " << s << std::endl;
  std::cout << "Type: " << s.type() << std::endl;
  std::cout << "Bytesize: " << s.byteSize() << std::endl;
  std::cout << "Members: " << s.length() << std::endl;

  if (s.isObject()) {
    Slice ss = s.get("l");  // Now ss points to the subvalue under "l"
    std::cout << "Length of .l: " << ss.length() << std::endl;
    std::cout << "Second entry of .l:" << ss.at(1).getInt() << std::endl;
  }

  Slice sss = s.get("name");
  if (sss.isString()) {
    auto str = sss.stringView();
    std::cout << "Name in .name: " << str << std::endl;
  }
}

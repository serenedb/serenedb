import os
import re

from . import source
from .common import DUCKDB_INCLUDE, camel, cpp_bool, cpp_char, cpp_str

CATALOG_TYPE = os.path.join(DUCKDB_INCLUDE, 'common', 'enums',
                            'catalog_type.hpp')
PRIVILEGES = os.path.join(DUCKDB_INCLUDE, 'catalog', 'privileges.hpp')

CONTAINERS = ('NONE', 'CLUSTER', 'DATABASE', 'SCHEMA', 'RELATION')

USES = (('Own', 33), ('Drop', 34), ('Rename', 35), ('SetRole', 37),
        ('Any', 38))

KINDS = (
    ('OBJECT_TABLE', 'SCHEMA', ('TABLE_ENTRY', ('VIEW_ENTRY', 'OBJECT_VIEW'))),
    ('OBJECT_SEQUENCE', 'SCHEMA', ('SEQUENCE_ENTRY',)),
    ('OBJECT_INDEX', 'RELATION', ('INDEX_ENTRY',)),
    ('OBJECT_TYPE', 'SCHEMA', ('TYPE_ENTRY',)),
    ('OBJECT_FUNCTION', 'SCHEMA', ('MACRO_ENTRY', 'TABLE_MACRO_ENTRY')),
    ('OBJECT_TSDICTIONARY', 'SCHEMA', ('TOKENIZER_ENTRY',)),
    ('OBJECT_SCHEMA', 'DATABASE', ('SCHEMA_ENTRY',)),
    ('OBJECT_FOREIGN_SERVER', 'DATABASE', ('FOREIGN_SERVER_ENTRY',)),
    ('OBJECT_SUBSCRIPTION', 'DATABASE', ('SUBSCRIPTION_ENTRY',)),
    ('OBJECT_DATABASE', 'CLUSTER', ('DATABASE_ENTRY',)),
    ('OBJECT_TABLESPACE', 'CLUSTER', ('TABLESPACE_ENTRY',)),
    ('OBJECT_ROLE', 'CLUSTER', ('ROLE_ENTRY',)),
    ('OBJECT_FDW', 'DATABASE', ('FOREIGN_DATA_WRAPPER_ENTRY',)),
    (None, 'SCHEMA', ('JOB_ENTRY',)),
    (None, 'NONE', ('INVALID', 'PREPARED_STATEMENT', 'COLLATION_ENTRY',
                    'COORDINATE_SYSTEM_ENTRY', 'TRIGGER_ENTRY',
                    'REPLICATION_ORIGIN_ENTRY',
                    ('TABLE_FUNCTION_ENTRY', 'OBJECT_FUNCTION'),
                    ('SCALAR_FUNCTION_ENTRY', 'OBJECT_FUNCTION'),
                    ('AGGREGATE_FUNCTION_ENTRY', 'OBJECT_AGGREGATE'),
                    ('PRAGMA_FUNCTION_ENTRY', 'OBJECT_FUNCTION'),
                    'COPY_FUNCTION_ENTRY',
                    ('WINDOW_FUNCTION_ENTRY', 'OBJECT_FUNCTION'),
                    'DELETED_ENTRY', 'RENAMED_ENTRY', 'SECRET_ENTRY',
                    'SECRET_TYPE_ENTRY', 'SECRET_FUNCTION_ENTRY',
                    'DEPENDENCY_ENTRY')),
)


def named_kinds():
    for objtype, container, kinds in KINDS:
        named = [(kind, objtype) if isinstance(kind, str) else kind
                 for kind in kinds]
        yield objtype, container, named


def catalog_types():
    with open(CATALOG_TYPE) as header:
        text = header.read()
    body = text[text.index('enum class CatalogType'):]
    return re.findall(r'^\s*(\w+) = \d+', body[:body.index('};')], re.M)


def check(classes, defaults, nouns):
    declared = catalog_types()
    listed = [kind for _, _, kinds in named_kinds() for kind, _ in kinds]
    missing = [kind for kind in declared if kind not in listed]
    unknown = sorted({kind for kind in listed if kind not in declared} |
                     {kind for kind in listed if listed.count(kind) > 1})
    known = classes.keys() | defaults.keys() | nouns.keys()
    unmatched = sorted({name for _, _, kinds in named_kinds()
                        for _, name in kinds if name and name not in known})
    if missing or unknown or unmatched:
        raise SystemExit(
            'scripts/pg_catalog/privileges.py: KINDS must name every '
            f'CatalogType once (missing {missing}, unknown or repeated '
            f'{unknown}) and only PostgreSQL object types (unknown {unmatched})')


def generate(gen):
    bits = source.privilege_bits()
    order = [name for name, _ in bits]
    defaults = source.default_privileges()
    classes = source.object_type_classes()
    nouns = source.object_nouns()
    shared = {relation.oid for relation in gen.relations.values()
              if relation.shared}
    check(classes, defaults, nouns)

    def mode(names, prefix=''):
        return ' | '.join(
            f'{prefix}AclMode::{camel(name.lower())}'
            for name in sorted(set(names), key=order.index)) or (
                f'{prefix}AclMode::NoRights')

    letters = source.privilege_letters()
    names = source.privilege_names()
    lines = [
        '#pragma once',
        '',
        '#include "duckdb/common/constants.hpp"',
        '#include "duckdb/common/enums/catalog_type.hpp"',
        '',
        '#include <array>',
        '#include <limits>',
        '#include <string_view>',
        '#include <type_traits>',
        '',
        'namespace duckdb {',
        '',
        'enum class AclMode : uint64_t {',
        '\tNoRights = 0x0,',
    ]
    lines += [f'\t{camel(name.lower())} = {hex(1 << bit)},'
              for name, bit in bits]
    lines += [f'\t{name} = {hex(1 << bit)},' for name, bit in USES]
    lines += [
        '};',
        '',
        'constexpr AclMode operator|(AclMode a, AclMode b) {',
        '\treturn static_cast<AclMode>(static_cast<uint64_t>(a) | '
        'static_cast<uint64_t>(b));',
        '}',
        'constexpr AclMode operator&(AclMode a, AclMode b) {',
        '\treturn static_cast<AclMode>(static_cast<uint64_t>(a) & '
        'static_cast<uint64_t>(b));',
        '}',
        'constexpr AclMode operator~(AclMode a) {',
        '\treturn static_cast<AclMode>(~static_cast<uint64_t>(a));',
        '}',
        'constexpr AclMode &operator|=(AclMode &a, AclMode b) {',
        '\ta = a | b;',
        '\treturn a;',
        '}',
        'constexpr AclMode &operator&=(AclMode &a, AclMode b) {',
        '\ta = a & b;',
        '\treturn a;',
        '}',
        '',
        'struct AclPrivilege {',
        '\tAclMode mode;',
        '\tchar letter;',
        '\tstd::string_view keyword;',
        '};',
        '',
        'inline constexpr AclPrivilege ACL_PRIVILEGES[] = {',
    ]
    lines += [f'\t{{AclMode::{camel(name.lower())}, {cpp_char(letters[name])}, '
              f'{cpp_str(names[name])}}},' for name in order]
    lines += [
        '};',
        '',
        'inline constexpr AclMode ACL_COLUMN_PRIVILEGES = '
        f'{mode(source.all_rights()["COLUMN"])};',
        f'inline constexpr AclMode ACL_GRANTABLE = {mode(order)};',
        '',
        f'enum class ObjectContainer : uint8_t {{ {", ".join(CONTAINERS)} }};',
        '',
        'struct ObjectKind {',
        '\tCatalogType acl_class;',
        '\tObjectContainer container;',
        '\tAclMode privileges;',
        '\tAclMode public_privileges;',
        '\tidx_t pg_class;',
        '\tbool shared;',
        '\tstd::string_view noun;',
        '};',
        '',
        'constexpr ObjectKind MakeObjectKind(CatalogType type) {',
        '\tswitch (type) {',
    ]
    unnamed = []
    for objtype, container, kinds in named_kinds():
        public, owner = defaults.get(objtype, ([], []))
        pg_class = classes.get(objtype, 0)
        for kind, name in kinds:
            if container == 'NONE' and name not in nouns:
                unnamed.append(kind)
                continue
            acl_class = kind if container == 'NONE' else kinds[0][0]
            lines.append(f'\tcase CatalogType::{kind}:')
            lines.append(f'\t\treturn {{CatalogType::{acl_class}, '
                         f'ObjectContainer::{container}, {mode(owner)}, '
                         f'{mode(public)}, {pg_class}, '
                         f'{cpp_bool(pg_class in shared)}, '
                         f'{cpp_str(nouns.get(name, ""))}}};')
    lines += [f'\tcase CatalogType::{kind}:' for kind in unnamed]
    lines += [
        '\t\tbreak;',
        '\t}',
        '\treturn {type, ObjectContainer::NONE, AclMode::NoRights, '
        'AclMode::NoRights, 0, false, {}};',
        '}',
        '',
        'inline constexpr auto OBJECT_KINDS = [] {',
        '\tstd::array<ObjectKind, std::numeric_limits<std::underlying_type_t'
        '<CatalogType>>::max() + 1> kinds {};',
        '\tfor (idx_t type = 0; type < kinds.size(); type++) {',
        '\t\tkinds[type] = MakeObjectKind(static_cast<CatalogType>(type));',
        '\t}',
        '\treturn kinds;',
        '}();',
        '',
        'constexpr const ObjectKind &ObjectKindOf(CatalogType type) {',
        '\treturn OBJECT_KINDS[static_cast<std::underlying_type_t<CatalogType>>'
        '(type)];',
        '}',
        '',
        'constexpr bool IsCatalogLevel(CatalogType type) {',
        '\tconst auto container = ObjectKindOf(type).container;',
        '\treturn type != CatalogType::SCHEMA_ENTRY && '
        '(container == ObjectContainer::CLUSTER || '
        'container == ObjectContainer::DATABASE);',
        '}',
        '',
        '} // namespace duckdb',
    ]
    gen.write_path(PRIVILEGES, lines)

    lines = ['inline constexpr DefaultAcl kDefaultAcls[] = {']
    for letter, objtype in source.default_privilege_letters():
        public, owner = defaults[objtype]
        lines.append(f'  {{{cpp_char(letter)}, {mode(public, "duckdb::")}, '
                     f'{mode(owner, "duckdb::")}}},')
    lines.append('};')
    gen.write('acl_defaults.gen.inc', lines)

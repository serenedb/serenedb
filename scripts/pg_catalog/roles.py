import os

from .common import DUCKDB_INCLUDE, camel, cpp_str

ROLE_CAPABILITY = os.path.join(DUCKDB_INCLUDE, 'common', 'enums',
                               'role_capability.hpp')

OPTIONS = {
    'rolsuper': 'Superuser',
    'rolinherit': 'Inherit',
    'rolcreaterole': 'CreateRole',
    'rolcreatedb': 'CreateDb',
    'rolcanlogin': 'Login',
    'rolreplication': 'Replication',
    'rolbypassrls': 'BypassRls',
}


def member_array(name):
    return f'k{camel(name)}MemberOf'


def predefined(gen):
    return {row['rolname']: row for row in gen.rows['pg_authid']
            if row['rolsuper'] == 'f'}


def constants(gen):
    return [f'inline constexpr duckdb::idx_t k{camel(name)}Role = '
            f'{row["oid"]};'
            for name, row in predefined(gen).items()]


def generate(gen):
    roles = predefined(gen)
    member_of = {}
    for granted, member in gen.initdb.role_grants:
        member_of.setdefault(member, []).append(granted)
    ordered = []

    def visit(name):
        if name not in ordered:
            for granted in member_of.get(name, []):
                visit(granted)
            ordered.append(name)

    for name in roles:
        visit(name)
    lines = [f'inline constexpr duckdb::idx_t {member_array(name)}[] = {{'
             + ', '.join(roles[granted]['oid'] for granted in member_of[name])
             + '};'
             for name in ordered if name in member_of]
    lines.append('inline constexpr PredefinedRole kPredefinedRoles[] = {')
    for name in ordered:
        row = roles[name]
        options = ' | '.join(f'RoleOption::{option}'
                             for column, option in OPTIONS.items()
                             if row[column] == 't') or 'RoleOption::None'
        members = member_array(name) if name in member_of else '{}'
        lines.append(f'  {{{row["oid"]}, {cpp_str(name)}, {options}, '
                     f'{members}}},')
    lines.append('};')
    gen.write('predefined_roles.gen.inc', lines)
    capabilities(gen, roles)


def capabilities(gen, roles):
    names = {name: camel(name.removeprefix('pg_')) for name in roles}
    lines = [
        '#pragma once',
        '',
        '#include "duckdb/common/constants.hpp"',
        '',
        'namespace duckdb {',
        '',
        'enum class RoleCapability : uint32_t {',
        '\tNone = 0x0,',
    ]
    lines += [f'\t{capability} = {hex(1 << bit)},'
              for bit, capability in enumerate(names.values())]
    lines += [
        f'\tSuperuser = {hex(1 << len(names))},',
        '};',
        '',
        'constexpr RoleCapability operator|(RoleCapability a, '
        'RoleCapability b) {',
        '\treturn static_cast<RoleCapability>(static_cast<uint32_t>(a) | '
        'static_cast<uint32_t>(b));',
        '}',
        'constexpr RoleCapability operator&(RoleCapability a, '
        'RoleCapability b) {',
        '\treturn static_cast<RoleCapability>(static_cast<uint32_t>(a) & '
        'static_cast<uint32_t>(b));',
        '}',
        'constexpr RoleCapability &operator|=(RoleCapability &a, '
        'RoleCapability b) {',
        '\ta = a | b;',
        '\treturn a;',
        '}',
        '',
        'inline constexpr RoleCapability PREDEFINED_ROLES[] = {'
        + ', '.join(f'RoleCapability::{capability}'
                    for capability in names.values()) + '};',
        '',
        'constexpr idx_t PredefinedRoleOid(RoleCapability role) {',
        '\tswitch (role) {',
    ]
    for name, capability in names.items():
        lines += [f'\tcase RoleCapability::{capability}:',
                  f'\t\treturn {roles[name]["oid"]};']
    lines += [
        '\tdefault:',
        '\t\treturn 0;',
        '\t}',
        '}',
        '',
        '} // namespace duckdb',
    ]
    gen.write_path(ROLE_CAPABILITY, lines)

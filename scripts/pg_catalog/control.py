import re

from . import source
from .common import camel, cpp_str

CONTROL_HEADER = 'catalog/pg_control.h'
TYPEDEF_HEADERS = ('c.h', 'postgres_ext.h', 'pgtime.h', 'access/xlogdefs.h',
                   'access/transam.h', 'port/pg_crc32c.h')
CONSTANTS = (
    ('uint32_t', CONTROL_HEADER, 'PG_CONTROL_VERSION'),
    ('uint32_t', 'catalog/catversion.h', 'CATALOG_VERSION_NO'),
    ('size_t', CONTROL_HEADER, 'PG_CONTROL_FILE_SIZE'),
    ('size_t', CONTROL_HEADER, 'PG_CONTROL_MAX_SAFE_SIZE'),
    ('size_t', CONTROL_HEADER, 'MOCK_AUTH_NONCE_LEN'),
    ('double', CONTROL_HEADER, 'FLOATFORMAT_VALUE'),
    ('uint64_t', 'access/xlogdefs.h', 'FirstNormalUnloggedLSN'),
)
ENUMS = ('DBState',)
STRUCTS = ('CheckPoint', 'ControlFileData')
CPP_TYPES = {
    'bool': 'bool', 'char': 'char', 'double': 'double', 'int': 'int32_t',
    'unsigned int': 'uint32_t', 'int32_t': 'int32_t', 'int64_t': 'int64_t',
    'uint32_t': 'uint32_t', 'uint64_t': 'uint64_t',
}
COMMENT = re.compile(r'/\*.*?\*/', re.S)
TYPEDEF = re.compile(r'^typedef ((?:unsigned )?\w+)\s+(\w+);', re.M)
WRAPPER = re.compile(r'^typedef struct (\w+)\s*\{\s*(\w+)\s+\w+;\s*\}\s*\1;',
                     re.M)
FIELD = re.compile(r'(\w+)\s+(\w+)(?:\[(\w+)\])?;')
ENUMERATOR = re.compile(r'(\w+)(?:\s*=\s*(\d+))?\s*,')


def header(name):
    return COMMENT.sub('', source.read('src', 'include', name))


def snake(name):
    return re.sub(r'(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', '_',
                  name).lower()


def constant(macro):
    return 'k' + camel(snake(macro))


def macro(name, text):
    raw = re.search(rf'^#define\s+{name}\s+(.+?)\s*$', text, re.M).group(1)
    return re.fullmatch(r'\(\(\w+\)\s*(\S+)\)|(\S+)', raw).group(1, 2)


def typedefs():
    out = {}
    for name in TYPEDEF_HEADERS:
        text = header(name)
        out.update({alias: base for base, alias in TYPEDEF.findall(text)})
        out.update(dict(WRAPPER.findall(text)))
    return out


def body(text, kind, name):
    match = re.search(rf'^typedef {kind} {name}\n\{{\n(.*?)\n\}} {name};',
                      text, re.S | re.M)
    if not match:
        raise SystemExit(f'{CONTROL_HEADER} defines no {kind} {name}')
    return match.group(1)


def enum_lines(text, name):
    lines, value = [f'enum class {camel(snake(name))} : int32_t {{'], 0
    for enumerator, explicit in ENUMERATOR.findall(body(text, 'enum', name)):
        value = int(explicit) if explicit else value
        lines.append(f'  {constant(enumerator)} = {value},')
        value += 1
    return lines + ['};']


def cpp_type(name, aliases, known):
    if name in known:
        return known[name]
    while name not in CPP_TYPES:
        if name not in aliases:
            raise SystemExit(f'{CONTROL_HEADER}: no C++ type for {name}')
        name = aliases[name]
    return CPP_TYPES[name]


def struct_lines(text, name, aliases, known):
    lines = [f'struct {name} {{']
    for line in body(text, 'struct', name).splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        field = FIELD.fullmatch(line)
        if not field:
            raise SystemExit(f'{CONTROL_HEADER}: cannot read the {name} field '
                             f'"{line}"')
        type_name, field_name, length = field.groups()
        cpp = cpp_type(type_name, aliases, known)
        if length:
            cpp = f'std::array<{cpp}, {constant(length)}>'
        lines.append(f'  {cpp} {snake(field_name)};')
    return lines + ['};']


def control_lines():
    lines = []
    for cpp, name, define in CONSTANTS:
        value = ''.join(v for v in macro(define, header(name)) if v)
        lines.append(f'inline constexpr {cpp} {constant(define)} = {value};')
    text = header(CONTROL_HEADER)
    known = {}
    for name in ENUMS:
        lines += enum_lines(text, name)
        known[name] = camel(snake(name))
    aliases = typedefs()
    for name in STRUCTS:
        lines += struct_lines(text, name, aliases, known)
        known[name] = name
    return lines


def version_lines():
    version = source.version()
    major, minor = (int(part) for part in version.split('.'))
    return [
        f'inline constexpr std::string_view kPgServerVersion = '
        f'{cpp_str(version)};',
        f'inline constexpr int32_t kPgServerVersionNum = '
        f'{major * 10000 + minor};',
        f'inline constexpr int32_t kPgMajorVersion = {major};',
    ]


def generate(gen):
    gen.write('pg_control.gen.inc', control_lines())
    gen.write('version.gen.inc', version_lines())

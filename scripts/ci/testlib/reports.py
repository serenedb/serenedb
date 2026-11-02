import re
from dataclasses import dataclass
from pathlib import Path

_SAN_HEAD = re.compile(
    r"^(?:==(?:[^=\s]+==)?\d+==\s*)?(?:ERROR|WARNING): (AddressSanitizer|LeakSanitizer|ThreadSanitizer|"
    r"MemorySanitizer|UndefinedBehaviorSanitizer): ([\w-]+(?: [\w-]+)*)"
)
_UBSAN_HEAD = re.compile(r"^(\S+:\d+:\d+): runtime error: (.*)$")
_UBSAN_ADDRESS = re.compile(r"0x[0-9a-f]+")
_UBSAN_VALUE = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?(?:e[+-]?\d+)?(?![\w.])")
_FRAME = re.compile(r"^\s*#\d+ 0x[0-9a-f]+ in (.+?)(?: (\S+:\d+(?::\d+)?))?$")
_CRASH_HEAD = re.compile(r"\*\*\* (SIG\w+) received at time=")
_ASSERT = re.compile(r"assertion failed in (\S+) \[([^\]]*)\]: (.*)")
_ABSL_FRAME = re.compile(r"^(?:\[state=\w+\] )?\s*@\s+0x[0-9a-f]+\s+\d+\s+(.+)$")
_RUNTIME = ("__sanitizer", "__asan", "__tsan", "__msan", "__ubsan", "__lsan", "__interceptor",
            "___interceptor", "operator new", "operator delete", "malloc", "calloc", "realloc",
            "free", "std::__", "absl::", "__libc", "__cxa", "_start", "start_thread", "clone")


@dataclass
class Report:
    kind: str
    signature: str
    path: str
    excerpt: str


def _frames(lines, start, pattern=_FRAME, limit=3):
    names = []
    for line in lines[start:start + 80]:
        m = pattern.match(line)
        if not m:
            if names and not line.strip():
                break
            continue
        name = re.sub(r"\(.*", "", m.group(1)).strip()
        if any(name.startswith(r) for r in _RUNTIME):
            continue
        names.append(name)
        if len(names) == limit:
            break
    return names


def scan_sanitizer_text(text, path):
    reports = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = _SAN_HEAD.match(line)
        if m:
            tool, kind = m.group(1), re.split(r" (?:on\b|0x)", m.group(2))[0]
            frames = _frames(lines, i + 1)
            sig = f"{tool}: {kind}" + (f" in {' < '.join(frames)}" if frames else "")
            reports.append(Report(tool, sig, str(path), "\n".join(lines[i:i + 40])))
            continue
        m = _UBSAN_HEAD.match(line)
        if m:
            where = re.sub(r":\d+:\d+$", "", m.group(1))
            what = _UBSAN_VALUE.sub("N", _UBSAN_ADDRESS.sub("0x...", m.group(2)))
            sig = f"UndefinedBehaviorSanitizer: {what[:120]} at {where}"
            reports.append(Report("UndefinedBehaviorSanitizer", sig, str(path),
                                  "\n".join(lines[i:i + 30])))
    return reports


def scan_sanitizer_files(directory):
    directory = Path(directory)
    if not directory.is_dir():
        return
    for path in sorted(directory.iterdir()):
        if not re.match(r"^(asan|ubsan|tsan|msan|lsan)\.", path.name):
            continue
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        yield path, scan_sanitizer_text(text, path)


def scan_sanitizer_dir(directory):
    reports = [r for _, found in scan_sanitizer_files(directory) for r in found]
    seen = set()
    unique = []
    for r in reports:
        if r.signature not in seen:
            seen.add(r.signature)
            unique.append(r)
    return unique


def scan_crash(text, path):
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = _ASSERT.search(line)
        if m:
            where = re.sub(r":\d+$", "", m.group(1))
            return Report("assertion", f"assertion failed in {m.group(2) or where}: {m.group(3)[:160]}",
                          str(path), "\n".join(lines[i:i + 60]))
    for i, line in enumerate(lines):
        m = _CRASH_HEAD.search(line)
        if m:
            frames = _frames(lines, i + 1, pattern=_ABSL_FRAME)
            sig = f"{m.group(1)}" + (f" in {' < '.join(frames)}" if frames else "")
            return Report("crash", sig, str(path), "\n".join(lines[i:i + 60]))
    return None


def tail(path, limit=6000):
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - limit))
            return fh.read().decode("utf-8", "replace")
    except OSError:
        return ""

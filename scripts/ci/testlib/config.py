import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    name: str
    slowdown: float
    units_per_server: int


CONFIGS = {
    "perf": Config("perf", 1.0, 16),
    "dev": Config("dev", 2.0, 16),
    "asan": Config("asan", 8.0, 8),
    "tsan": Config("tsan", 12.0, 4),
    "msan": Config("msan", 8.0, 8),
}

SANITIZER_CONFIG = {
    "UndefinedAddress": "asan",
    "Address": "asan",
    "Thread": "tsan",
    "Memory": "msan",
    "MemoryWithOrigins": "msan",
}


def _cmake_cache(build_dir):
    values = {}
    try:
        text = (build_dir / "CMakeCache.txt").read_text()
    except OSError:
        return values
    for line in text.splitlines():
        if not line or line[0] in "#/" or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.partition(":")[0]] = value
    return values


def _on(value):
    return value.upper() in ("ON", "TRUE", "1", "YES")


@dataclass(frozen=True)
class Build:
    root: Path
    dir: Path
    sanitize: str
    gtests: bool
    fault_injection: bool
    coverage: bool
    duckdb_unittests: bool
    build_type: str

    @classmethod
    def absent(cls, root):
        return cls(root=root, dir=None, sanitize="", gtests=False, fault_injection=False,
                   coverage=False, duckdb_unittests=False, build_type="")

    @classmethod
    def load(cls, root, build_dir):
        cache = _cmake_cache(build_dir)
        if not cache:
            raise SystemExit(f"{build_dir} has no CMakeCache.txt: not a build directory")
        return cls(
            root=root,
            dir=build_dir,
            sanitize=cache.get("SDB_SANITIZE", ""),
            gtests=_on(cache.get("SDB_GTEST", "OFF")),
            fault_injection=_on(cache.get("SDB_FAULT_INJECTION", "OFF")),
            coverage=_on(cache.get("SDB_COVERAGE", "OFF")),
            duckdb_unittests=_on(cache.get("SDB_BUILD_DUCKDB_UNITTESTS", "OFF")),
            build_type=cache.get("CMAKE_BUILD_TYPE", ""),
        )

    def config_name(self):
        if self.sanitize:
            return SANITIZER_CONFIG.get(self.sanitize, self.sanitize.lower())
        return "dev" if self.gtests else "perf"

    def bin(self, name):
        return self.dir / "bin" / name

    def sanitizer_env(self, log_dir):
        suppressions = self.root / "resources" / "suppressions"
        common = "log_exe_name=true:print_suppressions=0"
        env = {}

        def opts(*parts, supp=None):
            items = [common, *parts]
            if supp and (suppressions / supp).is_file():
                items.append(f"suppressions={suppressions / supp}")
            return ":".join(items)

        san = self.sanitize
        if "Address" in san:
            env["ASAN_OPTIONS"] = opts(
                f"log_path={log_dir}/asan",
                "handle_ioctl=true",
                "check_initialization_order=true",
                "detect_odr_violation=1",
                "strict_init_order=true",
                supp="asan.txt",
            )
            env["LSAN_OPTIONS"] = opts(supp="lsan.txt")
        if "Undefined" in san:
            env["UBSAN_OPTIONS"] = opts(
                f"log_path={log_dir}/ubsan", "print_stacktrace=1", supp="ubsan.txt"
            )
        if san == "Thread":
            env["TSAN_OPTIONS"] = opts(
                f"log_path={log_dir}/tsan",
                "detect_deadlocks=true",
                "second_deadlock_stack=1",
                "history_size=0",
                supp="tsan.txt",
            )
        if san.startswith("Memory"):
            env["MSAN_OPTIONS"] = opts(
                f"log_path={log_dir}/msan", "poison_in_dtor=1", supp="msan.txt"
            )
        return env

    def coverage_env(self, profile_dir):
        if not self.coverage:
            return {}
        return {"LLVM_PROFILE_FILE": f"{profile_dir}/%4m.profraw"}


def cpu_count():
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 4


def memory_gb():
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) / (1 << 20)
    except OSError:
        pass
    try:
        return int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True,
                                  check=True).stdout) / (1 << 30)
    except (OSError, ValueError, subprocess.CalledProcessError):
        return 16.0

#!/usr/bin/env python3

import argparse
import json
import os
import re
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile


HEADER_SUFFIXES = {
    "", ".h", ".hh", ".hpp", ".hxx", ".tpp", ".inc", ".def", ".ipp", ".inl", ".cuh", ".tcc",
}
SYSTEM_LIBRARIES = {
    "c", "m", "dl", "pthread", "rt", "atomic", "System", "iconv", "resolv", "util",
    "gcc", "gcc_s", "gcc_eh",
}
PROFILE_OPTIONS = {"-fprofile-instr-generate", "-fprofile-generate", "-fcs-profile-generate"}


def expand_response_files(arguments, cwd):
    expanded = []
    for argument in arguments:
        if argument.startswith("@"):
            path = Path(argument[1:])
            if not path.is_absolute():
                path = cwd / path
            expanded.extend(expand_response_files(shlex.split(path.read_text()), cwd))
        else:
            expanded.append(argument)
    return expanded


def capture(path, command):
    cwd = Path.cwd()
    arguments = expand_response_files(command, cwd)
    if PROFILE_OPTIONS.intersection(arguments):
        driver = subprocess.run([*command, "-###"], check=True, capture_output=True, text=True)
        runtimes = {
            argument
            for line in driver.stderr.splitlines() if line.lstrip().startswith('"')
            for argument in shlex.split(line)
            if re.fullmatch(r"libclang_rt\.profile(?:[-_].+)?\.a", Path(argument).name)
        }
        if len(runtimes) != 1:
            raise RuntimeError("the Clang link command must select one profiling runtime archive")
        runtime = runtimes.pop()
        command = [command[0], runtime, *command[1:]]
        arguments.insert(1, runtime)
    subprocess.run(command, check=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"cwd": str(cwd), "arguments": arguments}))


def link_inputs(link, *, validate_files=True):
    data = json.loads(link.read_text())
    cwd = Path(data["cwd"])
    arguments = [
        value
        for argument in data["arguments"][1:]
        for value in (argument.split(",")[1:] if argument.startswith("-Wl,") else [argument])
    ]
    files, libraries, options = [], [], []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        index += 1
        if argument in {"-o", "-L", "-isysroot", "-arch", "-target"}:
            index += 1
            continue
        if argument == "-framework":
            options.append("SHELL:-framework " + shlex.quote(arguments[index]))
            index += 1
            continue
        if argument == "-pthread" or argument in PROFILE_OPTIONS:
            options.append(argument)
            continue
        if argument.startswith("-l"):
            name = argument[2:]
            if name not in SYSTEM_LIBRARIES:
                raise RuntimeError(f"unbundled library in link command: {argument}")
            libraries.append(name)
            continue
        if not argument or argument.startswith("-"):
            continue
        path = Path(argument)
        shared = re.fullmatch(r"lib(.+?)\.so(?:\..+)?|lib(.+?)\.dylib", path.name)
        if not shared and path.suffix not in {".a", ".o", ".obj"}:
            continue
        if not path.is_absolute():
            path = cwd / path
        path = path.resolve()
        if shared:
            name = (shared[1] or shared[2]).removesuffix(".B")
            if name not in SYSTEM_LIBRARIES or not str(path).startswith(
                ("/usr/lib/", "/lib/", "/usr/lib64/", "/lib64/", "/System/Library/")
            ):
                raise RuntimeError(f"non-system shared dependency: {path}")
            libraries.append(name)
        elif not ("iresearch-library-link.dir" in str(path) and path.name in {"probe.cpp.o", "probe.cpp.obj"}):
            if validate_files and not path.is_file():
                raise RuntimeError(f"link input does not exist: {path}")
            files.append(path)
    return list(dict.fromkeys(files)), list(dict.fromkeys(libraries)), list(dict.fromkeys(options))


def read_usage(path):
    usage = {}
    for line in path.read_text().splitlines():
        key, _, value = line.partition("=")
        if value:
            usage.setdefault(key, []).append(value)
    return {key: list(dict.fromkeys(values)) for key, values in usage.items()}


def cmake_quote(value):
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def stage_headers(usage_path, output):
    usage = read_usage(usage_path)
    source = Path(usage["source"][0]).resolve()
    build = Path(usage["build"][0]).resolve()
    include_destination = usage.get("include_destination", ["include"])[0]
    include_root = output / "include"
    include_root.mkdir(parents=True, exist_ok=True)
    includes, installed = [], set()
    for raw in dict.fromkeys(usage.get("include", [])):
        root = Path(raw).resolve()
        if root == source:
            search = root / "iresearch"
            destination = include_root / "iresearch"
            installed_include = include_root
        elif root == build:
            search = root / "iresearch"
            installed_include = include_root / "_iresearch/generated"
            destination = installed_include / "iresearch"
        elif root.is_relative_to(build):
            search = root
            destination = include_root / "_iresearch/generated" / root.relative_to(build)
            installed_include = destination
        elif root.is_relative_to(source):
            search = root
            destination = include_root / "_iresearch" / root.relative_to(source)
            installed_include = destination
        else:
            raise RuntimeError(f"include directory outside the source and build trees: {root}")
        installed_include.mkdir(parents=True, exist_ok=True)
        relative = installed_include.relative_to(include_root)
        includes.append(include_destination if relative == Path(".") else include_destination + "/" + relative.as_posix())
        if not search.is_dir():
            continue
        for current, directories, names in os.walk(search, followlinks=True):
            directories[:] = [name for name in directories if not name.startswith(".")]
            current = Path(current)
            for name in names:
                path = current / name
                if name.startswith(".") or path.suffix not in HEADER_SUFFIXES:
                    continue
                if not path.suffix and b"\0" in path.read_bytes():
                    continue
                target = destination / path.relative_to(search)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
                installed.add(target)
    for path in include_root.rglob("*"):
        if path.is_file() and path not in installed:
            path.unlink()
    license_paths = {path for path in (source / "LICENSE", source / "NOTICE.md", source / "iresearch/LICENSE") if path.is_file()}
    for current, directories, names in os.walk(source / "third_party"):
        directories[:] = [name for name in directories if name != ".git"]
        for name in names:
            if name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE")):
                path = Path(current) / name
                if path.is_file():
                    license_paths.add(path)
    licenses = output / "licenses"
    licenses.mkdir(parents=True, exist_ok=True)
    installed_licenses = set()
    for path in sorted(license_paths):
        target = licenses / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        installed_licenses.add(target)
    for path in licenses.rglob("*"):
        if path.is_file() and path not in installed_licenses:
            path.unlink()
    _, libraries, link_options = link_inputs(output / "link.json", validate_files=False)
    properties = {
        "INTERFACE_INCLUDE_DIRECTORIES": list(dict.fromkeys(includes)),
        "INTERFACE_COMPILE_DEFINITIONS": usage.get("define", []),
        "INTERFACE_COMPILE_OPTIONS": usage.get("compile_option", []),
        "INTERFACE_LINK_OPTIONS": usage.get("link_option", []) + link_options,
        "INTERFACE_LINK_LIBRARIES": libraries,
    }
    lines = []
    for name, values in properties.items():
        if values:
            lines.append("set_property(TARGET iresearch::iresearch APPEND PROPERTY " + name)
            for value in values:
                if name == "INTERFACE_INCLUDE_DIRECTORIES":
                    value = "${PACKAGE_PREFIX_DIR}/" + value
                lines.append("    " + cmake_quote(value))
            lines.append(")")
    (output / "IResearchUsage.cmake").write_text("\n".join(lines) + "\n")
    print(f"IResearch: staged native headers and {len(set(includes))} include roots")


def bundle(link, usage, archive, archiver):
    files, _, _ = link_inputs(link)
    if not files:
        raise RuntimeError("the native link command contains no archive inputs")
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="iresearch-archive-", dir=archive.parent) as directory:
        temporary = Path(directory) / archive.name
        subprocess.run([archiver, "qcsL", str(temporary), *(str(path) for path in files)], check=True)
        with temporary.open("rb") as stream:
            if stream.read(8) != b"!<arch>\n":
                raise RuntimeError("the installed archive must not contain thin archive paths")
        temporary.replace(archive)
    stage_headers(usage, archive.parent)
    print(f"IResearch: bundled {len(files)} native link inputs")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "capture":
        capture(Path(sys.argv[2]), sys.argv[3:])
        return
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action", required=True)
    archive = subparsers.add_parser("bundle")
    archive.add_argument("--link", required=True, type=Path)
    archive.add_argument("--usage", required=True, type=Path)
    archive.add_argument("--archive", required=True, type=Path)
    archive.add_argument("--archiver", required=True)
    headers = subparsers.add_parser("headers")
    headers.add_argument("--usage", required=True, type=Path)
    headers.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    if arguments.action == "bundle":
        bundle(arguments.link, arguments.usage, arguments.archive, arguments.archiver)
    else:
        stage_headers(arguments.usage, arguments.output)


if __name__ == "__main__":
    main()

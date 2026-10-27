#!/usr/bin/env python3

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "iresearch_package.py"


class IResearchPackageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compiler = shutil.which(os.environ.get("CXX", "clang++"))
        cls.archiver = shutil.which(os.environ.get("AR", "llvm-ar"))
        if not cls.compiler or not cls.archiver:
            raise RuntimeError("CXX and AR must select Clang and llvm-ar")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="iresearch package ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source tree"
        self.build = self.root / "build tree"
        self.stage = self.build / "package"
        self.dependency = self.source / "third_party/dependency/include"
        self.dependency.mkdir(parents=True)
        (self.source / "iresearch").mkdir()
        (self.dependency / "answer.h").write_text("#pragma once\nconstexpr int kAnswer = 42;\n")
        (self.dependency / "unused.cpp").write_text("int unused;\n")
        (self.dependency / "yaclib_std").mkdir()
        (self.dependency / "yaclib_std/atomic").write_text("#pragma once\n#include <atomic>\n")
        (self.dependency / ".git").write_text("gitdir: /original/source/.git\n")
        (self.dependency / "timezone").write_bytes(b"TZif\0\0\0\0")
        (self.source / "iresearch/test.hpp").write_text(
            "#pragma once\n#include <answer.h>\n#include <yaclib_std/atomic>\n"
            "int Answer();\nint Second();\nint Choice();\n"
            "inline int Expected() { std::atomic<int> answer{kAnswer}; return answer.load(); }\n"
        )
        license = self.source / "third_party/dependency/private/License.txt"
        license.parent.mkdir()
        license.write_text("dependency license\n")
        (self.source / "LICENSE").write_text("project license\n")
        (self.source / "NOTICE.md").write_bytes(b"project attribution\r\n")
        self.stage.mkdir(parents=True)
        self.archives = []
        for name, function, value in (("first", "Answer", 42), ("second", "Second", 43)):
            directory = self.build / name
            directory.mkdir()
            source = directory / "same.cpp"
            source.write_text(
                "int " + function + "() { return " + str(value) + "; }\n"
                "__attribute__((weak)) int Choice() { return " + ("1" if name == "first" else "2") + "; }\n"
            )
            object = directory / "same.o"
            self.run_command(self.compiler, "-c", str(source), "-o", str(object))
            archive = self.build / (name + " library.a")
            options = "rcsT" if name == "first" and sys.platform != "darwin" else "rcs"
            format = "darwin" if sys.platform == "darwin" else "gnu"
            self.run_command(self.archiver, "--format=" + format, options, str(archive), str(object))
            self.archives.append(archive)
        directory = self.build / "CMakeFiles/iresearch-library-link.dir"
        directory.mkdir(parents=True)
        main = directory / "probe.cpp"
        main.write_text(
            "int Answer(); int Second(); int Choice();\n"
            "int main() { return Answer() != 42 || Second() != 43 || Choice() != 1; }\n"
        )
        self.probe = directory / "probe.cpp.o"
        self.run_command(self.compiler, "-c", str(main), "-o", str(self.probe))
        self.link = self.stage / "link.json"
        self.usage = self.stage / "usage.txt"
        self.usage.write_text(
            "source=" + str(self.source) + "\nbuild=" + str(self.build) + "\n"
            "include=" + str(self.source) + "\ninclude=" + str(self.dependency) + "\n"
        )
        self.bundle = self.stage / "libiresearch-bundle.a"

    def run_command(self, *arguments):
        return subprocess.run(arguments, cwd=self.build, check=True, capture_output=True, text=True)

    def invoke(self, *arguments, check=True):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *arguments],
            cwd=self.build, check=check, capture_output=True, text=True,
        )

    def capture(self, *libraries):
        inner = self.build / "inner response.rsp"
        inner.write_text(shlex.join([str(self.probe), *(str(path) for path in libraries), "-o", str(self.build / "probe"), "-lm"]))
        outer = self.build / "responses/outer.rsp"
        outer.parent.mkdir(exist_ok=True)
        outer.write_text(shlex.quote("@" + inner.name))
        self.invoke("capture", str(self.link), self.compiler, "@" + str(outer))

    def make_bundle(self):
        self.capture(*self.archives)
        self.run_command(str(self.build / "probe"))
        self.invoke(
            "bundle", "--link", str(self.link), "--usage", str(self.usage),
            "--archive", str(self.bundle), "--archiver", self.archiver,
        )

    def compile_consumer(self, prefix, expression):
        source = self.root / "consumer.cpp"
        source.write_text('#include <iresearch/test.hpp>\nint main() { return !(' + expression + "); }\n")
        executable = self.root / "consumer"
        subprocess.run(
            [
                self.compiler, str(source), "-I" + str(prefix / "include"),
                "-I" + str(prefix / "include/_iresearch/third_party/dependency/include"),
                str(prefix / "libiresearch-bundle.a"), "-o", str(executable),
            ],
            check=True, capture_output=True, text=True,
        )
        subprocess.run([str(executable)], check=True)

    def test_relocated_bundle_links_without_original_archives(self):
        self.make_bundle()
        relocated = self.root / "relocated prefix"
        shutil.copytree(self.stage, relocated)
        shutil.rmtree(self.source)
        shutil.rmtree(self.build)
        self.compile_consumer(relocated, "Answer() == Expected() && Second() == 43 && Choice() == 1")
        self.assertTrue((relocated / "licenses/third_party/dependency/private/License.txt").is_file())
        self.assertEqual((relocated / "licenses/NOTICE.md").read_bytes(), b"project attribution\r\n")
        self.assertFalse((relocated / "include/_iresearch/third_party/dependency/include/unused.cpp").exists())
        self.assertFalse((relocated / "include/_iresearch/third_party/dependency/include/.git").exists())
        self.assertFalse((relocated / "include/_iresearch/third_party/dependency/include/timezone").exists())

    def test_headers_refresh_when_timestamp_and_size_are_unchanged(self):
        self.make_bundle()
        header = self.dependency / "answer.h"
        stat = header.stat()
        header.write_text("#pragma once\nconstexpr int kAnswer = 43;\n")
        os.utime(header, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.invoke("headers", "--usage", str(self.usage), "--output", str(self.stage))
        self.compile_consumer(self.stage, "Expected() == 43")

    def test_shared_dependencies_are_rejected_before_packaging(self):
        versioned = self.build / "libunexpected.so.1"
        source = self.build / "first/same.cpp"
        self.run_command(self.compiler, "-shared", "-fPIC", str(source), "-o", str(versioned))
        library = self.build / "libunexpected.so"
        library.symlink_to(versioned.name)
        for path in (versioned, library):
            with self.subTest(path=path.name):
                self.capture(path, self.archives[1])
                result = self.invoke(
                    "bundle", "--link", str(self.link), "--usage", str(self.usage),
                    "--archive", str(self.bundle), "--archiver", self.archiver, check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("non-system shared dependency", result.stderr)

    def test_cmake_package_rebuilds_missing_artifacts_and_relocates(self):
        source = self.root / "cmake source"
        build = self.root / "cmake build"
        (source / "cmake").mkdir(parents=True)
        (source / "iresearch").mkdir()
        for name in ("IResearchLibrary.cmake", "IResearchConfig.cmake.in", "iresearch_package.py"):
            shutil.copy2(SCRIPT.parent / name, source / "cmake" / name)
        (source / "iresearch/toy.hpp").write_text("#pragma once\nint NativeValue();\n")
        if sys.platform == "darwin":
            native = 'extern "C" double CFAbsoluteTimeGetCurrent();\nint NativeValue() { return CFAbsoluteTimeGetCurrent() > 0 ? 42 : 0; }\n'
            libraries = "-nodefaultlibs -lSystem"
            dependency = "-Wl,-framework,CoreFoundation"
        else:
            native = 'extern "C" double cos(double);\nint NativeValue() { volatile double zero = 0; return cos(zero) == 1 ? 42 : 0; }\n'
            libraries = "-nodefaultlibs -lc"
            dependency = "-Wl,-lm"
        (source / "toy.cpp").write_text('#include "iresearch/toy.hpp"\n' + native)
        (source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.30)\n"
            "project(IResearchPackageTest LANGUAGES CXX)\n"
            "find_package(Python3 REQUIRED COMPONENTS Interpreter)\n"
            "include(GNUInstallDirs)\n"
            f'set(CMAKE_CXX_STANDARD_LIBRARIES "{libraries}")\n'
            "add_library(iresearch STATIC toy.cpp)\n"
            'target_include_directories(iresearch PUBLIC "${PROJECT_SOURCE_DIR}" "${PROJECT_BINARY_DIR}")\n'
            "target_compile_features(iresearch PUBLIC cxx_std_26)\n"
            f'target_link_libraries(iresearch PUBLIC "{dependency}")\n'
            "include(cmake/IResearchLibrary.cmake)\n"
        )
        configure = [
            "cmake", "-S", str(source), "-B", str(build), "-G", "Ninja",
            "-DCMAKE_CXX_COMPILER=" + self.compiler, "-DCMAKE_BUILD_TYPE=Release",
            "-DCMAKE_INSTALL_INCLUDEDIR=include/native", "-DCMAKE_INSTALL_LIBDIR=lib/native",
        ]
        self.run_command(*configure)
        command = ["cmake", "--build", str(build), "--target", "iresearch-library"]
        self.run_command(*command)
        probe = build / "iresearch-library-link"
        original = probe.stat().st_mtime_ns
        self.run_command(*configure)
        self.run_command(*command)
        self.assertEqual(probe.stat().st_mtime_ns, original)
        for name in ("link.json", "libiresearch-bundle.a"):
            artifact = build / "iresearch-package/Release" / name
            artifact.unlink()
            self.run_command(*command)
            self.assertTrue(artifact.is_file())
        prefix = self.root / "installed"
        self.run_command("cmake", "--install", str(build), "--prefix", str(prefix), "--component", "IResearch")
        relocated = self.root / "relocated cmake prefix"
        prefix.rename(relocated)
        shutil.rmtree(source)
        shutil.rmtree(build)
        consumer = self.root / "cmake consumer"
        consumer.mkdir()
        (consumer / "main.cpp").write_text(
            "#include <iresearch/toy.hpp>\nint main() { return NativeValue() != 42; }\n"
        )
        (consumer / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.30)\n"
            "project(Consumer LANGUAGES CXX)\n"
            "find_package(IResearch CONFIG REQUIRED)\n"
            "add_executable(consumer main.cpp)\n"
            "target_link_libraries(consumer PRIVATE iresearch::iresearch)\n"
        )
        consumer_build = self.root / "cmake consumer build"
        self.run_command(
            "cmake", "-S", str(consumer), "-B", str(consumer_build), "-G", "Ninja",
            "-DCMAKE_CXX_COMPILER=" + self.compiler, "-DCMAKE_CXX_FLAGS=-Wall -Wextra -Werror",
            "-DIResearch_DIR=" + str(relocated / "lib/native/cmake/IResearch"),
        )
        self.run_command("cmake", "--build", str(consumer_build))
        self.run_command(str(consumer_build / "consumer"))

    def test_instrumented_cmake_packages_export_profile_runtimes(self):
        for index, flag in enumerate(("-fprofile-instr-generate", "-fprofile-generate", "-fcs-profile-generate")):
            with self.subTest(flag=flag):
                root = self.root / str(index)
                source, build = root / "source", root / "build"
                (source / "cmake").mkdir(parents=True)
                (source / "iresearch").mkdir()
                for name in ("IResearchLibrary.cmake", "IResearchConfig.cmake.in", "iresearch_package.py"):
                    shutil.copy2(SCRIPT.parent / name, source / "cmake" / name)
                (source / "iresearch/profile.hpp").write_text("#pragma once\nint ProfiledBranch(int value);\n")
                (source / "native.cpp").write_text("int ProfiledBranch(int value) { return value > 0 ? 42 : 0; }\n")
                builtins = self.run_command(self.compiler, "--print-libgcc-file-name", "--rtlib=compiler-rt").stdout.strip()
                libraries = "-nodefaultlibs " + shlex.quote(builtins)
                libraries += " -lSystem" if sys.platform == "darwin" else " -lc -lm"
                (source / "CMakeLists.txt").write_text(
                    "cmake_minimum_required(VERSION 3.30)\n"
                    "project(ProfilePackage LANGUAGES CXX)\n"
                    "find_package(Python3 REQUIRED COMPONENTS Interpreter)\n"
                    "include(GNUInstallDirs)\n"
                    f'set(CMAKE_CXX_STANDARD_LIBRARIES "{libraries}")\n'
                    f"add_compile_options({flag})\nadd_link_options({flag})\n"
                    "add_library(iresearch STATIC native.cpp)\n"
                    'target_include_directories(iresearch PUBLIC "${PROJECT_SOURCE_DIR}")\n'
                    "target_compile_features(iresearch PUBLIC cxx_std_26)\n"
                    "include(cmake/IResearchLibrary.cmake)\n"
                )
                self.run_command(
                    "cmake", "-S", str(source), "-B", str(build), "-G", "Ninja",
                    "-DCMAKE_CXX_COMPILER=" + self.compiler, "-DCMAKE_BUILD_TYPE=Release",
                )
                self.run_command("cmake", "--build", str(build), "--target", "iresearch-library")
                prefix = root / "installed"
                self.run_command("cmake", "--install", str(build), "--prefix", str(prefix), "--component", "IResearch")
                relocated = root / "relocated prefix"
                prefix.rename(relocated)
                shutil.rmtree(source)
                shutil.rmtree(build)
                consumer = root / "consumer"
                consumer.mkdir()
                (consumer / "main.cpp").write_text(
                    "#include <iresearch/profile.hpp>\nint main() { return ProfiledBranch(1) != 42; }\n"
                )
                (consumer / "CMakeLists.txt").write_text(
                    "cmake_minimum_required(VERSION 3.30)\n"
                    "project(ProfileConsumer LANGUAGES CXX)\n"
                    "find_package(IResearch CONFIG REQUIRED)\n"
                    "add_executable(consumer main.cpp)\n"
                    "target_link_libraries(consumer PRIVATE iresearch::iresearch)\n"
                )
                consumer_build = root / "consumer-build"
                self.run_command(
                    "cmake", "-S", str(consumer), "-B", str(consumer_build), "-G", "Ninja",
                    "-DCMAKE_CXX_COMPILER=" + self.compiler,
                    "-DIResearch_DIR=" + str(relocated / "lib/cmake/IResearch"),
                )
                self.run_command("cmake", "--build", str(consumer_build))
                profile = root / "native.profraw"
                subprocess.run(
                    [str(consumer_build / "consumer")], check=True, timeout=30,
                    env={**os.environ, "LLVM_PROFILE_FILE": str(profile)},
                )
                self.assertGreater(profile.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
